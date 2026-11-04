using System.Data;
using System.Text.Json;
using BatchFlow;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Postgres.Checkpoints;
using BatchFlow.Postgres.Options;
using BatchFlow.Postgres.Sources;
using FluentAssertions;
using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace BatchFlow.Postgres.Tests.Integration;

[Trait("Category", "PostgresIntegration")]
public sealed class PostgresIntegrationTests(PostgresFixture fixture) : IClassFixture<PostgresFixture>
{
    [PostgresTheory]
    [InlineData("string")]
    [InlineData("dataSource")]
    [InlineData("factory")]
    [InlineData("external")]
    [InlineData("adapterOwned")]
    public async Task Source_should_read_sql_jsonb_and_timestamp_and_resume_for_each_connection_mode(string mode)
    {
        await using var connection = mode == "external"
            ? await fixture.DataSource.OpenConnectionAsync()
            : new NpgsqlConnection(fixture.ConnectionString);
        var connectionOptions = mode switch
        {
            "string" => PostgresConnectionOptions.FromConnectionString(fixture.ConnectionString),
            "dataSource" => PostgresConnectionOptions.FromDataSource(fixture.DataSource),
            "factory" => PostgresConnectionOptions.FromConnectionFactory(() => new NpgsqlConnection(fixture.ConnectionString)),
            "external" => PostgresConnectionOptions.FromConnection(connection),
            _ => PostgresConnectionOptions.FromConnection(connection, PostgresConnectionOwnership.AdapterOwned),
        };
        var source = new PostgresBatchSource<DatabaseItem, long>(CreateSourceOptions(connectionOptions));
        var items = new List<DatabaseItem>();
        var sizes = new List<int>();
        await foreach (var batch in source.ReadAsync(Context(2, 4L), default))
        {
            sizes.Add(batch.Items.Count);
            items.AddRange(batch.Items);
        }

        sizes.Should().Equal(2, 2, 2);
        items.Select(item => item.Id).Should().Equal(5L, 6L, 7L, 8L, 9L, 10L);
        items.Should().OnlyContain(item => item.PayloadId == item.Id && item.OccurredAt.Offset == TimeSpan.Zero);
        items[0].OccurredAt.Should().Be(new DateTimeOffset(2026, 11, 4, 12, 0, 0, TimeSpan.Zero));
        if (mode == "external")
        {
            connection.State.Should().Be(ConnectionState.Open);
        }
        else if (mode == "adapterOwned")
        {
            var open = () => connection.OpenAsync();
            await open.Should().ThrowExactlyAsync<ObjectDisposedException>();
        }
    }

    [PostgresFact]
    public async Task Store_should_roundtrip_json_metadata_time_and_allow_only_one_concurrent_update()
    {
        var store = CreateStore<Cursor>(PostgresConnectionOptions.FromDataSource(fixture.DataSource));
        await store.EnsureStorageAsync();
        await store.EnsureStorageAsync();
        var id = Guid.NewGuid().ToString("N");
        (await store.LoadAsync(id, default)).Should().BeNull();
        var now = new DateTimeOffset(2026, 11, 4, 12, 0, 0, TimeSpan.Zero);
        var checkpoint = new StoredCheckpoint<Cursor>
        {
            Value = new Cursor(10L, now),
            SequenceNumber = 4,
            SavedAt = now,
            Metadata = new Dictionary<string, string> { ["причина"] = "тест JSONB" },
        };
        var saved = await store.SaveAsync(id, checkpoint, null, default);
        var loaded = await store.LoadAsync(id, default);
        loaded.Should().BeEquivalentTo(saved);

        async Task<bool> UpdateAsync(long position)
        {
            try
            {
                await store.SaveAsync(id, checkpoint with { Value = new Cursor(position, now) }, saved.ConcurrencyToken, default);
                return true;
            }
            catch (CheckpointConflictException)
            {
                return false;
            }
        }

        var results = await Task.WhenAll(UpdateAsync(11L), UpdateAsync(12L));
        results.Should().ContainSingle(result => result);
        var updated = await store.LoadAsync(id, default);
        updated!.ConcurrencyToken.Should().NotBe(saved.ConcurrencyToken);
        updated.Value.Id.Should().BeOneOf(11L, 12L);
    }

    [PostgresFact]
    public async Task Engine_should_save_and_resume_using_real_postgres_store()
    {
        var connectionOptions = PostgresConnectionOptions.FromDataSource(fixture.DataSource);
        var store = CreateStore<long>(connectionOptions);
        await store.EnsureStorageAsync();
        var id = Guid.NewGuid().ToString("N");
        var source = new PostgresBatchSource<DatabaseItem, long>(CreateSourceOptions(connectionOptions));
        var builder = BatchJob.Named("postgres-integration").From(source).WithBatchSize(3)
            .WithDegreeOfParallelism(3).WithCheckpoint(id, store);

        var first = await builder.RunBatchAsync((_, _, _) => ValueTask.CompletedTask);
        var second = await builder.RunBatchAsync((_, _, _) => ValueTask.CompletedTask);

        first.Status.Should().Be(JobCompletionStatus.Succeeded);
        first.SucceededItemCount.Should().Be(10);
        first.LastPersistedCheckpoint!.Value.Should().Be(10L);
        second.Status.Should().Be(JobCompletionStatus.Succeeded);
        second.ProcessedBatchCount.Should().Be(0);
        second.ResumeFrom!.Value.Should().Be(10L);
    }

    [PostgresFact]
    public async Task Cancelled_query_should_dispose_owned_connection_and_return_it_to_pool()
    {
        var builder = new NpgsqlConnectionStringBuilder(fixture.ConnectionString) { MaxPoolSize = 1, Timeout = 3 };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        var opened = new TaskCompletionSource<NpgsqlConnection>(TaskCreationOptions.RunContinuationsAsynchronously);
        var connectionOptions = PostgresConnectionOptions.FromConnectionFactory(async token =>
        {
            var connection = await dataSource.OpenConnectionAsync(token);
            opened.TrySetResult(connection);
            return connection;
        });
        var options = CreateSourceOptions(connectionOptions) with { CommandText = PostgresIntegrationQueries.Sleep };
        var source = new PostgresBatchSource<DatabaseItem, long>(options);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var enumerator = source.ReadAsync(Context(2), cancellation.Token).GetAsyncEnumerator();
        var read = enumerator.MoveNextAsync().AsTask();
        var ownedConnection = await opened.Task.WaitAsync(TimeSpan.FromSeconds(5));
        var processId = ownedConnection.ProcessID;
        await WaitUntilSleepingAsync(processId, cancellation.Token);
        cancellation.Cancel();
        var action = async () => await read;
        await action.Should().ThrowAsync<OperationCanceledException>();
        var reopen = () => ownedConnection.OpenAsync();
        await reopen.Should().ThrowExactlyAsync<ObjectDisposedException>();

        await using var ping = dataSource.CreateCommand(PostgresIntegrationQueries.Ping);
        (await ping.ExecuteScalarAsync()).Should().Be(42);
    }

    [PostgresFact]
    public async Task Early_enumeration_disposal_should_return_connection_to_pool()
    {
        var builder = new NpgsqlConnectionStringBuilder(fixture.ConnectionString) { MaxPoolSize = 1, Timeout = 3 };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        var source = new PostgresBatchSource<DatabaseItem, long>(CreateSourceOptions(PostgresConnectionOptions.FromDataSource(dataSource)));
        await using (var enumerator = source.ReadAsync(Context(2), default).GetAsyncEnumerator())
        {
            (await enumerator.MoveNextAsync()).Should().BeTrue();
        }

        await using var ping = dataSource.CreateCommand(PostgresIntegrationQueries.Ping);
        (await ping.ExecuteScalarAsync()).Should().Be(42);
    }

    [PostgresTheory]
    [InlineData("external")]
    [InlineData("string")]
    [InlineData("factory")]
    [InlineData("dataSource")]
    public async Task Store_should_support_ensure_load_and_multiple_saves_for_repeatable_connection_modes(string mode)
    {
        await using var connection = await fixture.DataSource.OpenConnectionAsync();
        var connectionOptions = mode switch
        {
            "external" => PostgresConnectionOptions.FromConnection(connection),
            "string" => PostgresConnectionOptions.FromConnectionString(fixture.ConnectionString),
            "factory" => PostgresConnectionOptions.FromConnectionFactory(() => new NpgsqlConnection(fixture.ConnectionString)),
            _ => PostgresConnectionOptions.FromDataSource(fixture.DataSource),
        };
        var store = CreateStore<long>(connectionOptions);
        await store.EnsureStorageAsync();
        var id = Guid.NewGuid().ToString("N");
        (await store.LoadAsync(id, default)).Should().BeNull();
        var first = await store.SaveAsync(id, new StoredCheckpoint<long> { Value = 1 }, null, default);
        await store.SaveAsync(id, first with { Value = 2 }, first.ConcurrencyToken, default);
        (await store.LoadAsync(id, default))!.Value.Should().Be(2L);
        connection.State.Should().Be(ConnectionState.Open);
    }

    [PostgresFact]
    public async Task ReadCommitted_keyset_should_explicitly_demonstrate_late_commit_limitation()
    {
        await using var delayed = await fixture.DataSource.OpenConnectionAsync();
        await using var transaction = await delayed.BeginTransactionAsync();
        await using (var insert = new NpgsqlCommand(fixture.InsertLateItemSql, delayed, transaction))
        {
            insert.Parameters.AddWithValue("id", 11L);
            await insert.ExecuteNonQueryAsync();
        }

        await using (var insert = fixture.DataSource.CreateCommand(fixture.InsertLateItemSql))
        {
            insert.Parameters.AddWithValue("id", 12L);
            await insert.ExecuteNonQueryAsync();
        }

        var source = CreateKeySource(PostgresConnectionOptions.FromDataSource(fixture.DataSource), fixture.ReadLateItemsSql);
        var observed = new List<long>();
        await foreach (var batch in source.ReadAsync(Context(10, 10L), default))
        {
            observed.AddRange(batch.Items);
        }
        observed.Should().Equal(12L);
        await transaction.CommitAsync();
        var resumed = new List<long>();
        await foreach (var batch in source.ReadAsync(Context(10, 12L), default))
        {
            resumed.AddRange(batch.Items);
        }
        resumed.Should().BeEmpty("позиция 11 уже находится позади сохраненного checkpoint 12");
    }

    [PostgresFact]
    public async Task Failed_authentication_should_dispose_owned_connection_and_preserve_driver_exception()
    {
        var builder = new NpgsqlConnectionStringBuilder(fixture.ConnectionString) { Password = Guid.NewGuid().ToString("N") };
        await using var connection = new NpgsqlConnection(builder.ConnectionString);
        var factory = BatchFlow.Postgres.Internal.PostgresConnectionFactoryResolver.Create(
            PostgresConnectionOptions.FromConnectionFactory(() => connection));
        var action = async () => await factory.OpenConnectionAsync(default);
        var exception = await action.Should().ThrowAsync<PostgresException>();
        exception.Which.SqlState.Should().Be(PostgresErrorCodes.InvalidPassword);
        var reopen = () => connection.OpenAsync();
        await reopen.Should().ThrowExactlyAsync<ObjectDisposedException>();
    }

    [PostgresFact]
    public async Task External_repeatable_read_transaction_should_keep_snapshot_across_batches()
    {
        await using var connection = await fixture.DataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead);
        var source = CreateKeySource(PostgresConnectionOptions.FromConnection(connection), fixture.ReadSnapshotItemsSql);
        var ids = new List<long>();
        await using (var enumerator = source.ReadAsync(Context(1), default).GetAsyncEnumerator())
        {
            (await enumerator.MoveNextAsync()).Should().BeTrue();
            ids.AddRange(enumerator.Current.Items);
            await using (var insert = fixture.DataSource.CreateCommand(fixture.InsertSnapshotItemSql))
            {
                await insert.ExecuteNonQueryAsync();
            }

            while (await enumerator.MoveNextAsync())
            {
                ids.AddRange(enumerator.Current.Items);
            }
        }

        ids.Should().Equal(1L, 2L);
        await transaction.CommitAsync();
        var afterSnapshot = new List<long>();
        await foreach (var batch in source.ReadAsync(Context(1, 2L), default))
        {
            afterSnapshot.AddRange(batch.Items);
        }
        afterSnapshot.Should().Equal(3L);
        connection.State.Should().Be(ConnectionState.Open);
    }

    private static PostgresBatchSource<long, long> CreateKeySource(PostgresConnectionOptions connection, string sql) => new(
        new PostgresBatchSourceOptions<long, long>
        {
            Connection = connection,
            CommandText = sql,
            ResumeCheckpointParameterType = NpgsqlDbType.Bigint,
            CheckpointProgressionValidator = (previous, next) => next > previous,
            RowMaterializer = (reader, _) =>
            {
                var id = reader.GetInt64(0);
                return ValueTask.FromResult(new PostgresBatchSourceRow<long, long>(id, id));
            },
        });

    private PostgresCheckpointStore<T> CreateStore<T>(PostgresConnectionOptions connection) => new(new PostgresCheckpointStoreOptions
    {
        Connection = connection,
        SchemaName = fixture.SchemaName,
        TableName = $"checkpoints_{Guid.NewGuid():N}",
    });

    private PostgresBatchSourceOptions<DatabaseItem, long> CreateSourceOptions(PostgresConnectionOptions connection) => new()
    {
        Connection = connection,
        CommandText = fixture.ReadItemsSql,
        ResumeCheckpointParameterType = NpgsqlDbType.Bigint,
        CheckpointProgressionValidator = (previous, next) => next > previous,
        RowMaterializer = (reader, _) =>
        {
            var id = reader.GetInt64(0);
            var occurredAt = reader.GetFieldValue<DateTimeOffset>(1);
            using var payload = JsonDocument.Parse(reader.GetString(2));
            return ValueTask.FromResult(new PostgresBatchSourceRow<DatabaseItem, long>(
                new DatabaseItem(id, occurredAt, payload.RootElement.GetProperty("id").GetInt64()), id));
        },
    };

    private async Task WaitUntilSleepingAsync(int processId, CancellationToken token)
    {
        var sleeping = false;
        while (!sleeping)
        {
            await using var command = fixture.DataSource.CreateCommand(PostgresIntegrationQueries.IsSleeping);
            command.Parameters.AddWithValue("pid", processId);
            sleeping = (bool)(await command.ExecuteScalarAsync(token))!;
            if (!sleeping)
            {
                await Task.Delay(10, token);
            }
        }
    }

    private static BatchSourceContext<long> Context(int batchSize, long? resume = null) => new()
    {
        Job = new JobExecutionContext { JobName = "integration", RunId = Guid.NewGuid().ToString("N") },
        RequestedBatchSize = batchSize,
        ResumeFrom = resume is null ? null : new StoredCheckpoint<long> { Value = resume.Value },
    };

    private sealed record DatabaseItem(long Id, DateTimeOffset OccurredAt, long PayloadId);
    public sealed record Cursor(long Id, DateTimeOffset SavedAt);
}
