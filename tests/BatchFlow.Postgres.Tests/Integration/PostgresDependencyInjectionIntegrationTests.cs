using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Extensions.DependencyInjection;
using BatchFlow.Postgres.Checkpoints;
using BatchFlow.Postgres.DependencyInjection;
using BatchFlow.Postgres.Options;
using BatchFlow.Postgres.Sources;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using NpgsqlTypes;

namespace BatchFlow.Postgres.Tests.Integration;

[Trait("Category", "PostgresIntegration")]
public sealed class PostgresDependencyInjectionIntegrationTests(PostgresFixture fixture) : IClassFixture<PostgresFixture>
{
    [PostgresTheory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Scoped_jobs_should_save_and_resume_with_explicit_services_and_borrowed_pool(bool batchMode, bool named)
    {
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var token = cancellation.Token;
        var name = named ? "orders" : null;
        var checkpointId = Guid.NewGuid().ToString("N");
        var serializer = new RecordingSerializer();
        var processed = new ConcurrentBag<long>();
        var services = new ServiceCollection().AddBatchFlow();
        services.AddBatchFlowPostgresDataSource(fixture.DataSource, name);
        services.AddBatchFlowPostgresSource<long, long>((_, connection) => new PostgresBatchSourceOptions<long, long>
        {
            Connection = connection,
            CommandText = fixture.ReadItemsSql,
            ResumeCheckpointParameterType = NpgsqlDbType.Bigint,
            CheckpointProgressionValidator = (previous, next) => next > previous,
            RowMaterializer = (reader, _) =>
            {
                var id = reader.GetInt64(0);
                return ValueTask.FromResult(new PostgresBatchSourceRow<long, long>(id, id));
            },
        }, dataSourceName: name, name: name);
        services.AddBatchFlowPostgresCheckpoint<long>(provider => new PostgresCheckpointStoreOptions
        {
            Connection = PostgresConnectionOptions.FromDataSource(Resolve<Npgsql.NpgsqlDataSource>(provider, name)),
            SchemaName = fixture.SchemaName,
            TableName = "di_" + checkpointId,
        }, name: name, serializerFactory: _ => serializer);

        await using (var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true }))
        {
            for (var pass = 0; pass < 2; pass++)
            {
                await using var scope = provider.CreateAsyncScope();
                var source = Resolve<PostgresBatchSource<long, long>>(scope.ServiceProvider, name);
                var store = Resolve<PostgresCheckpointStore<long>>(scope.ServiceProvider, name);
                await store.EnsureStorageAsync(token);
                var builder = BatchJob.Named("postgres-di").From(source).WithBatchSize(3)
                    .WithDegreeOfParallelism(3).WithCheckpoint(checkpointId, store);
                var runOptions = scope.ServiceProvider.GetRequiredService<BatchJobRunOptions>();
                var summary = batchMode
                    ? await builder.RunBatchAsync((batch, _, _) =>
                    {
                        foreach (var item in batch)
                        {
                            processed.Add(item);
                        }

                        return ValueTask.CompletedTask;
                    }, runOptions, token)
                    : await builder.RunEachAsync((item, _, _) =>
                    {
                        processed.Add(item);
                        return ValueTask.CompletedTask;
                    }, runOptions, token);

                summary.Status.Should().Be(JobCompletionStatus.Succeeded);
                summary.Exception.Should().BeNull();
                summary.SucceededItemCount.Should().Be(pass == 0 ? 10 : 0);
                (summary.LastPersistedCheckpoint ?? summary.ResumeFrom)!.Value.Should().Be(10);
            }
        }

        processed.OrderBy(id => id).Should().Equal(Enumerable.Range(1, 10).Select(id => (long)id));
        serializer.Writes.Should().BeGreaterThan(0);
        serializer.Reads.Should().BeGreaterThan(0);
        await using var command = fixture.DataSource.CreateCommand(PostgresIntegrationQueries.Ping);
        (await command.ExecuteScalarAsync(token)).Should().Be(42, "контейнер не владеет переданным пулом fixture");
    }

    private static T Resolve<T>(IServiceProvider provider, string? name) where T : notnull =>
        name is null ? provider.GetRequiredService<T>() : provider.GetRequiredKeyedService<T>(name);

    private sealed class RecordingSerializer : IPostgresCheckpointSerializer<long>
    {
        private int _writes;
        private int _reads;
        public int Writes => Volatile.Read(ref _writes);
        public int Reads => Volatile.Read(ref _reads);

        public string Serialize(long checkpoint)
        {
            Interlocked.Increment(ref _writes);
            return JsonSerializer.Serialize(checkpoint.ToString(CultureInfo.InvariantCulture));
        }

        public long Deserialize(string payload)
        {
            Interlocked.Increment(ref _reads);
            return long.Parse(JsonSerializer.Deserialize<string>(payload)!, CultureInfo.InvariantCulture);
        }
    }
}
