using System.Data;
using System.Data.Common;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Postgres.Internal;
using BatchFlow.Postgres.Options;
using BatchFlow.Postgres.Sources;
using FluentAssertions;
using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace BatchFlow.Postgres.Tests.Sources;

public sealed class PostgresBatchSourceTests
{
    [Theory]
    [InlineData("batch_size", "batch_size")]
    [InlineData("@batch_size", "BATCH_SIZE")]
    [InlineData(":batch_size", "@batch_size")]
    [InlineData("batch_size", "")]
    [InlineData("@", "resume")]
    [InlineData("batch_size", ":")]
    public void Constructor_should_reject_invalid_parameter_names(string batchName, string resumeName)
    {
        var options = CreateOptions() with
        {
            BatchSizeParameterName = batchName,
            ResumeCheckpointParameterName = resumeName,
        };
        var action = () => new PostgresBatchSource<int, long>(options);
        action.Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData(CommandBehavior.SchemaOnly)]
    [InlineData(CommandBehavior.KeyInfo)]
    [InlineData(CommandBehavior.SingleRow)]
    [InlineData((CommandBehavior)128)]
    public void Constructor_should_reject_reader_behavior_outside_batch_contract(CommandBehavior behavior)
    {
        var options = CreateOptions() with { ReaderBehavior = behavior };
        var action = () => new PostgresBatchSource<int, long>(options);
        action.Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ReadAsync_should_validate_batch_size_before_opening_connection(int batchSize)
    {
        var factory = new FakePostgresConnectionFactory();
        var source = new TestablePostgresBatchSource(CreateOptions(), factory);
        var action = async () => await ReadAllAsync(source, CreateLongContext(batchSize));
        await action.Should().ThrowAsync<ArgumentOutOfRangeException>();
        factory.OpenConnectionCalls.Should().Be(0);
    }

    [Theory]
    [InlineData(101L, 100L)]
    [InlineData(101L, 101L)]
    public async Task ReadAsync_should_validate_order_of_each_row_before_yielding_batch(long first, long second)
    {
        var options = CreateOptions() with
        {
            CheckpointProgressionValidator = (previous, next) => next > previous,
        };
        var source = new TestablePostgresBatchSource(options, new FakePostgresConnectionFactory(),
            CreateReader((1, first), (2, second)));
        var action = async () => await ReadAllAsync(source, CreateLongContext(2));
        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*допустимом порядке*");
    }

    [Fact]
    public async Task ReadAsync_should_support_descending_keyset_and_resume_boundary()
    {
        var options = CreateOptions() with
        {
            CheckpointProgressionValidator = (previous, next) => next < previous,
        };
        var source = new TestablePostgresBatchSource(options, new FakePostgresConnectionFactory(),
            CreateReader((1, 99L), (2, 98L)), CreateReader((3, 97L)), CreateReader());
        var context = CreateLongContext(2, new StoredCheckpoint<long> { Value = 100L });
        var batches = await ReadAllAsync(source, context);
        batches.Select(batch => batch.Checkpoint).Should().Equal(98L, 97L);
    }

    [Fact]
    public async Task ReadAsync_should_reject_first_row_that_does_not_follow_resume_checkpoint()
    {
        var options = CreateOptions() with
        {
            CheckpointProgressionValidator = (previous, next) => next > previous,
        };
        var source = new TestablePostgresBatchSource(options, new FakePostgresConnectionFactory(), CreateReader((1, 99L)));
        var action = async () => await ReadAllAsync(source, CreateLongContext(2, new StoredCheckpoint<long> { Value = 100L }));
        await action.Should().ThrowAsync<InvalidOperationException>();
    }

    [Theory]
    [InlineData(CommandBehavior.CloseConnection)]
    [InlineData(CommandBehavior.CloseConnection | CommandBehavior.SingleResult)]
    [InlineData(CommandBehavior.CloseConnection | CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)]
    public void Constructor_should_reject_close_connection_reader_behavior(CommandBehavior behavior)
    {
        var options = CreateOptions() with { ReaderBehavior = behavior };

        var action = () => new PostgresBatchSource<int, long>(options);

        var exception = action.Should().Throw<ArgumentException>()
            .WithMessage("*CloseConnection*");
        exception.Which.ParamName.Should().Be("options");
    }

    [Theory]
    [InlineData(CommandBehavior.Default)]
    [InlineData(CommandBehavior.SingleResult)]
    [InlineData(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)]
    public void Constructor_should_accept_reader_behavior_without_close_connection(CommandBehavior behavior)
    {
        var options = CreateOptions() with { ReaderBehavior = behavior };

        var action = () => new PostgresBatchSource<int, long>(options);

        action.Should().NotThrow();
    }

    [Fact]
    public async Task ReadAsync_should_read_multiple_batches_and_advance_checkpoint_between_iterations()
    {
        var options = CreateOptions();
        var connectionFactory = new FakePostgresConnectionFactory();
        var source = new TestablePostgresBatchSource(
            options,
            connectionFactory,
            CreateReader((1, 101L), (2, 102L)),
            CreateReader((3, 103L)),
            CreateReader());

        var context = CreateLongContext(
            batchSize: 2,
            resumeFrom: new StoredCheckpoint<long>
            {
                Value = 100L,
                SavedAt = DateTimeOffset.UtcNow,
            });

        var batches = await ReadAllAsync(source, context);

        batches.Should().HaveCount(2);
        batches[0].Items.Should().Equal(1, 2);
        batches[0].Checkpoint.Should().Be(102L);
        batches[1].Items.Should().Equal(3);
        batches[1].Checkpoint.Should().Be(103L);

        source.CapturedCommands.Should().HaveCount(3);
        source.CapturedCommands[0].Get<long>("resume_checkpoint").Should().Be(100L);
        source.CapturedCommands[1].Get<long>("resume_checkpoint").Should().Be(102L);
        source.CapturedCommands[2].Get<long>("resume_checkpoint").Should().Be(103L);
        source.CapturedCommands.Should().OnlyContain(command => command.Get<int>("batch_size") == 2);
        connectionFactory.OpenConnectionCalls.Should().Be(1);
    }

    [Fact]
    public async Task ReadAsync_should_support_custom_checkpoint_binding_for_composite_cursor()
    {
        var options = new PostgresBatchSourceOptions<int, CompositeCheckpoint>
        {
            Connection = PostgresConnectionOptions.FromConnectionString(ConnectionString),
            CommandTimeoutSeconds = 30,
            CommandText = "select 1 where 1 = 0",
            ResumeCheckpointParameterName = null,
            RowMaterializer = (reader, _) => ValueTask.FromResult(
                new PostgresBatchSourceRow<int, CompositeCheckpoint>(
                    reader.GetInt32(0),
                    new CompositeCheckpoint(reader.GetDateTime(1), reader.GetInt64(2)))),
            ParameterBinder = (parameters, context) =>
            {
                parameters.AddWithValue("batch_size_copy", NpgsqlDbType.Integer, context.BatchSize);
                parameters.Add(
                    new NpgsqlParameter("resume_created_at", NpgsqlDbType.Timestamp)
                    {
                        Value = (object?)context.ResumeFrom?.Value.CreatedAt ?? DBNull.Value,
                    });
                parameters.Add(
                    new NpgsqlParameter("resume_id", NpgsqlDbType.Bigint)
                    {
                        Value = (object?)context.ResumeFrom?.Value.Id ?? DBNull.Value,
                    });
            },
            EnforceCheckpointProgression = false,
        };

        var source = new TestableCompositeCheckpointSource(
            options,
            new FakePostgresConnectionFactory(),
            CreateCompositeReader((1, new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc), 101L)),
            CreateCompositeReader());

        var context = CreateCompositeContext(
            batchSize: 5,
            resumeFrom: new StoredCheckpoint<CompositeCheckpoint>
            {
                Value = new CompositeCheckpoint(new DateTime(2026, 6, 30, 9, 0, 0, DateTimeKind.Utc), 77L),
                SavedAt = DateTimeOffset.UtcNow,
            });

        var batches = await ReadAllAsync(source, context);

        batches.Should().HaveCount(1);
        source.CapturedCommands.Should().HaveCount(2);
        source.CapturedCommands[0].Get<int>("batch_size").Should().Be(5);
        source.CapturedCommands[0].Get<int>("batch_size_copy").Should().Be(5);
        source.CapturedCommands[0].Get<long>("resume_id").Should().Be(77L);
        source.CapturedCommands[0].Get<DateTime>("resume_created_at")
            .Should().Be(new DateTime(2026, 6, 30, 9, 0, 0, DateTimeKind.Utc));
        source.CapturedCommands[1].Get<long>("resume_id").Should().Be(101L);
        source.CapturedCommands[1].Get<DateTime>("resume_created_at")
            .Should().Be(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
    }

    [Fact]
    public async Task ReadAsync_should_throw_when_query_returns_more_rows_than_requested_batch_size()
    {
        var source = new TestablePostgresBatchSource(
            CreateOptions(),
            new FakePostgresConnectionFactory(),
            CreateReader((1, 101L), (2, 102L), (3, 103L)));

        var action = async () => await ReadAllAsync(source, CreateLongContext(batchSize: 2));

        await action.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*больше строк, чем разрешает runtime batch size*");
    }

    [Fact]
    public async Task ReadAsync_should_throw_when_checkpoint_does_not_advance()
    {
        var source = new TestablePostgresBatchSource(
            CreateOptions(),
            new FakePostgresConnectionFactory(),
            CreateReader((1, 200L)));

        var context = CreateLongContext(
            batchSize: 10,
            resumeFrom: new StoredCheckpoint<long>
            {
                Value = 200L,
                SavedAt = DateTimeOffset.UtcNow,
            });

        var action = async () => await ReadAllAsync(source, context);

        await action.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*не продвинул checkpoint*");
    }

    [Fact]
    public async Task ReadAsync_should_throw_when_null_resume_parameter_requires_explicit_type()
    {
        var options = CreateOptions() with
        {
            ResumeCheckpointParameterType = null,
        };

        var source = new TestablePostgresBatchSource(
            options,
            new FakePostgresConnectionFactory(),
            CreateReader());

        var action = async () => await ReadAllAsync(source, CreateLongContext(batchSize: 2));

        await action.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*ResumeCheckpointParameterType*");
    }

    private static async Task<List<SourceBatch<int, long>>> ReadAllAsync(
        IBatchSource<int, long> source,
        BatchSourceContext<long> context)
    {
        var batches = new List<SourceBatch<int, long>>();

        await foreach (var batch in source.ReadAsync(context, CancellationToken.None))
        {
            batches.Add(batch);
        }

        return batches;
    }

    private static async Task<List<SourceBatch<int, CompositeCheckpoint>>> ReadAllAsync(
        IBatchSource<int, CompositeCheckpoint> source,
        BatchSourceContext<CompositeCheckpoint> context)
    {
        var batches = new List<SourceBatch<int, CompositeCheckpoint>>();

        await foreach (var batch in source.ReadAsync(context, CancellationToken.None))
        {
            batches.Add(batch);
        }

        return batches;
    }

    private static PostgresBatchSourceOptions<int, long> CreateOptions() =>
        new()
        {
            Connection = PostgresConnectionOptions.FromConnectionString(ConnectionString),
            CommandTimeoutSeconds = 30,
            CommandText = """
                          select id, cursor_id
                          from demo_source
                          where (@resume_checkpoint is null or cursor_id > @resume_checkpoint)
                          order by cursor_id
                          limit @batch_size
                          """,
            ResumeCheckpointParameterType = NpgsqlDbType.Bigint,
            RowMaterializer = (reader, _) => ValueTask.FromResult(
                new PostgresBatchSourceRow<int, long>(
                    reader.GetInt32(0),
                    reader.GetInt64(1))),
        };

    private static BatchSourceContext<long> CreateLongContext(
        int batchSize,
        StoredCheckpoint<long>? resumeFrom = null) =>
        new()
        {
            Job = new JobExecutionContext
            {
                JobName = "orders-sync",
                RunId = "run-1",
                StartedAt = DateTimeOffset.UtcNow,
            },
            RequestedBatchSize = batchSize,
            ResumeFrom = resumeFrom,
        };

    private static BatchSourceContext<CompositeCheckpoint> CreateCompositeContext(
        int batchSize,
        StoredCheckpoint<CompositeCheckpoint>? resumeFrom = null) =>
        new()
        {
            Job = new JobExecutionContext
            {
                JobName = "orders-sync",
                RunId = "run-1",
                StartedAt = DateTimeOffset.UtcNow,
            },
            RequestedBatchSize = batchSize,
            ResumeFrom = resumeFrom,
        };

    private static DbDataReader CreateReader(params (int Item, long Checkpoint)[] rows)
    {
        var table = new DataTable();
        table.Columns.Add("item", typeof(int));
        table.Columns.Add("checkpoint", typeof(long));

        foreach (var row in rows)
        {
            table.Rows.Add(row.Item, row.Checkpoint);
        }

        return table.CreateDataReader();
    }

    private static DbDataReader CreateCompositeReader(
        params (int Item, DateTime CreatedAt, long Id)[] rows)
    {
        var table = new DataTable();
        table.Columns.Add("item", typeof(int));
        table.Columns.Add("created_at", typeof(DateTime));
        table.Columns.Add("id", typeof(long));

        foreach (var row in rows)
        {
            table.Rows.Add(row.Item, row.CreatedAt, row.Id);
        }

        return table.CreateDataReader();
    }

    private const string ConnectionString =
        "Host=localhost;Port=5432;Database=batchflow;Username=batchflow;Password=batchflow";

    private sealed record CompositeCheckpoint(DateTime CreatedAt, long Id);

    private sealed class TestablePostgresBatchSource(
        PostgresBatchSourceOptions<int, long> options,
        IPostgresConnectionFactory connectionFactory,
        params DbDataReader[] readers)
        : PostgresBatchSource<int, long>(options, connectionFactory)
    {
        private readonly Queue<DbDataReader> _readers = new(readers);

        public List<CommandSnapshot> CapturedCommands { get; } = [];

        protected override ValueTask<DbDataReader> ExecuteReaderAsync(
            NpgsqlCommand command,
            CancellationToken cancellationToken)
        {
            CapturedCommands.Add(CommandSnapshot.Create(command.Parameters));
            return ValueTask.FromResult(_readers.Dequeue());
        }
    }

    private sealed class TestableCompositeCheckpointSource(
        PostgresBatchSourceOptions<int, CompositeCheckpoint> options,
        IPostgresConnectionFactory connectionFactory,
        params DbDataReader[] readers)
        : PostgresBatchSource<int, CompositeCheckpoint>(options, connectionFactory)
    {
        private readonly Queue<DbDataReader> _readers = new(readers);

        public List<CommandSnapshot> CapturedCommands { get; } = [];

        protected override ValueTask<DbDataReader> ExecuteReaderAsync(
            NpgsqlCommand command,
            CancellationToken cancellationToken)
        {
            CapturedCommands.Add(CommandSnapshot.Create(command.Parameters));
            return ValueTask.FromResult(_readers.Dequeue());
        }
    }

    private sealed class FakePostgresConnectionFactory : IPostgresConnectionFactory
    {
        public int OpenConnectionCalls { get; private set; }

        public ValueTask<IPostgresConnectionLease> OpenConnectionAsync(CancellationToken cancellationToken)
        {
            OpenConnectionCalls++;
            return ValueTask.FromResult<IPostgresConnectionLease>(
                new FakePostgresConnectionLease(new NpgsqlConnection(ConnectionString)));
        }
    }

    private sealed class FakePostgresConnectionLease(NpgsqlConnection connection) : IPostgresConnectionLease
    {
        public NpgsqlConnection Connection { get; } = connection;

        public ValueTask DisposeAsync()
        {
            Connection.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class CommandSnapshot
    {
        private readonly Dictionary<string, object?> _values;

        private CommandSnapshot(Dictionary<string, object?> values)
        {
            _values = values;
        }

        public T? Get<T>(string name)
        {
            if (!_values.TryGetValue(name, out var value) || value is null || value is DBNull)
            {
                return default;
            }

            return (T)value;
        }

        public static CommandSnapshot Create(NpgsqlParameterCollection parameters)
        {
            var values = parameters
                .Cast<NpgsqlParameter>()
                .ToDictionary(
                    parameter => parameter.ParameterName,
                    parameter => parameter.Value,
                    StringComparer.Ordinal);

            return new CommandSnapshot(values);
        }
    }
}
