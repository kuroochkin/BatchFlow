using BatchFlow;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Postgres.Checkpoints;
using BatchFlow.Postgres.Extensions;
using BatchFlow.Postgres.Options;
using BatchFlow.Postgres.Sources;
using FluentAssertions;
using Npgsql;
using Xunit;

namespace BatchFlow.Postgres.Tests.Extensions;

public sealed class BatchJobPostgresBuilderExtensionsTests
{
    [Fact]
    public void FromPostgres_should_register_postgres_source_in_job_definition()
    {
        var source = new FakePostgresSource(CreateSourceOptions());

        var job = BatchJob
            .Named("orders-sync")
            .FromPostgres(source)
            .ProcessEach((_, _, _) => ValueTask.CompletedTask)
            .Build();

        job.Source.Should().BeSameAs(source);
    }

    [Fact]
    public void WithPostgresCheckpoint_should_register_postgres_store_and_checkpoint_id()
    {
        var source = new FakePostgresSource(CreateSourceOptions());
        var checkpointStore = new PostgresCheckpointStore<long>(CreateCheckpointStoreOptions());

        var job = BatchJob
            .Named("orders-sync")
            .FromPostgres(source)
            .WithPostgresCheckpoint("orders-checkpoint", checkpointStore)
            .ProcessEach((_, _, _) => ValueTask.CompletedTask)
            .Build();

        job.CheckpointStore.Should().BeSameAs(checkpointStore);
        job.CheckpointId.Should().Be("orders-checkpoint");
    }

    private static PostgresBatchSourceOptions CreateSourceOptions() =>
        new()
        {
            Connection = PostgresConnectionOptions.FromConnectionFactory(
                () => new NpgsqlConnection(ConnectionString)),
            CommandTimeoutSeconds = 30,
        };

    private static PostgresCheckpointStoreOptions CreateCheckpointStoreOptions() =>
        new()
        {
            Connection = PostgresConnectionOptions.FromConnectionFactory(
                () => new NpgsqlConnection(ConnectionString)),
            SchemaName = "batchflow",
            TableName = "checkpoints",
            CommandTimeoutSeconds = 30,
        };

    private const string ConnectionString =
        "Host=localhost;Port=5432;Database=batchflow;Username=batchflow;Password=batchflow";

    private sealed class FakePostgresSource(PostgresBatchSourceOptions options)
        : PostgresBatchSource<int, long>(options)
    {
        protected override async IAsyncEnumerable<SourceBatch<int, long>> ReadCoreAsync(
            NpgsqlConnection connection,
            BatchSourceContext<long> context,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }
    }
}
