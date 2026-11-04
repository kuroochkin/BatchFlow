using BatchFlow;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Postgres.Checkpoints;
using BatchFlow.Postgres.Extensions;
using BatchFlow.Postgres.Options;
using BatchFlow.Postgres.Sources;
using FluentAssertions;
using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace BatchFlow.Postgres.Tests.Extensions;

public sealed class BatchJobPostgresBuilderExtensionsTests
{
    [Fact]
    public void FromPostgres_should_register_postgres_source_in_job_definition()
    {
        var source = new PostgresBatchSource<int, long>(CreateSourceOptions());

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
        var source = new PostgresBatchSource<int, long>(CreateSourceOptions());
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

    [Fact]
    public void FromPostgres_options_overload_should_create_source_and_register_it_in_job_definition()
    {
        var options = CreateSourceOptions();

        var job = BatchJob
            .Named("orders-sync")
            .FromPostgres(options)
            .ProcessEach((_, _, _) => ValueTask.CompletedTask)
            .Build();

        job.Source.Should().BeOfType<PostgresBatchSource<int, long>>();
    }

    private static PostgresBatchSourceOptions<int, long> CreateSourceOptions() =>
        new()
        {
            Connection = PostgresConnectionOptions.FromConnectionFactory(
                () => new NpgsqlConnection(ConnectionString)),
            CommandTimeoutSeconds = 30,
            CommandText = "select @batch_size as batch_size, @resume_checkpoint as resume_checkpoint where 1 = 0",
            ResumeCheckpointParameterType = NpgsqlDbType.Bigint,
            RowMaterializer = (_, _) => ValueTask.FromResult(new PostgresBatchSourceRow<int, long>(0, 0L)),
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
}
