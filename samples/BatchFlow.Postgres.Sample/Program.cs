using BatchFlow.Extensions.DependencyInjection;
using BatchFlow.Postgres.Checkpoints;
using BatchFlow.Postgres.DependencyInjection;
using BatchFlow.Postgres.Options;
using BatchFlow.Postgres.Sources;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using NpgsqlTypes;

var value = Environment.GetEnvironmentVariable("BATCHFLOW_POSTGRES_SAMPLE");
ArgumentException.ThrowIfNullOrWhiteSpace(value, "BATCHFLOW_POSTGRES_SAMPLE");
var connection = new NpgsqlConnectionStringBuilder(value);
if (connection.Database?.EndsWith("_tests", StringComparison.Ordinal) is not true)
{
    throw new InvalidOperationException("Пример разрешен только в отдельной PostgreSQL-БД с суффиксом _tests.");
}

using var shutdown = new CancellationTokenSource();
ConsoleCancelEventHandler onCancel = (_, args) =>
{
    args.Cancel = true;
    shutdown.Cancel();
};
Console.CancelKeyPress += onCancel;

try
{
    var schema = $"batchflow_sample_{Guid.NewGuid():N}";
    var queries = new PreparedPostgresSampleQueries(schema);
    var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["Database:ConnectionString"] = connection.ConnectionString,
        ["Checkpoint:SchemaName"] = schema,
        ["Checkpoint:TableName"] = "checkpoints",
        ["Checkpoint:CommandTimeoutSeconds"] = "15",
    }).Build();
    var services = new ServiceCollection();
    services.AddBatchFlow();
    services.AddBatchFlowPostgresDataSource(configuration.GetSection("Database"));
    services.AddBatchFlowPostgresCheckpoint<long>(configuration.GetSection("Checkpoint"));
    services.AddSingleton(queries);
    services.AddBatchFlowPostgresSource<OrderDto, long>((_, connectionOptions) => new PostgresBatchSourceOptions<OrderDto, long>
    {
        Connection = connectionOptions,
        CommandText = queries.Read,
        ResumeCheckpointParameterType = NpgsqlDbType.Bigint,
        RowMaterializer = (reader, _) =>
        {
            var order = new OrderDto(reader.GetInt64(0), reader.GetString(1));
            return ValueTask.FromResult(new PostgresBatchSourceRow<OrderDto, long>(order, order.Id));
        },
        CheckpointProgressionValidator = (previous, next) => next > previous,
    });
    services.AddScoped<PostgresSampleRunner>();
    await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
    var dataSource = provider.GetRequiredService<NpgsqlDataSource>();
    var completed = false;

    try
    {
        // DDL выполняется явно только в созданной этим запуском схеме, не при DI-регистрации.
        await using (var setup = await dataSource.OpenConnectionAsync(shutdown.Token))
        {
            await using var transaction = await setup.BeginTransactionAsync(shutdown.Token);
            await using var command = new NpgsqlCommand(queries.Initialize, setup, transaction);
            await command.ExecuteNonQueryAsync(shutdown.Token);
            await transaction.CommitAsync(shutdown.Token);
        }

        for (var pass = 1; pass <= 2; pass++)
        {
            await using var scope = provider.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<PostgresCheckpointStore<long>>().EnsureStorageAsync(shutdown.Token);
            await scope.ServiceProvider.GetRequiredService<PostgresSampleRunner>().RunAsync(pass, shutdown.Token);
        }

        await using var verify = dataSource.CreateCommand(queries.Count);
        var count = (long)(await verify.ExecuteScalarAsync(shutdown.Token))!;
        if (count != 6)
        {
            throw new InvalidOperationException($"Ожидалось 6 результатов без дубликатов, получено {count}.");
        }

        Console.WriteLine($"Результаты each и batch: {count}, без дубликатов.");
        completed = true;
    }
    finally
    {
        try
        {
            await using var cleanup = dataSource.CreateCommand(queries.Cleanup);
            cleanup.CommandTimeout = 5;
            await cleanup.ExecuteNonQueryAsync(CancellationToken.None);
        }
        catch (Exception cleanupError) when (!completed)
        {
            // Ошибка очистки не подменяет первичную ошибку и не затрагивает другие схемы.
            Console.Error.WriteLine($"Не удалось удалить демонстрационную схему {schema}: {cleanupError.Message}");
        }
    }
}
catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
{
    Console.Error.WriteLine("Пример отменен.");
    Environment.ExitCode = 130;
}
finally
{
    Console.CancelKeyPress -= onCancel;
}
