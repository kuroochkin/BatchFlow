using BatchFlow;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Core.Execution.Models;
using BatchFlow.Postgres.Checkpoints;
using BatchFlow.Postgres.Extensions;
using BatchFlow.Postgres.Sources;
using Npgsql;
using NpgsqlTypes;

internal sealed record OrderDto(long Id, string Number);

internal sealed class PostgresSampleRunner(
    PostgresBatchSource<OrderDto, long> source,
    PostgresCheckpointStore<long> checkpoints,
    NpgsqlDataSource dataSource,
    PreparedPostgresSampleQueries queries,
    BatchJobRunOptions runOptions)
{
    private static readonly BatchRetryOptions Retry = new()
    {
        MaxRetryAttempts = 2,
        Delay = TimeSpan.FromMilliseconds(20),
        ShouldHandle = exception => exception is TimeoutException or NpgsqlException { IsTransient: true },
    };

    public async Task RunAsync(int pass, CancellationToken token)
    {
        var each = await BatchJob.Named("sql-orders-each-v1").FromPostgres(source)
            .WithBatchSize(2).WithDegreeOfParallelism(2).WithPostgresCheckpoint(checkpoints).WithRetry(Retry)
            .RunEachAsync(async (order, context, cancellation) =>
            {
                if (order.Id == 2 && context.Attempt == 1)
                {
                    Console.WriteLine("Each: имитируем transient-ошибку, runtime выполнит retry.");
                    throw new TimeoutException("Демонстрационная transient-ошибка.");
                }

                await using var connection = await dataSource.OpenConnectionAsync(cancellation);
                await WriteAsync(connection, null, "each", order, cancellation);
            }, runOptions, token);
        Check(each, "each", pass, token);

        var batch = await BatchJob.Named("sql-orders-batch-v1").FromPostgres(source)
            .WithBatchSize(2).WithDegreeOfParallelism(2).WithPostgresCheckpoint(checkpoints).WithRetry(Retry)
            .RunBatchAsync(async (orders, context, cancellation) =>
            {
                if (context.Attempt == 1)
                {
                    Console.WriteLine("Batch: имитируем transient-ошибку до записи.");
                    throw new TimeoutException("Демонстрационная transient-ошибка.");
                }

                await using var connection = await dataSource.OpenConnectionAsync(cancellation);
                await using var transaction = await connection.BeginTransactionAsync(cancellation);
                foreach (var order in orders)
                {
                    await WriteAsync(connection, transaction, "batch", order, cancellation);
                }

                await transaction.CommitAsync(cancellation);
            }, runOptions, token);
        Check(batch, "batch", pass, token);
    }

    private async ValueTask WriteAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, string mode, OrderDto order, CancellationToken token)
    {
        await using var command = new NpgsqlCommand(queries.Upsert, connection, transaction);
        command.Parameters.AddWithValue("mode", NpgsqlDbType.Text, mode);
        command.Parameters.AddWithValue("id", NpgsqlDbType.Bigint, order.Id);
        command.Parameters.AddWithValue("number", NpgsqlDbType.Text, order.Number);
        await command.ExecuteNonQueryAsync(token);
    }

    private static void Check(BatchJobExecutionSummary<long> summary, string mode, int pass, CancellationToken token)
    {
        if (summary.Status == JobCompletionStatus.Cancelled)
        {
            throw new OperationCanceledException(summary.Reason, token);
        }

        if (summary.Status != JobCompletionStatus.Succeeded)
        {
            throw new InvalidOperationException($"{mode}: {summary.Status}: {summary.Reason}", summary.Exception);
        }

        var expected = pass == 1 ? 3 : 0;
        var checkpoint = summary.LastPersistedCheckpoint?.Value ?? summary.ResumeFrom?.Value;
        if (summary.SucceededItemCount != expected || checkpoint != 3)
        {
            throw new InvalidOperationException($"{mode}: ожидалось {expected} элементов и checkpoint 3.");
        }

        Console.WriteLine($"{mode}, запуск {pass}: {summary.Status}, обработано {summary.SucceededItemCount}, checkpoint {checkpoint}");
    }
}
