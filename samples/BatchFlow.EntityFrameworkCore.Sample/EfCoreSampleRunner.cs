using BatchFlow;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Checkpoints;
using BatchFlow.Core.Execution.Models;
using BatchFlow.EntityFrameworkCore.Extensions;
using BatchFlow.EntityFrameworkCore.Sources;
using Microsoft.EntityFrameworkCore;

internal sealed class EfCoreSampleRunner(
    EfCoreBatchSource<OrdersDbContext, OrderDto, long> source,
    IDbContextFactory<OrdersDbContext> factory,
    InMemoryCheckpointStore<long> checkpoints,
    BatchJobRunOptions runOptions)
{
    private static readonly BatchRetryOptions Retry = new()
    {
        MaxRetryAttempts = 2,
        Delay = TimeSpan.FromMilliseconds(20),
        ShouldHandle = exception => exception is TimeoutException,
    };

    public async Task RunAsync(int pass, CancellationToken token)
    {
        var each = await BatchJob.Named("ef-orders-each-v1").FromDbContext(source)
            .WithBatchSize(2).WithDegreeOfParallelism(1).WithCheckpoint(checkpoints).WithRetry(Retry)
            .RunEachAsync(async (order, context, cancellation) =>
            {
                if (order.Id == 2 && context.Attempt == 1)
                {
                    Console.WriteLine("Each: имитируем transient-ошибку, runtime выполнит retry.");
                    throw new TimeoutException("Демонстрационная transient-ошибка.");
                }

                // Контекст записи не разделяется с source или другими обработчиками.
                await using var target = await factory.CreateDbContextAsync(cancellation);
                await UpsertAsync(target, "each", order, cancellation);
                await target.SaveChangesAsync(cancellation);
            }, runOptions, token);
        Check(each, "each", pass, token);

        var batch = await BatchJob.Named("ef-orders-batch-v1").FromDbContext(source)
            .WithBatchSize(2).WithDegreeOfParallelism(1).WithCheckpoint(checkpoints).WithRetry(Retry)
            .RunBatchAsync(async (orders, context, cancellation) =>
            {
                if (context.Attempt == 1)
                {
                    Console.WriteLine("Batch: имитируем transient-ошибку до записи.");
                    throw new TimeoutException("Демонстрационная transient-ошибка.");
                }

                await using var target = await factory.CreateDbContextAsync(cancellation);
                await using var transaction = await target.Database.BeginTransactionAsync(cancellation);
                foreach (var order in orders)
                {
                    await UpsertAsync(target, "batch", order, cancellation);
                }

                await target.SaveChangesAsync(cancellation);
                await transaction.CommitAsync(cancellation);
            }, runOptions, token);
        Check(batch, "batch", pass, token);
    }

    private static async ValueTask UpsertAsync(OrdersDbContext target, string mode, OrderDto order, CancellationToken token)
    {
        var existing = await target.Exports.FindAsync([mode, order.Id], token);
        if (existing is null)
        {
            target.Exports.Add(new ExportRow { Mode = mode, Id = order.Id, Number = order.Number });
        }
        else
        {
            existing.Number = order.Number;
        }
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
