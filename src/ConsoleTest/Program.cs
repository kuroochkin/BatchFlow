using BatchFlow;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Results;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Checkpoints;

var cancellationToken = CancellationToken.None;

var orders = new List<Order>
{
    new(1, "A-1001", 12_500m),
    new(2, "A-1002", 9_900m),
    new(3, "A-1003", 18_750m),
    new(4, "A-1004", 4_200m),
    new(5, "A-1005", 27_300m),
    new(6, "A-1006", 7_800m),
    new(7, "A-1007", 12_500m),
    new(8, "A-1008", 9_900m),
    new(9, "A-1009", 18_750m),
    new(10, "A-10010", 4_200m),
    new(11, "A-10011", 27_300m),
    new(13, "A-10012", 7_800m),
    new(14, "A-10012", 7_800m),
    new(15, "A-10012", 7_800m),
    new(16, "A-10012", 7_800m),
    new(17, "A-10012", 7_800m),
    new(18, "A-10012", 7_800m),
    new(19, "A-10012", 7_800m),
    new(20, "A-10012", 7_800m),
    new(21, "A-10012", 7_800m),
    new(22, "A-10012", 7_800m),
    new(23, "A-10012", 7_800m),
    new(24, "A-10012", 7_800m),
    new(25, "A-10012", 7_800m),
    new(26, "A-10012", 7_800m),
    new(27, "A-10012", 7_800m),
    new(28, "A-10012", 7_800m),
    new(29, "A-10012", 7_800m),
    new(30, "A-10012", 7_800m),
};

var source = new OrdersBatchSource(orders);
var checkpointStore = new InMemoryCheckpointStore<int>();
var attemptsByOrderId = new Dictionary<int, int>();

// Имитируем, что предыдущий запуск уже подтвердил обработку первых двух заказов.
checkpointStore.Seed(
    checkpointId: "orders-demo-v1",
    checkpoint: new StoredCheckpoint<int>
    {
        Value = 2,
        SequenceNumber = 1,
        ConcurrencyToken = "seed-token",
        SavedAt = DateTimeOffset.UtcNow.AddMinutes(-10),
    });

var summary = await BatchJob
    .Named("orders-demo")
    .From(source)
    .WithBatchSize(10)
    .WithDegreeOfParallelism(2)
    .WithPrefetchCapacity(2)
    .WithCheckpoint("orders-demo-v1", checkpointStore)
    .WithRetry(2, TimeSpan.FromMilliseconds(150))
    .WithSkipOnFailure()
    .WithObserver((@event, _) =>
    {
        switch (@event)
        {
            case JobStartedEvent<int> started:
                Console.WriteLine(
                    $"[event] job started | runId={started.Job.RunId} | resumeFrom={started.ResumeFrom?.Value.ToString() ?? "<нет>"}");
                break;

            case BatchStartedEvent<int> batchStarted:
                Console.WriteLine(
                    $"[event] batch started | seq={batchStarted.BatchSequence} | items={batchStarted.ItemCount}");
                break;

            case BatchCompletedEvent<int> batchCompleted:
                Console.WriteLine(
                    $"[event] batch completed | seq={batchCompleted.BatchSequence} | " +
                    $"ok={batchCompleted.SucceededCount} | skipped={batchCompleted.SkippedCount}");
                break;

            case BatchFailedEvent<int> batchFailed:
                Console.WriteLine(
                    $"[event] batch failed | seq={batchFailed.BatchSequence} | attempt={batchFailed.Attempt} | " +
                    $"error={batchFailed.Exception.Message}");
                break;

            case CheckpointSavedEvent<int> checkpointSaved:
                Console.WriteLine(
                    $"[event] checkpoint saved | seq={checkpointSaved.Checkpoint.SequenceNumber} | " +
                    $"value={checkpointSaved.Checkpoint.Value}");
                break;

            case JobCompletedEvent<int> completed:
                Console.WriteLine(
                    $"[event] job completed | status={completed.Status} | reason={completed.Reason ?? "<нет>"}");
                break;
        }

        return ValueTask.CompletedTask;
    })
    .RunEachAsync(
        async (order, context, token) =>
        {
            attemptsByOrderId.TryGetValue(order.Id, out var currentAttempts);
            var attempt = currentAttempts + 1;
            attemptsByOrderId[order.Id] = attempt;

            await Task.Delay(50, token);

            if (order.Id == 5 && attempt == 1)
            {
                Console.WriteLine(
                    $"Временный сбой для заказа {order.Number} на попытке #{context.Attempt}. " +
                    "Runtime выполнит retry.");

                throw new InvalidOperationException("Временный сбой при выгрузке заказа.");
            }

            if (order.Id == 8)
            {
                Console.WriteLine(
                    $"Постоянный сбой для заказа {order.Number} на попытке #{context.Attempt}. " +
                    "После исчерпания retry элемент будет пропущен.");

                throw new InvalidOperationException("Постоянный сбой при выгрузке заказа.");
            }

            Console.WriteLine(
                $"Обработка заказа {order.Number} | Сумма: {order.Amount:N0} | " +
                $"Batch #{context.BatchSequence} | Item #{context.ItemIndex} | Attempt #{context.Attempt}");

            return ProcessingResult.Success($"Заказ {order.Number} успешно выгружен.");
        },
        new BatchJobRunOptions
        {
            RunId = $"orders-demo-{DateTime.UtcNow:yyyyMMddHHmmss}",
        },
        cancellationToken);

Console.WriteLine();
Console.WriteLine("Итог выполнения:");
Console.WriteLine($"Статус: {summary.Status}");
Console.WriteLine($"Обработано пакетов: {summary.ProcessedBatchCount}");
Console.WriteLine($"Успешно обработано элементов: {summary.SucceededItemCount}");
Console.WriteLine($"Пропущено элементов: {summary.SkippedItemCount}");
Console.WriteLine($"Причина завершения: {summary.Reason ?? "<нет>"}");
Console.WriteLine($"Точка возобновления на старте: {summary.ResumeFrom?.Value.ToString() ?? "<нет>"}");
Console.WriteLine($"Последняя сохраненная контрольная точка: {summary.LastPersistedCheckpoint?.Value.ToString() ?? "<нет>"}");

if (checkpointStore.TryGet("orders-demo-v1", out var currentCheckpoint))
{
    Console.WriteLine(
        $"Checkpoint в store после завершения: value={currentCheckpoint!.Value}, " +
        $"sequence={currentCheckpoint.SequenceNumber}, " +
        $"savedAt={currentCheckpoint.SavedAt:O}");
}

internal sealed record Order(int Id, string Number, decimal Amount);

internal sealed class OrdersBatchSource(IReadOnlyList<Order> orders) : IBatchSource<Order, int>
{
    private readonly IReadOnlyList<Order> _orders = orders ?? throw new ArgumentNullException(nameof(orders));

    public async IAsyncEnumerable<SourceBatch<Order, int>> ReadAsync(
        BatchSourceContext<int> context,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);

        var lastProcessedOrderId = context.ResumeFrom?.Value ?? 0;

        Console.WriteLine(
            $"Источник стартует с позиции > {lastProcessedOrderId}, requested batch size = {context.RequestedBatchSize}");

        var pendingOrders = _orders
            .Where(order => order.Id > lastProcessedOrderId)
            .ToArray();

        for (var index = 0; index < pendingOrders.Length; index += context.RequestedBatchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var batchItems = pendingOrders
                .Skip(index)
                .Take(context.RequestedBatchSize)
                .ToArray();

            if (batchItems.Length == 0)
            {
                yield break;
            }

            var lastOrderInBatch = batchItems[^1];

            await Task.Delay(25, cancellationToken);

            yield return new SourceBatch<Order, int>
            {
                Items = batchItems,
                Checkpoint = lastOrderInBatch.Id,
                Metadata = new Dictionary<string, string>
                {
                    ["fromOrderIdExclusive"] = lastProcessedOrderId.ToString(),
                    ["lastOrderIdInBatch"] = lastOrderInBatch.Id.ToString(),
                },
            };
        }
    }
}
