using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Core.Execution.Buffering;
using BatchFlow.Core.Execution.Models;

namespace BatchFlow.Core.Execution.Parallel;

/// <summary>
/// Базовый тип уведомлений, которыми batch worker сообщает coordinator-у о ходе обработки.
/// </summary>
internal abstract record BatchWorkerNotification<TItem, TCheckpoint>
{
    /// <summary>
    /// Batch, к которому относится уведомление.
    /// </summary>
    public required BufferedSourceBatch<TItem, TCheckpoint> BufferedBatch { get; init; }

    /// <summary>
    /// Контекст обработки batch-а, сформированный worker-ом.
    /// </summary>
    public required BatchProcessingContext<TCheckpoint> Context { get; init; }
}

/// <summary>
/// Сообщает coordinator-у, что worker приступил к обработке batch-а.
/// </summary>
internal sealed record BatchWorkerStartedNotification<TItem, TCheckpoint> : BatchWorkerNotification<TItem, TCheckpoint>;

/// <summary>
/// Сообщает coordinator-у, что worker завершил обработку batch-а и получил окончательный результат.
/// </summary>
internal sealed record BatchWorkerCompletedNotification<TItem, TCheckpoint> : BatchWorkerNotification<TItem, TCheckpoint>
{
    /// <summary>
    /// Окончательный результат обработки batch-а.
    /// </summary>
    public required ProcessingExecutionResult ProcessingResult { get; init; }
}

/// <summary>
/// Сообщает coordinator-у, что worker завершил batch terminal-сбоем.
/// </summary>
internal sealed record BatchWorkerFailedNotification<TItem, TCheckpoint> : BatchWorkerNotification<TItem, TCheckpoint>
{
    /// <summary>
    /// Итоговый статус job, который должен быть установлен coordinator-ом.
    /// </summary>
    public required JobCompletionStatus Status { get; init; }

    /// <summary>
    /// Пояснение к terminal-сбою.
    /// </summary>
    public required string Reason { get; init; }

    /// <summary>
    /// Исключение, ставшее первопричиной terminal-сбоя.
    /// </summary>
    public required Exception Exception { get; init; }

    /// <summary>
    /// Номер попытки, на которой был зафиксирован terminal-сбой.
    /// </summary>
    public required int Attempt { get; init; }

    /// <summary>
    /// Индекс элемента, если terminal-сбой произошел во время поэлементной обработки.
    /// Для batch mode всегда отсутствует.
    /// </summary>
    public int? ItemIndex { get; init; }
}
