using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Observability;

namespace BatchFlow.Core.Execution.Models;

/// <summary>
/// Представляет итог выполнения пакетной задачи.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с выполнением задачи.</typeparam>
public sealed record BatchJobExecutionSummary<TCheckpoint>
{
    /// <summary>
    /// Контекст выполненной задачи.
    /// </summary>
    public required JobExecutionContext Job { get; init; }

    /// <summary>
    /// Итоговый статус выполнения задачи.
    /// </summary>
    public required JobCompletionStatus Status { get; init; }

    /// <summary>
    /// Контрольная точка, загруженная при старте выполнения.
    /// </summary>
    public StoredCheckpoint<TCheckpoint>? ResumeFrom { get; init; }

    /// <summary>
    /// Последняя успешно сохраненная контрольная точка.
    /// </summary>
    public StoredCheckpoint<TCheckpoint>? LastPersistedCheckpoint { get; init; }

    /// <summary>
    /// Момент завершения задачи.
    /// </summary>
    public DateTimeOffset CompletedAt { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Количество обработанных пакетов.
    /// </summary>
    public long ProcessedBatchCount { get; init; }

    /// <summary>
    /// Количество элементов, обработанных успешно.
    /// </summary>
    public long SucceededItemCount { get; init; }

    /// <summary>
    /// Количество элементов, завершившихся пропуском.
    /// </summary>
    public long SkippedItemCount { get; init; }

    /// <summary>
    /// Пояснение к итоговому статусу
    /// </summary>
    public string? Reason { get; init; }

    /// <summary>
    /// Исключение, из-за которого выполнение завершилось ошибкой, если оно было.
    /// </summary>
    public Exception? Exception { get; init; }
}
