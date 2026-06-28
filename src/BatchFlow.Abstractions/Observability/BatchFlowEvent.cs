using BatchFlow.Abstractions.Execution;

namespace BatchFlow.Abstractions.Observability;

/// <summary>
/// Базовый тип событий, публикуемых во время выполнения пакетной задачи.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с выполнением задачи.</typeparam>
public abstract record BatchFlowEvent<TCheckpoint>
{
    /// <summary>
    /// Контекст выполнения задачи, к которому относится событие.
    /// </summary>
    public required JobExecutionContext Job { get; init; }

    /// <summary>
    /// Момент возникновения события.
    /// </summary>
    public DateTimeOffset OccurredAt { get; init; } = DateTimeOffset.UtcNow;
}
