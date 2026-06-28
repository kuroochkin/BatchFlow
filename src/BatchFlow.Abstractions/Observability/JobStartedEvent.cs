using BatchFlow.Abstractions.Checkpoints;

namespace BatchFlow.Abstractions.Observability;

/// <summary>
/// Публикуется в момент запуска задачи.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с задачей.</typeparam>
public sealed record JobStartedEvent<TCheckpoint> : BatchFlowEvent<TCheckpoint>
{
    /// <summary>
    /// Контрольная точка, от которой задача возобновляет выполнение, если она существует.
    /// </summary>
    public StoredCheckpoint<TCheckpoint>? ResumeFrom { get; init; }
}