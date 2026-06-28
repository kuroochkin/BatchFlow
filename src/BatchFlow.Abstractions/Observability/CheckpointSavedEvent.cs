using BatchFlow.Abstractions.Checkpoints;

namespace BatchFlow.Abstractions.Observability;

/// <summary>
/// Публикуется после успешного сохранения контрольной точки.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с задачей.</typeparam>
public sealed record CheckpointSavedEvent<TCheckpoint> : BatchFlowEvent<TCheckpoint>
{
    /// <summary>
    /// Возвращает сохраненную контрольную точку.
    /// </summary>
    public required StoredCheckpoint<TCheckpoint> Checkpoint { get; init; }
}