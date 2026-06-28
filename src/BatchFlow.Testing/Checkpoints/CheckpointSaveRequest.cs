using BatchFlow.Abstractions.Checkpoints;

namespace BatchFlow.Testing.Checkpoints;

/// <summary>
/// Описывает попытку сохранения контрольной точки, выполненную через тестовое хранилище.
/// </summary>
/// <typeparam name="TCheckpoint">Тип значения контрольной точки.</typeparam>
public sealed record CheckpointSaveRequest<TCheckpoint>
{
    /// <summary>
    /// Логический идентификатор потока контрольных точек.
    /// </summary>
    public required string CheckpointId { get; init; }

    /// <summary>
    /// Контрольная точка, которую тестируемый код пытается сохранить.
    /// </summary>
    public required StoredCheckpoint<TCheckpoint> Checkpoint { get; init; }

    /// <summary>
    /// Ожидаемый токен оптимистичной конкуренции, переданный тестируемым кодом.
    /// </summary>
    public string? ExpectedConcurrencyToken { get; init; }
}
