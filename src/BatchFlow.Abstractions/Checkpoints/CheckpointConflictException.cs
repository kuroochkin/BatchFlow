namespace BatchFlow.Abstractions.Checkpoints;

/// <summary>
/// Представляет конфликт оптимистичной конкуренции при сохранении контрольной точки.
/// </summary>
public sealed class CheckpointConflictException : Exception
{
    public CheckpointConflictException(string checkpointId)
        : base($"Не удалось сохранить контрольную точку '{checkpointId}', потому что уже существует более новая версия.")
    {
        CheckpointId = checkpointId;
    }

    public CheckpointConflictException(string checkpointId, Exception innerException)
        : base($"Не удалось сохранить контрольную точку '{checkpointId}', потому что уже существует более новая версия.", innerException)
    {
        CheckpointId = checkpointId;
    }

    /// <summary>
    /// Возвращает логический идентификатор потока контрольных точек, в котором возник конфликт.
    /// </summary>
    public string CheckpointId { get; }
}
