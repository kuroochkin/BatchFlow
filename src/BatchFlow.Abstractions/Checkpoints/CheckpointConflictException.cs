namespace BatchFlow.Abstractions.Checkpoints;

/// <summary>
/// Представляет конфликт оптимистичной конкуренции при сохранении контрольной точки.
/// </summary>
public sealed class CheckpointConflictException : Exception
{
    /// <summary>
    /// Создает исключение конфликта версий для указанного потока контрольных точек.
    /// </summary>
    /// <param name="checkpointId">Логический идентификатор потока контрольных точек.</param>
    public CheckpointConflictException(string checkpointId)
        : base($"Не удалось сохранить контрольную точку '{checkpointId}', потому что уже существует более новая версия.")
    {
        CheckpointId = checkpointId;
    }

    /// <summary>
    /// Создает исключение конфликта версий для указанного потока контрольных точек и сохраняет исходную причину ошибки.
    /// </summary>
    /// <param name="checkpointId">Логический идентификатор потока контрольных точек.</param>
    /// <param name="innerException">Исходное исключение, которое привело к конфликту сохранения.</param>
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
