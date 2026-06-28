namespace BatchFlow.Abstractions.Checkpoints;

/// <summary>
/// Представляет сохраненную контрольную точку вместе с метаданными фиксации прогресса.
/// </summary>
/// <typeparam name="TCheckpoint">Тип значения контрольной точки, понятного источнику данных.</typeparam>
public sealed record StoredCheckpoint<TCheckpoint>
{
    /// <summary>
    /// Значение контрольной точки, которое источник данных может использовать для возобновления чтения.
    /// </summary>
    public required TCheckpoint Value { get; init; }

    /// <summary>
    /// Номер последнего подтвержденного пакета, известный движку выполнения.
    /// </summary>
    public long? SequenceNumber { get; init; }

    /// <summary>
    /// Токен оптимистичной конкуренции, назначенный хранилищем контрольных точек.
    /// </summary>
    public string? ConcurrencyToken { get; init; }

    /// <summary>
    /// Время сохранения контрольной точки.
    /// </summary>
    public DateTimeOffset SavedAt { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Дополнительные метаданные, сохраненные вместе с контрольной точкой.
    /// </summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = new Dictionary<string, string>();
}
