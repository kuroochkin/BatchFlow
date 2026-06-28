namespace BatchFlow.Abstractions.Observability;

/// <summary>
/// Публикуется в момент начала обработки пакета.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с пакетом.</typeparam>
public sealed record BatchStartedEvent<TCheckpoint> : BatchFlowEvent<TCheckpoint>
{
    /// <summary>
    /// Порядковый номер пакета в рамках текущего запуска.
    /// </summary>
    public required long BatchSequence { get; init; }

    /// <summary>
    /// Контрольная точка, соответствующую пакету.
    /// </summary>
    public required TCheckpoint BatchCheckpoint { get; init; }

    /// <summary>
    /// Количество элементов в пакете.
    /// </summary>
    public required int ItemCount { get; init; }

    /// <summary>
    /// Номер текущей попытки обработки пакета.
    /// </summary>
    public required int Attempt { get; init; }

    /// <summary>
    /// Метаданные источника, связанные с пакетом.
    /// </summary>
    public IReadOnlyDictionary<string, string> SourceMetadata { get; init; } = new Dictionary<string, string>();
}
