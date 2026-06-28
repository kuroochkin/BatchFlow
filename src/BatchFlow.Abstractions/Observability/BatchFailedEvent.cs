namespace BatchFlow.Abstractions.Observability;

/// <summary>
/// Публикуется, когда обработка пакета завершается ошибкой.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с пакетом.</typeparam>
public sealed record BatchFailedEvent<TCheckpoint> : BatchFlowEvent<TCheckpoint>
{
    /// <summary>
    /// Возвращает порядковый номер пакета в рамках текущего запуска.
    /// </summary>
    public required long BatchSequence { get; init; }

    /// <summary>
    /// Возвращает контрольную точку, соответствующую пакету с ошибкой.
    /// </summary>
    public required TCheckpoint BatchCheckpoint { get; init; }

    /// <summary>
    /// Возвращает количество элементов в пакете, завершившемся ошибкой.
    /// </summary>
    public required int ItemCount { get; init; }

    /// <summary>
    /// Возвращает номер попытки, на которой произошла ошибка.
    /// </summary>
    public required int Attempt { get; init; }

    /// <summary>
    /// Возвращает исключение, которым завершилась обработка пакета.
    /// </summary>
    public required Exception Exception { get; init; }

    /// <summary>
    /// Возвращает индекс элемента, если ошибка произошла во время поэлементной обработки.
    /// </summary>
    public int? ItemIndex { get; init; }

    /// <summary>
    /// Возвращает метаданные источника, связанные с пакетом с ошибкой.
    /// </summary>
    public IReadOnlyDictionary<string, string> SourceMetadata { get; init; } = new Dictionary<string, string>();
}
