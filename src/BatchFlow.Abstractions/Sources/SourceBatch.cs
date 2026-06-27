namespace BatchFlow.Abstractions.Sources;

/// <summary>
/// Представляет пакет данных, полученный из источника, вместе с контрольной точкой, до которой он продвигает обработку.
/// </summary>
/// <typeparam name="TItem">Тип элементов, входящих в пакет.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с пакетом.</typeparam>
public sealed record SourceBatch<TItem, TCheckpoint>
{
    /// <summary>
    /// Элементы, входящие в этот пакет.
    /// </summary>
    public required IReadOnlyList<TItem> Items { get; init; }

    /// <summary>
    /// Контрольная точка, соответствующую этому пакету.
    /// </summary>
    public required TCheckpoint Checkpoint { get; init; }

    /// <summary>
    /// Дополнительные метаданные источника, связанные с пакетом.
    /// </summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = new Dictionary<string, string>();
}
