using BatchFlow.Abstractions.Results;

namespace BatchFlow.Abstractions.Observability;

/// <summary>
/// Публикуется, когда пакет достигает окончательного состояния и его результат полностью известен.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с пакетом.</typeparam>
public sealed record BatchCompletedEvent<TCheckpoint> : BatchFlowEvent<TCheckpoint>
{
    /// <summary>
    /// Возвращает порядковый номер пакета в рамках текущего запуска.
    /// </summary>
    public required long BatchSequence { get; init; }

    /// <summary>
    /// Возвращает контрольную точку, соответствующую завершенному пакету.
    /// </summary>
    public required TCheckpoint BatchCheckpoint { get; init; }

    /// <summary>
    /// Возвращает общее количество элементов в пакете.
    /// </summary>
    public required int ItemCount { get; init; }

    /// <summary>
    /// Возвращает номер попытки, потребовавшейся для окончательного завершения пакета.
    /// Для поэлементной обработки это максимальный номер попытки, который понадобился хотя бы одному элементу пакета.
    /// </summary>
    public required int Attempt { get; init; }

    /// <summary>
    /// Возвращает количество элементов, обработанных успешно.
    /// </summary>
    public required int SucceededCount { get; init; }

    /// <summary>
    /// Возвращает количество элементов, завершившихся пропуском.
    /// </summary>
    public required int SkippedCount { get; init; }

    /// <summary>Подробности исходов текущего пакета, переданные обработчиком или сформированные политикой пропуска.</summary>
    public IReadOnlyList<BatchProcessingIssue> Issues { get; init; } = [];

    /// <summary>
    /// Возвращает метаданные источника, связанные с завершенным пакетом.
    /// </summary>
    public IReadOnlyDictionary<string, string> SourceMetadata { get; init; } = new Dictionary<string, string>();
}
