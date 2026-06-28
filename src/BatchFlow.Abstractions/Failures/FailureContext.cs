using BatchFlow.Abstractions.Execution;

namespace BatchFlow.Abstractions.Failures;

/// <summary>
/// Содержит сведения об ошибке, необходимые политике обработки сбоев для принятия решения.
/// </summary>
/// <typeparam name="TItem">Тип элемента, связанного с ошибкой обработки.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с ошибкой обработки.</typeparam>
public sealed record FailureContext<TItem, TCheckpoint>
{
    /// <summary>
    /// Контекст выполнения текущей задачи.
    /// </summary>
    public required JobExecutionContext Job { get; init; }

    /// <summary>
    /// Порядковый номер пакета, в котором произошла ошибка.
    /// </summary>
    public required long BatchSequence { get; init; }

    /// <summary>
    /// Контрольная точка, соответствующую пакету с ошибкой.
    /// </summary>
    public required TCheckpoint BatchCheckpoint { get; init; }

    /// <summary>
    /// Элементы, входящие в пакет, в котором произошла ошибка.
    /// </summary>
    public required IReadOnlyList<TItem> BatchItems { get; init; }

    /// <summary>
    /// Метаданные источника, связанные с пакетом, в котором произошла ошибка.
    /// </summary>
    public IReadOnlyDictionary<string, string> SourceMetadata { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// Номер текущей попытки обработки.
    /// </summary>
    public required int Attempt { get; init; }

    /// <summary>
    /// Исключение, которое привело к ошибке.
    /// </summary>
    public required Exception Exception { get; init; }

    /// <summary>
    /// Уровень обработки, на котором произошел сбой.
    /// </summary>
    public required FailureScope Scope { get; init; }

    /// <summary>
    /// Индекс элемента, если ошибка произошла во время поэлементной обработки.
    /// </summary>
    public int? ItemIndex { get; init; }

    /// <summary>
    /// Элемент, на котором произошла ошибка во время поэлементной обработки.
    /// </summary>
    public TItem? FailedItem { get; init; }
}
