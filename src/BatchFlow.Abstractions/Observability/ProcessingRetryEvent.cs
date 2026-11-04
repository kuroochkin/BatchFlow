using BatchFlow.Abstractions.Failures;

namespace BatchFlow.Abstractions.Observability;

/// <summary>
/// Публикуется после решения повторить обработку, до задержки и следующего вызова обработчика.
/// Решение о retry не гарантирует следующую попытку: запуск еще может быть отменен.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
public sealed record ProcessingRetryEvent<TCheckpoint> : BatchFlowEvent<TCheckpoint>
{
    /// <summary>Номер пакета в текущем запуске.</summary>
    public required long BatchSequence { get; init; }

    /// <summary>Позиция источника, соответствующая пакету.</summary>
    public required TCheckpoint BatchCheckpoint { get; init; }

    /// <summary>Область сбоя: отдельный элемент или весь пакет.</summary>
    public required FailureScope Scope { get; init; }

    /// <summary>Индекс элемента, начиная с нуля; отсутствует при повторе всего пакета.</summary>
    public int? ItemIndex { get; init; }

    /// <summary>Номер неудачной попытки, начиная с единицы.</summary>
    public required int Attempt { get; init; }

    /// <summary>Запланированная задержка перед повтором; отсутствие значения означает немедленный повтор.</summary>
    public TimeSpan? Delay { get; init; }

    /// <summary>Исключение, по которому политика разрешила повтор.</summary>
    public required Exception Exception { get; init; }

    /// <summary>Объяснение решения политики, если оно задано.</summary>
    public string? Reason { get; init; }
}
