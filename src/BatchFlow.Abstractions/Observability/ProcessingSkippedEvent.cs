using BatchFlow.Abstractions.Failures;
using BatchFlow.Abstractions.Results;

namespace BatchFlow.Abstractions.Observability;

/// <summary>
/// Сообщает об окончательном пропуске по результату обработчика или решению политики.
/// Это исход обработки, а не подтверждение сохранения checkpoint; после сбоя событие может повториться.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
public sealed record ProcessingSkippedEvent<TCheckpoint> : BatchFlowEvent<TCheckpoint>
{
    /// <summary>Номер пакета в текущем запуске.</summary>
    public required long BatchSequence { get; init; }

    /// <summary>Позиция источника, соответствующая пакету.</summary>
    public required TCheckpoint BatchCheckpoint { get; init; }

    /// <summary>Область результата: отдельный элемент или агрегированный результат пакета.</summary>
    public required FailureScope Scope { get; init; }

    /// <summary>Индекс пропущенного элемента; отсутствует для агрегированного результата пакета.</summary>
    public int? ItemIndex { get; init; }

    /// <summary>Количество элементов, затронутых пропуском.</summary>
    public required int SkippedCount { get; init; }

    /// <summary>Номер последней попытки обработки.</summary>
    public required int Attempt { get; init; }

    /// <summary>Понятное объяснение пропуска от обработчика или политики.</summary>
    public string? Reason { get; init; }

    /// <summary>Исходное исключение при пропуске по политике; у явного результата обработчика может отсутствовать.</summary>
    public Exception? Exception { get; init; }

    /// <summary>Подробности пропусков одного пакета, без накопления истории всей задачи.</summary>
    public IReadOnlyList<BatchProcessingIssue> Issues { get; init; } = [];
}
