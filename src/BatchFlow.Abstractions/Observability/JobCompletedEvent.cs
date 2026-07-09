using BatchFlow.Abstractions.Checkpoints;

namespace BatchFlow.Abstractions.Observability;

/// <summary>
/// Публикуется, когда выполнение задачи достигает окончательного состояния.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с задачей.</typeparam>
public sealed record JobCompletedEvent<TCheckpoint> : BatchFlowEvent<TCheckpoint>
{
    /// <summary>
    /// Возвращает итоговый статус выполнения задачи.
    /// </summary>
    public required JobCompletionStatus Status { get; init; }

    /// <summary>
    /// Возвращает необязательное пояснение к итоговому статусу.
    /// </summary>
    public string? Reason { get; init; }

    /// <summary>
    /// Возвращает количество пакетов, достигших окончательного состояния обработки.
    /// </summary>
    public long ProcessedBatchCount { get; init; }

    /// <summary>
    /// Возвращает накопленное количество элементов, обработанных успешно.
    /// </summary>
    public long SucceededItemCount { get; init; }

    /// <summary>
    /// Возвращает накопленное количество элементов, завершившихся окончательным пропуском.
    /// </summary>
    public long SkippedItemCount { get; init; }

    /// <summary>
    /// Возвращает последнюю контрольную точку, которую runtime успел сохранить в ходе текущего запуска.
    /// </summary>
    public StoredCheckpoint<TCheckpoint>? LastPersistedCheckpoint { get; init; }

    /// <summary>
    /// Возвращает исключение, которым завершился запуск, если задача пришла к ошибочному исходу.
    /// </summary>
    public Exception? Exception { get; init; }
}