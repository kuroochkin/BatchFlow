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
}