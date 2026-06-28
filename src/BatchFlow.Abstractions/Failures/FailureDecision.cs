namespace BatchFlow.Abstractions.Failures;

/// <summary>
/// Представляет решение, которое политика обработки ошибок приняла для текущего сбоя.
/// </summary>
public sealed record FailureDecision
{
    /// <summary>
    /// Действие, которое должен выполнить движок обработки.
    /// </summary>
    public required FailureAction Action { get; init; }

    /// <summary>
    /// Необязательная задержку перед следующей попыткой.
    /// </summary>
    public TimeSpan? Delay { get; init; }

    /// <summary>
    /// Возвращает человекочитаемое пояснение к принятому решению.
    /// </summary>
    public string? Reason { get; init; }

    /// <summary>
    /// Создает решение о повторной попытке обработки.
    /// </summary>
    public static FailureDecision Retry(TimeSpan? delay = null, string? reason = null) =>
        new()
        {
            Action = FailureAction.Retry,
            Delay = delay,
            Reason = reason,
        };

    /// <summary>
    /// Создает решение о пропуске ошибочного результата.
    /// </summary>
    public static FailureDecision Skip(string? reason = null) =>
        new()
        {
            Action = FailureAction.Skip,
            Reason = reason,
        };

    /// <summary>
    /// Создает решение об остановке выполнения задачи.
    /// </summary>
    public static FailureDecision Stop(string? reason = null) =>
        new()
        {
            Action = FailureAction.Stop,
            Reason = reason,
        };

    /// <summary>
    /// Создает решение о переводе задачи в приостановленное состояние.
    /// </summary>
    public static FailureDecision Suspend(string? reason = null) =>
        new()
        {
            Action = FailureAction.Suspend,
            Reason = reason,
        };

    /// <summary>
    /// Создает решение об отправке результата ошибки в карантин.
    /// </summary>
    public static FailureDecision DeadLetter(string? reason = null) =>
        new()
        {
            Action = FailureAction.DeadLetter,
            Reason = reason,
        };
}
