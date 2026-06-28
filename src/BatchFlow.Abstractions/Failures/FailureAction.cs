namespace BatchFlow.Abstractions.Failures;

/// <summary>
/// Определяет действие, которое движок выполнения должен предпринять после ошибки обработки.
/// </summary>
public enum FailureAction
{
    Retry = 0,
    Skip = 1,
    Stop = 2,
    Suspend = 3,
    DeadLetter = 4,
}
