namespace BatchFlow.Abstractions.Observability;

/// <summary>
/// Определяет итоговое состояние выполнения задачи.
/// </summary>
public enum JobCompletionStatus
{
    Succeeded = 0,
    CompletedWithSkips = 1,
    Failed = 2,
    Cancelled = 3,
    Suspended = 4,
}
