namespace BatchFlow.Abstractions.Observability;

/// <summary>
/// Определяет итоговое состояние выполнения задачи.
/// </summary>
public enum JobCompletionStatus
{
    /// <summary>
    /// Задача полностью завершилась успешно без пропусков.
    /// </summary>
    Succeeded = 0,

    /// <summary>
    /// Задача завершилась, но часть элементов была окончательно пропущена.
    /// </summary>
    CompletedWithSkips = 1,

    /// <summary>
    /// Задача завершилась ошибкой.
    /// </summary>
    Failed = 2,

    /// <summary>
    /// Задача была отменена внешним запросом.
    /// </summary>
    Cancelled = 3,

    /// <summary>
    /// Задача завершилась в приостановленном состоянии.
    /// </summary>
    Suspended = 4,
}
