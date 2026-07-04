namespace BatchFlow;

/// <summary>
/// Определяет встроенное поведение после исчерпания повторных попыток
/// или при ошибке, для которой retry применять не нужно.
/// </summary>
public enum BatchFailureTerminalAction
{
    /// <summary>
    /// Остановить выполнение задачи с ошибкой.
    /// </summary>
    Stop = 0,

    /// <summary>
    /// Окончательно пропустить неуспешный item или batch и продолжить выполнение.
    /// </summary>
    Skip = 1,
}
