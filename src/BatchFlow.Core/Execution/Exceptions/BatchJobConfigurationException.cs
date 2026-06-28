namespace BatchFlow.Core.Execution.Exceptions;

/// <summary>
/// Представляет ошибку конфигурации пакетной задачи.
/// </summary>
public sealed class BatchJobConfigurationException : Exception
{
    /// <summary>
    /// Создает исключение, указывающее на ошибку конфигурации пакетной задачи.
    /// </summary>
    /// <param name="message">Пояснение, описывающее найденную проблему конфигурации.</param>
    public BatchJobConfigurationException(string message)
        : base(message)
    {
    }
}
