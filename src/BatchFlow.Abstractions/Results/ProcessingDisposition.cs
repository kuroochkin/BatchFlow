namespace BatchFlow.Abstractions.Results;

/// <summary>
/// Определяет исход выполнения обработчика.
/// </summary>
public enum ProcessingDisposition
{
    /// <summary>
    /// Обработчик успешно выполнился.
    /// </summary>
    Succeeded = 0,
    
    /// <summary>
    /// Обработчик пропустил обработку.
    /// </summary>
    Skipped = 1,
}
