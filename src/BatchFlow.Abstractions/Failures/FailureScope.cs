namespace BatchFlow.Abstractions.Failures;

/// <summary>
/// Определяет уровень обработки, на котором произошла ошибка.
/// </summary>
public enum FailureScope
{
    /// <summary>
    /// Сбой относится ко всему пакету целиком.
    /// </summary>
    Batch = 0,

    /// <summary>
    /// Сбой относится к отдельному элементу внутри пакета.
    /// </summary>
    Item = 1,
}
