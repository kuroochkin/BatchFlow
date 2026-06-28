namespace BatchFlow.Abstractions.Failures;

/// <summary>
/// Определяет уровень обработки, на котором произошла ошибка.
/// </summary>
public enum FailureScope
{
    Batch = 0,
    Item = 1,
}
