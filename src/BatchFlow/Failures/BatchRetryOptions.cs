namespace BatchFlow;

/// <summary>
/// Описывает параметры базовой fixed retry-стратегии для item- и batch-обработки.
/// </summary>
public sealed record BatchRetryOptions
{
    /// <summary>
    /// Количество повторных попыток сверх исходного вызова обработчика.
    /// Значение <c>3</c> означает, что runtime выполнит до четырех вызовов: один исходный и три повтора.
    /// </summary>
    public int MaxRetryAttempts { get; init; }

    /// <summary>
    /// Фиксированная задержка между повторными попытками.
    /// </summary>
    public TimeSpan Delay { get; init; } = TimeSpan.Zero;

    /// <summary>
    /// Предикат, который определяет, нужно ли применять retry к конкретному исключению.
    /// Если предикат не задан, runtime будет повторять все исключения, кроме <see cref="OperationCanceledException" />.
    /// </summary>
    public Func<Exception, bool>? ShouldHandle { get; init; }
}
