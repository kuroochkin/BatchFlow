using BatchFlow.Abstractions.Failures;

namespace BatchFlow;

/// <summary>
/// Представляет встроенную fixed retry-стратегию с заданным количеством повторов и фиксированной задержкой.
/// </summary>
/// <typeparam name="TItem">Тип элемента, связанного со сбоем.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
public sealed class FixedRetryFailurePolicy<TItem, TCheckpoint> : IFailurePolicy<TItem, TCheckpoint>
{
    private readonly TimeSpan _delay;
    private readonly int _maxRetryAttempts;
    private readonly Func<Exception, bool> _shouldHandle;

    /// <summary>
    /// Создает built-in retry policy с указанными параметрами.
    /// </summary>
    /// <param name="options">Настройки fixed retry-стратегии.</param>
    public FixedRetryFailurePolicy(BatchRetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.MaxRetryAttempts < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.MaxRetryAttempts,
                "Количество повторных попыток не может быть отрицательным.");
        }

        if (options.Delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.Delay,
                "Задержка между повторными попытками не может быть отрицательной.");
        }

        _maxRetryAttempts = options.MaxRetryAttempts;
        _delay = options.Delay;
        _shouldHandle = options.ShouldHandle ?? DefaultShouldHandle;
    }

    /// <inheritdoc />
    public ValueTask<FailureDecision> DecideAsync(
        FailureContext<TItem, TCheckpoint> context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!_shouldHandle(context.Exception))
        {
            return ValueTask.FromResult(
                FailureDecision.Stop("Исключение не подпадает под встроенную стратегию повторных попыток."));
        }

        if (context.Attempt <= _maxRetryAttempts)
        {
            return ValueTask.FromResult(FailureDecision.Retry(_delay));
        }

        return ValueTask.FromResult(
            FailureDecision.Stop("Повторные попытки обработки исчерпаны."));
    }

    private static bool DefaultShouldHandle(Exception exception) =>
        exception is not OperationCanceledException;
}
