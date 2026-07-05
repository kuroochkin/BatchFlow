using BatchFlow.Abstractions.Failures;

namespace BatchFlow;

/// <summary>
/// Представляет встроенную fixed retry-стратегию с заданным количеством повторов,
/// фиксированной задержкой и поведением после окончательной неудачи.
/// </summary>
/// <typeparam name="TItem">Тип элемента, связанного со сбоем.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
public sealed class FixedRetryFailurePolicy<TItem, TCheckpoint> : IFailurePolicy<TItem, TCheckpoint>
{
    private readonly TimeSpan _delay;
    private readonly int _maxRetryAttempts;
    private readonly Func<Exception, bool> _shouldHandle;
    private readonly BatchFailureTerminalAction _terminalAction;

    /// <summary>
    /// Создает built-in retry policy с поведением по умолчанию, которое останавливает задачу после окончательной неудачи.
    /// </summary>
    /// <param name="options">Настройки fixed retry-стратегии.</param>
    public FixedRetryFailurePolicy(BatchRetryOptions options)
        : this(options, BatchFailureTerminalAction.Stop)
    {
    }

    /// <summary>
    /// Создает built-in retry policy с указанными параметрами и terminal-поведением после исчерпания повторных попыток.
    /// </summary>
    /// <param name="options">Настройки fixed retry-стратегии.</param>
    /// <param name="terminalAction">Конечное действие runtime после окончательной неудачи.</param>
    public FixedRetryFailurePolicy(
        BatchRetryOptions options,
        BatchFailureTerminalAction terminalAction)
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

        if (!Enum.IsDefined(terminalAction))
        {
            throw new ArgumentOutOfRangeException(nameof(terminalAction), terminalAction,
                "Указано неизвестное terminal-действие встроенной политики ошибок.");
        }

        _maxRetryAttempts = options.MaxRetryAttempts;
        _delay = options.Delay;
        _shouldHandle = options.ShouldHandle ?? DefaultShouldHandle;
        _terminalAction = terminalAction;
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
                CreateTerminalDecision("Исключение не подпадает под встроенную стратегию повторных попыток."));
        }

        if (context.Attempt <= _maxRetryAttempts)
        {
            return ValueTask.FromResult(FailureDecision.Retry(_delay));
        }

        return ValueTask.FromResult(
            CreateTerminalDecision("Повторные попытки обработки исчерпаны."));
    }

    private FailureDecision CreateTerminalDecision(string reason) =>
        _terminalAction switch
        {
            BatchFailureTerminalAction.Stop => FailureDecision.Stop(reason),
            BatchFailureTerminalAction.Skip => FailureDecision.Skip(reason),
            _ => throw new InvalidOperationException("Указано неизвестное terminal-действие встроенной политики ошибок."),
        };

    private static bool DefaultShouldHandle(Exception exception) =>
        exception is not OperationCanceledException;
}
