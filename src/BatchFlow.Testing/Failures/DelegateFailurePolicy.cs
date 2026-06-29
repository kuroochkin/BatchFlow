using BatchFlow.Abstractions.Failures;

namespace BatchFlow.Testing.Failures;

/// <summary>
/// Представляет тестовую политику обработки ошибок, основанную на делегате.
/// </summary>
/// <typeparam name="TItem">Тип элемента, связанного со сбоем.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
public sealed class DelegateFailurePolicy<TItem, TCheckpoint> : IFailurePolicy<TItem, TCheckpoint>
{
    private readonly Func<FailureContext<TItem, TCheckpoint>, CancellationToken, ValueTask<FailureDecision>> _handler;

    /// <summary>
    /// Создает тестовую политику обработки ошибок.
    /// </summary>
    /// <param name="handler">Делегат, который принимает решение для каждого сбоя.</param>
    public DelegateFailurePolicy(
        Func<FailureContext<TItem, TCheckpoint>, CancellationToken, ValueTask<FailureDecision>> handler)
    {
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
    }

    /// <inheritdoc />
    public ValueTask<FailureDecision> DecideAsync(
        FailureContext<TItem, TCheckpoint> context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        return _handler(context, cancellationToken);
    }
}
