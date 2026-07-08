using BatchFlow.Abstractions.Observability;

namespace BatchFlow.Observability;

/// <summary>
/// Представляет наблюдателя runtime-событий, построенного на делегате.
/// Подходит для быстрого подключения логирования, метрик и отладочной диагностики без создания отдельного класса.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с наблюдаемой задачей.</typeparam>
public sealed class DelegateExecutionObserver<TCheckpoint> : IExecutionObserver<TCheckpoint>
{
    private readonly Func<BatchFlowEvent<TCheckpoint>, CancellationToken, ValueTask> _handler;

    /// <summary>
    /// Создает наблюдателя, который будет передавать все runtime-события в указанный делегат.
    /// </summary>
    /// <param name="handler">Делегат обработки событий выполнения.</param>
    public DelegateExecutionObserver(Func<BatchFlowEvent<TCheckpoint>, CancellationToken, ValueTask> handler)
    {
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
    }

    /// <inheritdoc />
    public ValueTask OnEventAsync(BatchFlowEvent<TCheckpoint> @event, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(@event);
        return _handler(@event, cancellationToken);
    }
}
