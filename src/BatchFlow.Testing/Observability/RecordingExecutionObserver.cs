using BatchFlow.Abstractions.Observability;

namespace BatchFlow.Testing.Observability;

/// <summary>
/// Представляет тестового наблюдателя, который запоминает все runtime-события.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с наблюдаемыми событиями.</typeparam>
public sealed class RecordingExecutionObserver<TCheckpoint> : IExecutionObserver<TCheckpoint>
{
    private readonly Func<BatchFlowEvent<TCheckpoint>, CancellationToken, ValueTask>? _onEventAsync;
    private readonly List<BatchFlowEvent<TCheckpoint>> _events = [];

    /// <summary>
    /// Создает наблюдателя, который сохраняет события и при необходимости выполняет дополнительную пользовательскую логику.
    /// </summary>
    public RecordingExecutionObserver(
        Func<BatchFlowEvent<TCheckpoint>, CancellationToken, ValueTask>? onEventAsync = null)
    {
        _onEventAsync = onEventAsync;
    }

    /// <summary>
    /// Возвращает все зафиксированные события в порядке их получения.
    /// </summary>
    public IReadOnlyList<BatchFlowEvent<TCheckpoint>> Events => _events;

    /// <inheritdoc />
    public async ValueTask OnEventAsync(BatchFlowEvent<TCheckpoint> @event, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(@event);

        _events.Add(@event);

        if (_onEventAsync is not null)
        {
            await _onEventAsync(@event, cancellationToken);
        }
    }
}
