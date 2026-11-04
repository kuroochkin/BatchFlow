using BatchFlow.Abstractions.Observability;
using BatchFlow.Core.Execution.Exceptions;

namespace BatchFlow.Core.Execution;

/// <summary>
/// Последовательно доставляет события одного запуска, в том числе от параллельных обработчиков.
/// Финальное уведомление не меняет выбранный исход и доставляется остальным наблюдателям даже при ошибке одного из них.
/// </summary>
internal sealed class ExecutionObserverDispatcher<TCheckpoint>(IReadOnlyList<IExecutionObserver<TCheckpoint>> observers, TimeProvider timeProvider)
    : IExecutionObserver<TCheckpoint>, IDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);

    public async ValueTask OnEventAsync(BatchFlowEvent<TCheckpoint> @event, CancellationToken cancellationToken)
    {
        if (@event is ProcessingRetryEvent<TCheckpoint> or ProcessingSkippedEvent<TCheckpoint>)
        {
            @event = @event with { OccurredAt = timeProvider.GetUtcNow() };
        }
        await _gate.WaitAsync(cancellationToken);

        try
        {
            foreach (var observer in observers)
            {
                try
                {
                    await observer.OnEventAsync(@event, cancellationToken);
                }
                catch (Exception) when (@event is JobCompletedEvent<TCheckpoint>)
                {
                    // Финальная диагностика не должна подменять итог или мешать другим наблюдателям.
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new BatchJobObserverException(observer.GetType(), @event.GetType(), ex);
                }
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose() => _gate.Dispose();
}
