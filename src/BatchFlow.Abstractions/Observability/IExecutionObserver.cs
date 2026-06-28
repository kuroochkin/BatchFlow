namespace BatchFlow.Abstractions.Observability;

/// <summary>
/// Определяет наблюдателя за событиями, возникающими во время выполнения пакетной задачи.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с наблюдаемой задачей.</typeparam>
public interface IExecutionObserver<TCheckpoint>
{
    /// <summary>
    /// Обрабатывает событие, опубликованное движком выполнения.
    /// </summary>
    ValueTask OnEventAsync(BatchFlowEvent<TCheckpoint> @event, CancellationToken cancellationToken);
}
