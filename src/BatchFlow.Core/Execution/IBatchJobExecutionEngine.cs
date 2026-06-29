using BatchFlow.Core.Execution.Models;

namespace BatchFlow.Core.Execution;

/// <summary>
/// Определяет движок выполнения пакетных задач.
/// </summary>
public interface IBatchJobExecutionEngine
{
    /// <summary>
    /// Выполняет пакетную задачу по переданному запросу и возвращает итог выполнения.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, используемой источником данных.</typeparam>
    ValueTask<BatchJobExecutionSummary<TCheckpoint>> ExecuteAsync<TItem, TCheckpoint>(
        BatchJobExecutionRequest<TItem, TCheckpoint> request,
        CancellationToken cancellationToken = default);
}
