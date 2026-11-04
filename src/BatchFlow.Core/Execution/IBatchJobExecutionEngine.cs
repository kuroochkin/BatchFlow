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
    /// <remarks>
    /// После проверки конфигурации внешняя отмена возвращается как статус Cancelled в summary.
    /// Перед возвратом движок ожидает producer и всех обработчиков; игнорирование токена может задержать остановку.
    /// Первичная ошибка обработки сохраняется при внутренней отмене остальных workers.
    /// Завершающее событие получает независимый от рабочей отмены токен; ошибки его наблюдателей не меняют итог.
    /// </remarks>
    ValueTask<BatchJobExecutionSummary<TCheckpoint>> ExecuteAsync<TItem, TCheckpoint>(
        BatchJobExecutionRequest<TItem, TCheckpoint> request,
        CancellationToken cancellationToken = default);
}
