using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Failures;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;

namespace BatchFlow.Builders;

/// <summary>
/// Определяет fluent-стадию настройки пакетной задачи для источника, который может работать с контрольными точками.
/// </summary>
/// <typeparam name="TItem">Тип элементов, которые будет обрабатывать задача.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
public interface IResumableBatchJobBuilder<TItem, TCheckpoint>
{
    /// <summary>
    /// Задает желаемый размер пакета, который runtime будет запрашивать у источника данных.
    /// </summary>
    /// <param name="batchSize">Положительное количество элементов в пакете.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobBuilder<TItem, TCheckpoint> WithBatchSize(int batchSize);

    /// <summary>
    /// Задает степень параллелизма, с которой runtime должен обрабатывать пакеты или элементы.
    /// </summary>
    /// <param name="degreeOfParallelism">Положительное количество конкурентных обработчиков.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobBuilder<TItem, TCheckpoint> WithDegreeOfParallelism(int degreeOfParallelism);

    /// <summary>
    /// Задает емкость bounded queue между чтением и обработкой.
    /// Значение определяет, сколько пакетов runtime может заранее прочитать, не допуская неконтролируемого роста памяти.
    /// </summary>
    /// <param name="prefetchCapacity">Положительное количество пакетов, которые можно буферизовать перед обработкой.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobBuilder<TItem, TCheckpoint> WithPrefetchCapacity(int prefetchCapacity);

    /// <summary>
    /// Подключает стратегию обработки сбоев, которую runtime будет использовать для item- и batch-обработки.
    /// </summary>
    /// <param name="failurePolicy">Политика обработки ошибок.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobBuilder<TItem, TCheckpoint> WithFailurePolicy(IFailurePolicy<TItem, TCheckpoint> failurePolicy);

    /// <summary>
    /// Настраивает встроенную retry-модель runtime и заменяет ранее заданные built-in параметры повторных попыток.
    /// </summary>
    /// <param name="options">Параметры встроенной retry-стратегии.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobBuilder<TItem, TCheckpoint> WithRetryOptions(BatchRetryOptions options);

    /// <summary>
    /// Задает terminal-поведение встроенной модели обработки ошибок после исчерпания retry.
    /// </summary>
    /// <param name="terminalAction">Конечное действие runtime после неуспешной обработки.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobBuilder<TItem, TCheckpoint> WithTerminalFailureAction(BatchFailureTerminalAction terminalAction);

    /// <summary>
    /// Подключает наблюдателя runtime-событий и добавляет его в конец текущей цепочки observers.
    /// </summary>
    /// <param name="observer">Наблюдатель, который будет получать события выполнения задачи.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobBuilder<TItem, TCheckpoint> WithObserver(IExecutionObserver<TCheckpoint> observer);

    /// <summary>
    /// Подключает наблюдателя runtime-событий в виде делегата.
    /// </summary>
    /// <param name="handler">Делегат, который будет вызываться для каждого события выполнения.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobBuilder<TItem, TCheckpoint> WithObserver(
        Func<BatchFlowEvent<TCheckpoint>, CancellationToken, ValueTask> handler);

    /// <summary>
    /// Подключает несколько наблюдателей runtime-событий, сохраняя порядок их регистрации.
    /// </summary>
    /// <param name="observers">Наблюдатели, которые будут получать события выполнения задачи.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobBuilder<TItem, TCheckpoint> WithObservers(params IExecutionObserver<TCheckpoint>[] observers);

    /// <summary>
    /// Подключает хранилище контрольных точек, используя имя задачи как идентификатор потока checkpoint-данных.
    /// </summary>
    /// <param name="checkpointStore">Хранилище контрольных точек.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobBuilder<TItem, TCheckpoint> WithCheckpoint(ICheckpointStore<TCheckpoint> checkpointStore);

    /// <summary>
    /// Подключает хранилище контрольных точек и явно задает идентификатор потока checkpoint-данных.
    /// </summary>
    /// <param name="checkpointId">Стабильный идентификатор потока checkpoint-данных.</param>
    /// <param name="checkpointStore">Хранилище контрольных точек.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobBuilder<TItem, TCheckpoint> WithCheckpoint(
        string checkpointId,
        ICheckpointStore<TCheckpoint> checkpointStore);

    /// <summary>
    /// Выбирает поэлементный обработчик.
    /// </summary>
    /// <param name="processor">Реализация поэлементного обработчика.</param>
    /// <returns>Стадию builder, на которой задача уже готова к сборке.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> ProcessEach(IItemProcessor<TItem, TCheckpoint> processor);

    /// <summary>
    /// Выбирает поэлементный обработчик в виде делегата, который возвращает итог обработки элемента.
    /// </summary>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <returns>Стадию builder, на которой задача уже готова к сборке.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler);

    /// <summary>
    /// Выбирает поэлементный обработчик в виде делегата, для которого успешное завершение трактуется как успешная обработка элемента.
    /// </summary>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <returns>Стадию builder, на которой задача уже готова к сборке.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler);

    /// <summary>
    /// Выбирает batch-обработчик, который принимает пакет целиком.
    /// </summary>
    /// <param name="processor">Реализация batch-обработчика.</param>
    /// <returns>Стадию builder, на которой задача уже готова к сборке.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> ProcessBatch(IBatchProcessor<TItem, TCheckpoint> processor);

    /// <summary>
    /// Выбирает batch-обработчик в виде делегата, который возвращает агрегированный итог обработки пакета.
    /// </summary>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <returns>Стадию builder, на которой задача уже готова к сборке.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler);

    /// <summary>
    /// Выбирает batch-обработчик в виде делегата, для которого успешное завершение трактуется как успешная обработка всего пакета.
    /// </summary>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <returns>Стадию builder, на которой задача уже готова к сборке.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler);
}
