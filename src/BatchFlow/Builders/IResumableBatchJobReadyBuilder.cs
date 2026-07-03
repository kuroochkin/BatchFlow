using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Failures;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;
using BatchFlow.Core.Jobs;

namespace BatchFlow.Builders;

/// <summary>
/// Определяет fluent-стадию настройки пакетной задачи после выбора способа обработки для источника, работающего с контрольными точками.
/// </summary>
/// <typeparam name="TItem">Тип элементов, которые будет обрабатывать задача.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
public interface IResumableBatchJobReadyBuilder<TItem, TCheckpoint>
{
    /// <summary>
    /// Задает желаемый размер пакета, который runtime будет запрашивать у источника данных.
    /// </summary>
    /// <param name="batchSize">Положительное количество элементов в пакете.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithBatchSize(int batchSize);

    /// <summary>
    /// Задает степень параллелизма, с которой runtime должен обрабатывать пакеты или элементы.
    /// </summary>
    /// <param name="degreeOfParallelism">Положительное количество конкурентных обработчиков.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithDegreeOfParallelism(int degreeOfParallelism);

    /// <summary>
    /// Подключает стратегию обработки сбоев и заменяет ранее настроенную политику, если она уже была задана.
    /// </summary>
    /// <param name="failurePolicy">Политика обработки ошибок.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithFailurePolicy(IFailurePolicy<TItem, TCheckpoint> failurePolicy);

    /// <summary>
    /// Подключает хранилище контрольных точек, используя имя задачи как идентификатор потока checkpoint-данных.
    /// </summary>
    /// <param name="checkpointStore">Хранилище контрольных точек.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithCheckpoint(ICheckpointStore<TCheckpoint> checkpointStore);

    /// <summary>
    /// Подключает хранилище контрольных точек и явно задает идентификатор потока checkpoint-данных.
    /// </summary>
    /// <param name="checkpointId">Стабильный идентификатор потока checkpoint-данных.</param>
    /// <param name="checkpointStore">Хранилище контрольных точек.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithCheckpoint(
        string checkpointId,
        ICheckpointStore<TCheckpoint> checkpointStore);

    /// <summary>
    /// Выбирает поэлементный обработчик и заменяет ранее настроенный способ обработки, если он уже был задан.
    /// </summary>
    /// <param name="processor">Реализация поэлементного обработчика.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> ProcessEach(IItemProcessor<TItem, TCheckpoint> processor);

    /// <summary>
    /// Выбирает поэлементный обработчик в виде делегата и заменяет ранее настроенный способ обработки, если он уже был задан.
    /// </summary>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler);

    /// <summary>
    /// Выбирает поэлементный обработчик в виде делегата, для которого успешное завершение трактуется как успешная обработка элемента.
    /// </summary>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler);

    /// <summary>
    /// Выбирает batch-обработчик и заменяет ранее настроенный способ обработки, если он уже был задан.
    /// </summary>
    /// <param name="processor">Реализация batch-обработчика.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> ProcessBatch(IBatchProcessor<TItem, TCheckpoint> processor);

    /// <summary>
    /// Выбирает batch-обработчик в виде делегата и заменяет ранее настроенный способ обработки, если он уже был задан.
    /// </summary>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler);

    /// <summary>
    /// Выбирает batch-обработчик в виде делегата, для которого успешное завершение трактуется как успешная обработка всего пакета.
    /// </summary>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler);

    /// <summary>
    /// Собирает immutable-описание пакетной задачи.
    /// </summary>
    /// <returns>Готовое описание пакетной задачи для передачи в runtime.</returns>
    BatchJobDefinition<TItem, TCheckpoint> Build();
}
