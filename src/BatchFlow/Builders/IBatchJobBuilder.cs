using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Failures;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;

namespace BatchFlow.Builders;

/// <summary>
/// Определяет fluent-стадию настройки пакетной задачи до выбора конкретного способа обработки.
/// </summary>
/// <typeparam name="TItem">Тип элементов, которые будет обрабатывать задача.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
public interface IBatchJobBuilder<TItem, TCheckpoint>
{
    /// <summary>
    /// Задает желаемый размер пакета, который runtime будет запрашивать у источника данных.
    /// </summary>
    /// <param name="batchSize">Положительное количество элементов в пакете.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IBatchJobBuilder<TItem, TCheckpoint> WithBatchSize(int batchSize);

    /// <summary>
    /// Задает степень параллелизма, с которой runtime должен обрабатывать пакеты или элементы.
    /// </summary>
    /// <param name="degreeOfParallelism">Положительное количество конкурентных обработчиков.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IBatchJobBuilder<TItem, TCheckpoint> WithDegreeOfParallelism(int degreeOfParallelism);

    /// <summary>
    /// Подключает стратегию обработки сбоев, которую runtime будет использовать для item- и batch-обработки.
    /// </summary>
    /// <param name="failurePolicy">Политика обработки ошибок.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    IBatchJobBuilder<TItem, TCheckpoint> WithFailurePolicy(IFailurePolicy<TItem, TCheckpoint> failurePolicy);

    /// <summary>
    /// Выбирает поэлементный обработчик.
    /// </summary>
    /// <param name="processor">Реализация поэлементного обработчика.</param>
    /// <returns>Стадию builder, на которой задача уже готова к сборке.</returns>
    IBatchJobReadyBuilder<TItem, TCheckpoint> ProcessEach(IItemProcessor<TItem, TCheckpoint> processor);

    /// <summary>
    /// Выбирает поэлементный обработчик в виде делегата, который возвращает итог обработки элемента.
    /// </summary>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <returns>Стадию builder, на которой задача уже готова к сборке.</returns>
    IBatchJobReadyBuilder<TItem, TCheckpoint> ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler);

    /// <summary>
    /// Выбирает поэлементный обработчик в виде делегата, для которого успешное завершение трактуется как успешная обработка элемента.
    /// </summary>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <returns>Стадию builder, на которой задача уже готова к сборке.</returns>
    IBatchJobReadyBuilder<TItem, TCheckpoint> ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler);

    /// <summary>
    /// Выбирает batch-обработчик, который принимает пакет целиком.
    /// </summary>
    /// <param name="processor">Реализация batch-обработчика.</param>
    /// <returns>Стадию builder, на которой задача уже готова к сборке.</returns>
    IBatchJobReadyBuilder<TItem, TCheckpoint> ProcessBatch(IBatchProcessor<TItem, TCheckpoint> processor);

    /// <summary>
    /// Выбирает batch-обработчик в виде делегата, который возвращает агрегированный итог обработки пакета.
    /// </summary>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <returns>Стадию builder, на которой задача уже готова к сборке.</returns>
    IBatchJobReadyBuilder<TItem, TCheckpoint> ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler);

    /// <summary>
    /// Выбирает batch-обработчик в виде делегата, для которого успешное завершение трактуется как успешная обработка всего пакета.
    /// </summary>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <returns>Стадию builder, на которой задача уже готова к сборке.</returns>
    IBatchJobReadyBuilder<TItem, TCheckpoint> ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler);
}