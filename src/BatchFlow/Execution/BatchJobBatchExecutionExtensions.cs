using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;
using BatchFlow.Builders;
using BatchFlow.Core.Execution.Models;

namespace BatchFlow;

/// <summary>
/// Предоставляет удобные методы запуска пакетной задачи в режиме batch-обработки.
/// </summary>
public static class BatchJobBatchExecutionExtensions
{
    /// <summary>
    /// Настраивает batch-обработчик и сразу запускает пакетную задачу.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="processor">Batch-обработчик.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunBatchAsync<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        IBatchProcessor<TItem, TCheckpoint> processor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(processor);

        return builder.ProcessBatch(processor).RunAsync(cancellationToken);
    }

    /// <summary>
    /// Настраивает batch-обработчик и сразу запускает пакетную задачу с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="processor">Batch-обработчик.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunBatchAsync<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        IBatchProcessor<TItem, TCheckpoint> processor,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(processor);
        ArgumentNullException.ThrowIfNull(options);

        return builder.ProcessBatch(processor).RunAsync(options, cancellationToken);
    }

    /// <summary>
    /// Настраивает batch-обработчик в виде делегата, который возвращает агрегированный итог обработки пакета,
    /// и сразу запускает пакетную задачу.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunBatchAsync<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);

        return builder.ProcessBatch(handler).RunAsync(cancellationToken);
    }

    /// <summary>
    /// Настраивает batch-обработчик в виде делегата, который возвращает агрегированный итог обработки пакета,
    /// и сразу запускает пакетную задачу с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunBatchAsync<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(options);

        return builder.ProcessBatch(handler).RunAsync(options, cancellationToken);
    }

    /// <summary>
    /// Настраивает batch-обработчик в виде делегата, для которого успешное завершение трактуется
    /// как успешная обработка всего пакета, и сразу запускает пакетную задачу.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunBatchAsync<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);

        return builder.ProcessBatch(handler).RunAsync(cancellationToken);
    }

    /// <summary>
    /// Настраивает batch-обработчик в виде делегата, для которого успешное завершение трактуется
    /// как успешная обработка всего пакета, и сразу запускает пакетную задачу с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunBatchAsync<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(options);

        return builder.ProcessBatch(handler).RunAsync(options, cancellationToken);
    }

    /// <summary>
    /// Настраивает batch-обработчик для источника с поддержкой контрольных точек и сразу запускает пакетную задачу.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="processor">Batch-обработчик.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunBatchAsync<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        IBatchProcessor<TItem, TCheckpoint> processor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(processor);

        return builder.ProcessBatch(processor).RunAsync(cancellationToken);
    }

    /// <summary>
    /// Настраивает batch-обработчик для источника с поддержкой контрольных точек и сразу запускает
    /// пакетную задачу с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="processor">Batch-обработчик.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunBatchAsync<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        IBatchProcessor<TItem, TCheckpoint> processor,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(processor);
        ArgumentNullException.ThrowIfNull(options);

        return builder.ProcessBatch(processor).RunAsync(options, cancellationToken);
    }

    /// <summary>
    /// Настраивает batch-обработчик в виде делегата для источника с поддержкой контрольных точек
    /// и сразу запускает пакетную задачу.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunBatchAsync<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);

        return builder.ProcessBatch(handler).RunAsync(cancellationToken);
    }

    /// <summary>
    /// Настраивает batch-обработчик в виде делегата для источника с поддержкой контрольных точек
    /// и сразу запускает пакетную задачу с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunBatchAsync<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(options);

        return builder.ProcessBatch(handler).RunAsync(options, cancellationToken);
    }

    /// <summary>
    /// Настраивает batch-обработчик в виде делегата для источника с поддержкой контрольных точек,
    /// для которого успешное завершение трактуется как успешная обработка всего пакета, и сразу запускает задачу.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunBatchAsync<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);

        return builder.ProcessBatch(handler).RunAsync(cancellationToken);
    }

    /// <summary>
    /// Настраивает batch-обработчик в виде делегата для источника с поддержкой контрольных точек,
    /// для которого успешное завершение трактуется как успешная обработка всего пакета,
    /// и сразу запускает задачу с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат обработки пакета.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunBatchAsync<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(options);

        return builder.ProcessBatch(handler).RunAsync(options, cancellationToken);
    }
}
