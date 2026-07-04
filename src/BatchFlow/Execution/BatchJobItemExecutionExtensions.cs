using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;
using BatchFlow.Builders;
using BatchFlow.Core.Execution.Models;

namespace BatchFlow;

/// <summary>
/// Предоставляет удобные методы запуска пакетной задачи в поэлементном режиме обработки.
/// </summary>
public static class BatchJobItemExecutionExtensions
{
    /// <summary>
    /// Настраивает поэлементный обработчик и сразу запускает пакетную задачу.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="processor">Поэлементный обработчик.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunEachAsync<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        IItemProcessor<TItem, TCheckpoint> processor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(processor);

        return builder.ProcessEach(processor).RunAsync(cancellationToken);
    }

    /// <summary>
    /// Настраивает поэлементный обработчик и сразу запускает пакетную задачу с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="processor">Поэлементный обработчик.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunEachAsync<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        IItemProcessor<TItem, TCheckpoint> processor,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(processor);
        ArgumentNullException.ThrowIfNull(options);

        return builder.ProcessEach(processor).RunAsync(options, cancellationToken);
    }

    /// <summary>
    /// Настраивает поэлементный обработчик в виде делегата, который возвращает итог обработки элемента,
    /// и сразу запускает пакетную задачу.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunEachAsync<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);

        return builder.ProcessEach(handler).RunAsync(cancellationToken);
    }

    /// <summary>
    /// Настраивает поэлементный обработчик в виде делегата, который возвращает итог обработки элемента,
    /// и сразу запускает пакетную задачу с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunEachAsync<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(options);

        return builder.ProcessEach(handler).RunAsync(options, cancellationToken);
    }

    /// <summary>
    /// Настраивает поэлементный обработчик в виде делегата, для которого успешное завершение
    /// трактуется как успешная обработка элемента, и сразу запускает пакетную задачу.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunEachAsync<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);

        return builder.ProcessEach(handler).RunAsync(cancellationToken);
    }

    /// <summary>
    /// Настраивает поэлементный обработчик в виде делегата, для которого успешное завершение
    /// трактуется как успешная обработка элемента, и сразу запускает пакетную задачу
    /// с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunEachAsync<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(options);

        return builder.ProcessEach(handler).RunAsync(options, cancellationToken);
    }

    /// <summary>
    /// Настраивает поэлементный обработчик для источника с поддержкой контрольных точек и сразу запускает задачу.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="processor">Поэлементный обработчик.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunEachAsync<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        IItemProcessor<TItem, TCheckpoint> processor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(processor);

        return builder.ProcessEach(processor).RunAsync(cancellationToken);
    }

    /// <summary>
    /// Настраивает поэлементный обработчик для источника с поддержкой контрольных точек и сразу запускает задачу
    /// с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="processor">Поэлементный обработчик.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunEachAsync<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        IItemProcessor<TItem, TCheckpoint> processor,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(processor);
        ArgumentNullException.ThrowIfNull(options);

        return builder.ProcessEach(processor).RunAsync(options, cancellationToken);
    }

    /// <summary>
    /// Настраивает поэлементный обработчик в виде делегата для источника с поддержкой контрольных точек
    /// и сразу запускает задачу.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunEachAsync<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);

        return builder.ProcessEach(handler).RunAsync(cancellationToken);
    }

    /// <summary>
    /// Настраивает поэлементный обработчик в виде делегата для источника с поддержкой контрольных точек
    /// и сразу запускает задачу с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunEachAsync<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(options);

        return builder.ProcessEach(handler).RunAsync(options, cancellationToken);
    }

    /// <summary>
    /// Настраивает поэлементный обработчик в виде делегата для источника с поддержкой контрольных точек,
    /// для которого успешное завершение трактуется как успешная обработка элемента, и сразу запускает задачу.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunEachAsync<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);

        return builder.ProcessEach(handler).RunAsync(cancellationToken);
    }

    /// <summary>
    /// Настраивает поэлементный обработчик в виде делегата для источника с поддержкой контрольных точек,
    /// для которого успешное завершение трактуется как успешная обработка элемента, и сразу запускает задачу
    /// с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Fluent-цепочка настройки задачи до выбора способа обработки.</param>
    /// <param name="handler">Делегат поэлементной обработки.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunEachAsync<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(options);

        return builder.ProcessEach(handler).RunAsync(options, cancellationToken);
    }
}
