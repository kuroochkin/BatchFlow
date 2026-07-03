using BatchFlow.Builders;
using BatchFlow.Core.Execution;
using BatchFlow.Core.Execution.Models;
using BatchFlow.Core.Jobs;

namespace BatchFlow;

/// <summary>
/// Предоставляет удобные методы запуска пакетной задачи без явного создания runtime-движка.
/// </summary>
public static class BatchJobExecutionExtensions
{
    /// <summary>
    /// Собирает и запускает пакетную задачу, используя стандартный runtime-движок.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Сконфигурированная fluent-цепочка, готовая к выполнению.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunAsync<TItem, TCheckpoint>(
        this IBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return ExecuteAsync(builder.Build(), options: null, cancellationToken);
    }

    /// <summary>
    /// Собирает и запускает пакетную задачу с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Сконфигурированная fluent-цепочка, готовая к выполнению.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunAsync<TItem, TCheckpoint>(
        this IBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);
        return ExecuteAsync(builder.Build(), options, cancellationToken);
    }

    /// <summary>
    /// Собирает и запускает пакетную задачу для источника с поддержкой контрольных точек,
    /// используя стандартный runtime-движок.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Сконфигурированная fluent-цепочка, готовая к выполнению.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunAsync<TItem, TCheckpoint>(
        this IResumableBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return ExecuteAsync(builder.Build(), options: null, cancellationToken);
    }

    /// <summary>
    /// Собирает и запускает пакетную задачу для источника с поддержкой контрольных точек
    /// с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="builder">Сконфигурированная fluent-цепочка, готовая к выполнению.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunAsync<TItem, TCheckpoint>(
        this IResumableBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);
        return ExecuteAsync(builder.Build(), options, cancellationToken);
    }

    /// <summary>
    /// Запускает уже собранное описание пакетной задачи, используя стандартный runtime-движок.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="job">Готовое immutable-описание задачи.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunAsync<TItem, TCheckpoint>(
        this BatchJobDefinition<TItem, TCheckpoint> job,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);
        return ExecuteAsync(job, options: null, cancellationToken);
    }

    /// <summary>
    /// Запускает уже собранное описание пакетной задачи с дополнительными параметрами запуска.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных.</typeparam>
    /// <param name="job">Готовое immutable-описание задачи.</param>
    /// <param name="options">Дополнительные параметры запуска.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Итог выполнения пакетной задачи.</returns>
    public static ValueTask<BatchJobExecutionSummary<TCheckpoint>> RunAsync<TItem, TCheckpoint>(
        this BatchJobDefinition<TItem, TCheckpoint> job,
        BatchJobRunOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(options);
        return ExecuteAsync(job, options, cancellationToken);
    }

    private static ValueTask<BatchJobExecutionSummary<TCheckpoint>> ExecuteAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        BatchJobRunOptions? options,
        CancellationToken cancellationToken)
    {
        if (options is not null)
        {
            ArgumentNullException.ThrowIfNull(options.TimeProvider);
        }

        var executionEngine = options?.ExecutionEngine ?? new BatchJobExecutionEngine();

        return executionEngine.ExecuteAsync(
            new BatchJobExecutionRequest<TItem, TCheckpoint>
            {
                Job = job,
                RunId = options?.RunId,
                TimeProvider = options?.TimeProvider ?? TimeProvider.System,
            },
            cancellationToken);
    }
}
