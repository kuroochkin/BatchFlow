using BatchFlow.Abstractions.Observability;
using BatchFlow.Builders;
using BatchFlow.Core.Jobs;
using Microsoft.Extensions.Logging;

namespace BatchFlow.Extensions.Logging;

/// <summary>
/// Содержит fluent-расширения для подключения ILogger-адаптера к runtime-событиям BatchFlow.
/// </summary>
public static class BatchFlowProgressLoggingBuilderExtensions
{
    /// <summary>
    /// Подключает logging adapter к builder-стадии до выбора обработчика.
    /// </summary>
    public static IBatchJobBuilder<TItem, TCheckpoint> WithProgressLogging<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        ILogger logger,
        Action<BatchFlowProgressLoggingOptions<TCheckpoint>>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithObserver(CreateObserver(logger, configure));
    }

    /// <summary>
    /// Подключает logging adapter к builder-стадии до выбора обработчика для resumable source.
    /// </summary>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> WithProgressLogging<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        ILogger logger,
        Action<BatchFlowProgressLoggingOptions<TCheckpoint>>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithObserver(CreateObserver(logger, configure));
    }

    /// <summary>
    /// Подключает logging adapter к builder-стадии после выбора обработчика.
    /// </summary>
    public static IBatchJobReadyBuilder<TItem, TCheckpoint> WithProgressLogging<TItem, TCheckpoint>(
        this IBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        ILogger logger,
        Action<BatchFlowProgressLoggingOptions<TCheckpoint>>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithObserver(CreateObserver(logger, configure));
    }

    /// <summary>
    /// Подключает logging adapter к builder-стадии после выбора обработчика для resumable source.
    /// </summary>
    public static IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithProgressLogging<TItem, TCheckpoint>(
        this IResumableBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        ILogger logger,
        Action<BatchFlowProgressLoggingOptions<TCheckpoint>>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithObserver(CreateObserver(logger, configure));
    }

    /// <summary>
    /// Возвращает копию immutable job definition с подключенным logging observer-ом.
    /// </summary>
    public static BatchJobDefinition<TItem, TCheckpoint> WithProgressLogging<TItem, TCheckpoint>(
        this BatchJobDefinition<TItem, TCheckpoint> job,
        ILogger logger,
        Action<BatchFlowProgressLoggingOptions<TCheckpoint>>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(job);

        IExecutionObserver<TCheckpoint> observer = CreateObserver(logger, configure);

        return job with
        {
            Observers = [.. job.Observers, observer],
        };
    }

    private static BatchFlowProgressLoggerObserver<TCheckpoint> CreateObserver<TCheckpoint>(
        ILogger logger,
        Action<BatchFlowProgressLoggingOptions<TCheckpoint>>? configure)
    {
        ArgumentNullException.ThrowIfNull(logger);

        var options = new BatchFlowProgressLoggingOptions<TCheckpoint>();
        configure?.Invoke(options);

        return new BatchFlowProgressLoggerObserver<TCheckpoint>(logger, options);
    }
}
