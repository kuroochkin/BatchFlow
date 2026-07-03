using BatchFlow.Abstractions.Failures;
using BatchFlow.Builders;

namespace BatchFlow;

/// <summary>
/// Предоставляет fluent-настройку встроенной retry-стратегии для batch job.
/// </summary>
public static class BatchJobRetryBuilderExtensions
{
    /// <summary>
    /// Подключает встроенную retry-стратегию с заданным количеством повторных попыток и нулевой задержкой.
    /// </summary>
    public static IBatchJobBuilder<TItem, TCheckpoint> WithRetry<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        int maxRetryAttempts) =>
        builder.WithRetry(new BatchRetryOptions
        {
            MaxRetryAttempts = maxRetryAttempts,
        });

    /// <summary>
    /// Подключает встроенную retry-стратегию с заданным количеством повторных попыток и фиксированной задержкой.
    /// </summary>
    public static IBatchJobBuilder<TItem, TCheckpoint> WithRetry<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        int maxRetryAttempts,
        TimeSpan delay) =>
        builder.WithRetry(new BatchRetryOptions
        {
            MaxRetryAttempts = maxRetryAttempts,
            Delay = delay,
        });

    /// <summary>
    /// Подключает встроенную retry-стратегию с детальной настройкой параметров.
    /// </summary>
    public static IBatchJobBuilder<TItem, TCheckpoint> WithRetry<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder,
        BatchRetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);

        return builder.WithFailurePolicy(CreateFailurePolicy<TItem, TCheckpoint>(options));
    }

    /// <summary>
    /// Подключает встроенную retry-стратегию с заданным количеством повторных попыток и нулевой задержкой.
    /// </summary>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> WithRetry<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        int maxRetryAttempts) =>
        builder.WithRetry(new BatchRetryOptions
        {
            MaxRetryAttempts = maxRetryAttempts,
        });

    /// <summary>
    /// Подключает встроенную retry-стратегию с заданным количеством повторных попыток и фиксированной задержкой.
    /// </summary>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> WithRetry<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        int maxRetryAttempts,
        TimeSpan delay) =>
        builder.WithRetry(new BatchRetryOptions
        {
            MaxRetryAttempts = maxRetryAttempts,
            Delay = delay,
        });

    /// <summary>
    /// Подключает встроенную retry-стратегию с детальной настройкой параметров.
    /// </summary>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> WithRetry<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        BatchRetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);

        return builder.WithFailurePolicy(CreateFailurePolicy<TItem, TCheckpoint>(options));
    }

    /// <summary>
    /// Подключает встроенную retry-стратегию с заданным количеством повторных попыток и нулевой задержкой.
    /// </summary>
    public static IBatchJobReadyBuilder<TItem, TCheckpoint> WithRetry<TItem, TCheckpoint>(
        this IBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        int maxRetryAttempts) =>
        builder.WithRetry(new BatchRetryOptions
        {
            MaxRetryAttempts = maxRetryAttempts,
        });

    /// <summary>
    /// Подключает встроенную retry-стратегию с заданным количеством повторных попыток и фиксированной задержкой.
    /// </summary>
    public static IBatchJobReadyBuilder<TItem, TCheckpoint> WithRetry<TItem, TCheckpoint>(
        this IBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        int maxRetryAttempts,
        TimeSpan delay) =>
        builder.WithRetry(new BatchRetryOptions
        {
            MaxRetryAttempts = maxRetryAttempts,
            Delay = delay,
        });

    /// <summary>
    /// Подключает встроенную retry-стратегию с детальной настройкой параметров.
    /// </summary>
    public static IBatchJobReadyBuilder<TItem, TCheckpoint> WithRetry<TItem, TCheckpoint>(
        this IBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        BatchRetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);

        return builder.WithFailurePolicy(CreateFailurePolicy<TItem, TCheckpoint>(options));
    }

    /// <summary>
    /// Подключает встроенную retry-стратегию с заданным количеством повторных попыток и нулевой задержкой.
    /// </summary>
    public static IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithRetry<TItem, TCheckpoint>(
        this IResumableBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        int maxRetryAttempts) =>
        builder.WithRetry(new BatchRetryOptions
        {
            MaxRetryAttempts = maxRetryAttempts,
        });

    /// <summary>
    /// Подключает встроенную retry-стратегию с заданным количеством повторных попыток и фиксированной задержкой.
    /// </summary>
    public static IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithRetry<TItem, TCheckpoint>(
        this IResumableBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        int maxRetryAttempts,
        TimeSpan delay) =>
        builder.WithRetry(new BatchRetryOptions
        {
            MaxRetryAttempts = maxRetryAttempts,
            Delay = delay,
        });

    /// <summary>
    /// Подключает встроенную retry-стратегию с детальной настройкой параметров.
    /// </summary>
    public static IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithRetry<TItem, TCheckpoint>(
        this IResumableBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        BatchRetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(options);

        return builder.WithFailurePolicy(CreateFailurePolicy<TItem, TCheckpoint>(options));
    }

    private static IFailurePolicy<TItem, TCheckpoint> CreateFailurePolicy<TItem, TCheckpoint>(BatchRetryOptions options) =>
        new FixedRetryFailurePolicy<TItem, TCheckpoint>(options);
}
