using BatchFlow.Builders;

namespace BatchFlow;

/// <summary>
/// Предоставляет fluent-настройку оведения встроенной модели обработки ошибок.
/// </summary>
public static class BatchJobTerminalFailureBuilderExtensions
{
    /// <summary>
    /// Настраивает runtime так, чтобы после окончательной неудачи обработки проблемный item или batch пропускался, а job продолжала выполнение.
    /// </summary>
    public static IBatchJobBuilder<TItem, TCheckpoint> WithSkipOnFailure<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithTerminalFailureAction(BatchFailureTerminalAction.Skip);
    }

    /// <summary>
    /// Настраивает runtime так, чтобы после окончательной неудачи обработки проблемный item или batch пропускался, а job продолжала выполнение.
    /// </summary>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> WithSkipOnFailure<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithTerminalFailureAction(BatchFailureTerminalAction.Skip);
    }

    /// <summary>
    /// Настраивает runtime так, чтобы после окончательной неудачи обработки проблемный item или batch пропускался, а job продолжала выполнение.
    /// </summary>
    public static IBatchJobReadyBuilder<TItem, TCheckpoint> WithSkipOnFailure<TItem, TCheckpoint>(
        this IBatchJobReadyBuilder<TItem, TCheckpoint> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithTerminalFailureAction(BatchFailureTerminalAction.Skip);
    }

    /// <summary>
    /// Настраивает runtime так, чтобы после окончательной неудачи обработки проблемный item или batch пропускался, а job продолжала выполнение.
    /// </summary>
    public static IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithSkipOnFailure<TItem, TCheckpoint>(
        this IResumableBatchJobReadyBuilder<TItem, TCheckpoint> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithTerminalFailureAction(BatchFailureTerminalAction.Skip);
    }

    /// <summary>
    /// Настраивает runtime так, чтобы после окончательной неудачи обработки выполнение job завершалось ошибкой.
    /// </summary>
    public static IBatchJobBuilder<TItem, TCheckpoint> WithStopOnFailure<TItem, TCheckpoint>(
        this IBatchJobBuilder<TItem, TCheckpoint> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithTerminalFailureAction(BatchFailureTerminalAction.Stop);
    }

    /// <summary>
    /// Настраивает runtime так, чтобы после окончательной неудачи обработки выполнение job завершалось ошибкой.
    /// </summary>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> WithStopOnFailure<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithTerminalFailureAction(BatchFailureTerminalAction.Stop);
    }

    /// <summary>
    /// Настраивает runtime так, чтобы после окончательной неудачи обработки выполнение job завершалось ошибкой.
    /// </summary>
    public static IBatchJobReadyBuilder<TItem, TCheckpoint> WithStopOnFailure<TItem, TCheckpoint>(
        this IBatchJobReadyBuilder<TItem, TCheckpoint> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithTerminalFailureAction(BatchFailureTerminalAction.Stop);
    }

    /// <summary>
    /// Настраивает runtime так, чтобы после окончательной неудачи обработки выполнение job завершалось ошибкой.
    /// </summary>
    public static IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithStopOnFailure<TItem, TCheckpoint>(
        this IResumableBatchJobReadyBuilder<TItem, TCheckpoint> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.WithTerminalFailureAction(BatchFailureTerminalAction.Stop);
    }
}
