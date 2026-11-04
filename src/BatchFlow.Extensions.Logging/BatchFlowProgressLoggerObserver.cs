using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Observability;
using Microsoft.Extensions.Logging;

namespace BatchFlow.Extensions.Logging;

/// <summary>
/// Адаптирует runtime-события BatchFlow в структурированные записи <see cref="ILogger"/>.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с выполняемой задачей.</typeparam>
public sealed class BatchFlowProgressLoggerObserver<TCheckpoint> : IExecutionObserver<TCheckpoint>
{
    private readonly ILogger _logger;
    private readonly BatchFlowProgressLoggingOptions<TCheckpoint> _options;

    /// <summary>
    /// Создает observer, который будет писать runtime-события BatchFlow в <see cref="ILogger"/>.
    /// </summary>
    /// <param name="logger">Логгер, в который нужно отправлять события выполнения.</param>
    /// <param name="options">Настройки отображения runtime-событий в логах.</param>
    public BatchFlowProgressLoggerObserver(
        ILogger logger,
        BatchFlowProgressLoggingOptions<TCheckpoint>? options = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = options ?? new BatchFlowProgressLoggingOptions<TCheckpoint>();
    }

    /// <inheritdoc />
    public ValueTask OnEventAsync(BatchFlowEvent<TCheckpoint> @event, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(@event);

        switch (@event)
        {
            case JobStartedEvent<TCheckpoint> jobStarted:
                LogJobStarted(jobStarted);
                break;

            case BatchStartedEvent<TCheckpoint> batchStarted:
                LogBatchStarted(batchStarted);
                break;

            case BatchCompletedEvent<TCheckpoint> batchCompleted:
                LogBatchCompleted(batchCompleted);
                break;

            case BatchFailedEvent<TCheckpoint> batchFailed:
                LogBatchFailed(batchFailed);
                break;

            case ProcessingRetryEvent<TCheckpoint> retry:
                LogRetry(retry);
                break;

            case ProcessingSkippedEvent<TCheckpoint> skipped:
                LogSkipped(skipped);
                break;

            case CheckpointSavedEvent<TCheckpoint> checkpointSaved:
                LogCheckpointSaved(checkpointSaved);
                break;

            case JobCompletedEvent<TCheckpoint> jobCompleted:
                LogJobCompleted(jobCompleted);
                break;
        }

        return ValueTask.CompletedTask;
    }

    private void LogJobStarted(JobStartedEvent<TCheckpoint> @event)
    {
        var level = _options.JobStartedLogLevel;

        if (!ShouldLog(level))
        {
            return;
        }

        if (_options.IncludeCheckpointValue && @event.ResumeFrom is not null)
        {
            _logger.Log(
                level,
                BatchFlowLoggingEventIds.JobStarted,
                "Запуск batch job {JobName}. RunId={RunId}, BatchSize={BatchSize}, DegreeOfParallelism={DegreeOfParallelism}, PrefetchCapacity={PrefetchCapacity}, ResumeSequence={ResumeSequence}, ResumeCheckpoint={ResumeCheckpoint}",
                @event.Job.JobName,
                @event.Job.RunId,
                @event.Job.BatchSize,
                @event.Job.DegreeOfParallelism,
                @event.Job.PrefetchCapacity,
                @event.ResumeFrom.SequenceNumber,
                FormatCheckpoint(@event.ResumeFrom.Value));

            return;
        }

        _logger.Log(
            level,
            BatchFlowLoggingEventIds.JobStarted,
            "Запуск batch job {JobName}. RunId={RunId}, BatchSize={BatchSize}, DegreeOfParallelism={DegreeOfParallelism}, PrefetchCapacity={PrefetchCapacity}, ResumeSequence={ResumeSequence}",
            @event.Job.JobName,
            @event.Job.RunId,
            @event.Job.BatchSize,
            @event.Job.DegreeOfParallelism,
            @event.Job.PrefetchCapacity,
            @event.ResumeFrom?.SequenceNumber);
    }

    private void LogBatchStarted(BatchStartedEvent<TCheckpoint> @event)
    {
        var level = _options.BatchStartedLogLevel;

        if (!ShouldLog(level))
        {
            return;
        }

        if (_options.IncludeSourceMetadata)
        {
            _logger.Log(
                level,
                BatchFlowLoggingEventIds.BatchStarted,
                "Старт batch-а для job {JobName}. RunId={RunId}, BatchSequence={BatchSequence}, ItemCount={ItemCount}, Attempt={Attempt}, SourceMetadata={SourceMetadata}",
                @event.Job.JobName,
                @event.Job.RunId,
                @event.BatchSequence,
                @event.ItemCount,
                @event.Attempt,
                FormatSourceMetadata(@event.SourceMetadata));

            return;
        }

        _logger.Log(
            level,
            BatchFlowLoggingEventIds.BatchStarted,
            "Старт batch-а для job {JobName}. RunId={RunId}, BatchSequence={BatchSequence}, ItemCount={ItemCount}, Attempt={Attempt}",
            @event.Job.JobName,
            @event.Job.RunId,
            @event.BatchSequence,
            @event.ItemCount,
            @event.Attempt);
    }

    private void LogBatchCompleted(BatchCompletedEvent<TCheckpoint> @event)
    {
        var level = _options.BatchCompletedLogLevel;

        if (!ShouldLog(level))
        {
            return;
        }

        if (_options.IncludeSourceMetadata)
        {
            _logger.Log(
                level,
                BatchFlowLoggingEventIds.BatchCompleted,
                "Завершен batch для job {JobName}. RunId={RunId}, BatchSequence={BatchSequence}, ItemCount={ItemCount}, SucceededCount={SucceededCount}, SkippedCount={SkippedCount}, Attempt={Attempt}, SourceMetadata={SourceMetadata}",
                @event.Job.JobName,
                @event.Job.RunId,
                @event.BatchSequence,
                @event.ItemCount,
                @event.SucceededCount,
                @event.SkippedCount,
                @event.Attempt,
                FormatSourceMetadata(@event.SourceMetadata));

            return;
        }

        _logger.Log(
            level,
            BatchFlowLoggingEventIds.BatchCompleted,
            "Завершен batch для job {JobName}. RunId={RunId}, BatchSequence={BatchSequence}, ItemCount={ItemCount}, SucceededCount={SucceededCount}, SkippedCount={SkippedCount}, Attempt={Attempt}",
            @event.Job.JobName,
            @event.Job.RunId,
            @event.BatchSequence,
            @event.ItemCount,
            @event.SucceededCount,
            @event.SkippedCount,
            @event.Attempt);
    }

    private void LogBatchFailed(BatchFailedEvent<TCheckpoint> @event)
    {
        var level = _options.BatchFailedLogLevel;

        if (!ShouldLog(level))
        {
            return;
        }

        if (_options.IncludeSourceMetadata)
        {
            _logger.Log(
                level,
                BatchFlowLoggingEventIds.BatchFailed,
                @event.Exception,
                "Ошибка batch-а для job {JobName}. RunId={RunId}, BatchSequence={BatchSequence}, ItemCount={ItemCount}, Attempt={Attempt}, ItemIndex={ItemIndex}, SourceMetadata={SourceMetadata}",
                @event.Job.JobName,
                @event.Job.RunId,
                @event.BatchSequence,
                @event.ItemCount,
                @event.Attempt,
                @event.ItemIndex,
                FormatSourceMetadata(@event.SourceMetadata));

            return;
        }

        _logger.Log(
            level,
            BatchFlowLoggingEventIds.BatchFailed,
            @event.Exception,
            "Ошибка batch-а для job {JobName}. RunId={RunId}, BatchSequence={BatchSequence}, ItemCount={ItemCount}, Attempt={Attempt}, ItemIndex={ItemIndex}",
            @event.Job.JobName,
            @event.Job.RunId,
            @event.BatchSequence,
            @event.ItemCount,
            @event.Attempt,
            @event.ItemIndex);
    }

    private void LogCheckpointSaved(CheckpointSavedEvent<TCheckpoint> @event)
    {
        var level = _options.CheckpointSavedLogLevel;

        if (!ShouldLog(level))
        {
            return;
        }

        if (_options.IncludeCheckpointValue)
        {
            _logger.Log(
                level,
                BatchFlowLoggingEventIds.CheckpointSaved,
                "Сохранен checkpoint для job {JobName}. RunId={RunId}, SequenceNumber={SequenceNumber}, CheckpointValue={CheckpointValue}",
                @event.Job.JobName,
                @event.Job.RunId,
                @event.Checkpoint.SequenceNumber,
                FormatCheckpoint(@event.Checkpoint.Value));

            return;
        }

        _logger.Log(
            level,
            BatchFlowLoggingEventIds.CheckpointSaved,
            "Сохранен checkpoint для job {JobName}. RunId={RunId}, SequenceNumber={SequenceNumber}",
            @event.Job.JobName,
            @event.Job.RunId,
            @event.Checkpoint.SequenceNumber);
    }

    private void LogJobCompleted(JobCompletedEvent<TCheckpoint> @event)
    {
        var level = _options.JobCompletedLogLevelSelector(@event.Status);

        if (!ShouldLog(level))
        {
            return;
        }

        if (_options.IncludeCheckpointValue && @event.LastPersistedCheckpoint is not null)
        {
            _logger.Log(
                level,
                BatchFlowLoggingEventIds.JobCompleted,
                @event.Exception,
                "Завершен batch job {JobName}. RunId={RunId}, Status={Status}, ProcessedBatchCount={ProcessedBatchCount}, SucceededItemCount={SucceededItemCount}, SkippedItemCount={SkippedItemCount}, LastCheckpointSequence={LastCheckpointSequence}, LastCheckpointValue={LastCheckpointValue}, Reason={Reason}",
                @event.Job.JobName,
                @event.Job.RunId,
                @event.Status,
                @event.ProcessedBatchCount,
                @event.SucceededItemCount,
                @event.SkippedItemCount,
                @event.LastPersistedCheckpoint.SequenceNumber,
                FormatCheckpoint(@event.LastPersistedCheckpoint.Value),
                @event.Reason);

            return;
        }

        _logger.Log(
            level,
            BatchFlowLoggingEventIds.JobCompleted,
            @event.Exception,
            "Завершен batch job {JobName}. RunId={RunId}, Status={Status}, ProcessedBatchCount={ProcessedBatchCount}, SucceededItemCount={SucceededItemCount}, SkippedItemCount={SkippedItemCount}, LastCheckpointSequence={LastCheckpointSequence}, Reason={Reason}",
            @event.Job.JobName,
            @event.Job.RunId,
            @event.Status,
            @event.ProcessedBatchCount,
            @event.SucceededItemCount,
            @event.SkippedItemCount,
            @event.LastPersistedCheckpoint?.SequenceNumber,
            @event.Reason);
    }

    private void LogRetry(ProcessingRetryEvent<TCheckpoint> @event)
    {
        var level = _options.ProcessingRetryLogLevel;
        if (!ShouldLog(level))
        {
            return;
        }

        _logger.Log(level, BatchFlowLoggingEventIds.ProcessingRetry, @event.Exception,
            "Повтор обработки job {JobName}. RunId={RunId}, BatchSequence={BatchSequence}, Scope={Scope}, ItemIndex={ItemIndex}, Attempt={Attempt}, Delay={Delay}, Reason={Reason}",
            @event.Job.JobName, @event.Job.RunId, @event.BatchSequence, @event.Scope, @event.ItemIndex,
            @event.Attempt, @event.Delay, @event.Reason);
    }

    private void LogSkipped(ProcessingSkippedEvent<TCheckpoint> @event)
    {
        var level = _options.ProcessingSkippedLogLevel;
        if (!ShouldLog(level))
        {
            return;
        }

        _logger.Log(level, BatchFlowLoggingEventIds.ProcessingSkipped, @event.Exception,
            "Пропуск обработки job {JobName}. RunId={RunId}, BatchSequence={BatchSequence}, Scope={Scope}, ItemIndex={ItemIndex}, Attempt={Attempt}, SkippedCount={SkippedCount}, Reason={Reason}",
            @event.Job.JobName, @event.Job.RunId, @event.BatchSequence, @event.Scope, @event.ItemIndex,
            @event.Attempt, @event.SkippedCount, @event.Reason);
    }

    private bool ShouldLog(LogLevel level) => level != LogLevel.None && _logger.IsEnabled(level);

    private string FormatCheckpoint(TCheckpoint checkpoint) =>
        _options.CheckpointFormatter?.Invoke(checkpoint)
        ?? checkpoint?.ToString()
        ?? "<null>";

    private string FormatSourceMetadata(IReadOnlyDictionary<string, string> metadata)
    {
        if (metadata.Count == 0)
        {
            return "<empty>";
        }

        return _options.SourceMetadataFormatter?.Invoke(metadata)
            ?? string.Join(", ", metadata.OrderBy(static pair => pair.Key).Select(static pair => $"{pair.Key}={pair.Value}"));
    }
}
