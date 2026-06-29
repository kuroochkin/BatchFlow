using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Failures;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Core.Execution.Exceptions;
using BatchFlow.Core.Execution.Models;
using BatchFlow.Core.Jobs;
using BatchFlow.Core.Processing;

namespace BatchFlow.Core.Execution;

/// <summary>
/// Реализует базовый последовательный движок выполнения пакетных задач.
/// </summary>
public sealed class BatchJobExecutionEngine : IBatchJobExecutionEngine
{
    /// <summary>
    /// Выполняет пакетную задачу, управляет чтением данных, обработкой, retry-логикой и фиксацией прогресса.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, которые обрабатываются в рамках задачи.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с источником данных задачи.</typeparam>
    /// <param name="request">Описание выполняемой задачи и runtime-параметров запуска.</param>
    /// <param name="cancellationToken">Токен отмены текущего запуска.</param>
    /// <returns>Итог выполнения задачи, включая статус, счетчики и сведения о checkpoint.</returns>
    public async ValueTask<BatchJobExecutionSummary<TCheckpoint>> ExecuteAsync<TItem, TCheckpoint>(
        BatchJobExecutionRequest<TItem, TCheckpoint> request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Job);
        ArgumentNullException.ThrowIfNull(request.TimeProvider);

        var job = request.Job;
        Validate(job);
        
        var checkpointId = ResolveCheckpointId(job);
        var timeProvider = request.TimeProvider;
        
        var jobContext = new JobExecutionContext
        {
            JobName = job.JobName,
            CheckpointId = checkpointId,
            RunId = string.IsNullOrWhiteSpace(request.RunId)
                ? Guid.NewGuid().ToString("N")
                : request.RunId,
            StartedAt = timeProvider.GetUtcNow(),
            BatchSize = job.BatchSize,
            DegreeOfParallelism = job.DegreeOfParallelism,
        };

        var runtime = new BatchJobRuntimeState<TCheckpoint>(jobContext, timeProvider);

        try
        {
            runtime.ResumeFrom = await LoadCheckpointAsync(job, checkpointId, cancellationToken);
            
            await PublishAsync(job.Observers, new JobStartedEvent<TCheckpoint>
            {
                Job = jobContext,
                OccurredAt = timeProvider.GetUtcNow(),
                ResumeFrom = runtime.ResumeFrom,
            }, cancellationToken);

            var sourceContext = new BatchSourceContext<TCheckpoint>
            {
                Job = jobContext,
                ResumeFrom = runtime.ResumeFrom,
                RequestedBatchSize = job.BatchSize,
            };

            var batches = job.Source.ReadAsync(sourceContext, cancellationToken)
                ?? throw new InvalidOperationException("Источник данных вернул пустую последовательность пакетов.");

            await foreach (var batch in batches.WithCancellation(cancellationToken))
            {
                ValidateBatch(job, batch);
                runtime.BatchSequence++;

                var processingContext = new BatchProcessingContext<TCheckpoint>
                {
                    Job = jobContext,
                    BatchSequence = runtime.BatchSequence,
                    BatchCheckpoint = batch.Checkpoint,
                    SourceMetadata = batch.Metadata,
                    Attempt = 1,
                    ItemCount = batch.Items.Count,
                };

                await PublishAsync(job.Observers, new BatchStartedEvent<TCheckpoint>
                {
                    Job = jobContext,
                    OccurredAt = timeProvider.GetUtcNow(),
                    BatchSequence = runtime.BatchSequence,
                    BatchCheckpoint = batch.Checkpoint,
                    Attempt = 1,
                    ItemCount = batch.Items.Count,
                    SourceMetadata = batch.Metadata,
                }, cancellationToken);

                ProcessingExecutionResult processingResult;

                try
                {
                    processingResult = await ExecuteProcessingAsync(job, batch, processingContext, cancellationToken);
                }
                catch (BatchJobTerminationException ex)
                {
                    await TryPublishFailureAsync(jobContext, job.Observers, batch, processingContext,
                        ex.InnerException ?? ex, ex.Attempt, ex.ItemIndex, timeProvider, cancellationToken);

                    runtime.Status = ex.Status;
                    runtime.Reason = ex.Message;
                    runtime.Exception = ex.InnerException ?? ex;

                    return await CompleteAsync(job.Observers, runtime, cancellationToken, suppressObserverFailures: true);
                }
                catch (BatchJobProcessingException ex)
                {
                    await TryPublishFailureAsync(jobContext, job.Observers, batch, processingContext, ex.InnerException ?? ex,
                        ex.Attempt, ex.ItemIndex, timeProvider, cancellationToken);

                    runtime.Status = JobCompletionStatus.Failed;
                    runtime.Reason = ex.Message;
                    runtime.Exception = ex.InnerException ?? ex;

                    return await CompleteAsync(job.Observers, runtime, cancellationToken, suppressObserverFailures: true);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    runtime.Status = JobCompletionStatus.Cancelled;
                    runtime.Reason = "Выполнение задачи было отменено.";
                    
                    return await CompleteAsync(job.Observers, runtime, cancellationToken, suppressObserverFailures: true);
                }
                catch (Exception ex)
                {
                    await TryPublishFailureAsync(jobContext, job.Observers, batch, processingContext, ex, attempt: 1,
                        itemIndex: null, timeProvider, cancellationToken);

                    runtime.Status = JobCompletionStatus.Failed;
                    runtime.Reason = "Обработка пакета завершилась ошибкой.";
                    runtime.Exception = ex;

                    return await CompleteAsync(job.Observers, runtime, cancellationToken, suppressObserverFailures: true);
                }

                runtime.ProcessedBatchCount++;
                runtime.SucceededItemCount += processingResult.Result.SucceededCount;
                runtime.SkippedItemCount += processingResult.Result.SkippedCount;

                await PublishAsync(job.Observers, new BatchCompletedEvent<TCheckpoint>
                {
                    Job = jobContext,
                    OccurredAt = timeProvider.GetUtcNow(),
                    BatchSequence = runtime.BatchSequence,
                    BatchCheckpoint = batch.Checkpoint,
                    Attempt = processingResult.AttemptCount,
                    ItemCount = processingResult.Result.ItemCount,
                    SucceededCount = processingResult.Result.SucceededCount,
                    SkippedCount = processingResult.Result.SkippedCount,
                    SourceMetadata = batch.Metadata,
                }, cancellationToken);

                if (checkpointId is null) 
                    continue;
                
                runtime.LastPersistedCheckpoint = await PersistCheckpointAsync(
                    job,
                    checkpointId,
                    batch,
                    runtime,
                    cancellationToken);

                await PublishAsync(job.Observers, new CheckpointSavedEvent<TCheckpoint>
                {
                    Job = jobContext,
                    OccurredAt = timeProvider.GetUtcNow(),
                    Checkpoint = runtime.LastPersistedCheckpoint,
                }, cancellationToken);
            }

            runtime.Status = runtime.SkippedItemCount > 0
                ? JobCompletionStatus.CompletedWithSkips
                : JobCompletionStatus.Succeeded;

            runtime.Reason = runtime.Status == JobCompletionStatus.CompletedWithSkips
                ? "Задача завершена с пропусками."
                : "Задача завершена успешно.";

            return await CompleteAsync(job.Observers, runtime, cancellationToken, suppressObserverFailures: true);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            runtime.Status = JobCompletionStatus.Cancelled;
            runtime.Reason = "Выполнение задачи было отменено.";
            return await CompleteAsync(job.Observers, runtime, cancellationToken, suppressObserverFailures: true);
        }
        catch (Exception ex)
        {
            runtime.Status = JobCompletionStatus.Failed;
            runtime.Reason = BuildRuntimeFailureReason(ex);
            runtime.Exception = ex;

            return await CompleteAsync(job.Observers, runtime, cancellationToken, suppressObserverFailures: true);
        }
    }

    private static string? ResolveCheckpointId<TItem, TCheckpoint>(BatchJobDefinition<TItem, TCheckpoint> job) =>
        ShouldPersistCheckpoint(job) ? job.ResolveCheckpointId() : null;

    private static bool ShouldPersistCheckpoint<TItem, TCheckpoint>(BatchJobDefinition<TItem, TCheckpoint> job) =>
        job.Source.SupportsResume && job.CheckpointStore is not null;

    private static void Validate<TItem, TCheckpoint>(BatchJobDefinition<TItem, TCheckpoint> job)
    {
        ArgumentNullException.ThrowIfNull(job.Source);
        ArgumentNullException.ThrowIfNull(job.ProcessingMode);
        ArgumentNullException.ThrowIfNull(job.Observers);

        if (string.IsNullOrWhiteSpace(job.JobName))
        {
            throw new BatchJobConfigurationException("Имя задачи не может быть пустым.");
        }

        if (job.BatchSize <= 0)
        {
            throw new BatchJobConfigurationException("Размер пакета должен быть больше нуля.");
        }

        if (job.DegreeOfParallelism <= 0)
        {
            throw new BatchJobConfigurationException("Степень параллелизма должна быть больше нуля.");
        }

        if (!job.Source.SupportsResume && job.CheckpointStore is not null)
        {
            throw new BatchJobConfigurationException(
                "Для источника без поддержки возобновления нельзя указывать хранилище контрольных точек.");
        }

        if (job.Observers.Any(_ => false))
        {
            throw new BatchJobConfigurationException("Список наблюдателей выполнения не должен содержать пустые значения.");
        }
    }

    private static void ValidateBatch<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        SourceBatch<TItem, TCheckpoint> batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(batch.Items);
        ArgumentNullException.ThrowIfNull(batch.Metadata);

        if (batch.Items.Count == 0)
        {
            throw new InvalidOperationException("Источник данных не должен возвращать пустые пакеты. " +
                "Для завершения чтения нужно завершить последовательность.");
        }

        if (job.Source.SupportsResume && batch.Checkpoint is null)
        {
            throw new BatchJobConfigurationException(
                "Источник данных с поддержкой возобновления вернул пакет без контрольной точки.");
        }
    }

    private static async ValueTask<StoredCheckpoint<TCheckpoint>?> LoadCheckpointAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        string? checkpointId,
        CancellationToken cancellationToken)
    {
        if (checkpointId is null)
        {
            return null;
        }

        var checkpoint = await job.CheckpointStore!.LoadAsync(checkpointId, cancellationToken);

        if (checkpoint is not null)
        {
            ValidateStoredCheckpoint(checkpoint, checkpointId);
        }

        return checkpoint;
    }

    private static async ValueTask<StoredCheckpoint<TCheckpoint>> PersistCheckpointAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        string checkpointId,
        SourceBatch<TItem, TCheckpoint> batch,
        BatchJobRuntimeState<TCheckpoint> runtime,
        CancellationToken cancellationToken)
    {
        var checkpoint = new StoredCheckpoint<TCheckpoint>
        {
            Value = batch.Checkpoint,
            SequenceNumber = runtime.BatchSequence,
            SavedAt = runtime.TimeProvider.GetUtcNow(),
            ConcurrencyToken = runtime.LastPersistedCheckpoint?.ConcurrencyToken ?? runtime.ResumeFrom?.ConcurrencyToken,
            Metadata = batch.Metadata,
        };

        var savedCheckpoint = await job.CheckpointStore!.SaveAsync(
            checkpointId,
            checkpoint,
            runtime.LastPersistedCheckpoint?.ConcurrencyToken ?? runtime.ResumeFrom?.ConcurrencyToken,
            cancellationToken);

        ValidateStoredCheckpoint(savedCheckpoint, checkpointId);

        return savedCheckpoint;
    }

    private static async ValueTask<ProcessingExecutionResult> ExecuteProcessingAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        SourceBatch<TItem, TCheckpoint> batch,
        BatchProcessingContext<TCheckpoint> context,
        CancellationToken cancellationToken)
    {
        return job.ProcessingMode switch
        {
            BatchProcessorRegistration<TItem, TCheckpoint> batchProcessor =>
                await ExecuteBatchProcessorAsync(job, batch, context, batchProcessor.Processor, cancellationToken),
            
            ItemProcessorRegistration<TItem, TCheckpoint> itemProcessor =>
                await ExecuteItemProcessorAsync(job, batch, context, itemProcessor.Processor, cancellationToken),
            
            _ => throw new BatchJobConfigurationException("Указан неподдерживаемый режим обработки."),
        };
    }

    private static async ValueTask<ProcessingExecutionResult> ExecuteBatchProcessorAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        SourceBatch<TItem, TCheckpoint> batch,
        BatchProcessingContext<TCheckpoint> context,
        IBatchProcessor<TItem, TCheckpoint> processor,
        CancellationToken cancellationToken)
    {
        var attempt = 1;
        ProcessingExecutionResult? processingResult = null;

        while (processingResult is null)
        {
            try
            {
                var result = await processor.ProcessAsync(
                    batch.Items,
                    context with { Attempt = attempt },
                    cancellationToken);

                ValidateBatchProcessingResult(result, batch.Items.Count, attempt);

                processingResult = new ProcessingExecutionResult(result, attempt);
            }
            catch (BatchJobProcessingException)
            {
                throw;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var failureHandlingResult = await HandleBatchProcessingFailureAsync(
                    job, batch, context, attempt, ex, cancellationToken);

                attempt = failureHandlingResult.NextAttempt;
                processingResult = failureHandlingResult.ProcessingResult;
            }
        }

        return processingResult;
    }

    private static async ValueTask<ProcessingExecutionResult> ExecuteItemProcessorAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        SourceBatch<TItem, TCheckpoint> batch,
        BatchProcessingContext<TCheckpoint> context,
        IItemProcessor<TItem, TCheckpoint> processor,
        CancellationToken cancellationToken)
    {
        var succeeded = 0;
        var skipped = 0;
        var issues = new List<BatchProcessingIssue>();
        var maxAttempt = 1;

        for (var index = 0; index < batch.Items.Count; index++)
        {
            var item = batch.Items[index];
            var attempt = 1;
            var isItemCompleted = false;

            while (!isItemCompleted)
            {
                try
                {
                    maxAttempt = Math.Max(maxAttempt, attempt);

                    var result = await processor.ProcessAsync(
                        item,
                        new ItemProcessingContext<TCheckpoint>
                        {
                            Job = context.Job,
                            BatchSequence = context.BatchSequence,
                            BatchCheckpoint = context.BatchCheckpoint,
                            SourceMetadata = context.SourceMetadata,
                            ItemIndex = index,
                            BatchItemCount = batch.Items.Count,
                            Attempt = attempt,
                        },
                        cancellationToken);

                    ValidateItemProcessingResult(result, attempt, index);

                    if (result.Disposition == ProcessingDisposition.Succeeded)
                    {
                        succeeded++;
                    }
                    else
                    {
                        skipped++;
                        issues.Add(new BatchProcessingIssue
                        {
                            ItemIndex = index,
                            Code = result.Code,
                            Message = result.Reason,
                        });
                    }

                    isItemCompleted = true;
                }
                catch (BatchJobProcessingException)
                {
                    throw;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    var failureHandlingResult = await HandleItemProcessingFailureAsync(
                        job, batch, context, item, index, attempt, ex, cancellationToken);

                    attempt = failureHandlingResult.NextAttempt;
                    isItemCompleted = failureHandlingResult.IsItemCompleted;

                    if (failureHandlingResult.Issue is not null)
                    {
                        skipped++;
                        issues.Add(failureHandlingResult.Issue);
                        maxAttempt = Math.Max(maxAttempt, attempt);
                    }
                }
            }
        }

        return new ProcessingExecutionResult(
            BatchProcessingResult.Completed(
                batch.Items.Count,
                succeededCount: succeeded,
                skippedCount: skipped,
                issues,
                skipped > 0 ? "Пакет обработан с пропущенными элементами." : null),
            maxAttempt);
    }

    private static void ValidateBatchProcessingResult(BatchProcessingResult? result,
        int expectedItemCount, int attempt)
    {
        if (result is null)
        {
            throw new BatchJobProcessingException("Пакетный обработчик вернул пустой результат.",
                attempt, itemIndex: null);
        }

        if (result.ItemCount != expectedItemCount)
        {
            throw new BatchJobProcessingException("Пакетный обработчик вернул результат с некорректным количеством элементов.",
                attempt, itemIndex: null);
        }

        if (!result.IsFullyAccountedFor)
        {
            throw new BatchJobProcessingException("Пакетный обработчик обязан вернуть окончательный исход для каждого элемента пакета.",
                attempt, itemIndex: null);
        }

        if (result.SucceededCount < 0 || result.SkippedCount < 0)
        {
            throw new BatchJobProcessingException("Пакетный обработчик вернул отрицательные счетчики результата.",
                attempt, itemIndex: null);
        }

        if (result.Issues is null)
        {
            throw new BatchJobProcessingException("Пакетный обработчик вернул результат без коллекции проблем.",
                attempt, itemIndex: null);
        }

        foreach (var issue in result.Issues)
        {
            if (issue is null)
            {
                throw new BatchJobProcessingException("Пакетный обработчик вернул коллекцию проблем с пустым элементом.",
                    attempt, itemIndex: null);
            }

            if (issue.ItemIndex is < 0 || issue.ItemIndex >= expectedItemCount)
            {
                throw new BatchJobProcessingException("Пакетный обработчик вернул проблему с некорректным индексом элемента.",
                    attempt, issue.ItemIndex);
            }
        }
    }

    private static async ValueTask<BatchProcessingFailureHandlingResult> HandleBatchProcessingFailureAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        SourceBatch<TItem, TCheckpoint> batch,
        BatchProcessingContext<TCheckpoint> context,
        int attempt,
        Exception exception,
        CancellationToken cancellationToken)
    {
        if (job.FailurePolicy is null)
        {
            throw new BatchJobProcessingException("Обработка пакета завершилась необработанным исключением.",
                attempt, itemIndex: null, exception);
        }

        FailureDecision decision;

        try
        {
            decision = await job.FailurePolicy.DecideAsync(new FailureContext<TItem, TCheckpoint>
            {
                Job = context.Job,
                BatchSequence = context.BatchSequence,
                BatchCheckpoint = context.BatchCheckpoint,
                BatchItems = batch.Items,
                SourceMetadata = batch.Metadata,
                Attempt = attempt,
                Exception = exception,
                Scope = FailureScope.Batch,
            }, cancellationToken);
        }
        catch (Exception policyException) when (policyException is not OperationCanceledException)
        {
            throw new BatchJobProcessingException("Политика обработки ошибок завершилась исключением.",
                attempt, itemIndex: null, policyException);
        }

        ValidateFailureDecision(decision, attempt, itemIndex: null);

        switch (decision.Action)
        {
            case FailureAction.Retry:
            {
                if (decision.Delay is { } delay)
                {
                    await Task.Delay(delay, cancellationToken);
                }

                return new BatchProcessingFailureHandlingResult(attempt + 1, ProcessingResult: null);
            }
            case FailureAction.Skip:
            {
                return new BatchProcessingFailureHandlingResult(attempt,
                    new ProcessingExecutionResult(CreateSkippedBatchProcessingResult(batch.Items.Count, decision.Reason), attempt));
            }
            case FailureAction.Stop:
            {
                throw new BatchJobTerminationException(
                    JobCompletionStatus.Failed,
                    decision.Reason ?? "Политика обработки ошибок остановила выполнение задачи.",
                    attempt,
                    itemIndex: null,
                    exception);
            }
            case FailureAction.Suspend:
            {
                throw new BatchJobTerminationException(
                    JobCompletionStatus.Suspended,
                    decision.Reason ?? "Политика обработки ошибок приостановила выполнение задачи.",
                    attempt,
                    itemIndex: null,
                    exception);
            }
            case FailureAction.DeadLetter:
            {
                throw new BatchJobProcessingException(
                    "Поддержка отправки ошибок в карантин будет добавлена на следующем этапе развития движка.",
                    attempt,
                    itemIndex: null,
                    exception);
            }
            default:
            {
                throw new BatchJobProcessingException(
                    "Политика обработки ошибок вернула неизвестное действие.",
                    attempt,
                    itemIndex: null);
            }
        }
    }

    private static BatchProcessingResult CreateSkippedBatchProcessingResult(int itemCount, string? reason) =>
        BatchProcessingResult.Completed(
            itemCount,
            succeededCount: 0,
            skippedCount: itemCount,
            issues:
            [
                new BatchProcessingIssue
                {
                    Code = reason is null ? "batch-skipped" : "batch-skipped-by-policy",
                    Message = reason ?? "Пакет был пропущен политикой обработки ошибок.",
                },
            ],
            reason: reason);

    private static async ValueTask<ItemProcessingFailureHandlingResult> HandleItemProcessingFailureAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        SourceBatch<TItem, TCheckpoint> batch,
        BatchProcessingContext<TCheckpoint> context,
        TItem item,
        int itemIndex,
        int attempt,
        Exception exception,
        CancellationToken cancellationToken)
    {
        if (job.FailurePolicy is null)
        {
            throw new BatchJobProcessingException("Обработка элемента завершилась необработанным исключением.",
                attempt, itemIndex, exception);
        }

        FailureDecision decision;

        try
        {
            decision = await job.FailurePolicy.DecideAsync(new FailureContext<TItem, TCheckpoint>
            {
                Job = context.Job,
                BatchSequence = context.BatchSequence,
                BatchCheckpoint = context.BatchCheckpoint,
                BatchItems = batch.Items,
                SourceMetadata = batch.Metadata,
                Attempt = attempt,
                Exception = exception,
                Scope = FailureScope.Item,
                ItemIndex = itemIndex,
                FailedItem = item,
            }, cancellationToken);
        }
        catch (Exception policyException) when (policyException is not OperationCanceledException)
        {
            throw new BatchJobProcessingException("Политика обработки ошибок завершилась исключением.",
                attempt, itemIndex, policyException);
        }

        ValidateFailureDecision(decision, attempt, itemIndex);

        switch (decision.Action)
        {
            case FailureAction.Retry:
            {
                if (decision.Delay is { } delay)
                {
                    await Task.Delay(delay, cancellationToken);
                }

                return new ItemProcessingFailureHandlingResult(
                    NextAttempt: attempt + 1,
                    IsItemCompleted: false,
                    Issue: null);
            }
            case FailureAction.Skip:
            {
                return new ItemProcessingFailureHandlingResult(
                    NextAttempt: attempt,
                    IsItemCompleted: true,
                    Issue: CreateSkippedItemProcessingIssue(itemIndex, decision.Reason));
            }
            case FailureAction.Stop:
            {
                throw new BatchJobTerminationException(
                    JobCompletionStatus.Failed,
                    decision.Reason ?? "Политика обработки ошибок остановила выполнение задачи.",
                    attempt, itemIndex, exception);
            }
            case FailureAction.Suspend:
            {
                throw new BatchJobTerminationException(
                    JobCompletionStatus.Suspended,
                    decision.Reason ?? "Политика обработки ошибок приостановила выполнение задачи.",
                    attempt, itemIndex, exception);
            }
            case FailureAction.DeadLetter:
            {
                throw new BatchJobProcessingException(
                    "Поддержка отправки ошибок в карантин будет добавлена на следующем этапе развития движка.",
                    attempt, itemIndex, exception);
            }
            default:
            {
                throw new BatchJobProcessingException("Политика обработки ошибок вернула неизвестное действие.",
                    attempt, itemIndex);
            }
        }
    }

    private static BatchProcessingIssue CreateSkippedItemProcessingIssue(int itemIndex, string? reason) =>
        new()
        {
            ItemIndex = itemIndex,
            Code = reason is null ? "item-skipped" : "item-skipped-by-policy",
            Message = reason ?? "Элемент был пропущен политикой обработки ошибок.",
        };

    private static void ValidateItemProcessingResult(ProcessingResult? result,
        int attempt, int itemIndex)
    {
        if (result is null)
        {
            throw new BatchJobProcessingException("Поэлементный обработчик вернул пустой результат.",
                attempt, itemIndex);
        }

        if (!Enum.IsDefined(result.Disposition))
        {
            throw new BatchJobProcessingException("Поэлементный обработчик вернул неизвестный итог обработки.",
                attempt, itemIndex);
        }

        if (result.Metadata is null)
        {
            throw new BatchJobProcessingException("Поэлементный обработчик вернул результат без коллекции метаданных.",
                attempt, itemIndex);
        }
    }

    private static void ValidateFailureDecision(FailureDecision? decision, int attempt, int? itemIndex)
    {
        if (decision is null)
        {
            throw new BatchJobProcessingException("Политика обработки ошибок вернула пустое решение.",
                attempt, itemIndex);
        }

        if (!Enum.IsDefined(decision.Action))
        {
            throw new BatchJobProcessingException("Политика обработки ошибок вернула неизвестное действие.",
                attempt, itemIndex);
        }

        if (decision.Action is not FailureAction.Retry && decision.Delay is not null)
        {
            throw new BatchJobProcessingException("Политика обработки ошибок может задавать задержку только для повторной попытки.",
                attempt, itemIndex);
        }

        if (decision.Delay < TimeSpan.Zero)
        {
            throw new BatchJobProcessingException("Политика обработки ошибок вернула отрицательную задержку перед повторной попыткой.",
                attempt, itemIndex);
        }
    }

    private static void ValidateStoredCheckpoint<TCheckpoint>(
        StoredCheckpoint<TCheckpoint>? checkpoint,
        string checkpointId)
    {
        if (checkpoint is null)
        {
            throw new InvalidOperationException($"Хранилище контрольных точек вернуло пустое значение для '{checkpointId}'.");
        }

        if (checkpoint.Metadata is null)
        {
            throw new InvalidOperationException(
                $"Хранилище контрольных точек вернуло контрольную точку '{checkpointId}' без коллекции метаданных.");
        }

        if (checkpoint.Value is null)
        {
            throw new InvalidOperationException($"Хранилище контрольных точек вернуло контрольную точку '{checkpointId}' " +
                $"без значения checkpoint.");
        }

        if (checkpoint.SequenceNumber < 0)
        {
            throw new InvalidOperationException($"Хранилище контрольных точек вернуло контрольную точку '{checkpointId}'" +
                $" с отрицательным номером последовательности.");
        }
    }

    private static string BuildRuntimeFailureReason(Exception exception) =>
        exception switch
        {
            CheckpointConflictException => "Не удалось зафиксировать прогресс задачи из-за конфликта контрольной точки.",
            BatchJobObserverException => "Наблюдатель выполнения завершился исключением.",
            BatchJobConfigurationException => exception.Message,
            InvalidOperationException => exception.Message,
            _ => "Выполнение задачи завершилось ошибкой вне пользовательского обработчика."
        };

    private static async ValueTask TryPublishFailureAsync<TItem, TCheckpoint>(
        JobExecutionContext jobContext,
        IReadOnlyList<IExecutionObserver<TCheckpoint>> observers,
        SourceBatch<TItem, TCheckpoint> batch,
        BatchProcessingContext<TCheckpoint> context,
        Exception exception,
        int attempt,
        int? itemIndex,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        try
        {
            await PublishFailureAsync(jobContext, observers, batch, context, exception, attempt, itemIndex,
                timeProvider, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (BatchJobObserverException)
        {
            // Первичная причина сбоя обработки важнее ошибок вторичного уведомления наблюдателей.
        }
    }

    private static async ValueTask PublishFailureAsync<TItem, TCheckpoint>(
        JobExecutionContext jobContext,
        IReadOnlyList<IExecutionObserver<TCheckpoint>> observers,
        SourceBatch<TItem, TCheckpoint> batch,
        BatchProcessingContext<TCheckpoint> context,
        Exception exception,
        int attempt,
        int? itemIndex,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        await PublishAsync(observers, new BatchFailedEvent<TCheckpoint>
        {
            Job = jobContext,
            OccurredAt = timeProvider.GetUtcNow(),
            BatchSequence = context.BatchSequence,
            BatchCheckpoint = context.BatchCheckpoint,
            ItemCount = context.ItemCount,
            Attempt = attempt,
            Exception = exception,
            ItemIndex = itemIndex,
            SourceMetadata = batch.Metadata,
        }, cancellationToken);
    }

    private static async ValueTask<BatchJobExecutionSummary<TCheckpoint>> CompleteAsync<TCheckpoint>(
        IReadOnlyList<IExecutionObserver<TCheckpoint>> observers,
        BatchJobRuntimeState<TCheckpoint> runtime,
        CancellationToken cancellationToken,
        bool suppressObserverFailures)
    {
        var completedAt = runtime.TimeProvider.GetUtcNow();

        if (suppressObserverFailures)
        {
            try
            {
                await PublishAsync(observers, new JobCompletedEvent<TCheckpoint>
                {
                    Job = runtime.Job,
                    OccurredAt = completedAt,
                    Status = runtime.Status,
                    Reason = runtime.Reason,
                }, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (BatchJobObserverException)
            {
                // Финальное summary важнее вторичной ошибки наблюдателя во время публикации завершающего события.
            }
        }
        else
        {
            await PublishAsync(observers, new JobCompletedEvent<TCheckpoint>
            {
                Job = runtime.Job,
                OccurredAt = completedAt,
                Status = runtime.Status,
                Reason = runtime.Reason,
            }, cancellationToken);
        }

        return new BatchJobExecutionSummary<TCheckpoint>
        {
            Job = runtime.Job,
            Status = runtime.Status,
            ResumeFrom = runtime.ResumeFrom,
            LastPersistedCheckpoint = runtime.LastPersistedCheckpoint,
            CompletedAt = completedAt,
            ProcessedBatchCount = runtime.ProcessedBatchCount,
            SucceededItemCount = runtime.SucceededItemCount,
            SkippedItemCount = runtime.SkippedItemCount,
            Reason = runtime.Reason,
            Exception = runtime.Exception,
        };
    }

    private static async ValueTask PublishAsync<TCheckpoint>(
        IReadOnlyList<IExecutionObserver<TCheckpoint>> observers,
        BatchFlowEvent<TCheckpoint> @event,
        CancellationToken cancellationToken)
    {
        foreach (var observer in observers)
        {
            try
            {
                await observer.OnEventAsync(@event, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new BatchJobObserverException(observer.GetType(), @event.GetType(), ex);
            }
        }
    }

    private sealed record BatchProcessingFailureHandlingResult(int NextAttempt, ProcessingExecutionResult? ProcessingResult);

    private sealed record ItemProcessingFailureHandlingResult(int NextAttempt, bool IsItemCompleted, BatchProcessingIssue? Issue);
}
