using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Failures;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Core.Execution.Buffering;
using BatchFlow.Core.Execution.Exceptions;
using BatchFlow.Core.Execution.Models;
using BatchFlow.Core.Execution.Parallel;
using BatchFlow.Core.Jobs;
using BatchFlow.Core.Processing;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;

namespace BatchFlow.Core.Execution;

/// <summary>
/// Реализует базовый движок выполнения пакетных задач
/// с bounded prefetch-очередью и управляемым параллелизмом обработки.
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
            PrefetchCapacity = job.PrefetchCapacity,
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

            await using var bufferedBatches = BufferedBatchChannel<TItem, TCheckpoint>.Start(
                job,
                batches,
                cancellationToken);

            return await ExecuteBufferedPipelineAsync(
                job,
                bufferedBatches,
                runtime,
                checkpointId,
                cancellationToken);
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

    private static ValueTask<BatchJobExecutionSummary<TCheckpoint>> ExecuteBufferedPipelineAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        BufferedBatchChannel<TItem, TCheckpoint> bufferedBatches,
        BatchJobRuntimeState<TCheckpoint> runtime,
        string? checkpointId,
        CancellationToken cancellationToken)
    {
        return job.ProcessingMode is BatchProcessorRegistration<TItem, TCheckpoint> && job.DegreeOfParallelism > 1
            ? ExecuteParallelBatchPipelineAsync(job, bufferedBatches, runtime, checkpointId, cancellationToken)
            : ExecuteSequentialPipelineAsync(job, bufferedBatches, runtime, checkpointId, cancellationToken);
    }

    private static async ValueTask<BatchJobExecutionSummary<TCheckpoint>> ExecuteSequentialPipelineAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        BufferedBatchChannel<TItem, TCheckpoint> bufferedBatches,
        BatchJobRuntimeState<TCheckpoint> runtime,
        string? checkpointId,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var bufferedBatch in bufferedBatches.Reader.ReadAllAsync(cancellationToken))
            {
                var batch = bufferedBatch.Batch;

                ValidateBatch(job, batch);
                runtime.BatchSequence = bufferedBatch.Sequence;

                var processingContext = CreateBatchProcessingContext(runtime.Job, bufferedBatch);

                await PublishBatchStartedAsync(job.Observers, runtime.TimeProvider, bufferedBatch, processingContext, cancellationToken);

                ProcessingExecutionResult processingResult;

                try
                {
                    processingResult = await ExecuteProcessingAsync(job, batch, processingContext, cancellationToken);
                }
                catch (BatchJobTerminationException ex)
                {
                    await TryPublishFailureAsync(runtime.Job, job.Observers, batch, processingContext,
                        ex.InnerException ?? ex, ex.Attempt, ex.ItemIndex, runtime.TimeProvider, cancellationToken);

                    runtime.Status = ex.Status;
                    runtime.Reason = ex.Message;
                    runtime.Exception = ex.InnerException ?? ex;

                    await bufferedBatches.CompleteAsync(suppressProducerException: true);
                    return await CompleteAsync(job.Observers, runtime, cancellationToken, suppressObserverFailures: true);
                }
                catch (BatchJobProcessingException ex)
                {
                    await TryPublishFailureAsync(runtime.Job, job.Observers, batch, processingContext, ex.InnerException ?? ex,
                        ex.Attempt, ex.ItemIndex, runtime.TimeProvider, cancellationToken);

                    runtime.Status = JobCompletionStatus.Failed;
                    runtime.Reason = ex.Message;
                    runtime.Exception = ex.InnerException ?? ex;

                    await bufferedBatches.CompleteAsync(suppressProducerException: true);
                    return await CompleteAsync(job.Observers, runtime, cancellationToken, suppressObserverFailures: true);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    runtime.Status = JobCompletionStatus.Cancelled;
                    runtime.Reason = "Выполнение задачи было отменено.";

                    await bufferedBatches.CompleteAsync(suppressProducerException: true);
                    return await CompleteAsync(job.Observers, runtime, cancellationToken, suppressObserverFailures: true);
                }
                catch (Exception ex)
                {
                    await TryPublishFailureAsync(runtime.Job, job.Observers, batch, processingContext, ex, attempt: 1,
                        itemIndex: null, runtime.TimeProvider, cancellationToken);

                    runtime.Status = JobCompletionStatus.Failed;
                    runtime.Reason = "Обработка пакета завершилась ошибкой.";
                    runtime.Exception = ex;

                    await bufferedBatches.CompleteAsync(suppressProducerException: true);
                    return await CompleteAsync(job.Observers, runtime, cancellationToken, suppressObserverFailures: true);
                }

                runtime.ProcessedBatchCount++;
                runtime.SucceededItemCount += processingResult.Result.SucceededCount;
                runtime.SkippedItemCount += processingResult.Result.SkippedCount;

                if (checkpointId is not null)
                {
                    runtime.LastPersistedCheckpoint = await PersistCheckpointAsync(
                        job,
                        checkpointId,
                        batch,
                        bufferedBatch.Sequence,
                        runtime,
                        cancellationToken);
                }

                await PublishBatchCompletedAsync(job.Observers, runtime.TimeProvider, bufferedBatch, processingContext,
                    processingResult, cancellationToken);

                if (checkpointId is null)
                {
                    continue;
                }

                var persistedCheckpoint = runtime.LastPersistedCheckpoint
                    ?? throw new InvalidOperationException(
                        "Движок не сохранил контрольную точку после подтвержденной обработки пакета.");

                await PublishCheckpointSavedAsync(job.Observers, runtime.TimeProvider, runtime.Job, persistedCheckpoint,
                    cancellationToken);
            }

            await bufferedBatches.CompleteAsync(suppressProducerException: false);
            return await CompleteSuccessfulExecutionAsync(job.Observers, runtime, cancellationToken);
        }
        catch
        {
            await bufferedBatches.CompleteAsync(suppressProducerException: true);
            throw;
        }
    }

    private static async ValueTask<BatchJobExecutionSummary<TCheckpoint>> ExecuteParallelBatchPipelineAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        BufferedBatchChannel<TItem, TCheckpoint> bufferedBatches,
        BatchJobRuntimeState<TCheckpoint> runtime,
        string? checkpointId,
        CancellationToken cancellationToken)
    {
        using var processingCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var notifications = Channel.CreateUnbounded<BatchWorkerNotification<TItem, TCheckpoint>>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false,
            });

        var workerTasks = Enumerable.Range(0, job.DegreeOfParallelism)
            .Select(_ => RunBatchWorkerAsync(
                bufferedBatches.Reader,
                notifications.Writer,
                job,
                runtime.Job,
                processingCancellationSource.Token))
            .ToArray();

        var completeNotificationsTask = CompleteBatchNotificationWriterAsync(workerTasks, notifications.Writer);
        var checkpointCoordinator = CreateParallelBatchCheckpointCoordinator(job, runtime, checkpointId);
        BatchWorkerFailedNotification<TItem, TCheckpoint>? terminalFailure = null;

        try
        {
            await foreach (var notification in notifications.Reader.ReadAllAsync(cancellationToken))
            {
                switch (notification)
                {
                    case BatchWorkerStartedNotification<TItem, TCheckpoint> started when terminalFailure is null:
                    {
                        await PublishBatchStartedAsync(job.Observers, runtime.TimeProvider, started.BufferedBatch,
                            started.Context, cancellationToken);
                        break;
                    }
                    case BatchWorkerCompletedNotification<TItem, TCheckpoint> completed:
                    {
                        runtime.ProcessedBatchCount++;
                        runtime.SucceededItemCount += completed.ProcessingResult.Result.SucceededCount;
                        runtime.SkippedItemCount += completed.ProcessingResult.Result.SkippedCount;

                        if (checkpointId is null)
                        {
                            await PublishBatchCompletedAsync(job.Observers, runtime.TimeProvider, completed.BufferedBatch,
                                completed.Context, completed.ProcessingResult, cancellationToken);
                            break;
                        }

                        if (checkpointCoordinator!.TryTrackCompletion(completed))
                        {
                            await checkpointCoordinator.FlushAsync(cancellationToken);
                        }

                        break;
                    }
                    case BatchWorkerFailedNotification<TItem, TCheckpoint> failed when terminalFailure is null:
                    {
                        terminalFailure = failed;
                        runtime.Status = failed.Status;
                        runtime.Reason = failed.Reason;
                        runtime.Exception = failed.Exception;

                        checkpointCoordinator?.RegisterTerminalFailure(failed.BufferedBatch.Sequence);

                        await TryPublishFailureAsync(runtime.Job, job.Observers, failed.BufferedBatch.Batch, failed.Context,
                            failed.Exception, failed.Attempt, failed.ItemIndex, runtime.TimeProvider, cancellationToken);

                        await processingCancellationSource.CancelAsync();
                        break;
                    }
                }
            }

            await completeNotificationsTask.ConfigureAwait(false);
            await bufferedBatches.CompleteAsync(suppressProducerException: terminalFailure is not null);

            if (terminalFailure is not null)
            {
                return await CompleteAsync(job.Observers, runtime, cancellationToken, suppressObserverFailures: true);
            }

            return await CompleteSuccessfulExecutionAsync(job.Observers, runtime, cancellationToken);
        }
        catch
        {
            await processingCancellationSource.CancelAsync();
            await bufferedBatches.CompleteAsync(suppressProducerException: true);
            throw;
        }
    }

    private static ParallelBatchCheckpointCoordinator<TItem, TCheckpoint>? CreateParallelBatchCheckpointCoordinator<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        BatchJobRuntimeState<TCheckpoint> runtime,
        string? checkpointId)
    {
        if (checkpointId is null)
        {
            return null;
        }

        return new ParallelBatchCheckpointCoordinator<TItem, TCheckpoint>(
            runtime,
            persistCheckpointAsync: (completedBatch, cancellationToken) =>
                PersistCheckpointAsync(
                    job,
                    checkpointId,
                    completedBatch.BufferedBatch.Batch,
                    completedBatch.BufferedBatch.Sequence,
                    runtime,
                    cancellationToken),
            publishBatchCompletedAsync: (completedBatch, cancellationToken) =>
                PublishBatchCompletedAsync(
                    job.Observers,
                    runtime.TimeProvider,
                    completedBatch.BufferedBatch,
                    completedBatch.Context,
                    completedBatch.ProcessingResult,
                    cancellationToken),
            publishCheckpointSavedAsync: (persistedCheckpoint, cancellationToken) =>
                PublishCheckpointSavedAsync(
                    job.Observers,
                    runtime.TimeProvider,
                    runtime.Job,
                    persistedCheckpoint,
                    cancellationToken));
    }

    private static async Task RunBatchWorkerAsync<TItem, TCheckpoint>(
        ChannelReader<BufferedSourceBatch<TItem, TCheckpoint>> reader,
        ChannelWriter<BatchWorkerNotification<TItem, TCheckpoint>> notifications,
        BatchJobDefinition<TItem, TCheckpoint> job,
        JobExecutionContext jobContext,
        CancellationToken cancellationToken)
    {
        if (job.ProcessingMode is not BatchProcessorRegistration<TItem, TCheckpoint> batchProcessor)
        {
            throw new BatchJobConfigurationException("Параллельный batch pipeline можно запускать только для batch processor режима.");
        }

        await foreach (var bufferedBatch in reader.ReadAllAsync(cancellationToken))
        {
            var batch = bufferedBatch.Batch;
            ValidateBatch(job, batch);

            var processingContext = CreateBatchProcessingContext(jobContext, bufferedBatch);

            await notifications.WriteAsync(
                new BatchWorkerStartedNotification<TItem, TCheckpoint>
                {
                    BufferedBatch = bufferedBatch,
                    Context = processingContext,
                },
                cancellationToken);

            try
            {
                var processingResult = await ExecuteBatchProcessorAsync(
                    job,
                    batch,
                    processingContext,
                    batchProcessor.Processor,
                    cancellationToken);

                await notifications.WriteAsync(
                    new BatchWorkerCompletedNotification<TItem, TCheckpoint>
                    {
                        BufferedBatch = bufferedBatch,
                        Context = processingContext,
                        ProcessingResult = processingResult,
                    },
                    cancellationToken);
            }
            catch (BatchJobTerminationException ex)
            {
                await notifications.WriteAsync(
                    new BatchWorkerFailedNotification<TItem, TCheckpoint>
                    {
                        BufferedBatch = bufferedBatch,
                        Context = processingContext,
                        Status = ex.Status,
                        Reason = ex.Message,
                        Exception = ex.InnerException ?? ex,
                        Attempt = ex.Attempt,
                        ItemIndex = ex.ItemIndex,
                    },
                    cancellationToken);

                break;
            }
            catch (BatchJobProcessingException ex)
            {
                await notifications.WriteAsync(
                    new BatchWorkerFailedNotification<TItem, TCheckpoint>
                    {
                        BufferedBatch = bufferedBatch,
                        Context = processingContext,
                        Status = JobCompletionStatus.Failed,
                        Reason = ex.Message,
                        Exception = ex.InnerException ?? ex,
                        Attempt = ex.Attempt,
                        ItemIndex = ex.ItemIndex,
                    },
                    cancellationToken);

                break;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                await notifications.WriteAsync(
                    new BatchWorkerFailedNotification<TItem, TCheckpoint>
                    {
                        BufferedBatch = bufferedBatch,
                        Context = processingContext,
                        Status = JobCompletionStatus.Failed,
                        Reason = "Обработка пакета завершилась ошибкой.",
                        Exception = ex,
                        Attempt = 1,
                        ItemIndex = null,
                    },
                    cancellationToken);

                break;
            }
        }
    }

    private static async Task CompleteBatchNotificationWriterAsync<TItem, TCheckpoint>(
        Task[] workerTasks,
        ChannelWriter<BatchWorkerNotification<TItem, TCheckpoint>> writer)
    {
        try
        {
            await Task.WhenAll(workerTasks).ConfigureAwait(false);
            writer.TryComplete();
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
        }
    }

    private static BatchProcessingContext<TCheckpoint> CreateBatchProcessingContext<TItem, TCheckpoint>(
        JobExecutionContext jobContext,
        BufferedSourceBatch<TItem, TCheckpoint> bufferedBatch) =>
        new()
        {
            Job = jobContext,
            BatchSequence = bufferedBatch.Sequence,
            BatchCheckpoint = bufferedBatch.Batch.Checkpoint,
            SourceMetadata = bufferedBatch.Batch.Metadata,
            Attempt = 1,
            ItemCount = bufferedBatch.Batch.Items.Count,
        };

    private static async ValueTask PublishBatchStartedAsync<TItem, TCheckpoint>(
        IReadOnlyList<IExecutionObserver<TCheckpoint>> observers,
        TimeProvider timeProvider,
        BufferedSourceBatch<TItem, TCheckpoint> bufferedBatch,
        BatchProcessingContext<TCheckpoint> processingContext,
        CancellationToken cancellationToken)
    {
        await PublishAsync(observers, new BatchStartedEvent<TCheckpoint>
        {
            Job = processingContext.Job,
            OccurredAt = timeProvider.GetUtcNow(),
            BatchSequence = bufferedBatch.Sequence,
            BatchCheckpoint = bufferedBatch.Batch.Checkpoint,
            Attempt = processingContext.Attempt,
            ItemCount = processingContext.ItemCount,
            SourceMetadata = bufferedBatch.Batch.Metadata,
        }, cancellationToken);
    }

    private static async ValueTask PublishBatchCompletedAsync<TItem, TCheckpoint>(
        IReadOnlyList<IExecutionObserver<TCheckpoint>> observers,
        TimeProvider timeProvider,
        BufferedSourceBatch<TItem, TCheckpoint> bufferedBatch,
        BatchProcessingContext<TCheckpoint> processingContext,
        ProcessingExecutionResult processingResult,
        CancellationToken cancellationToken)
    {
        await PublishAsync(observers, new BatchCompletedEvent<TCheckpoint>
        {
            Job = processingContext.Job,
            OccurredAt = timeProvider.GetUtcNow(),
            BatchSequence = bufferedBatch.Sequence,
            BatchCheckpoint = bufferedBatch.Batch.Checkpoint,
            Attempt = processingResult.AttemptCount,
            ItemCount = processingResult.Result.ItemCount,
            SucceededCount = processingResult.Result.SucceededCount,
            SkippedCount = processingResult.Result.SkippedCount,
            SourceMetadata = bufferedBatch.Batch.Metadata,
        }, cancellationToken);
    }

    private static async ValueTask PublishCheckpointSavedAsync<TCheckpoint>(
        IReadOnlyList<IExecutionObserver<TCheckpoint>> observers,
        TimeProvider timeProvider,
        JobExecutionContext jobContext,
        StoredCheckpoint<TCheckpoint> checkpoint,
        CancellationToken cancellationToken)
    {
        await PublishAsync(observers, new CheckpointSavedEvent<TCheckpoint>
        {
            Job = jobContext,
            OccurredAt = timeProvider.GetUtcNow(),
            Checkpoint = checkpoint,
        }, cancellationToken);
    }

    private static ValueTask<BatchJobExecutionSummary<TCheckpoint>> CompleteSuccessfulExecutionAsync<TCheckpoint>(
        IReadOnlyList<IExecutionObserver<TCheckpoint>> observers,
        BatchJobRuntimeState<TCheckpoint> runtime,
        CancellationToken cancellationToken)
    {
        runtime.Status = runtime.SkippedItemCount > 0
            ? JobCompletionStatus.CompletedWithSkips
            : JobCompletionStatus.Succeeded;

        runtime.Reason = runtime.Status == JobCompletionStatus.CompletedWithSkips
            ? "Задача завершена с пропусками."
            : "Задача завершена успешно.";

        return CompleteAsync(observers, runtime, cancellationToken, suppressObserverFailures: true);
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

        if (job.PrefetchCapacity <= 0)
        {
            throw new BatchJobConfigurationException("Емкость очереди предварительного чтения должна быть больше нуля.");
        }

        if (job.CheckpointStore is null && !string.IsNullOrWhiteSpace(job.CheckpointId))
        {
            throw new BatchJobConfigurationException(
                "Нельзя указывать checkpoint id без подключения хранилища контрольных точек.");
        }

        if (!job.Source.SupportsResume && job.CheckpointStore is not null)
        {
            throw new BatchJobConfigurationException(
                "Для источника без поддержки возобновления нельзя указывать хранилище контрольных точек.");
        }

        if (job.Observers.Any(static observer => observer is null))
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
        long batchSequence,
        BatchJobRuntimeState<TCheckpoint> runtime,
        CancellationToken cancellationToken)
    {
        var checkpoint = new StoredCheckpoint<TCheckpoint>
        {
            Value = batch.Checkpoint,
            SequenceNumber = batchSequence,
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
        return job.DegreeOfParallelism <= 1 || batch.Items.Count <= 1
            ? await ExecuteItemProcessorSequentialAsync(job, batch, context, processor, cancellationToken)
            : await ExecuteItemProcessorParallelAsync(job, batch, context, processor, cancellationToken);
    }

    private static async ValueTask<ProcessingExecutionResult> ExecuteItemProcessorSequentialAsync<TItem, TCheckpoint>(
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
            var itemResult = await ProcessSingleItemAsync(
                job,
                batch,
                context,
                batch.Items[index],
                index,
                processor,
                cancellationToken);

            maxAttempt = Math.Max(maxAttempt, itemResult.AttemptCount);

            if (itemResult.Disposition == ProcessingDisposition.Succeeded)
            {
                succeeded++;
                continue;
            }

            skipped++;

            if (itemResult.Issue is not null)
            {
                issues.Add(itemResult.Issue);
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

    private static async ValueTask<ProcessingExecutionResult> ExecuteItemProcessorParallelAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        SourceBatch<TItem, TCheckpoint> batch,
        BatchProcessingContext<TCheckpoint> context,
        IItemProcessor<TItem, TCheckpoint> processor,
        CancellationToken cancellationToken)
    {
        using var itemProcessingCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var processingToken = itemProcessingCancellationSource.Token;
        var workerCount = Math.Min(job.DegreeOfParallelism, batch.Items.Count);
        var nextIndex = -1;
        var succeeded = 0;
        var skipped = 0;
        var maxAttempt = 1;
        var issuesByIndex = new BatchProcessingIssue?[batch.Items.Count];
        ExceptionDispatchInfo? firstFailure = null;
        var sync = new object();

        var workerTasks = Enumerable.Range(0, workerCount)
            .Select(_ => RunItemWorkerAsync())
            .ToArray();

        async Task RunItemWorkerAsync()
        {
            while (true)
            {
                var itemIndex = Interlocked.Increment(ref nextIndex);

                if (itemIndex >= batch.Items.Count)
                {
                    break;
                }

                try
                {
                    var itemResult = await ProcessSingleItemAsync(
                        job,
                        batch,
                        context,
                        batch.Items[itemIndex],
                        itemIndex,
                        processor,
                        processingToken);

                    lock (sync)
                    {
                        maxAttempt = Math.Max(maxAttempt, itemResult.AttemptCount);

                        if (itemResult.Disposition == ProcessingDisposition.Succeeded)
                        {
                            succeeded++;
                            continue;
                        }

                        skipped++;
                        issuesByIndex[itemIndex] = itemResult.Issue;
                    }
                }
                catch (OperationCanceledException) when (processingToken.IsCancellationRequested
                                                         && !cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    var shouldCancel = false;

                    lock (sync)
                    {
                        if (firstFailure is null)
                        {
                            firstFailure = ExceptionDispatchInfo.Capture(ex);
                            shouldCancel = true;
                        }
                    }

                    if (shouldCancel)
                    {
                        await itemProcessingCancellationSource.CancelAsync();
                    }

                    break;
                }
            }
        }

        try
        {
            await Task.WhenAll(workerTasks).ConfigureAwait(false);
        }
        catch when (firstFailure is not null)
        {
            // Первичную ошибку повторно выбросим ниже с сохранением исходного стека.
        }

        if (firstFailure is not null)
        {
            firstFailure.Throw();
        }

        cancellationToken.ThrowIfCancellationRequested();

        return new ProcessingExecutionResult(
            BatchProcessingResult.Completed(
                batch.Items.Count,
                succeededCount: succeeded,
                skippedCount: skipped,
                issues: issuesByIndex.Where(static issue => issue is not null).Cast<BatchProcessingIssue>().ToArray(),
                reason: skipped > 0 ? "Пакет обработан с пропущенными элементами." : null),
            maxAttempt);
    }

    private static async ValueTask<ItemProcessingExecutionResult> ProcessSingleItemAsync<TItem, TCheckpoint>(
        BatchJobDefinition<TItem, TCheckpoint> job,
        SourceBatch<TItem, TCheckpoint> batch,
        BatchProcessingContext<TCheckpoint> context,
        TItem item,
        int itemIndex,
        IItemProcessor<TItem, TCheckpoint> processor,
        CancellationToken cancellationToken)
    {
        var attempt = 1;

        while (true)
        {
            try
            {
                var result = await processor.ProcessAsync(
                    item,
                    CreateItemProcessingContext(context, batch.Items.Count, itemIndex, attempt),
                    cancellationToken);

                ValidateItemProcessingResult(result, attempt, itemIndex);

                return result.Disposition == ProcessingDisposition.Succeeded
                    ? new ItemProcessingExecutionResult(ProcessingDisposition.Succeeded, attempt, Issue: null)
                    : new ItemProcessingExecutionResult(
                        ProcessingDisposition.Skipped,
                        attempt,
                        CreateItemProcessingIssue(itemIndex, result));
            }
            catch (BatchJobProcessingException)
            {
                throw;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var failureHandlingResult = await HandleItemProcessingFailureAsync(
                    job, batch, context, item, itemIndex, attempt, ex, cancellationToken);

                if (!failureHandlingResult.IsItemCompleted)
                {
                    attempt = failureHandlingResult.NextAttempt;
                    continue;
                }

                return new ItemProcessingExecutionResult(
                    ProcessingDisposition.Skipped,
                    attempt,
                    failureHandlingResult.Issue);
            }
        }
    }

    private static ItemProcessingContext<TCheckpoint> CreateItemProcessingContext<TCheckpoint>(
        BatchProcessingContext<TCheckpoint> batchContext,
        int batchItemCount,
        int itemIndex,
        int attempt) =>
        new()
        {
            Job = batchContext.Job,
            BatchSequence = batchContext.BatchSequence,
            BatchCheckpoint = batchContext.BatchCheckpoint,
            SourceMetadata = batchContext.SourceMetadata,
            ItemIndex = itemIndex,
            BatchItemCount = batchItemCount,
            Attempt = attempt,
        };

    private static BatchProcessingIssue CreateItemProcessingIssue(
        int itemIndex,
        ProcessingResult result) =>
        new()
        {
            ItemIndex = itemIndex,
            Code = result.Code,
            Message = result.Reason,
        };

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
