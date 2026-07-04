using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Failures;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Results;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Core.Execution;
using BatchFlow.Core.Execution.Exceptions;
using BatchFlow.Core.Execution.Models;
using BatchFlow.Core.Jobs;
using BatchFlow.Core.Processing;
using BatchFlow.Testing.Checkpoints;
using BatchFlow.Testing.Failures;
using BatchFlow.Testing.Observability;
using BatchFlow.Testing.Processing;
using BatchFlow.Testing.Sources;
using FluentAssertions;
using Xunit;

namespace BatchFlow.Core.Tests.Execution;

public sealed class BatchJobExecutionEngineTests
{
    private readonly BatchJobExecutionEngine _engine = new();

    [Fact]
    public async Task ExecuteAsync_WithSuccessfulItemProcessing_SavesCheckpointAndPublishesEvents()
    {
        var loadedCheckpoint = new StoredCheckpoint<long>
        {
            Value = 40,
            SequenceNumber = 8,
            ConcurrencyToken = "seed-token",
            SavedAt = new DateTimeOffset(2026, 6, 28, 12, 0, 0, TimeSpan.Zero),
        };

        var checkpointStore = new InMemoryCheckpointStore<long>();
        checkpointStore.Seed("users-migration", loadedCheckpoint);

        var source = new DelegateBatchSource<int, long>(
            (context, _) => ToAsyncEnumerable(
                CreateBatch([1, 2], checkpoint: 41L, metadata: "page-41"),
                CreateBatch([3], checkpoint: 42L, metadata: "page-42")));

        var observer = new RecordingExecutionObserver<long>();
        var itemAttempts = new List<int>();

        var job = new BatchJobDefinition<int, long>
        {
            JobName = "users-import",
            CheckpointId = "users-migration",
            Source = source,
            ProcessingMode = new ItemProcessorRegistration<int, long>(
                new DelegateItemProcessor<int, long>((item, context, _) =>
                {
                    itemAttempts.Add(context.Attempt);
                    return new ValueTask<ProcessingResult>(ProcessingResult.Success($"processed-{item}"));
                })),
            BatchSize = 2,
            DegreeOfParallelism = 1,
            CheckpointStore = checkpointStore,
            Observers = [observer],
        };

        var summary = await _engine.ExecuteAsync(new BatchJobExecutionRequest<int, long>
        {
            Job = job,
            RunId = "run-success-001",
        });

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        summary.ProcessedBatchCount.Should().Be(2);
        summary.SucceededItemCount.Should().Be(3);
        summary.SkippedItemCount.Should().Be(0);
        summary.ResumeFrom.Should().BeEquivalentTo(loadedCheckpoint);
        summary.LastPersistedCheckpoint.Should().NotBeNull();
        summary.LastPersistedCheckpoint!.Value.Should().Be(42);
        summary.LastPersistedCheckpoint.SequenceNumber.Should().Be(2);
        summary.Exception.Should().BeNull();

        source.CapturedContexts.Should().ContainSingle();
        source.CapturedContexts[0].RequestedBatchSize.Should().Be(2);
        source.CapturedContexts[0].ResumeFrom.Should().BeEquivalentTo(loadedCheckpoint);

        checkpointStore.LoadCallCount.Should().Be(1);
        checkpointStore.SaveCallCount.Should().Be(2);
        checkpointStore.SaveRequests[0].ExpectedConcurrencyToken.Should().Be("seed-token");
        checkpointStore.SavedCheckpoints.Should().HaveCount(2);
        checkpointStore.SaveRequests[1].ExpectedConcurrencyToken.Should().Be(checkpointStore.SavedCheckpoints[0].ConcurrencyToken);

        itemAttempts.Should().Equal(1, 1, 1);

        observer.Events.Select(@event => @event.GetType()).Should().Equal(
            typeof(JobStartedEvent<long>),
            typeof(BatchStartedEvent<long>),
            typeof(BatchCompletedEvent<long>),
            typeof(CheckpointSavedEvent<long>),
            typeof(BatchStartedEvent<long>),
            typeof(BatchCompletedEvent<long>),
            typeof(CheckpointSavedEvent<long>),
            typeof(JobCompletedEvent<long>));

        observer.Events.OfType<JobStartedEvent<long>>().Single().ResumeFrom.Should().BeEquivalentTo(loadedCheckpoint);
        observer.Events.OfType<JobCompletedEvent<long>>().Single().Status.Should().Be(JobCompletionStatus.Succeeded);
    }

    [Fact]
    public async Task ExecuteAsync_WithCheckpointStoreAndNoExplicitCheckpointId_UsesJobNameAsCheckpointId()
    {
        var loadedCheckpoint = new StoredCheckpoint<long>
        {
            Value = 9,
            SequenceNumber = 2,
            ConcurrencyToken = "seed-token",
        };

        var checkpointStore = new InMemoryCheckpointStore<long>();
        checkpointStore.Seed("users-import", loadedCheckpoint);

        var source = new DelegateBatchSource<int, long>(
            (context, _) => ToAsyncEnumerable(CreateBatch([10, 11], checkpoint: 12L, metadata: "page-12")));

        var job = new BatchJobDefinition<int, long>
        {
            JobName = "users-import",
            Source = source,
            ProcessingMode = new ItemProcessorRegistration<int, long>(
                new DelegateItemProcessor<int, long>((_, _, _) =>
                    new ValueTask<ProcessingResult>(ProcessingResult.Success()))),
            BatchSize = 2,
            DegreeOfParallelism = 1,
            CheckpointStore = checkpointStore,
        };

        var summary = await _engine.ExecuteAsync(new BatchJobExecutionRequest<int, long>
        {
            Job = job,
            RunId = "run-default-checkpoint-id",
        });

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        summary.Job.CheckpointId.Should().Be("users-import");
        summary.ResumeFrom.Should().BeEquivalentTo(loadedCheckpoint);
        summary.LastPersistedCheckpoint.Should().NotBeNull();
        summary.LastPersistedCheckpoint!.Value.Should().Be(12L);

        source.CapturedContexts.Should().ContainSingle();
        source.CapturedContexts[0].ResumeFrom.Should().BeEquivalentTo(loadedCheckpoint);

        checkpointStore.LoadCallCount.Should().Be(1);
        checkpointStore.SaveCallCount.Should().Be(1);
        checkpointStore.SaveRequests.Should().ContainSingle();
        checkpointStore.SaveRequests[0].CheckpointId.Should().Be("users-import");
    }

    [Fact]
    public async Task ExecuteAsync_WithNonResumableSourceAndCheckpointStore_ThrowsConfigurationException()
    {
        var source = new DelegateBatchSource<int, NoCheckpoint>(
            (_, _) => ToAsyncEnumerable(CreateBatch([1], NoCheckpoint.Value, metadata: "single")),
            supportsResume: false);

        var job = new BatchJobDefinition<int, NoCheckpoint>
        {
            JobName = "non-resumable",
            Source = source,
            ProcessingMode = new ItemProcessorRegistration<int, NoCheckpoint>(
                new DelegateItemProcessor<int, NoCheckpoint>((_, _, _) =>
                    new ValueTask<ProcessingResult>(ProcessingResult.Success()))),
            CheckpointStore = new InMemoryCheckpointStore<NoCheckpoint>(),
        };

        var action = () => _engine.ExecuteAsync(new BatchJobExecutionRequest<int, NoCheckpoint>
        {
            Job = job,
        }).AsTask();

        await action.Should()
            .ThrowAsync<BatchJobConfigurationException>()
            .WithMessage("*хранилище контрольных точек*");
    }

    [Fact]
    public async Task ExecuteAsync_WithCheckpointIdAndNoCheckpointStore_ThrowsConfigurationException()
    {
        var source = new DelegateBatchSource<int, long>(
            (_, _) => ToAsyncEnumerable(CreateBatch([1], checkpoint: 10L, metadata: "single")));

        var job = new BatchJobDefinition<int, long>
        {
            JobName = "users-import",
            CheckpointId = "users-import-v1",
            Source = source,
            ProcessingMode = new ItemProcessorRegistration<int, long>(
                new DelegateItemProcessor<int, long>((_, _, _) =>
                    new ValueTask<ProcessingResult>(ProcessingResult.Success()))),
        };

        var action = () => _engine.ExecuteAsync(new BatchJobExecutionRequest<int, long>
        {
            Job = job,
        }).AsTask();

        await action.Should()
            .ThrowAsync<BatchJobConfigurationException>()
            .WithMessage("*checkpoint id*");
    }

    [Fact]
    public async Task ExecuteAsync_WithResumableSourceAndNoCheckpointStore_DoesNotParticipateInCheckpointLifecycle()
    {
        var observer = new RecordingExecutionObserver<long>();
        var source = new DelegateBatchSource<int, long>(
            (context, _) => ToAsyncEnumerable(CreateBatch([1, 2], checkpoint: 15L, metadata: "page-15")));

        var job = new BatchJobDefinition<int, long>
        {
            JobName = "resumable-without-store",
            Source = source,
            ProcessingMode = new ItemProcessorRegistration<int, long>(
                new DelegateItemProcessor<int, long>((_, _, _) =>
                    new ValueTask<ProcessingResult>(ProcessingResult.Success()))),
            BatchSize = 2,
            DegreeOfParallelism = 1,
            Observers = [observer],
        };

        var summary = await _engine.ExecuteAsync(new BatchJobExecutionRequest<int, long>
        {
            Job = job,
            RunId = "run-resumable-without-store",
        });

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        summary.Job.CheckpointId.Should().BeNull();
        summary.ResumeFrom.Should().BeNull();
        summary.LastPersistedCheckpoint.Should().BeNull();

        source.CapturedContexts.Should().ContainSingle();
        source.CapturedContexts[0].ResumeFrom.Should().BeNull();

        observer.Events.OfType<JobStartedEvent<long>>().Single().ResumeFrom.Should().BeNull();
        observer.Events.OfType<CheckpointSavedEvent<long>>().Should().BeEmpty();
    }

    [Fact]
    public async Task ExecuteAsync_WhenFailurePolicyRetriesThenSkipsItem_ReturnsCompletedWithSkips()
    {
        var observer = new RecordingExecutionObserver<long>();

        var job = new BatchJobDefinition<int, long>
        {
            JobName = "skip-after-retry",
            Source = new DelegateBatchSource<int, long>((_, _) => ToAsyncEnumerable(CreateBatch([7], 100L, metadata: "single"))),
            ProcessingMode = new ItemProcessorRegistration<int, long>(
                new DelegateItemProcessor<int, long>((_, _, _) =>
                    throw new InvalidOperationException("temporary item failure"))),
            FailurePolicy = new DelegateFailurePolicy<int, long>((context, _) =>
                new ValueTask<FailureDecision>(
                    context.Attempt == 1
                        ? FailureDecision.Retry(TimeSpan.FromMilliseconds(1), "retry-once")
                        : FailureDecision.Skip("skip-after-retry"))),
            Observers = [observer],
        };

        var summary = await _engine.ExecuteAsync(new BatchJobExecutionRequest<int, long>
        {
            Job = job,
            RunId = "run-skip-001",
        });

        summary.Status.Should().Be(JobCompletionStatus.CompletedWithSkips);
        summary.ProcessedBatchCount.Should().Be(1);
        summary.SucceededItemCount.Should().Be(0);
        summary.SkippedItemCount.Should().Be(1);
        summary.Exception.Should().BeNull();

        var batchCompleted = observer.Events.OfType<BatchCompletedEvent<long>>().Single();
        batchCompleted.Attempt.Should().Be(2);
        batchCompleted.SkippedCount.Should().Be(1);

        observer.Events.OfType<BatchFailedEvent<long>>().Should().BeEmpty();
        observer.Events.OfType<JobCompletedEvent<long>>().Single().Status.Should().Be(JobCompletionStatus.CompletedWithSkips);
    }

    [Fact]
    public async Task ExecuteAsync_WhenBatchCompletesWithSkips_StillSavesCheckpoint()
    {
        var checkpointStore = new InMemoryCheckpointStore<long>();
        var observer = new RecordingExecutionObserver<long>();

        var job = new BatchJobDefinition<int, long>
        {
            JobName = "skip-persists-checkpoint",
            Source = new DelegateBatchSource<int, long>((_, _) => ToAsyncEnumerable(CreateBatch([1, 2], 44L, metadata: "page-44"))),
            ProcessingMode = new ItemProcessorRegistration<int, long>(
                new DelegateItemProcessor<int, long>((item, _, _) =>
                    new ValueTask<ProcessingResult>(
                        item == 2
                            ? ProcessingResult.Skipped("skip-item-2", code: "skip-2")
                            : ProcessingResult.Success()))),
            BatchSize = 2,
            DegreeOfParallelism = 1,
            CheckpointStore = checkpointStore,
            Observers = [observer],
        };

        var summary = await _engine.ExecuteAsync(new BatchJobExecutionRequest<int, long>
        {
            Job = job,
            RunId = "run-skips-save-checkpoint",
        });

        summary.Status.Should().Be(JobCompletionStatus.CompletedWithSkips);
        summary.ProcessedBatchCount.Should().Be(1);
        summary.SucceededItemCount.Should().Be(1);
        summary.SkippedItemCount.Should().Be(1);
        summary.LastPersistedCheckpoint.Should().NotBeNull();
        summary.LastPersistedCheckpoint!.Value.Should().Be(44L);
        summary.LastPersistedCheckpoint.SequenceNumber.Should().Be(1);

        checkpointStore.SaveCallCount.Should().Be(1);
        observer.Events.OfType<CheckpointSavedEvent<long>>().Should().ContainSingle();
        observer.Events.OfType<JobCompletedEvent<long>>().Single().Status.Should().Be(JobCompletionStatus.CompletedWithSkips);
    }

    [Fact]
    public async Task ExecuteAsync_WhenBatchProcessingFails_DoesNotSaveCheckpoint()
    {
        var checkpointStore = new InMemoryCheckpointStore<long>();

        var job = new BatchJobDefinition<int, long>
        {
            JobName = "failed-batch-without-checkpoint-save",
            Source = new DelegateBatchSource<int, long>((_, _) => ToAsyncEnumerable(CreateBatch([1], 99L, metadata: "single"))),
            ProcessingMode = new ItemProcessorRegistration<int, long>(
                new DelegateItemProcessor<int, long>((_, _, _) =>
                    throw new InvalidOperationException("processor failed"))),
            BatchSize = 1,
            DegreeOfParallelism = 1,
            CheckpointStore = checkpointStore,
        };

        var summary = await _engine.ExecuteAsync(new BatchJobExecutionRequest<int, long>
        {
            Job = job,
            RunId = "run-failed-batch-without-checkpoint-save",
        });

        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.ProcessedBatchCount.Should().Be(0);
        summary.LastPersistedCheckpoint.Should().BeNull();
        summary.Exception.Should().BeOfType<InvalidOperationException>();
        checkpointStore.SaveCallCount.Should().Be(0);
    }

    [Fact]
    public async Task ExecuteAsync_WhenCheckpointSaveConflicts_ReturnsFailedSummary()
    {
        var checkpointStore = new InMemoryCheckpointStore<long>
        {
            OnSaveAsync = (request, _) => ValueTask.FromException<StoredCheckpoint<long>>(
                new CheckpointConflictException(request.CheckpointId)),
        };

        var job = new BatchJobDefinition<int, long>
        {
            JobName = "checkpoint-conflict",
            Source = new DelegateBatchSource<int, long>((_, _) => ToAsyncEnumerable(CreateBatch([1], 11L, metadata: "single"))),
            ProcessingMode = new ItemProcessorRegistration<int, long>(
                new DelegateItemProcessor<int, long>((_, _, _) =>
                    new ValueTask<ProcessingResult>(ProcessingResult.Success()))),
            CheckpointStore = checkpointStore,
        };

        var summary = await _engine.ExecuteAsync(new BatchJobExecutionRequest<int, long>
        {
            Job = job,
            RunId = "run-checkpoint-conflict",
        });

        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.ProcessedBatchCount.Should().Be(1);
        summary.SucceededItemCount.Should().Be(1);
        summary.Exception.Should().BeOfType<CheckpointConflictException>();
        summary.Reason.Should().Be("Не удалось зафиксировать прогресс задачи из-за конфликта контрольной точки.");
        summary.LastPersistedCheckpoint.Should().BeNull();
    }

    [Fact]
    public async Task ExecuteAsync_WhenBatchCompletedObserverThrows_PersistsCheckpointBeforeFailingJob()
    {
        var checkpointStore = new InMemoryCheckpointStore<long>();
        var observer = new RecordingExecutionObserver<long>((@event, _) =>
        {
            if (@event is BatchCompletedEvent<long>)
            {
                throw new InvalidOperationException("observer failed after batch completion");
            }

            return ValueTask.CompletedTask;
        });

        var job = new BatchJobDefinition<int, long>
        {
            JobName = "observer-failure-after-completion",
            Source = new DelegateBatchSource<int, long>((_, _) => ToAsyncEnumerable(CreateBatch([1, 2], 77L, metadata: "page-77"))),
            ProcessingMode = new ItemProcessorRegistration<int, long>(
                new DelegateItemProcessor<int, long>((_, _, _) =>
                    new ValueTask<ProcessingResult>(ProcessingResult.Success()))),
            BatchSize = 2,
            DegreeOfParallelism = 1,
            CheckpointStore = checkpointStore,
            Observers = [observer],
        };

        var summary = await _engine.ExecuteAsync(new BatchJobExecutionRequest<int, long>
        {
            Job = job,
            RunId = "run-observer-failure-after-completion",
        });

        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.Reason.Should().Be("Наблюдатель выполнения завершился исключением.");
        summary.Exception.Should().NotBeNull();
        summary.Exception!.GetType().Name.Should().Be("BatchJobObserverException");
        summary.ProcessedBatchCount.Should().Be(1);
        summary.LastPersistedCheckpoint.Should().NotBeNull();
        summary.LastPersistedCheckpoint!.Value.Should().Be(77L);
        summary.LastPersistedCheckpoint.SequenceNumber.Should().Be(1);

        checkpointStore.SaveCallCount.Should().Be(1);
        observer.Events.OfType<BatchCompletedEvent<long>>().Should().ContainSingle();
        observer.Events.OfType<CheckpointSavedEvent<long>>().Should().BeEmpty();
        observer.Events.OfType<JobCompletedEvent<long>>().Should().ContainSingle();
    }

    [Fact]
    public async Task ExecuteAsync_WhenObserverThrowsOnJobCompleted_ReturnsSummary()
    {
        var observer = new RecordingExecutionObserver<long>((@event, _) =>
        {
            if (@event is JobCompletedEvent<long>)
            {
                throw new InvalidOperationException("observer failure");
            }

            return ValueTask.CompletedTask;
        });

        var job = new BatchJobDefinition<int, long>
        {
            JobName = "observer-failure",
            Source = new DelegateBatchSource<int, long>((_, _) => ToAsyncEnumerable(CreateBatch([1, 2], 5L, metadata: "single"))),
            ProcessingMode = new ItemProcessorRegistration<int, long>(
                new DelegateItemProcessor<int, long>((_, _, _) =>
                    new ValueTask<ProcessingResult>(ProcessingResult.Success()))),
            Observers = [observer],
        };

        var summary = await _engine.ExecuteAsync(new BatchJobExecutionRequest<int, long>
        {
            Job = job,
            RunId = "run-observer-failure",
        });

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        summary.ProcessedBatchCount.Should().Be(1);
        summary.Exception.Should().BeNull();
    }

    private static SourceBatch<int, TCheckpoint> CreateBatch<TCheckpoint>(
        IReadOnlyList<int> items,
        TCheckpoint checkpoint,
        string metadata)
        where TCheckpoint : notnull
    {
        return new SourceBatch<int, TCheckpoint>
        {
            Items = items,
            Checkpoint = checkpoint,
            Metadata = new Dictionary<string, string>
            {
                ["page"] = metadata,
            },
        };
    }

    private static async IAsyncEnumerable<SourceBatch<int, TCheckpoint>> ToAsyncEnumerable<TCheckpoint>(
        params SourceBatch<int, TCheckpoint>[] batches)
    {
        foreach (var batch in batches)
        {
            yield return batch;
            await Task.Yield();
        }
    }
}
