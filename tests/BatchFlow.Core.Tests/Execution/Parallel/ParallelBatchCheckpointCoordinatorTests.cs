using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Results;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Core.Execution.Buffering;
using BatchFlow.Core.Execution.Models;
using BatchFlow.Core.Execution.Parallel;
using FluentAssertions;
using Xunit;

namespace BatchFlow.Core.Tests.Execution.Parallel;

public sealed class ParallelBatchCheckpointCoordinatorTests
{
    [Fact]
    public async Task FlushAsync_should_commit_only_contiguous_completed_batches_in_order()
    {
        var runtime = CreateRuntimeState();
        var persistedSequences = new List<long>();
        var publishedBatchSequences = new List<long>();
        var publishedCheckpointSequences = new List<long>();

        var coordinator = CreateCoordinator(
            runtime,
            persistedSequences,
            publishedBatchSequences,
            publishedCheckpointSequences);

        coordinator.TryTrackCompletion(CreateCompletedBatch(sequence: 2, checkpoint: 102L)).Should().BeTrue();

        await coordinator.FlushAsync(CancellationToken.None);

        persistedSequences.Should().BeEmpty();
        publishedBatchSequences.Should().BeEmpty();
        publishedCheckpointSequences.Should().BeEmpty();
        coordinator.NextCheckpointSequence.Should().Be(1);
        coordinator.BufferedCompletionCount.Should().Be(1);

        coordinator.TryTrackCompletion(CreateCompletedBatch(sequence: 1, checkpoint: 101L)).Should().BeTrue();

        await coordinator.FlushAsync(CancellationToken.None);

        persistedSequences.Should().Equal(1, 2);
        publishedBatchSequences.Should().Equal(1, 2);
        publishedCheckpointSequences.Should().Equal(1, 2);
        coordinator.NextCheckpointSequence.Should().Be(3);
        coordinator.BufferedCompletionCount.Should().Be(0);
        runtime.BatchSequence.Should().Be(2);
        runtime.LastPersistedCheckpoint.Should().NotBeNull();
        runtime.LastPersistedCheckpoint!.SequenceNumber.Should().Be(2);
        runtime.LastPersistedCheckpoint.Value.Should().Be(102L);
    }

    [Fact]
    public async Task RegisterTerminalFailure_should_prune_uncommittable_batches_and_reject_future_sequences()
    {
        var runtime = CreateRuntimeState();
        var persistedSequences = new List<long>();
        var publishedBatchSequences = new List<long>();
        var publishedCheckpointSequences = new List<long>();

        var coordinator = CreateCoordinator(
            runtime,
            persistedSequences,
            publishedBatchSequences,
            publishedCheckpointSequences);

        coordinator.TryTrackCompletion(CreateCompletedBatch(sequence: 4, checkpoint: 104L)).Should().BeTrue();
        coordinator.BufferedCompletionCount.Should().Be(1);

        coordinator.RegisterTerminalFailure(failedBatchSequence: 3);

        coordinator.StopBeforeSequence.Should().Be(3);
        coordinator.BufferedCompletionCount.Should().Be(0);
        coordinator.TryTrackCompletion(CreateCompletedBatch(sequence: 3, checkpoint: 103L)).Should().BeFalse();
        coordinator.TryTrackCompletion(CreateCompletedBatch(sequence: 5, checkpoint: 105L)).Should().BeFalse();
        coordinator.TryTrackCompletion(CreateCompletedBatch(sequence: 2, checkpoint: 102L)).Should().BeTrue();

        await coordinator.FlushAsync(CancellationToken.None);

        persistedSequences.Should().BeEmpty();

        coordinator.TryTrackCompletion(CreateCompletedBatch(sequence: 1, checkpoint: 101L)).Should().BeTrue();

        await coordinator.FlushAsync(CancellationToken.None);

        persistedSequences.Should().Equal(1, 2);
        publishedBatchSequences.Should().Equal(1, 2);
        publishedCheckpointSequences.Should().Equal(1, 2);
        coordinator.NextCheckpointSequence.Should().Be(3);
        coordinator.StopBeforeSequence.Should().Be(3);
        coordinator.BufferedCompletionCount.Should().Be(0);
        runtime.LastPersistedCheckpoint.Should().NotBeNull();
        runtime.LastPersistedCheckpoint!.SequenceNumber.Should().Be(2);
        runtime.LastPersistedCheckpoint.Value.Should().Be(102L);
    }

    private static ParallelBatchCheckpointCoordinator<int, long> CreateCoordinator(
        BatchJobRuntimeState<long> runtime,
        List<long> persistedSequences,
        List<long> publishedBatchSequences,
        List<long> publishedCheckpointSequences)
    {
        return new ParallelBatchCheckpointCoordinator<int, long>(
            runtime,
            persistCheckpointAsync: (completedBatch, _) =>
            {
                persistedSequences.Add(completedBatch.BufferedBatch.Sequence);

                return new ValueTask<StoredCheckpoint<long>>(new StoredCheckpoint<long>
                {
                    Value = completedBatch.BufferedBatch.Batch.Checkpoint,
                    SequenceNumber = completedBatch.BufferedBatch.Sequence,
                    ConcurrencyToken = $"token-{completedBatch.BufferedBatch.Sequence}",
                    SavedAt = new DateTimeOffset(2026, 7, 5, 12, 0, 0, TimeSpan.Zero),
                    Metadata = completedBatch.BufferedBatch.Batch.Metadata,
                });
            },
            publishBatchCompletedAsync: (completedBatch, _) =>
            {
                publishedBatchSequences.Add(completedBatch.BufferedBatch.Sequence);
                return ValueTask.CompletedTask;
            },
            publishCheckpointSavedAsync: (checkpoint, _) =>
            {
                publishedCheckpointSequences.Add(checkpoint.SequenceNumber ?? 0);
                return ValueTask.CompletedTask;
            });
    }

    private static BatchJobRuntimeState<long> CreateRuntimeState() =>
        new(
            new JobExecutionContext
            {
                JobName = "parallel-checkpoint-test",
                CheckpointId = "parallel-checkpoint-test",
                RunId = "run-parallel-checkpoint-test",
                BatchSize = 1,
                DegreeOfParallelism = 4,
                PrefetchCapacity = 2,
            },
            TimeProvider.System);

    private static BatchWorkerCompletedNotification<int, long> CreateCompletedBatch(long sequence, long checkpoint) =>
        new()
        {
            BufferedBatch = new BufferedSourceBatch<int, long>
            {
                Sequence = sequence,
                Batch = new SourceBatch<int, long>
                {
                    Items = [checked((int)checkpoint)],
                    Checkpoint = checkpoint,
                    Metadata = new Dictionary<string, string>
                    {
                        ["page"] = $"page-{checkpoint}",
                    },
                },
            },
            Context = new BatchProcessingContext<long>
            {
                Job = new JobExecutionContext
                {
                    JobName = "parallel-checkpoint-test",
                    CheckpointId = "parallel-checkpoint-test",
                    RunId = $"run-{sequence}",
                    BatchSize = 1,
                    DegreeOfParallelism = 4,
                    PrefetchCapacity = 2,
                },
                BatchSequence = sequence,
                BatchCheckpoint = checkpoint,
                SourceMetadata = new Dictionary<string, string>
                {
                    ["page"] = $"page-{checkpoint}",
                },
                Attempt = 1,
                ItemCount = 1,
            },
            ProcessingResult = new ProcessingExecutionResult(
                BatchProcessingResult.AllSucceeded(itemCount: 1),
                AttemptCount: 1),
        };
}