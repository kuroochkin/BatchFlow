using System.Runtime.CompilerServices;
using BatchFlow;
using BatchFlow.Abstractions.Failures;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Results;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Testing.Observability;
using BatchFlow.Testing.Failures;
using BatchFlow.Testing.Sources;
using FluentAssertions;
using Xunit;

namespace BatchFlow.Core.Tests.Execution;

public sealed class ProcessingObservabilityTests
{
    [Fact]
    public async Task Queued_progress_should_be_delivered_after_another_worker_reports_terminal_failure()
    {
        var observer = new RecordingExecutionObserver<int>();
        var fatal = new InvalidOperationException("first batch stopped");
        var skipped = new InvalidOperationException("second batch skipped");
        var policy = new DelegateFailurePolicy<int, int>((context, _) => ValueTask.FromResult(
            context.BatchSequence == 1
                ? FailureDecision.Stop()
                : context.Attempt == 1 ? FailureDecision.Retry() : FailureDecision.Skip("skip-reason")));
        var source = new DelegateBatchSource<int, int>((_, _) => ImmediateBatchesAsync());
        var summary = await BatchJob.Named("queued-progress-after-stop").From(source)
            .WithPrefetchCapacity(2).WithDegreeOfParallelism(2).WithFailurePolicy(policy).WithObserver(observer)
            .RunBatchAsync((_, context, _) => ValueTask.FromException(context.BatchSequence == 1 ? fatal : skipped));

        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.Exception.Should().BeSameAs(fatal);
        observer.Events.OfType<BatchStartedEvent<int>>().Select(@event => @event.BatchSequence).Should().Equal(1, 2);
        observer.Events.OfType<ProcessingRetryEvent<int>>().Should().ContainSingle()
            .Which.Exception.Should().BeSameAs(skipped);
        observer.Events.OfType<ProcessingSkippedEvent<int>>().Should().ContainSingle()
            .Which.Exception.Should().BeSameAs(skipped);
    }

    [Theory]
    [InlineData(false, 1)]
    [InlineData(false, 3)]
    [InlineData(true, 1)]
    [InlineData(true, 3)]
    public async Task Foreign_cancellation_should_follow_configured_skip_policy(bool batchMode, int parallelism)
    {
        var observer = new RecordingExecutionObserver<int>();
        var expected = new TaskCanceledException("upstream request timed out");
        var builder = BatchJob.Named("skip-foreign-cancellation").From(Source(1, [1, 2, 3]))
            .WithDegreeOfParallelism(parallelism).WithSkipOnFailure().WithObserver(observer);
        var summary = batchMode
            ? await builder.RunBatchAsync((_, _, _) => ValueTask.FromException(expected))
            : await builder.RunEachAsync((_, _, _) => ValueTask.FromException<ProcessingResult>(expected));

        summary.Status.Should().Be(JobCompletionStatus.CompletedWithSkips);
        summary.SkippedItemCount.Should().Be(3);
        observer.Events.OfType<ProcessingSkippedEvent<int>>().Should().HaveCount(batchMode ? 1 : 3)
            .And.OnlyContain(@event => @event.Exception == expected);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Foreign_cancellation_should_be_retried_when_explicitly_enabled(bool batchMode)
    {
        var observer = new RecordingExecutionObserver<int>();
        var builder = BatchJob.Named("retry-foreign-cancellation").From(Source(1)).WithObserver(observer)
            .WithRetry(new BatchRetryOptions
            {
                MaxRetryAttempts = 1,
                ShouldHandle = exception => exception is TaskCanceledException,
            });
        var summary = batchMode
            ? await builder.RunBatchAsync((items, context, _) => context.Attempt == 1
                ? ValueTask.FromException<BatchProcessingResult>(new TaskCanceledException("upstream timeout"))
                : ValueTask.FromResult(BatchProcessingResult.AllSucceeded(items.Count)))
            : await builder.RunEachAsync((_, context, _) => context.Attempt == 1
                ? ValueTask.FromException<ProcessingResult>(new TaskCanceledException("upstream timeout"))
                : ValueTask.FromResult(ProcessingResult.Success()));

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        observer.Events.OfType<ProcessingRetryEvent<int>>().Should().ContainSingle();
    }

    [Fact]
    public async Task External_cancellation_should_not_be_skipped_by_failure_policy()
    {
        using var cancellation = new CancellationTokenSource();
        var observer = new RecordingExecutionObserver<int>();
        var summary = await BatchJob.Named("external-cancellation-not-skipped").From(Source(1)).WithSkipOnFailure()
            .WithObserver(observer).RunEachAsync((_, _, token) =>
            {
                cancellation.Cancel();
                token.ThrowIfCancellationRequested();
                return ValueTask.FromResult(ProcessingResult.Success());
            }, cancellation.Token);

        summary.Status.Should().Be(JobCompletionStatus.Cancelled);
        observer.Events.OfType<ProcessingSkippedEvent<int>>().Should().BeEmpty();
        observer.Events.OfType<ProcessingRetryEvent<int>>().Should().BeEmpty();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Foreign_cancellation_should_not_be_retried_by_default(bool batchMode)
    {
        var calls = 0;
        var observer = new RecordingExecutionObserver<int>();
        var builder = BatchJob.Named("default-foreign-cancellation").From(Source(1)).WithRetry(10).WithObserver(observer);
        var expected = new TaskCanceledException("local timeout");
        var summary = batchMode
            ? await builder.RunBatchAsync((_, _, _) =>
            {
                calls++;
                return ValueTask.FromException(expected);
            })
            : await builder.RunEachAsync((_, _, _) =>
            {
                calls++;
                return ValueTask.FromException<ProcessingResult>(expected);
            });

        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.Exception.Should().BeSameAs(expected);
        calls.Should().Be(1);
        observer.Events.OfType<ProcessingRetryEvent<int>>().Should().BeEmpty();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Foreign_cancellation_in_policy_should_preserve_failure_attempt_and_item_index(bool batchMode)
    {
        var observer = new RecordingExecutionObserver<int>();
        var expected = new TaskCanceledException("policy request timed out");
        var policy = new DelegateFailurePolicy<int, int>((context, _) => context.Attempt == 1
            ? ValueTask.FromResult(FailureDecision.Retry())
            : ValueTask.FromException<FailureDecision>(expected));
        var builder = BatchJob.Named("policy-local-cancellation").From(Source(1, [1, 2]))
            .WithFailurePolicy(policy).WithObserver(observer);
        var summary = batchMode
            ? await builder.RunBatchAsync((_, _, _) => ValueTask.FromException(new InvalidOperationException("handler failed")))
            : await builder.RunEachAsync((item, _, _) => item == 1
                ? ValueTask.FromResult(ProcessingResult.Success())
                : ValueTask.FromException<ProcessingResult>(new InvalidOperationException("handler failed")));

        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.Exception.Should().BeSameAs(expected);
        var failure = observer.Events.OfType<BatchFailedEvent<int>>().Single();
        failure.Attempt.Should().Be(2);
        failure.ItemIndex.Should().Be(batchMode ? null : 1);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task Batch_retry_and_skip_should_preserve_reasons_and_per_batch_event_order(int parallelism)
    {
        var expected = new InvalidOperationException("temporary failure");
        var observer = new RecordingExecutionObserver<int>();
        var summary = await BatchJob.Named("observed-batches").From(Source(3))
            .WithDegreeOfParallelism(parallelism).WithObserver(observer)
            .WithRetry(2).WithSkipOnFailure()
            .WithCheckpoint(new BatchFlow.Checkpoints.InMemoryCheckpointStore<int>())
            .RunBatchAsync((_, _, _) => ValueTask.FromException(expected));

        summary.Status.Should().Be(JobCompletionStatus.CompletedWithSkips);
        summary.LastPersistedCheckpoint!.Value.Should().Be(3);
        var retries = observer.Events.OfType<ProcessingRetryEvent<int>>().ToArray();
        retries.Should().HaveCount(6).And.OnlyContain(@event => @event.Exception == expected && @event.Scope == FailureScope.Batch);
        var skips = observer.Events.OfType<ProcessingSkippedEvent<int>>().ToArray();
        skips.Should().HaveCount(3).And.OnlyContain(@event => @event.Exception == expected && @event.SkippedCount == 1);
        skips.Should().OnlyContain(@event => @event.Issues.Count == 1 && !string.IsNullOrWhiteSpace(@event.Issues[0].Message));
        observer.Events.OfType<BatchCompletedEvent<int>>().Should().OnlyContain(@event => @event.Issues.Count == 1);

        for (var sequence = 1; sequence <= 3; sequence++)
        {
            retries.Where(@event => @event.BatchSequence == sequence).Select(@event => @event.Attempt).Should().Equal(1, 2);
            var startedIndex = observer.Events.ToList().FindIndex(@event => @event is BatchStartedEvent<int> started && started.BatchSequence == sequence);
            var retryIndex = observer.Events.ToList().FindIndex(@event => @event is ProcessingRetryEvent<int> retry && retry.BatchSequence == sequence);
            retryIndex.Should().BeGreaterThan(startedIndex);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task Item_retry_and_skip_should_include_item_index_and_deliver_observers_serially(int parallelism)
    {
        var activeObservers = 0;
        var observer = new RecordingExecutionObserver<int>(async (_, _) =>
        {
            Interlocked.Increment(ref activeObservers).Should().Be(1);
            await Task.Yield();
            Interlocked.Decrement(ref activeObservers);
        });
        var expected = new InvalidOperationException("item failure");
        var summary = await BatchJob.Named("observed-items").From(Source(1, [1, 2, 3]))
            .WithDegreeOfParallelism(parallelism).WithObserver(observer).WithRetry(1).WithSkipOnFailure()
            .RunEachAsync((_, _, _) => ValueTask.FromException<ProcessingResult>(expected));

        summary.Status.Should().Be(JobCompletionStatus.CompletedWithSkips);
        summary.SkippedItemCount.Should().Be(3);
        observer.Events.OfType<ProcessingRetryEvent<int>>().Select(@event => @event.ItemIndex).Should().BeEquivalentTo(new int?[] { 0, 1, 2 });
        observer.Events.OfType<ProcessingSkippedEvent<int>>().Should().HaveCount(3)
            .And.OnlyContain(@event => @event.Exception == expected && @event.Scope == FailureScope.Item && @event.Attempt == 2);
        observer.Events.OfType<BatchCompletedEvent<int>>().Single().Issues.Should().HaveCount(3);
    }

    [Fact]
    public async Task Explicit_item_skip_should_expose_code_and_reason_without_exception()
    {
        var observer = new RecordingExecutionObserver<int>();
        var summary = await BatchJob.Named("explicit-skip").From(Source(1)).WithObserver(observer)
            .RunEachAsync((_, _, _) => ValueTask.FromResult(ProcessingResult.Skipped("невалидные данные", "invalid-data")));

        summary.Status.Should().Be(JobCompletionStatus.CompletedWithSkips);
        var skipped = observer.Events.OfType<ProcessingSkippedEvent<int>>().Single();
        skipped.Reason.Should().Be("невалидные данные");
        skipped.Exception.Should().BeNull();
        skipped.Issues.Single().Code.Should().Be("invalid-data");
    }

    [Fact]
    public async Task Observer_failure_on_retry_should_not_be_retried_as_handler_failure()
    {
        var calls = 0;
        var observer = new RecordingExecutionObserver<int>((@event, _) => @event is ProcessingRetryEvent<int>
            ? ValueTask.FromException(new InvalidOperationException("observer failed"))
            : ValueTask.CompletedTask);
        var summary = await BatchJob.Named("retry-observer-failure").From(Source(1)).WithObserver(observer).WithRetry(100)
            .RunEachAsync((_, _, _) =>
            {
                Interlocked.Increment(ref calls);
                return ValueTask.FromException<ProcessingResult>(new InvalidOperationException("handler failed"));
            });

        summary.Status.Should().Be(JobCompletionStatus.Failed);
        calls.Should().Be(1);
        summary.Exception!.InnerException!.Message.Should().Be("observer failed");
    }

    private static IBatchSource<int, int> Source(int count, int[]? items = null) => new DelegateBatchSource<int, int>(
        (_, token) => ReadAsync(count, items, token));

    private static async IAsyncEnumerable<SourceBatch<int, int>> ImmediateBatchesAsync()
    {
        // Синхронное чтение позволяет обоим workers поставить исходы в очередь до начала ее обработки.
        await Task.CompletedTask;
        yield return new SourceBatch<int, int> { Items = [1], Checkpoint = 1 };
        yield return new SourceBatch<int, int> { Items = [2], Checkpoint = 2 };
    }

    private static async IAsyncEnumerable<SourceBatch<int, int>> ReadAsync(
        int count, int[]? items, [EnumeratorCancellation] CancellationToken token)
    {
        for (var i = 1; i <= count; i++)
        {
            token.ThrowIfCancellationRequested();
            yield return new SourceBatch<int, int> { Items = items ?? [i], Checkpoint = i };
            await Task.Yield();
        }
    }
}
