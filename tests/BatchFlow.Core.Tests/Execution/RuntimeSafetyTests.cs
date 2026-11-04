using System.Runtime.CompilerServices;
using BatchFlow;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Results;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Core.Execution.Buffering;
using BatchFlow.Core.Jobs;
using BatchFlow.Core.Processing;
using BatchFlow.Testing.Observability;
using BatchFlow.Testing.Processing;
using BatchFlow.Testing.Sources;
using FluentAssertions;
using Xunit;

namespace BatchFlow.Core.Tests.Execution;

public sealed class RuntimeSafetyTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Source_factory_failure_should_return_failed_summary(bool returnsNull)
    {
        var expected = new InvalidOperationException("source factory failed");
        var source = new DelegateBatchSource<int, int>((_, _) => returnsNull ? null! : throw expected);
        var observer = new RecordingExecutionObserver<int>();
        var summary = await BatchJob.Named("source-factory-failure").From(source).WithObserver(observer)
            .RunBatchAsync((_, _, _) => ValueTask.CompletedTask).AsTask().WaitAsync(Timeout);

        summary.Status.Should().Be(JobCompletionStatus.Failed);
        if (returnsNull)
        {
            summary.Exception.Should().BeOfType<InvalidOperationException>()
                .Which.Message.Should().Contain("пустую последовательность");
        }
        else
        {
            summary.Exception.Should().BeSameAs(expected);
        }
        observer.Events.OfType<JobStartedEvent<int>>().Should().ContainSingle();
        observer.Events.OfType<JobCompletedEvent<int>>().Should().ContainSingle();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task Internal_shutdown_should_cancel_token_passed_to_source_method(int parallelism)
    {
        using var externalCancellation = new CancellationTokenSource();
        var source = new CapturedReadTokenSource();
        var expected = new InvalidOperationException("handler failed while source was reading");
        var run = BatchJob.Named("source-method-token").From(source).WithDegreeOfParallelism(parallelism)
            .RunBatchAsync(async (_, _, _) =>
            {
                await source.ReadStarted.Task.WaitAsync(Timeout);
                throw expected;
            }, externalCancellation.Token).AsTask();

        try
        {
            await source.EnumerationCancelled.Task.WaitAsync(Timeout);
            source.ReadToken.IsCancellationRequested.Should().BeTrue();
            externalCancellation.IsCancellationRequested.Should().BeFalse();
        }
        finally
        {
            // Освобождаем зависший source и на старой реализации, чтобы регрессия не оставляла фоновых задач.
            externalCancellation.Cancel();
            await run.WaitAsync(Timeout);
        }

        var summary = await run.WaitAsync(Timeout);
        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.Exception.Should().BeSameAs(expected);
    }

    [Fact]
    public async Task Buffer_should_not_read_beyond_unconfirmed_window_until_commit()
    {
        var readCount = 0;
        var source = new DelegateBatchSource<int, int>((_, token) => ReadAsync(token));
        async IAsyncEnumerable<SourceBatch<int, int>> ReadAsync([EnumeratorCancellation] CancellationToken token)
        {
            for (var i = 1; i <= 2000; i++)
            {
                token.ThrowIfCancellationRequested();
                Interlocked.Increment(ref readCount);
                yield return new SourceBatch<int, int> { Items = [i], Checkpoint = i };
                await Task.Yield();
            }
        }

        var job = new BatchJobDefinition<int, int>
        {
            JobName = "bounded-window",
            Source = source,
            PrefetchCapacity = 1,
            DegreeOfParallelism = 2,
            ProcessingMode = new BatchProcessorRegistration<int, int>(new DelegateBatchProcessor<int, int>(
                (items, _, _) => ValueTask.FromResult(BatchProcessingResult.AllSucceeded(items.Count)))),
        };
        var context = new BatchSourceContext<int>
        {
            Job = new JobExecutionContext { JobName = job.JobName, RunId = "window" },
            RequestedBatchSize = 1,
        };

        await using var buffer = BufferedBatchChannel<int, int>.Start(job, token => source.ReadAsync(context, token), default);
        for (var i = 0; i < 3; i++)
        {
            await buffer.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        }

        var fourth = buffer.Reader.ReadAsync().AsTask();
        fourth.IsCompleted.Should().BeFalse();
        Volatile.Read(ref readCount).Should().Be(3);

        buffer.ConfirmBatch();
        (await fourth.WaitAsync(Timeout)).Sequence.Should().Be(4);
    }

    [Fact]
    public async Task Engine_should_wait_for_handlers_after_observer_failure()
    {
        var bothStarted = Signal();
        var cancellationObserved = Signal();
        var releaseHandlers = Signal();
        var active = 0;
        var started = 0;
        var expected = new InvalidOperationException("observer failed");
        var observer = new RecordingExecutionObserver<int>(async (@event, _) =>
        {
            if (@event is BatchStartedEvent<int>)
            {
                await bothStarted.Task.WaitAsync(Timeout);
                throw expected;
            }
        });
        var run = BatchJob.Named("shutdown-observer").From(Source(2))
            .WithDegreeOfParallelism(2).WithObserver(observer)
            .RunBatchAsync(async (_, _, token) =>
            {
                Interlocked.Increment(ref active);
                using var registration = token.Register(() => cancellationObserved.TrySetResult());
                if (Interlocked.Increment(ref started) == 2)
                {
                    bothStarted.TrySetResult();
                }

                try
                {
                    await releaseHandlers.Task.WaitAsync(Timeout);
                }
                finally
                {
                    Interlocked.Decrement(ref active);
                }
            }).AsTask();

        try
        {
            await cancellationObserved.Task.WaitAsync(Timeout);
            run.IsCompleted.Should().BeFalse();
            Volatile.Read(ref active).Should().Be(2);
        }
        finally
        {
            releaseHandlers.TrySetResult();
        }

        var summary = await run.WaitAsync(Timeout);
        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.Exception!.InnerException.Should().BeSameAs(expected);
        Volatile.Read(ref active).Should().Be(0);
    }

    [Fact]
    public async Task Engine_should_preserve_handler_failure_when_idle_workers_are_cancelled()
    {
        var started = Signal();
        var expected = new InvalidOperationException("original failure");
        var source = new DelegateBatchSource<int, int>((_, token) => ReadAsync(token));
        async IAsyncEnumerable<SourceBatch<int, int>> ReadAsync([EnumeratorCancellation] CancellationToken token)
        {
            yield return new SourceBatch<int, int> { Items = [1], Checkpoint = 1 };
            yield return new SourceBatch<int, int> { Items = [2], Checkpoint = 2 };
            await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, token);
        }

        var summary = await BatchJob.Named("primary-failure").From(source).WithDegreeOfParallelism(3)
            .RunBatchAsync(async (items, _, token) =>
            {
                if (items[0] == 1)
                {
                    started.TrySetResult();
                    await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, token);
                }
                else
                {
                    await started.Task.WaitAsync(Timeout);
                    throw expected;
                }
            }).AsTask().WaitAsync(Timeout);

        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.Exception.Should().BeSameAs(expected);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task Engine_should_return_cancelled_summary_independent_of_final_observer(int parallelism)
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var recording = new RecordingExecutionObserver<int>();
        var throwing = new RecordingExecutionObserver<int>((_, _) =>
            ValueTask.FromException(new OperationCanceledException("diagnostic cancellation")));

        var summary = await BatchJob.Named("cancel-before-start").From(Source(2))
            .WithDegreeOfParallelism(parallelism).WithObservers(throwing, recording)
            .RunBatchAsync((_, _, _) => ValueTask.CompletedTask, cancellation.Token);

        summary.Status.Should().Be(JobCompletionStatus.Cancelled);
        recording.Events.OfType<JobCompletedEvent<int>>().Should().ContainSingle()
            .Which.Status.Should().Be(JobCompletionStatus.Cancelled);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task Engine_should_await_handlers_and_return_cancelled_summary_during_processing(int parallelism)
    {
        using var cancellation = new CancellationTokenSource();
        var entered = Signal();
        var finished = 0;
        var observer = new RecordingExecutionObserver<int>((_, token) =>
        {
            token.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        });
        var run = BatchJob.Named("cancel-processing").From(Source(20))
            .WithDegreeOfParallelism(parallelism).WithObserver(observer)
            .RunBatchAsync(async (_, _, token) =>
            {
                entered.TrySetResult();
                try
                {
                    await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, token);
                }
                finally
                {
                    Interlocked.Increment(ref finished);
                }
            }, cancellation.Token).AsTask();

        await entered.Task.WaitAsync(Timeout);
        cancellation.Cancel();
        var summary = await run.WaitAsync(Timeout);

        summary.Status.Should().Be(JobCompletionStatus.Cancelled);
        Volatile.Read(ref finished).Should().BeGreaterThan(0);
        observer.Events.OfType<JobCompletedEvent<int>>().Should().ContainSingle();
    }

    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Fact]
    public async Task Long_first_batch_should_bound_out_of_order_completions_and_resume_after_commit()
    {
        var firstRelease = Signal();
        var windowProcessed = Signal();
        var readCount = 0;
        var source = new DelegateBatchSource<int, int>((_, token) => ReadAsync(token));
        async IAsyncEnumerable<SourceBatch<int, int>> ReadAsync([EnumeratorCancellation] CancellationToken token)
        {
            for (var i = 1; i <= 2000; i++)
            {
                token.ThrowIfCancellationRequested();
                Interlocked.Increment(ref readCount);
                yield return new SourceBatch<int, int> { Items = [i], Checkpoint = i };
                await Task.Yield();
            }
        }

        var store = new BatchFlow.Testing.Checkpoints.InMemoryCheckpointStore<int>();
        var run = BatchJob.Named("long-first-batch").From(source).WithPrefetchCapacity(1)
            .WithDegreeOfParallelism(2).WithCheckpoint(store)
            .RunBatchAsync(async (items, _, token) =>
            {
                if (items[0] == 1)
                {
                    await firstRelease.Task.WaitAsync(Timeout, token);
                }
                else if (items[0] == 3)
                {
                    windowProcessed.TrySetResult();
                }
            }).AsTask();

        try
        {
            await windowProcessed.Task.WaitAsync(Timeout);
            Volatile.Read(ref readCount).Should().Be(3);
            store.SaveCallCount.Should().Be(0);
            run.IsCompleted.Should().BeFalse();
        }
        finally
        {
            firstRelease.TrySetResult();
        }

        var summary = await run.WaitAsync(Timeout);
        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        summary.LastPersistedCheckpoint!.Value.Should().Be(2000);
        store.SavedCheckpoints.Should().HaveCount(2000);
    }

    [Theory]
    [InlineData("source", 1)]
    [InlineData("source", 3)]
    [InlineData("retry", 1)]
    [InlineData("retry", 3)]
    [InlineData("save", 1)]
    [InlineData("save", 3)]
    public async Task Engine_should_return_cancelled_summary_at_each_io_boundary(string site, int parallelism)
    {
        using var cancellation = new CancellationTokenSource();
        var entered = Signal();
        var store = new BatchFlow.Testing.Checkpoints.InMemoryCheckpointStore<int>
        {
            OnSaveAsync = async (_, token) =>
            {
                entered.TrySetResult();
                await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("Недостижимое сохранение после отмены.");
            },
        };
        var source = new DelegateBatchSource<int, int>((_, token) => ReadAsync(token));
        async IAsyncEnumerable<SourceBatch<int, int>> ReadAsync([EnumeratorCancellation] CancellationToken token)
        {
            if (site == "source")
            {
                entered.TrySetResult();
                await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, token);
            }

            yield return new SourceBatch<int, int> { Items = [1], Checkpoint = 1 };
        }

        var observer = new RecordingExecutionObserver<int>((@event, _) =>
        {
            if (@event is ProcessingRetryEvent<int>)
            {
                entered.TrySetResult();
            }
            return ValueTask.CompletedTask;
        });
        var run = BatchJob.Named("cancel-boundaries").From(source).WithDegreeOfParallelism(parallelism)
            .WithCheckpoint(store).WithObserver(observer)
            .WithRetry(new BatchRetryOptions { MaxRetryAttempts = 1, Delay = TimeSpan.FromHours(1) })
            .RunBatchAsync((_, _, _) => site == "retry"
                ? ValueTask.FromException(new InvalidOperationException("transient"))
                : ValueTask.CompletedTask, cancellation.Token).AsTask();

        await entered.Task.WaitAsync(Timeout);
        cancellation.Cancel();
        var summary = await run.WaitAsync(Timeout);
        summary.Status.Should().Be(JobCompletionStatus.Cancelled);
        summary.LastPersistedCheckpoint.Should().BeNull();
        observer.Events.OfType<JobCompletedEvent<int>>().Should().ContainSingle();
    }

    [Fact]
    public async Task Engine_should_not_treat_unrelated_operation_cancelled_exception_as_external_cancellation()
    {
        var expected = new OperationCanceledException("foreign cancellation");
        var summary = await BatchJob.Named("foreign-cancellation").From(Source(1))
            .RunBatchAsync((_, _, _) => ValueTask.FromException(expected));
        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.Exception.Should().BeSameAs(expected);
    }

    [Theory]
    [InlineData("source")]
    [InlineData("save")]
    public async Task Engine_should_stop_and_await_handlers_after_source_or_checkpoint_failure(string failureSite)
    {
        var entered = Signal();
        var cancelled = Signal();
        var release = Signal();
        var expected = new InvalidOperationException($"{failureSite} failed");
        var active = 0;
        var source = new DelegateBatchSource<int, int>((_, token) => ReadAsync(token));
        async IAsyncEnumerable<SourceBatch<int, int>> ReadAsync([EnumeratorCancellation] CancellationToken token)
        {
            yield return new SourceBatch<int, int> { Items = [1], Checkpoint = 1 };
            if (failureSite == "source")
            {
                await entered.Task.WaitAsync(Timeout, token);
                throw expected;
            }

            yield return new SourceBatch<int, int> { Items = [2], Checkpoint = 2 };
        }

        var store = new BatchFlow.Testing.Checkpoints.InMemoryCheckpointStore<int>
        {
            OnSaveAsync = (_, _) => ValueTask.FromException<StoredCheckpoint<int>>(expected),
        };
        var run = BatchJob.Named("shutdown-dependencies").From(source).WithDegreeOfParallelism(2)
            .WithCheckpoint(store)
            .RunBatchAsync(async (items, _, token) =>
            {
                if (failureSite == "save" && items[0] == 1)
                {
                    await entered.Task.WaitAsync(Timeout, token);
                    return;
                }

                Interlocked.Increment(ref active);
                using var registration = token.Register(() => cancelled.TrySetResult());
                entered.TrySetResult();
                try
                {
                    await release.Task.WaitAsync(Timeout);
                }
                finally
                {
                    Interlocked.Decrement(ref active);
                }
            }).AsTask();

        try
        {
            await cancelled.Task.WaitAsync(Timeout);
            run.IsCompleted.Should().BeFalse();
            Volatile.Read(ref active).Should().Be(1);
        }
        finally
        {
            release.TrySetResult();
        }

        var summary = await run.WaitAsync(Timeout);
        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.Exception.Should().BeSameAs(expected);
        Volatile.Read(ref active).Should().Be(0);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(8)]
    public async Task Engine_stress_should_keep_entire_unconfirmed_window_bounded_and_commit_all_batches(int parallelism)
    {
        var read = 0;
        var committed = 0;
        var source = new DelegateBatchSource<int, int>((_, token) => ReadAsync(token));
        async IAsyncEnumerable<SourceBatch<int, int>> ReadAsync([EnumeratorCancellation] CancellationToken token)
        {
            for (var i = 1; i <= 2000; i++)
            {
                token.ThrowIfCancellationRequested();
                var pending = Interlocked.Increment(ref read) - Volatile.Read(ref committed);
                pending.Should().BeLessThanOrEqualTo(parallelism + 1);
                yield return new SourceBatch<int, int> { Items = [i], Checkpoint = i };
                await Task.Yield();
            }
        }

        var store = new BatchFlow.Testing.Checkpoints.InMemoryCheckpointStore<int>
        {
            OnSaveAsync = (request, _) =>
            {
                Volatile.Write(ref committed, request.Checkpoint.Value);
                return ValueTask.FromResult(request.Checkpoint with { ConcurrencyToken = Guid.NewGuid().ToString("N") });
            },
        };
        var summary = await BatchJob.Named("window-stress").From(source).WithDegreeOfParallelism(parallelism)
            .WithPrefetchCapacity(1).WithCheckpoint(store)
            .RunBatchAsync(async (_, _, _) => await Task.Yield()).AsTask().WaitAsync(Timeout);

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        summary.ProcessedBatchCount.Should().Be(2000);
        summary.LastPersistedCheckpoint!.Value.Should().Be(2000);
        store.SaveRequests.Select(request => request.Checkpoint.SequenceNumber)
            .Should().Equal(Enumerable.Range(1, 2000).Select(i => (long?)i));
    }

    private static IBatchSource<int, int> Source(int count) => new DelegateBatchSource<int, int>(
        (_, token) => ReadRangeAsync(count, token));

    private static async IAsyncEnumerable<SourceBatch<int, int>> ReadRangeAsync(
        int count,
        [EnumeratorCancellation] CancellationToken token)
    {
        for (var i = 1; i <= count; i++)
        {
            token.ThrowIfCancellationRequested();
            yield return new SourceBatch<int, int> { Items = [i], Checkpoint = i };
            await Task.Yield();
        }
    }

    private sealed class CapturedReadTokenSource : IBatchSource<int, int>
    {
        public TaskCompletionSource ReadStarted { get; } = Signal();
        public TaskCompletionSource EnumerationCancelled { get; } = Signal();
        public CancellationToken ReadToken { get; private set; }

        public IAsyncEnumerable<SourceBatch<int, int>> ReadAsync(BatchSourceContext<int> context, CancellationToken cancellationToken)
        {
            ReadToken = cancellationToken;
            return ReadCoreAsync(cancellationToken);
        }

        private async IAsyncEnumerable<SourceBatch<int, int>> ReadCoreAsync(
            CancellationToken readToken,
            [EnumeratorCancellation] CancellationToken enumerationToken = default)
        {
            using var registration = enumerationToken.Register(() => EnumerationCancelled.TrySetResult());
            yield return new SourceBatch<int, int> { Items = [1], Checkpoint = 1 };
            enumerationToken.ThrowIfCancellationRequested();
            ReadStarted.TrySetResult();
            await Task.Delay(System.Threading.Timeout.InfiniteTimeSpan, readToken);
        }
    }
}
