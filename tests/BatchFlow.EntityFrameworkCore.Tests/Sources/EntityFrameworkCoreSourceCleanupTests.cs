using BatchFlow;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Sources;
using BatchFlow.EntityFrameworkCore.Extensions;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using FluentAssertions;
using Xunit;

namespace BatchFlow.EntityFrameworkCore.Tests.Sources;

public sealed class EntityFrameworkCoreSourceCleanupTests
{
    [Theory]
    [InlineData(ReadFailureStage.ReadCore)]
    [InlineData(ReadFailureStage.GetEnumerator)]
    [InlineData(ReadFailureStage.MoveNext)]
    [InlineData(ReadFailureStage.Current)]
    public async Task ReadAsync_should_preserve_read_failure_when_both_cleanup_operations_fail(ReadFailureStage stage)
    {
        var primary = new InvalidOperationException("Первичная ошибка чтения.");
        var context = new TestDbContext { DisposeException = new InvalidOperationException("Ошибка освобождения контекста.") };
        var sequence = new CleanupProbeSequence
        {
            GetEnumeratorException = stage == ReadFailureStage.GetEnumerator ? primary : null,
            MoveNextException = stage == ReadFailureStage.MoveNext ? primary : null,
            CurrentException = stage == ReadFailureStage.Current ? primary : null,
            DisposeException = new InvalidOperationException("Ошибка освобождения перечислителя."),
        };
        var source = new TestBatchSource(Owned(context), (_, _, _) =>
            stage == ReadFailureStage.ReadCore ? throw primary : sequence);

        var action = () => source.ReadAllAsync();

        (await action.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(primary);
        context.DisposeCount.Should().Be(1);
        sequence.DisposeCount.Should().Be(stage is ReadFailureStage.ReadCore or ReadFailureStage.GetEnumerator ? 0 : 1);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAsync_should_preserve_cancellation_when_context_cleanup_fails(bool duringIteration)
    {
        using var cancellation = new CancellationTokenSource();
        var primary = new OperationCanceledException(cancellation.Token);
        var context = new TestDbContext { DisposeException = new InvalidOperationException("Ошибка освобождения контекста.") };
        var source = new TestBatchSource(Owned(context), (_, _, _) =>
        {
            if (duringIteration)
            {
                return CancelDuringIteration(cancellation, primary);
            }

            cancellation.Cancel();
            throw primary;
        });

        var action = () => source.ReadAllAsync(cancellationToken: cancellation.Token);

        var exception = (await action.Should().ThrowAsync<OperationCanceledException>()).Which;
        exception.Should().BeSameAs(primary);
        exception.CancellationToken.Should().Be(cancellation.Token);
        context.DisposeCount.Should().Be(1);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RunEachAsync_should_report_original_read_failure_when_context_cleanup_fails(bool duringIteration)
    {
        var primary = new InvalidOperationException("Первичная ошибка чтения.");
        var context = new TestDbContext { DisposeException = new InvalidOperationException("Ошибка освобождения контекста.") };
        var source = new TestBatchSource(Owned(context), (_, _, _) =>
            duringIteration ? FailDuringIteration(primary) : throw primary);

        var summary = await BatchJob.Named("ef-cleanup-failure")
            .FromEntityFrameworkCore(source)
            .RunEachAsync((_, _, _) => ValueTask.CompletedTask);

        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.Exception.Should().BeSameAs(primary);
        context.DisposeCount.Should().Be(1);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task ReadAsync_should_preserve_batch_validation_failure_when_cleanup_fails(int invalidBatch)
    {
        var batch = invalidBatch switch
        {
            0 => null!,
            1 => new SourceBatch<int, long> { Items = null!, Checkpoint = 0L },
            2 => TestBatchSource.CreateBatch(),
            _ => TestBatchSource.CreateBatch(1, 2, 3),
        };
        var context = new TestDbContext { DisposeException = new InvalidOperationException("Ошибка освобождения контекста.") };
        var source = new TestBatchSource(Owned(context), (_, _, _) => TestBatchSource.Batches(batch));

        var action = () => source.ReadAllAsync();

        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("Источник EF Core*");
        context.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task ReadAsync_should_preserve_null_sequence_failure_when_cleanup_fails()
    {
        var context = new TestDbContext { DisposeException = new InvalidOperationException("Ошибка освобождения контекста.") };
        var source = new TestBatchSource(Owned(context), (_, _, _) => null!);

        var action = () => source.ReadAllAsync();

        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*null вместо последовательности*");
        context.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task ReadAsync_should_preserve_null_enumerator_failure_when_cleanup_fails()
    {
        var context = new TestDbContext { DisposeException = new InvalidOperationException("Ошибка освобождения контекста.") };
        var sequence = new CleanupProbeSequence { ReturnNullEnumerator = true };
        var source = new TestBatchSource(Owned(context), (_, _, _) => sequence);

        var action = () => source.ReadAllAsync();

        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*null вместо перечислителя*");
        sequence.DisposeCount.Should().Be(0);
        context.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task ReadAsync_should_report_context_cleanup_failure_when_reading_succeeds()
    {
        var cleanup = new InvalidOperationException("Ошибка освобождения контекста.");
        var context = new TestDbContext { DisposeException = cleanup };
        var source = new TestBatchSource(Owned(context));

        var action = () => source.ReadAllAsync();

        (await action.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(cleanup);
        context.DisposeCount.Should().Be(1);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAsync_should_preserve_first_cleanup_failure_and_still_dispose_context(bool stopEarly)
    {
        var cleanup = new InvalidOperationException("Ошибка освобождения перечислителя.");
        var context = new TestDbContext { DisposeException = new InvalidOperationException("Ошибка освобождения контекста.") };
        var sequence = new CleanupProbeSequence { DisposeException = cleanup };
        var source = new TestBatchSource(Owned(context), (_, _, _) => sequence);
        Func<Task> action = async () =>
        {
            await foreach (var _ in source.ReadAsync(TestBatchSource.CreateContext(), CancellationToken.None))
            {
                if (stopEarly)
                {
                    break;
                }
            }
        };

        (await action.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(cleanup);
        sequence.DisposeCount.Should().Be(1);
        context.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task ReadAsync_should_release_external_context_guard_even_when_enumerator_cleanup_fails()
    {
        await using var context = new TestDbContext();
        var cleanup = new InvalidOperationException("Ошибка освобождения перечислителя.");
        var sequence = new CleanupProbeSequence { DisposeException = cleanup };
        var calls = 0;
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromContext(context), (_, _, _) =>
            ++calls == 1 ? sequence : TestBatchSource.Batches());
        var action = () => source.ReadAllAsync();

        (await action.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(cleanup);
        await source.ReadAllAsync();

        context.DisposeCount.Should().Be(0);
        calls.Should().Be(2);
    }

    private static EntityFrameworkCoreContextOptions<TestDbContext> Owned(TestDbContext context) =>
        EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() => context);

    private static async IAsyncEnumerable<SourceBatch<int, long>> FailDuringIteration(Exception exception)
    {
        await Task.Yield();
        yield return TestBatchSource.CreateBatch(1);
        throw exception;
    }

    private static async IAsyncEnumerable<SourceBatch<int, long>> CancelDuringIteration(
        CancellationTokenSource cancellation,
        OperationCanceledException exception)
    {
        await Task.Yield();
        yield return TestBatchSource.CreateBatch(1);
        cancellation.Cancel();
        throw exception;
    }

    private sealed class CleanupProbeSequence : IAsyncEnumerable<SourceBatch<int, long>>, IAsyncEnumerator<SourceBatch<int, long>>
    {
        private bool _hasReturnedBatch;

        public Exception? GetEnumeratorException { get; init; }

        public Exception? MoveNextException { get; init; }

        public Exception? CurrentException { get; init; }

        public Exception? DisposeException { get; init; }

        public bool ReturnNullEnumerator { get; init; }

        public int DisposeCount { get; private set; }

        public SourceBatch<int, long> Current => CurrentException is { } exception
            ? throw exception
            : TestBatchSource.CreateBatch(1);

        public IAsyncEnumerator<SourceBatch<int, long>> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            if (GetEnumeratorException is { } exception)
            {
                throw exception;
            }

            return ReturnNullEnumerator ? null! : this;
        }

        public ValueTask<bool> MoveNextAsync()
        {
            if (MoveNextException is { } exception)
            {
                return ValueTask.FromException<bool>(exception);
            }

            var hasNext = !_hasReturnedBatch;
            _hasReturnedBatch = true;
            return ValueTask.FromResult(hasNext);
        }

        public async ValueTask DisposeAsync()
        {
            DisposeCount++;
            await Task.Yield();

            if (DisposeException is { } exception)
            {
                throw exception;
            }
        }
    }

    public enum ReadFailureStage
    {
        ReadCore,
        GetEnumerator,
        MoveNext,
        Current,
    }
}
