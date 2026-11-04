using System.Runtime.CompilerServices;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using FluentAssertions;
using Xunit;

namespace BatchFlow.EntityFrameworkCore.Tests.Sources;

public sealed class EntityFrameworkCoreBatchSourceBaseTests
{
    [Fact]
    public async Task ReadAsync_should_forward_context_and_batches_and_dispose_owned_context()
    {
        var dbContext = new TestDbContext();
        var context = TestBatchSource.CreateContext() with { ResumeFrom = new StoredCheckpoint<long> { Value = 10L } };
        var batch = TestBatchSource.CreateBatch(11, 12);
        var source = new TestBatchSource(Owned(dbContext), (db, actualContext, _) =>
        {
            db.Should().BeSameAs(dbContext);
            actualContext.Should().BeSameAs(context);
            db.DisposeCount.Should().Be(0);
            return TestBatchSource.Batches(batch);
        });

        var batches = await source.ReadAllAsync(context);

        batches.Should().ContainSingle().Which.Should().BeSameAs(batch);
        dbContext.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task ReadAsync_should_create_a_fresh_context_on_each_enumeration()
    {
        var contexts = new List<TestDbContext>();
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() =>
        {
            var context = new TestDbContext();
            contexts.Add(context);
            return context;
        }));

        await source.ReadAllAsync();
        await source.ReadAllAsync();

        contexts.Should().HaveCount(2).And.OnlyHaveUniqueItems();
        contexts.Should().OnlyContain(context => context.DisposeCount == 1);
    }

    [Fact]
    public async Task ReadAsync_should_dispose_owned_context_when_consumer_stops_early()
    {
        var context = new TestDbContext();
        var source = new TestBatchSource(Owned(context), (_, _, _) =>
            TestBatchSource.Batches(TestBatchSource.CreateBatch(1), TestBatchSource.CreateBatch(2)));

        await foreach (var _ in source.ReadAsync(TestBatchSource.CreateContext(), CancellationToken.None))
        {
            context.DisposeCount.Should().Be(0);
            break;
        }

        context.DisposeCount.Should().Be(1);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAsync_should_dispose_owned_context_after_read_failure(bool duringIteration)
    {
        var context = new TestDbContext();
        var failure = new InvalidOperationException("Ошибка чтения.");
        var source = new TestBatchSource(Owned(context), (_, _, _) =>
            duringIteration ? ThrowDuringIteration(failure) : throw failure);

        var action = () => source.ReadAllAsync();

        (await action.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(failure);
        context.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task ReadAsync_should_preserve_factory_failure()
    {
        var failure = new InvalidOperationException("Ошибка фабрики.");
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() => throw failure));

        var action = () => source.ReadAllAsync();

        (await action.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(failure);
    }

    [Fact]
    public async Task ReadAsync_should_reject_null_factory_result()
    {
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() => null!));

        var action = () => source.ReadAllAsync();

        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*null*");
    }

    [Fact]
    public async Task ReadAsync_should_not_invoke_factory_when_already_cancelled()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var calls = 0;
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() =>
        {
            calls++;
            return new TestDbContext();
        }));

        var action = () => source.ReadAllAsync(cancellationToken: cancellation.Token);

        await action.Should().ThrowAsync<OperationCanceledException>();
        calls.Should().Be(0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAsync_should_dispose_context_returned_after_cancellation_and_preserve_cancellation(bool disposalFails)
    {
        using var cancellation = new CancellationTokenSource();
        var context = new TestDbContext
        {
            DisposeException = disposalFails ? new InvalidOperationException("Ошибка освобождения.") : null,
        };
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(token =>
        {
            token.Should().Be(cancellation.Token);
            cancellation.Cancel();
            return ValueTask.FromResult(context);
        }));

        var action = () => source.ReadAllAsync(cancellationToken: cancellation.Token);

        (await action.Should().ThrowAsync<OperationCanceledException>()).Which.CancellationToken.Should().Be(cancellation.Token);
        context.DisposeCount.Should().Be(1);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAsync_should_forward_method_and_enumerator_cancellation(bool cancelEnumerator)
    {
        using var methodCancellation = new CancellationTokenSource();
        using var enumeratorCancellation = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var context = new TestDbContext();
        var source = new TestBatchSource(Owned(context), (_, _, token) => WaitForCancellation(entered, token));
        await using var enumerator = source.ReadAsync(TestBatchSource.CreateContext(), methodCancellation.Token)
            .GetAsyncEnumerator(enumeratorCancellation.Token);
        var move = enumerator.MoveNextAsync().AsTask();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        (cancelEnumerator ? enumeratorCancellation : methodCancellation).Cancel();

        Func<Task> action = async () => await move.WaitAsync(TimeSpan.FromSeconds(5));
        await action.Should().ThrowAsync<OperationCanceledException>();
        context.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task ReadAsync_should_keep_external_context_alive_between_enumerations()
    {
        await using var context = new TestDbContext();
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromContext(context));

        await source.ReadAllAsync();
        await source.ReadAllAsync();

        context.DisposeCount.Should().Be(0);
    }

    [Fact]
    public async Task ReadAsync_should_release_external_context_guard_after_failure()
    {
        await using var context = new TestDbContext();
        var calls = 0;
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromContext(context), (_, _, _) =>
            ++calls == 1 ? throw new InvalidOperationException("Ошибка чтения.") : TestBatchSource.Batches());
        var action = () => source.ReadAllAsync();

        await action.Should().ThrowAsync<InvalidOperationException>();
        await source.ReadAllAsync();

        calls.Should().Be(2);
        context.DisposeCount.Should().Be(0);
    }

    [Fact]
    public async Task ReadAsync_should_reject_concurrent_external_context_use_and_allow_reuse_after_disposal()
    {
        await using var context = new TestDbContext();
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromContext(context), (_, _, _) =>
            TestBatchSource.Batches(TestBatchSource.CreateBatch(1)));
        var first = source.ReadAsync(TestBatchSource.CreateContext(), CancellationToken.None).GetAsyncEnumerator();

        try
        {
            (await first.MoveNextAsync()).Should().BeTrue();
            var action = () => source.ReadAllAsync();
            await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*уже используется*");
            context.DisposeCount.Should().Be(0);
        }
        finally
        {
            await first.DisposeAsync();
        }

        (await source.ReadAllAsync()).Should().ContainSingle();
        context.DisposeCount.Should().Be(0);
    }

    [Fact]
    public async Task ReadAsync_should_allow_independent_factory_contexts_for_concurrent_enumerations()
    {
        var contexts = new List<TestDbContext>();
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() =>
        {
            var context = new TestDbContext();
            contexts.Add(context);
            return context;
        }), (_, _, _) => TestBatchSource.Batches(TestBatchSource.CreateBatch(1)));
        await using var first = source.ReadAsync(TestBatchSource.CreateContext(), CancellationToken.None).GetAsyncEnumerator();
        await using var second = source.ReadAsync(TestBatchSource.CreateContext(), CancellationToken.None).GetAsyncEnumerator();

        (await first.MoveNextAsync()).Should().BeTrue();
        (await second.MoveNextAsync()).Should().BeTrue();
        contexts.Should().HaveCount(2).And.OnlyHaveUniqueItems();
        contexts.Should().OnlyContain(context => context.DisposeCount == 0);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    public async Task ReadAsync_should_validate_batch_size_before_creating_context(int batchSize)
    {
        var calls = 0;
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() =>
        {
            calls++;
            return new TestDbContext();
        }));
        var action = () => source.ReadAllAsync(TestBatchSource.CreateContext(batchSize));

        await action.Should().ThrowAsync<ArgumentOutOfRangeException>();
        calls.Should().Be(0);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task ReadAsync_should_reject_invalid_batch_and_dispose_context(int invalidBatch)
    {
        var batch = invalidBatch switch
        {
            0 => null!,
            1 => new SourceBatch<int, long> { Items = null!, Checkpoint = 0L },
            2 => TestBatchSource.CreateBatch(),
            _ => TestBatchSource.CreateBatch(1, 2, 3),
        };
        var context = new TestDbContext();
        var source = new TestBatchSource(Owned(context), (_, _, _) => TestBatchSource.Batches(batch));
        var action = () => source.ReadAllAsync();

        await action.Should().ThrowAsync<InvalidOperationException>();
        context.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task ReadAsync_should_reject_null_sequence_and_dispose_context()
    {
        var context = new TestDbContext();
        var source = new TestBatchSource(Owned(context), (_, _, _) => null!);
        var action = () => source.ReadAllAsync();

        await action.Should().ThrowAsync<InvalidOperationException>();
        context.DisposeCount.Should().Be(1);
    }

    [Fact]
    public async Task ReadAsync_should_not_silently_ignore_resume_for_nonresumable_source()
    {
        var calls = 0;
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() =>
        {
            calls++;
            return new TestDbContext();
        }), supportsResume: false);
        var context = TestBatchSource.CreateContext() with { ResumeFrom = new StoredCheckpoint<long> { Value = 10L } };
        var action = () => source.ReadAllAsync(context);

        await action.Should().ThrowAsync<InvalidOperationException>();
        calls.Should().Be(0);
    }

    [Fact]
    public void Constructor_should_reject_null_options()
    {
        var action = () => new TestBatchSource(null!);

        action.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("contextOptions");
    }

    private static EntityFrameworkCoreContextOptions<TestDbContext> Owned(TestDbContext context) =>
        EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() => context);

    private static async IAsyncEnumerable<SourceBatch<int, long>> ThrowDuringIteration(Exception exception)
    {
        await Task.CompletedTask;
        yield return TestBatchSource.CreateBatch(1);
        throw exception;
    }

    private static async IAsyncEnumerable<SourceBatch<int, long>> WaitForCancellation(
        TaskCompletionSource entered,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        entered.TrySetResult();
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        yield break;
    }
}
