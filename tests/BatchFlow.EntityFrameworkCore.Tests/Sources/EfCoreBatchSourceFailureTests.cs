using BatchFlow;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Checkpoints;
using BatchFlow.EntityFrameworkCore.Extensions;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using FluentAssertions;
using Xunit;

namespace BatchFlow.EntityFrameworkCore.Tests.Sources;

[Trait("Category", "EntityFrameworkCoreIntegration")]
public sealed class EfCoreBatchSourceFailureTests
{
    [Theory]
    [InlineData("query")]
    [InlineData("ordering")]
    [InlineData("predicate")]
    [InlineData("selector")]
    [InlineData("comparer")]
    [InlineData("sql")]
    public async Task ReadAsync_should_preserve_primary_failure_when_context_disposal_also_fails(string stage)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 }, new SourceTestRow { Id = 2 });
        var primary = new InvalidOperationException("Первичная ошибка чтения порции.");
        database.Factory.ConfigureContext = context => context.DisposeFailure = new InvalidOperationException("Вторичная ошибка очистки.");
        var valid = database.CreateSourceOptions();
        var options = stage switch
        {
            "query" => valid with { QueryFactory = _ => throw primary },
            "ordering" => valid with { ApplyStableOrdering = _ => throw primary },
            "predicate" => valid with { ResumePredicateFactory = _ => throw primary },
            "selector" => valid with { CheckpointSelector = _ => throw primary },
            "comparer" => valid with { CheckpointComparer = Comparer<long>.Create((_, _) => throw primary) },
            _ => valid,
        };

        if (stage == "sql")
        {
            database.Commands.BeforeExecuteAsync = _ => Task.FromException(primary);
        }

        var action = () => EfCoreSourceTestData.ReadAllAsync(
            new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options), resume: new StoredCheckpoint<long> { Value = 0L });

        (await action.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(primary);
        database.Factory.Contexts.Should().ContainSingle().Which.DisposeCount.Should().Be(1);
        database.Factory.Contexts.Should().OnlyContain(context => context.IsDisposed);
    }

    [Fact]
    public async Task RunEachAsync_should_not_process_or_commit_batch_when_context_disposal_fails()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        var failure = new InvalidOperationException("Не удалось завершить чтение порции.");
        database.Factory.ConfigureContext = context => context.DisposeFailure = failure;
        var processed = 0;

        var summary = await BatchJob.Named("ef-page-dispose")
            .FromEntityFrameworkCore(database.CreateSourceOptions())
            .WithCheckpoint(new InMemoryCheckpointStore<long>())
            .RunEachAsync((_, _, _) =>
            {
                processed++;
                return ValueTask.CompletedTask;
            });

        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.Exception.Should().BeSameAs(failure);
        summary.LastPersistedCheckpoint.Should().BeNull();
        processed.Should().Be(0);
    }

    [Fact]
    public async Task ReadAsync_should_not_create_context_if_already_cancelled()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(database.CreateSourceOptions());
        var action = () => EfCoreSourceTestData.ReadAllAsync(source, cancellationToken: cancellation.Token);

        await action.Should().ThrowAsync<OperationCanceledException>();
        database.Factory.Tokens.Should().BeEmpty();
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ReadAsync_should_cancel_factory_or_query_and_preserve_token_despite_cleanup_failure(
        bool duringFactory,
        bool cancelEnumerator)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        using var methodCancellation = new CancellationTokenSource();
        using var enumeratorCancellation = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observedToken = CancellationToken.None;

        async Task WaitForCancellation(CancellationToken token)
        {
            observedToken = token;
            entered.TrySetResult();
            await release.Task.WaitAsync(token);
        }

        if (duringFactory)
        {
            database.Factory.BeforeCreateAsync = WaitForCancellation;
        }
        else
        {
            database.Commands.BeforeExecuteAsync = WaitForCancellation;
            database.Factory.ConfigureContext = context => context.DisposeFailure = new InvalidOperationException("Вторичная ошибка очистки.");
        }

        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(database.CreateSourceOptions());
        await using var enumerator = source.ReadAsync(EfCoreSourceTestData.Context<long>(), methodCancellation.Token)
            .GetAsyncEnumerator(enumeratorCancellation.Token);
        var move = enumerator.MoveNextAsync().AsTask();

        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            (cancelEnumerator ? enumeratorCancellation : methodCancellation).Cancel();
            Func<Task> action = async () => await move.WaitAsync(TimeSpan.FromSeconds(5));
            (await action.Should().ThrowAsync<OperationCanceledException>()).Which.CancellationToken.Should().Be(observedToken);
        }
        finally
        {
            release.TrySetResult();

            try
            {
                await move.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (OperationCanceledException)
            {
            }
        }

        database.Factory.Contexts.Should().HaveCount(duringFactory ? 0 : 1);
        database.Factory.Contexts.All(context => context.IsDisposed && context.DisposeCount == 1).Should().BeTrue();
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ReadAsync_should_not_yield_batch_if_cancelled_during_last_selector_or_disposal(bool duringDisposal, bool disposalFails)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        using var cancellation = new CancellationTokenSource();
        var options = database.CreateSourceOptions();
        var cleanup = new InvalidOperationException("Ошибка очистки после запроса отмены.");

        database.Factory.ConfigureContext = context =>
        {
            context.DisposeFailure = disposalFails ? cleanup : null;

            if (duringDisposal)
            {
                context.BeforeDisposeAsync = () =>
                {
                    cancellation.Cancel();
                    return Task.CompletedTask;
                };
            }
        };

        if (!duringDisposal)
        {
            options = options with
            {
                CheckpointSelector = row =>
                {
                    cancellation.Cancel();
                    return row.Id;
                },
            };
        }

        var action = () => EfCoreSourceTestData.ReadAllAsync(
            new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options), cancellationToken: cancellation.Token);

        var exception = (await action.Should().ThrowAsync<OperationCanceledException>()).Which;
        exception.CancellationToken.Should().Be(cancellation.Token);

        if (duringDisposal && disposalFails)
        {
            exception.InnerException.Should().BeSameAs(cleanup);
        }

        database.Factory.Contexts.Should().ContainSingle().Which.IsDisposed.Should().BeTrue();
    }

    [Fact]
    public async Task ReadAsync_should_preserve_existing_read_failure_even_if_cancellation_and_cleanup_failure_follow()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        using var cancellation = new CancellationTokenSource();
        var primary = new InvalidOperationException("Первичная ошибка извлечения checkpoint.");
        database.Factory.ConfigureContext = context => context.DisposeFailure = new InvalidOperationException("Вторичная ошибка очистки.");
        var options = database.CreateSourceOptions() with
        {
            CheckpointSelector = _ =>
            {
                cancellation.Cancel();
                throw primary;
            },
        };
        var action = () => EfCoreSourceTestData.ReadAllAsync(
            new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options), cancellationToken: cancellation.Token);

        (await action.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(primary);
        database.Factory.Contexts.Should().ContainSingle().Which.IsDisposed.Should().BeTrue();
    }

    [Fact]
    public async Task ReadAsync_should_wait_for_context_disposal_before_exposing_batch()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        database.Factory.ConfigureContext = context => context.BeforeDisposeAsync = async () =>
        {
            entered.TrySetResult();
            await release.Task;
        };
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(database.CreateSourceOptions());
        await using var enumerator = source.ReadAsync(EfCoreSourceTestData.Context<long>(), CancellationToken.None).GetAsyncEnumerator();
        var move = enumerator.MoveNextAsync().AsTask();

        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            move.IsCompleted.Should().BeFalse();
        }
        finally
        {
            release.TrySetResult();
            await move.WaitAsync(TimeSpan.FromSeconds(5));
        }

        (await move).Should().BeTrue();
        database.Factory.Contexts.Should().ContainSingle().Which.IsDisposed.Should().BeTrue();
    }
}
