using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit;

namespace BatchFlow.EntityFrameworkCore.Tests.Sources;

[Trait("Category", "EntityFrameworkCoreIntegration")]
public sealed class EfCoreBatchSourceIsolationTests
{
    [Fact]
    public async Task ReadAsync_should_keep_independent_cursors_and_contexts_for_concurrent_enumerations()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            Enumerable.Range(1, 5).Select(id => new SourceTestRow { Id = id }).ToArray());
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var queries = 0;
        database.Commands.BeforeExecuteAsync = async token =>
        {
            if (Interlocked.Increment(ref queries) == 2)
            {
                entered.TrySetResult();
            }

            await release.Task.WaitAsync(token);
        };
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(database.CreateSourceOptions());
        var first = EfCoreSourceTestData.ReadAllAsync(source);
        var second = EfCoreSourceTestData.ReadAllAsync(source);

        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
            database.Factory.Contexts.Should().HaveCount(2).And.OnlyHaveUniqueItems();
            database.Factory.Contexts.Should().OnlyContain(context => !context.IsDisposed);
        }
        finally
        {
            release.TrySetResult();
            await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(10));
        }

        (await first).SelectMany(batch => batch.Items).Select(row => row.Id).Should().Equal(1L, 2L, 3L, 4L, 5L);
        (await second).SelectMany(batch => batch.Items).Select(row => row.Id).Should().Equal(1L, 2L, 3L, 4L, 5L);
        database.Factory.Contexts.Should().HaveCount(6).And.OnlyContain(context => context.IsDisposed && context.DisposeCount == 1);
    }

    [Fact]
    public async Task ReadAsync_should_return_pooled_context_after_every_page_and_restore_ef_defaults()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            Enumerable.Range(1, 5).Select(id => new SourceTestRow { Id = id }).ToArray());
        var factory = new PooledDbContextFactory<SourceTestDbContext>(database.ContextOptions, poolSize: 1);
        var seen = new List<(SourceTestDbContext Context, int BeforeDispose)>();
        var options = database.CreateSourceOptions() with
        {
            Context = EntityFrameworkCoreContextOptions<SourceTestDbContext>.FromDbContextFactory(factory),
            QueryFactory = context =>
            {
                seen.Add((context, context.DisposeCount));
                return context.Rows;
            },
        };
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options);

        await foreach (var _ in source.ReadAsync(EfCoreSourceTestData.Context<long>(), CancellationToken.None))
        {
            seen[^1].Context.DisposeCount.Should().Be(seen[^1].BeforeDispose + 1);
        }

        seen.Should().HaveCount(3);
        seen.Select(scope => scope.Context).Distinct().Should().ContainSingle();
        await using var reused = await factory.CreateDbContextAsync();
        reused.Should().BeSameAs(seen[0].Context);
        reused.ChangeTracker.QueryTrackingBehavior.Should().Be(QueryTrackingBehavior.TrackAll);
        reused.ChangeTracker.LazyLoadingEnabled.Should().BeTrue();
        (await reused.Rows.CountAsync()).Should().Be(5);
    }

    [Fact]
    public async Task ReadAsync_should_override_explicit_tracking_for_entity_queries()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        var options = database.CreateSourceOptions() with { QueryFactory = context => context.Rows.AsTracking() };

        await EfCoreSourceTestData.ReadAllAsync(new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options));

        database.Factory.Contexts.Should().ContainSingle().Which.TrackedAtDispose.Should().Be(0);
    }

    [Fact]
    public async Task ReadAsync_should_reject_null_result_item_before_checkpoint_selection()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        var options = database.CreateSourceOptions() with
        {
            QueryFactory = context => context.Rows.Select(_ => (SourceTestRow)null!),
            ApplyStableOrdering = query => query.OrderBy(_ => 1),
            CheckpointSelector = _ => throw new InvalidOperationException("CheckpointSelector не должен получить null."),
        };
        var action = () => EfCoreSourceTestData.ReadAllAsync(new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options));

        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*null вместо элемента*");
        database.Factory.Contexts.Should().ContainSingle().Which.IsDisposed.Should().BeTrue();
    }

    [Fact]
    public async Task ReadAsync_should_fail_nontranslatable_query_without_materializing_all_data()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        var options = database.CreateSourceOptions() with { QueryFactory = context => context.Rows.Where(row => IsExportable(row.Id)) };
        var action = () => EfCoreSourceTestData.ReadAllAsync(new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options));

        await action.Should().ThrowAsync<InvalidOperationException>();
        database.Commands.Executed.Should().BeEmpty();
        database.Factory.Contexts.Should().ContainSingle().Which.IsDisposed.Should().BeTrue();
    }

    [Fact]
    public async Task ReadAsync_should_allow_explicit_descending_single_key()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            new SourceTestRow { Id = 1 }, new SourceTestRow { Id = 2 }, new SourceTestRow { Id = 3 });
        var options = database.CreateSourceOptions() with
        {
            ApplyStableOrdering = query => query.OrderByDescending(row => row.Id),
            ResumePredicateFactory = checkpoint => row => row.Id < checkpoint,
            CheckpointComparer = Comparer<long>.Create((left, right) => right.CompareTo(left)),
        };

        var batches = await EfCoreSourceTestData.ReadAllAsync(new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options), batchSize: 1);

        batches.Select(batch => batch.Checkpoint).Should().Equal(3L, 2L, 1L);
    }

    private static bool IsExportable(long id) => id.ToString(System.Globalization.CultureInfo.InvariantCulture).StartsWith('1');
}
