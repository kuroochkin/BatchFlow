using System.Runtime.CompilerServices;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using FluentAssertions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit;

namespace BatchFlow.EntityFrameworkCore.Tests.Integration;

public sealed class EntityFrameworkCoreContextPoolingTests
{
    [Theory]
    [InlineData(ReadOutcome.Completed)]
    [InlineData(ReadOutcome.StoppedEarly)]
    [InlineData(ReadOutcome.Cancelled)]
    [InlineData(ReadOutcome.Failed)]
    [Trait("Category", "EntityFrameworkCoreIntegration")]
    public async Task ReadAsync_should_return_context_to_real_pool_and_allow_reuse(ReadOutcome outcome)
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<PooledTestDbContext>().UseSqlite(connection).Options;
        var factory = new PooledDbContextFactory<PooledTestDbContext>(options, poolSize: 1);

        await using (var setup = await factory.CreateDbContextAsync())
        {
            await setup.Database.EnsureCreatedAsync();
            setup.Rows.AddRange(new TestRow { Id = 1 }, new TestRow { Id = 2 });
            await setup.SaveChangesAsync();
        }

        var source = new PooledTestSource(
            EntityFrameworkCoreContextOptions<PooledTestDbContext>.FromDbContextFactory(factory), outcome);

        await ExerciseReadAsync(source, outcome);

        source.ObservedContext.Should().NotBeNull();
        source.ObservedContext!.DisposeCount.Should().Be(source.DisposeCountAtRead + 1);

        await using var reused = await factory.CreateDbContextAsync();
        reused.Should().BeSameAs(source.ObservedContext);
        reused.ChangeTracker.Entries().Should().BeEmpty();
        var items = await reused.Rows.AsNoTracking().OrderBy(row => row.Id).Select(row => row.Id).ToArrayAsync();
        items.Should().Equal(1, 2);
    }

    private static async Task ExerciseReadAsync(PooledTestSource source, ReadOutcome outcome)
    {
        if (outcome == ReadOutcome.Cancelled)
        {
            using var cancellation = new CancellationTokenSource();
            await using var enumerator = source.ReadAsync(TestBatchSource.CreateContext(), cancellation.Token).GetAsyncEnumerator();
            (await enumerator.MoveNextAsync()).Should().BeTrue();
            var move = enumerator.MoveNextAsync().AsTask();

            try
            {
                await source.ReadBlocked.Task.WaitAsync(TimeSpan.FromSeconds(5));
                cancellation.Cancel();
                Func<Task> action = async () => await move.WaitAsync(TimeSpan.FromSeconds(5));
                (await action.Should().ThrowAsync<OperationCanceledException>()).Which.CancellationToken.Should().Be(cancellation.Token);
            }
            finally
            {
                // При регрессии передачи токена завершаем чтение, прежде чем освобождать перечислитель.
                source.ReleaseRead.TrySetResult();

                try
                {
                    await move.WaitAsync(TimeSpan.FromSeconds(5));
                }
                catch (OperationCanceledException)
                {
                }
            }

            return;
        }

        if (outcome == ReadOutcome.Failed)
        {
            Func<Task> action = async () =>
            {
                await foreach (var _ in source.ReadAsync(TestBatchSource.CreateContext(), CancellationToken.None))
                {
                }
            };

            (await action.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(source.Failure);
            return;
        }

        var count = 0;

        await foreach (var batch in source.ReadAsync(TestBatchSource.CreateContext(), CancellationToken.None))
        {
            count++;
            batch.Items.Should().Equal(1, 2);

            if (outcome == ReadOutcome.StoppedEarly)
            {
                break;
            }
        }

        count.Should().Be(1);
    }

    private sealed class PooledTestSource(
        EntityFrameworkCoreContextOptions<PooledTestDbContext> options,
        ReadOutcome outcome) : EntityFrameworkCoreBatchSourceBase<PooledTestDbContext, int, long>(options)
    {
        public override bool SupportsResume => true;

        public PooledTestDbContext? ObservedContext { get; private set; }

        public int DisposeCountAtRead { get; private set; }

        public TaskCompletionSource ReadBlocked { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource ReleaseRead { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Exception Failure { get; } = new InvalidOperationException("Ошибка чтения с контекстом из пула.");

        protected override async IAsyncEnumerable<SourceBatch<int, long>> ReadCoreAsync(
            PooledTestDbContext dbContext,
            BatchSourceContext<long> context,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ObservedContext = dbContext;
            DisposeCountAtRead = dbContext.DisposeCount;
            dbContext.ChangeTracker.Entries().Should().BeEmpty();
            var lastId = context.ResumeFrom?.Value ?? 0L;
            var items = await dbContext.Rows.AsNoTracking()
                .Where(row => row.Id > lastId)
                .OrderBy(row => row.Id)
                .Take(context.RequestedBatchSize)
                .Select(row => row.Id)
                .ToArrayAsync(cancellationToken);

            if (items.Length == 0)
            {
                yield break;
            }

            yield return new SourceBatch<int, long> { Items = items, Checkpoint = items[^1] };

            if (outcome == ReadOutcome.Cancelled)
            {
                ReadBlocked.TrySetResult();
                await ReleaseRead.Task.WaitAsync(cancellationToken);
            }

            if (outcome == ReadOutcome.Failed)
            {
                throw Failure;
            }
        }
    }

    public sealed class PooledTestDbContext(DbContextOptions<PooledTestDbContext> options) : DbContext(options)
    {
        public DbSet<TestRow> Rows => Set<TestRow>();

        public int DisposeCount { get; private set; }

        public override async ValueTask DisposeAsync()
        {
            DisposeCount++;
            await base.DisposeAsync();
        }
    }

    public sealed class TestRow
    {
        public int Id { get; set; }
    }

    public enum ReadOutcome
    {
        Completed,
        StoppedEarly,
        Cancelled,
        Failed,
    }
}
