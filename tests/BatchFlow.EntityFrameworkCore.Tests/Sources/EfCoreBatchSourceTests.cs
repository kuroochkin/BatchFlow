using BatchFlow;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Checkpoints;
using BatchFlow.EntityFrameworkCore.Extensions;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace BatchFlow.EntityFrameworkCore.Tests.Sources;

[Trait("Category", "EntityFrameworkCoreIntegration")]
public sealed class EfCoreBatchSourceTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(100)]
    public async Task ReadAsync_should_page_using_sql_limit_and_dispose_each_context_before_yield(int batchSize)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            Enumerable.Range(1, 5).Reverse().Select(id => new SourceTestRow { Id = id }).ToArray());
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(database.CreateSourceOptions());
        var ids = new List<long>();

        await foreach (var batch in source.ReadAsync(EfCoreSourceTestData.Context<long>(batchSize), CancellationToken.None))
        {
            batch.Items.Should().HaveCountLessThanOrEqualTo(batchSize).And.NotBeEmpty();
            batch.Checkpoint.Should().Be(batch.Items[^1].Id);
            database.Factory.Contexts.Should().OnlyContain(context => context.IsDisposed && context.DisposeCount == 1);
            batch.Items.Should().BeAssignableTo<ICollection<SourceTestRow>>().Which.IsReadOnly.Should().BeTrue();
            ids.AddRange(batch.Items.Select(row => row.Id));
        }

        ids.Should().Equal(1L, 2L, 3L, 4L, 5L);
        var expectedPages = (int)Math.Ceiling(5d / batchSize);
        database.Factory.Contexts.Should().HaveCount(expectedPages);
        database.Factory.Contexts.Should().OnlyContain(context => context.TrackedAtDispose == 0);
        database.Commands.Executed.Should().HaveCount(expectedPages).And.OnlyContain(command =>
            command.Sql.Contains("LIMIT", StringComparison.OrdinalIgnoreCase)
            && !command.Sql.Contains("OFFSET", StringComparison.OrdinalIgnoreCase)
            && command.Parameters.Contains(batchSize + 1));
    }

    [Fact]
    public async Task ReadAsync_should_finish_empty_query_without_yielding_empty_batch()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(database.CreateSourceOptions());

        (await EfCoreSourceTestData.ReadAllAsync(source)).Should().BeEmpty();

        database.Factory.Contexts.Should().ContainSingle().Which.IsDisposed.Should().BeTrue();
        database.Commands.Executed.Should().ContainSingle();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAsync_should_distinguish_missing_checkpoint_from_zero_and_negative_keys(bool resumeFromZero)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            Enumerable.Range(-2, 5).Select(id => new SourceTestRow { Id = id }).ToArray());
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(database.CreateSourceOptions());
        var resume = resumeFromZero ? new StoredCheckpoint<long> { Value = 0L } : null;

        var batches = await EfCoreSourceTestData.ReadAllAsync(source, batchSize: 1, resume);

        batches.SelectMany(batch => batch.Items).Select(row => row.Id)
            .Should().Equal(resumeFromZero ? [1L, 2L] : [-2L, -1L, 0L, 1L, 2L]);
    }

    [Fact]
    public async Task ReadAsync_should_preserve_filters_and_project_to_dto_without_tracking()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            new SourceTestRow { Id = 1, Enabled = true, Label = "one" },
            new SourceTestRow { Id = 2, Enabled = false, Label = "two" },
            new SourceTestRow { Id = 3, Enabled = true, Label = "three" });
        var options = new EfCoreBatchSourceOptions<SourceTestDbContext, RowDto, long>
        {
            Context = database.CreateSourceOptions().Context,
            QueryFactory = context => context.Rows.AsTracking().Where(row => row.Enabled)
                .Select(row => new RowDto { Id = row.Id, Label = row.Label }),
            ApplyStableOrdering = query => query.OrderBy(row => row.Id),
            ResumePredicateFactory = checkpoint => row => row.Id > checkpoint,
            CheckpointSelector = row => row.Id,
            CheckpointComparer = Comparer<long>.Default,
        };

        var batches = await EfCoreSourceTestData.ReadAllAsync(new EfCoreBatchSource<SourceTestDbContext, RowDto, long>(options), batchSize: 1);

        batches.SelectMany(batch => batch.Items).Should().Equal(new RowDto { Id = 1, Label = "one" }, new RowDto { Id = 3, Label = "three" });
        database.Factory.Contexts.Should().OnlyContain(context => context.TrackedAtDispose == 0 && context.IsDisposed);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAsync_should_support_mixed_direction_composite_keyset(bool resume)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            new SourceTestRow { Id = 1, Bucket = 2 }, new SourceTestRow { Id = 2, Bucket = 1 },
            new SourceTestRow { Id = 3, Bucket = 1 }, new SourceTestRow { Id = 4, Bucket = 2 });
        var options = new EfCoreBatchSourceOptions<SourceTestDbContext, SourceTestRow, RowPosition>
        {
            Context = database.CreateSourceOptions().Context,
            QueryFactory = context => context.Rows,
            ApplyStableOrdering = query => query.OrderByDescending(row => row.Bucket).ThenBy(row => row.Id),
            ResumePredicateFactory = checkpoint => row => row.Bucket < checkpoint.Bucket
                || (row.Bucket == checkpoint.Bucket && row.Id > checkpoint.Id),
            CheckpointSelector = row => new RowPosition(row.Bucket, row.Id),
            CheckpointComparer = Comparer<RowPosition>.Create((left, right) =>
                left.Bucket == right.Bucket ? left.Id.CompareTo(right.Id) : right.Bucket.CompareTo(left.Bucket)),
        };

        var batches = await EfCoreSourceTestData.ReadAllAsync(
            new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, RowPosition>(options), batchSize: 1,
            resume: resume ? new StoredCheckpoint<RowPosition> { Value = new RowPosition(2, 1) } : null);

        batches.SelectMany(batch => batch.Items).Select(row => row.Id).Should().Equal(resume ? [4L, 2L, 3L] : [1L, 4L, 2L, 3L]);
        batches[^1].Checkpoint.Should().Be(new RowPosition(1, 3));
    }

    [Fact]
    public async Task ReadAsync_should_support_string_checkpoint_with_explicit_ordinal_order()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            new SourceTestRow { Id = 1, Label = "b" }, new SourceTestRow { Id = 2, Label = "B" },
            new SourceTestRow { Id = 3, Label = "a" }, new SourceTestRow { Id = 4, Label = "A" });
        var options = new EfCoreBatchSourceOptions<SourceTestDbContext, SourceTestRow, string>
        {
            Context = database.CreateSourceOptions().Context,
            QueryFactory = context => context.Rows,
            ApplyStableOrdering = query => query.OrderBy(row => row.Label),
            ResumePredicateFactory = checkpoint => row => string.Compare(row.Label, checkpoint) > 0,
            CheckpointSelector = row => row.Label!,
            CheckpointComparer = StringComparer.Ordinal,
        };

        var batches = await EfCoreSourceTestData.ReadAllAsync(new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, string>(options), batchSize: 1);

        batches.Select(batch => batch.Checkpoint).Should().Equal("A", "B", "a", "b");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task ReadAsync_should_reject_duplicate_checkpoint_at_batch_boundary_before_processing(int batchSize)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            new SourceTestRow { Id = 1, Bucket = 1 }, new SourceTestRow { Id = 2, Bucket = batchSize == 1 ? 1 : 2 },
            new SourceTestRow { Id = 3, Bucket = 2 });
        var options = database.CreateSourceOptions() with
        {
            ApplyStableOrdering = query => query.OrderBy(row => row.Bucket).ThenBy(row => row.Id),
            CheckpointSelector = row => row.Bucket,
        };
        var calls = 0;

        var summary = await BatchJob.Named("duplicate-keyset")
            .FromEntityFrameworkCore(options)
            .WithBatchSize(batchSize)
            .WithCheckpoint(new InMemoryCheckpointStore<long>())
            .RunEachAsync((_, _, _) =>
            {
                calls++;
                return ValueTask.CompletedTask;
            });

        summary.Status.Should().Be(JobCompletionStatus.Failed);
        summary.Exception.Should().BeOfType<InvalidOperationException>();
        summary.LastPersistedCheckpoint.Should().BeNull();
        calls.Should().Be(0);
    }

    [Fact]
    public async Task RunEachAsync_should_restart_from_last_committed_checkpoint_without_read_context_in_handlers()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            Enumerable.Range(1, 5).Select(id => new SourceTestRow { Id = id }).ToArray());
        var processed = new List<long>();
        var fail = true;
        var options = new EfCoreBatchSourceOptions<SourceTestDbContext, LoadedRow, long>
        {
            Context = database.CreateSourceOptions().Context,
            QueryFactory = context =>
            {
                var contextId = context.ContextId.InstanceId;
                return context.Rows.Select(row => new LoadedRow { Id = row.Id, ContextId = contextId });
            },
            ApplyStableOrdering = query => query.OrderBy(row => row.Id),
            ResumePredicateFactory = checkpoint => row => row.Id > checkpoint,
            CheckpointSelector = row => row.Id,
            CheckpointComparer = Comparer<long>.Default,
        };
        var job = BatchJob.Named("ef-resume")
            .FromEntityFrameworkCore(options)
            .WithBatchSize(2)
            .WithCheckpoint(new InMemoryCheckpointStore<long>())
            .ProcessEach((row, _, _) =>
            {
                database.Factory.Contexts.Single(context => context.ContextId.InstanceId == row.ContextId).IsDisposed.Should().BeTrue();

                if (fail && row.Id == 3)
                {
                    throw new InvalidOperationException("Перезапуск после первого подтвержденного пакета.");
                }

                processed.Add(row.Id);
                return ValueTask.CompletedTask;
            })
            .Build();

        var first = await job.RunAsync();
        fail = false;
        var second = await job.RunAsync();

        first.Status.Should().Be(JobCompletionStatus.Failed);
        first.LastPersistedCheckpoint.Should().NotBeNull(first.Exception?.ToString());
        first.LastPersistedCheckpoint!.Value.Should().Be(2L);
        second.Status.Should().Be(JobCompletionStatus.Succeeded);
        second.ResumeFrom!.Value.Should().Be(2L);
        second.LastPersistedCheckpoint!.Value.Should().Be(5L);
        processed.Should().Equal(1L, 2L, 3L, 4L, 5L);
    }

    [Fact]
    public async Task ReadAsync_should_release_context_when_consumer_stops_after_first_batch()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            Enumerable.Range(1, 5).Select(id => new SourceTestRow { Id = id }).ToArray());
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(database.CreateSourceOptions());

        await foreach (var _ in source.ReadAsync(EfCoreSourceTestData.Context<long>(), CancellationToken.None))
        {
            database.Factory.Contexts.Should().ContainSingle().Which.IsDisposed.Should().BeTrue();
            break;
        }

        database.Factory.Contexts.Should().ContainSingle();
        database.Commands.Executed.Should().ContainSingle();
    }

    private sealed record RowDto
    {
        public required long Id { get; init; }

        public string? Label { get; init; }
    }

    private sealed record LoadedRow
    {
        public required long Id { get; init; }

        public required Guid ContextId { get; init; }
    }

    private sealed record RowPosition(int Bucket, long Id);
}
