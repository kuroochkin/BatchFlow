using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace BatchFlow.EntityFrameworkCore.Tests.Sources;

[Trait("Category", "EntityFrameworkCoreIntegration")]
public sealed class EfCoreBatchSourceValidationTests
{
    [Theory]
    [InlineData("Context")]
    [InlineData("QueryFactory")]
    [InlineData("ApplyStableOrdering")]
    [InlineData("ResumePredicateFactory")]
    [InlineData("CheckpointSelector")]
    [InlineData("CheckpointComparer")]
    public async Task Constructor_should_reject_null_dependencies_without_creating_context(string dependency)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var valid = database.CreateSourceOptions();
        var invalid = dependency switch
        {
            "Context" => valid with { Context = null! },
            "QueryFactory" => valid with { QueryFactory = null! },
            "ApplyStableOrdering" => valid with { ApplyStableOrdering = null! },
            "ResumePredicateFactory" => valid with { ResumePredicateFactory = null! },
            "CheckpointSelector" => valid with { CheckpointSelector = null! },
            _ => valid with { CheckpointComparer = null! },
        };
        Action action = () => new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(invalid);

        action.Should().Throw<ArgumentNullException>();
        database.Factory.Contexts.Should().BeEmpty();
        database.Commands.Executed.Should().BeEmpty();
    }

    [Fact]
    public void Constructor_should_reject_null_options()
    {
        Action action = () => new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(null!);

        action.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("options");
    }

    [Fact]
    public async Task ReadAsync_should_reject_null_read_context_before_factory()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(database.CreateSourceOptions());
        await using var enumerator = source.ReadAsync(null!, CancellationToken.None).GetAsyncEnumerator();
        Func<Task> action = async () => await enumerator.MoveNextAsync();

        (await action.Should().ThrowAsync<ArgumentNullException>()).Which.ParamName.Should().Be("context");
        database.Factory.Tokens.Should().BeEmpty();
    }

    [Fact]
    public async Task Constructor_should_reject_external_context_without_taking_ownership()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        await using var context = new SourceTestDbContext(database.ContextOptions);
        var options = database.CreateSourceOptions() with
        {
            Context = EntityFrameworkCoreContextOptions<SourceTestDbContext>.FromContext(context),
        };
        Action action = () => new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options);

        action.Should().Throw<ArgumentException>();
        context.IsDisposed.Should().BeFalse();
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(int.MaxValue)]
    public async Task ReadAsync_should_validate_batch_size_before_factory(int batchSize)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(database.CreateSourceOptions());
        var action = () => EfCoreSourceTestData.ReadAllAsync(source, batchSize);

        await action.Should().ThrowAsync<ArgumentOutOfRangeException>();
        database.Factory.Tokens.Should().BeEmpty();
    }

    [Theory]
    [InlineData("Skip")]
    [InlineData("Take")]
    [InlineData("SkipLast")]
    [InlineData("TakeLast")]
    [InlineData("Reverse")]
    public async Task ReadAsync_should_reject_user_pagination_before_sql(string operation)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        var options = database.CreateSourceOptions() with
        {
            QueryFactory = context => operation switch
            {
                "Skip" => context.Rows.OrderBy(row => row.Id).Skip(1),
                "Take" => context.Rows.OrderBy(row => row.Id).Take(1),
                "SkipLast" => context.Rows.OrderBy(row => row.Id).SkipLast(1),
                "TakeLast" => context.Rows.OrderBy(row => row.Id).TakeLast(1),
                _ => context.Rows.OrderBy(row => row.Id).Reverse(),
            },
        };
        var action = () => EfCoreSourceTestData.ReadAllAsync(new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options));

        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*основной цепочке*");
        database.Commands.Executed.Should().BeEmpty();
        database.Factory.Contexts.Should().OnlyContain(context => context.IsDisposed);
    }

    [Theory]
    [InlineData("Take")]
    [InlineData("Skip")]
    [InlineData("Where")]
    [InlineData("Replace")]
    [InlineData("NoOrder")]
    [InlineData("SecondOrder")]
    public async Task ReadAsync_should_reject_ordering_that_does_not_extend_filtered_input(string operation)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        IQueryable<SourceTestRow>? original = null;
        var options = database.CreateSourceOptions() with
        {
            QueryFactory = context => original = context.Rows.Where(row => true),
            ApplyStableOrdering = query => operation switch
            {
                "Take" => query.Take(1).OrderBy(row => row.Id),
                "Skip" => query.Skip(1).OrderBy(row => row.Id),
                "Where" => query.Where(row => row.Enabled).OrderBy(row => row.Id),
                "Replace" => original!.OrderBy(row => row.Id),
                "NoOrder" => (IOrderedQueryable<SourceTestRow>)query,
                _ => query.OrderBy(row => row.Bucket).OrderBy(row => row.Id),
            },
        };
        var action = () => EfCoreSourceTestData.ReadAllAsync(
            new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options), resume: new StoredCheckpoint<long> { Value = 0L });

        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*Стабильная сортировка*");
        database.Commands.Executed.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadAsync_should_allow_bounded_subquery_inside_business_filter()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            new SourceTestRow { Id = 1 }, new SourceTestRow { Id = 2 }, new SourceTestRow { Id = 3 });
        var options = database.CreateSourceOptions() with
        {
            QueryFactory = context => context.Rows.Where(row =>
                context.Rows.OrderBy(inner => inner.Id).Take(2).Select(inner => inner.Id).Contains(row.Id)),
        };

        var batches = await EfCoreSourceTestData.ReadAllAsync(new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options), batchSize: 1);

        batches.SelectMany(batch => batch.Items).Select(row => row.Id).Should().Equal(1L, 2L);
    }

    [Fact]
    public async Task ReadAsync_should_reject_query_from_another_context_without_disposing_external_context()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        await using var external = new SourceTestDbContext(database.ContextOptions);
        var options = database.CreateSourceOptions() with { QueryFactory = _ => external.Rows };
        var action = () => EfCoreSourceTestData.ReadAllAsync(new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options));

        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*контексту, переданному в QueryFactory*");
        database.Commands.Executed.Should().BeEmpty();
        external.IsDisposed.Should().BeFalse();
        database.Factory.Contexts.Should().ContainSingle().Which.IsDisposed.Should().BeTrue();
    }

    [Fact]
    public async Task ReadAsync_should_reject_linq_to_objects_without_client_side_fallback()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var options = database.CreateSourceOptions() with { QueryFactory = _ => new[] { new SourceTestRow { Id = 1 } }.AsQueryable() };
        var action = () => EfCoreSourceTestData.ReadAllAsync(new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options));

        await action.Should().ThrowAsync<InvalidOperationException>();
        database.Commands.Executed.Should().BeEmpty();
    }

    [Theory]
    [InlineData("query")]
    [InlineData("ordering")]
    [InlineData("predicate")]
    public async Task ReadAsync_should_reject_null_query_configuration_results(string result)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        var valid = database.CreateSourceOptions();
        var options = result switch
        {
            "query" => valid with { QueryFactory = _ => null! },
            "ordering" => valid with { ApplyStableOrdering = _ => null! },
            _ => valid with { ResumePredicateFactory = _ => null! },
        };
        var action = () => EfCoreSourceTestData.ReadAllAsync(
            new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options), resume: new StoredCheckpoint<long> { Value = 0L });

        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*null*");
        database.Factory.Contexts.Should().OnlyContain(context => context.IsDisposed);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAsync_should_reject_null_checkpoint_values(bool savedCheckpoint)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        var options = new EfCoreBatchSourceOptions<SourceTestDbContext, SourceTestRow, string>
        {
            Context = database.CreateSourceOptions().Context,
            QueryFactory = context => context.Rows,
            ApplyStableOrdering = query => query.OrderBy(row => row.Id),
            ResumePredicateFactory = _ => row => true,
            CheckpointSelector = _ => null!,
            CheckpointComparer = StringComparer.Ordinal,
        };
        var action = () => EfCoreSourceTestData.ReadAllAsync(
            new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, string>(options),
            resume: savedCheckpoint ? new StoredCheckpoint<string> { Value = null! } : null);

        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*null*");
        database.Factory.Contexts.Should().HaveCount(savedCheckpoint ? 0 : 1);
        database.Factory.Contexts.All(context => context.IsDisposed).Should().BeTrue();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadAsync_should_reject_non_strict_resume_predicate_and_stop_after_second_read(bool inclusivePredicate)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(
            new SourceTestRow { Id = 1 }, new SourceTestRow { Id = 2 }, new SourceTestRow { Id = 3 });
        var options = database.CreateSourceOptions() with
        {
            ResumePredicateFactory = checkpoint => row => inclusivePredicate ? row.Id >= checkpoint : true,
        };
        var action = () => EfCoreSourceTestData.ReadAllAsync(new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options), batchSize: 1);

        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*строго продвигаться*");
        database.Commands.Executed.Should().HaveCount(2);
    }

    [Fact]
    public async Task ReadAsync_should_reject_comparer_that_disagrees_with_descending_query()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 }, new SourceTestRow { Id = 2 });
        var options = database.CreateSourceOptions() with { ApplyStableOrdering = query => query.OrderByDescending(row => row.Id) };
        var action = () => EfCoreSourceTestData.ReadAllAsync(new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options), batchSize: 1);

        await action.Should().ThrowAsync<InvalidOperationException>().WithMessage("*строго продвигаться*");
    }
}
