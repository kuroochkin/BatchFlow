using BatchFlow;
using BatchFlow.Builders;
using BatchFlow.EntityFrameworkCore.Extensions;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace BatchFlow.EntityFrameworkCore.Tests.Extensions;

public sealed class FromDbContextTests
{
    [Fact]
    public async Task Factory_overload_should_configure_once_without_creating_context_until_run()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 }, new SourceTestRow { Id = 2 });
        var configured = 0;
        var builder = BatchJob.Named("from-factory").FromDbContext(database.Factory, context =>
        {
            configured++;
            return database.CreateSourceOptions() with { Context = context };
        });
        configured.Should().Be(1);
        database.Factory.Contexts.Should().BeEmpty();

        var result = await builder.WithBatchSize(1).RunEachAsync((_, _, _) => ValueTask.CompletedTask);

        result.SucceededItemCount.Should().Be(2);
        result.Exception.Should().BeNull();
        configured.Should().Be(1);
        database.Factory.Contexts.Should().HaveCount(2).And.OnlyContain(context => context.IsDisposed);
    }

    [Fact]
    public async Task Configure_delegate_should_prepare_options_once_and_create_context_per_portion_without_reflection()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 }, new SourceTestRow { Id = 2 });
        await using var probe = new SourceTestDbContext(database.ContextOptions);
        var connectionString = probe.Database.GetConnectionString();
        var configured = 0;
        var created = new List<SourceTestDbContext>();
        var builder = BatchJob.Named("from-configure").FromDbContext<SourceTestDbContext, SourceTestRow, long>(
            contextOptions =>
            {
                configured++;
                contextOptions.UseSqlite(connectionString);
            },
            options =>
            {
                var context = new SourceTestDbContext(options);
                created.Add(context);
                return context;
            },
            context => database.CreateSourceOptions() with { Context = context });
        created.Should().BeEmpty();

        var result = await builder.WithBatchSize(1).RunBatchAsync((_, _, _) => ValueTask.CompletedTask);

        result.SucceededItemCount.Should().Be(2);
        result.Exception.Should().BeNull();
        configured.Should().Be(1);
        created.Should().HaveCount(2).And.OnlyContain(context => context.IsDisposed);
    }

    [Fact]
    public async Task Options_source_and_controlled_factory_overloads_should_preserve_existing_semantics()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var options = database.CreateSourceOptions();
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options);
        var fromOptions = BatchJob.Named("options").FromDbContext(options).ProcessEach((_, _, _) => ValueTask.CompletedTask).Build();
        var fromSource = BatchJob.Named("source").FromDbContext(source).ProcessEach((_, _, _) => ValueTask.CompletedTask).Build();
        var fromFactory = BatchJob.Named("context-options").FromDbContext(options.Context, context => options with { Context = context })
            .ProcessEach((_, _, _) => ValueTask.CompletedTask).Build();

        fromOptions.Source.Should().BeOfType<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>();
        fromSource.Source.Should().BeSameAs(source);
        fromFactory.Source.Should().BeOfType<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>();
        database.Factory.Contexts.Should().BeEmpty();
    }

    [Fact]
    public async Task Configure_should_not_replace_provided_factory_or_return_null_options()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        Action rebound = () => BatchJob.Named("rebound").FromDbContext(database.Factory, _ => database.CreateSourceOptions());
        Action missing = () => BatchJob.Named("null-options").FromDbContext<SourceTestDbContext, SourceTestRow, long>(database.Factory, _ => null!);

        rebound.Should().Throw<ArgumentException>().WithMessage("*подменять фабрику*");
        missing.Should().Throw<InvalidOperationException>().WithMessage("*null*");
        database.Factory.Contexts.Should().BeEmpty();
    }

    [Fact]
    public async Task All_overloads_should_reject_null_arguments_without_invoking_configuration()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var options = database.CreateSourceOptions();
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options);
        var stage = BatchJob.Named("nulls");
        Func<EntityFrameworkCoreContextOptions<SourceTestDbContext>, EfCoreBatchSourceOptions<SourceTestDbContext, SourceTestRow, long>> configure =
            context => options with { Context = context };
        Action[] actions =
        [
            () => ((IBatchJobSourceSelectionStage)null!).FromDbContext(source),
            () => ((IBatchJobSourceSelectionStage)null!).FromDbContext(options),
            () => ((IBatchJobSourceSelectionStage)null!).FromDbContext(database.Factory, configure),
            () => ((IBatchJobSourceSelectionStage)null!).FromDbContext(options.Context, configure),
            () => ((IBatchJobSourceSelectionStage)null!).FromDbContext<SourceTestDbContext, SourceTestRow, long>(
                _ => throw new InvalidOperationException(), _ => throw new InvalidOperationException(), configure),
            () => stage.FromDbContext((EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>)null!),
            () => stage.FromDbContext((EfCoreBatchSourceOptions<SourceTestDbContext, SourceTestRow, long>)null!),
            () => stage.FromDbContext((IDbContextFactory<SourceTestDbContext>)null!, configure),
            () => stage.FromDbContext((EntityFrameworkCoreContextOptions<SourceTestDbContext>)null!, configure),
            () => stage.FromDbContext<SourceTestDbContext, SourceTestRow, long>(database.Factory, null!),
            () => stage.FromDbContext<SourceTestDbContext, SourceTestRow, long>(options.Context, null!),
            () => stage.FromDbContext<SourceTestDbContext, SourceTestRow, long>(null!, options => new SourceTestDbContext(options), configure),
            () => stage.FromDbContext<SourceTestDbContext, SourceTestRow, long>(_ => { }, null!, configure),
            () => stage.FromDbContext<SourceTestDbContext, SourceTestRow, long>(_ => { }, options => new SourceTestDbContext(options), null!),
        ];

        foreach (var action in actions)
        {
            action.Should().Throw<ArgumentNullException>();
        }

        database.Factory.Contexts.Should().BeEmpty();
    }
}
