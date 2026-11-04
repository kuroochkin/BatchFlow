using BatchFlow;
using BatchFlow.Builders;
using BatchFlow.EntityFrameworkCore.Extensions;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using FluentAssertions;
using Xunit;

namespace BatchFlow.EntityFrameworkCore.Tests.Extensions;

[Trait("Category", "EntityFrameworkCoreIntegration")]
public sealed class EfCoreBatchSourceBuilderTests
{
    [Fact]
    public async Task FromEntityFrameworkCore_should_create_concrete_source_from_options_without_io()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();

        var job = BatchJob.Named("ef-options")
            .FromEntityFrameworkCore(database.CreateSourceOptions())
            .ProcessEach((_, _, _) => ValueTask.CompletedTask)
            .Build();

        job.Source.Should().BeOfType<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>();
        database.Factory.Contexts.Should().BeEmpty();
        database.Commands.Executed.Should().BeEmpty();
    }

    [Fact]
    public async Task FromEntityFrameworkCore_should_register_existing_concrete_source()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(database.CreateSourceOptions());

        var job = BatchJob.Named("ef-concrete").FromEntityFrameworkCore(source).ProcessEach((_, _, _) => ValueTask.CompletedTask).Build();

        job.Source.Should().BeSameAs(source);
        database.Factory.Contexts.Should().BeEmpty();
    }

    [Fact]
    public async Task FromEntityFrameworkCore_should_reject_null_arguments_for_both_new_overloads()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var options = database.CreateSourceOptions();
        var source = new EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>(options);
        var stage = BatchJob.Named("ef-null-arguments");
        Action nullSourceStage = () => ((IBatchJobSourceSelectionStage)null!).FromEntityFrameworkCore(source);
        Action nullOptionsStage = () => ((IBatchJobSourceSelectionStage)null!).FromEntityFrameworkCore(options);
        Action nullSource = () => stage.FromEntityFrameworkCore((EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>)null!);
        Action nullOptions = () => stage.FromEntityFrameworkCore((EfCoreBatchSourceOptions<SourceTestDbContext, SourceTestRow, long>)null!);

        nullSourceStage.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("stage");
        nullOptionsStage.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("stage");
        nullSource.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("source");
        nullOptions.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("options");
    }
}
