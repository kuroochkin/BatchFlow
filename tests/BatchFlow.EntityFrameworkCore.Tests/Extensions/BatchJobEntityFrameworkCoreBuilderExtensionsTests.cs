using BatchFlow;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Builders;
using BatchFlow.Checkpoints;
using BatchFlow.Core.Execution;
using BatchFlow.EntityFrameworkCore.Extensions;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using FluentAssertions;
using Xunit;

namespace BatchFlow.EntityFrameworkCore.Tests.Extensions;

public sealed class BatchJobEntityFrameworkCoreBuilderExtensionsTests
{
    [Fact]
    public void FromEntityFrameworkCore_should_register_source_without_creating_context()
    {
        var calls = 0;
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() =>
        {
            calls++;
            return new TestDbContext();
        }));

        var job = BatchJob.Named("ef-orders")
            .FromEntityFrameworkCore(source)
            .WithBatchSize(100)
            .WithDegreeOfParallelism(4)
            .WithCheckpoint(new InMemoryCheckpointStore<long>())
            .ProcessEach((_, _, _) => ValueTask.CompletedTask)
            .Build();

        job.Source.Should().BeSameAs(source);
        calls.Should().Be(0);
    }

    [Fact]
    public void FromEntityFrameworkCore_should_reject_null_arguments()
    {
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() => new TestDbContext()));
        Action stage = () => ((IBatchJobSourceSelectionStage)null!).FromEntityFrameworkCore(source);
        Action nullSource = () => BatchJob.Named("ef-orders").FromEntityFrameworkCore((TestBatchSource)null!);

        stage.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("stage");
        nullSource.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("source");
    }

    [Fact]
    public void FromEntityFrameworkCore_should_not_allow_checkpoint_for_nonresumable_source()
    {
        var source = new TestBatchSource(
            EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() => new TestDbContext()),
            supportsResume: false);

        Action action = () => BatchJob.Named("ef-orders").FromEntityFrameworkCore(source)
            .WithCheckpoint(new InMemoryCheckpointStore<long>());

        action.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task RunEachAsync_should_use_standard_runtime_checkpoint_and_resume()
    {
        var contexts = new List<TestDbContext>();
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() =>
        {
            var context = new TestDbContext();
            contexts.Add(context);
            return context;
        }), (_, context, _) => TestBatchSource.Batches(
            Enumerable.Range(1, 3).Where(item => item > (context.ResumeFrom?.Value ?? 0L))
                .Select(item => TestBatchSource.CreateBatch(item)).ToArray()));
        var items = new List<int>();
        var job = BatchJob.Named("ef-orders")
            .FromEntityFrameworkCore(source)
            .WithBatchSize(2)
            .WithCheckpoint(new InMemoryCheckpointStore<long>())
            .ProcessEach((item, _, _) =>
            {
                items.Add(item);
                return ValueTask.CompletedTask;
            })
            .Build();

        var first = await job.RunAsync();
        var resumed = await job.RunAsync();

        first.Status.Should().Be(JobCompletionStatus.Succeeded);
        first.LastPersistedCheckpoint!.Value.Should().Be(3L);
        resumed.Status.Should().Be(JobCompletionStatus.Succeeded);
        resumed.ResumeFrom!.Value.Should().Be(3L);
        resumed.SucceededItemCount.Should().Be(0);
        items.Should().Equal(1, 2, 3);
        contexts.Should().HaveCount(2).And.OnlyContain(context => context.DisposeCount == 1);
    }

    [Fact]
    public void Infrastructure_dependencies_should_not_leak_into_core_or_abstractions()
    {
        var assemblies = new[] { typeof(BatchJobExecutionEngine).Assembly, typeof(Abstractions.Sources.IBatchSource<,>).Assembly };

        foreach (var assembly in assemblies)
        {
            assembly.GetReferencedAssemblies().Should().NotContain(reference =>
                reference.Name!.StartsWith("Microsoft.EntityFrameworkCore", StringComparison.Ordinal));
        }

        typeof(TestBatchSource).BaseType!.Assembly.GetReferencedAssemblies().Should()
            .NotContain(reference => reference.Name!.StartsWith("Npgsql", StringComparison.Ordinal));
    }
}
