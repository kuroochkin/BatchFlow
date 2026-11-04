using BatchFlow;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Checkpoints;
using BatchFlow.Core.Execution;
using BatchFlow.Core.Execution.Models;
using BatchFlow.Extensions.DependencyInjection;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BatchFlow.DependencyInjection.Tests;

public sealed class CommonRegistrationTests
{
    [Fact]
    public async Task AddBatchFlow_should_be_idempotent_and_connect_run_options_to_registered_services()
    {
        var services = new ServiceCollection();
        services.AddBatchFlow().AddBatchFlow();
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        var first = provider.GetRequiredService<BatchJobRunOptions>();
        var second = provider.GetRequiredService<BatchJobRunOptions>();

        first.Should().NotBeSameAs(second);
        first.ExecutionEngine.Should().BeSameAs(second.ExecutionEngine).And.BeOfType<BatchJobExecutionEngine>();
        first.TimeProvider.Should().BeSameAs(TimeProvider.System);
        services.Count(service => service.ServiceType == typeof(IBatchJobExecutionEngine)).Should().Be(1);
        services.Count(service => service.ServiceType == typeof(TimeProvider)).Should().Be(1);
    }

    [Fact]
    public async Task AddBatchFlow_should_preserve_user_engine_clock_and_run_options()
    {
        var engine = new CustomEngine();
        var clock = new CustomClock();
        var expected = new BatchJobRunOptions { ExecutionEngine = engine, TimeProvider = clock, RunId = "custom-run" };
        var services = new ServiceCollection();
        services.AddSingleton<IBatchJobExecutionEngine>(engine);
        services.AddSingleton<TimeProvider>(clock);
        services.AddSingleton(expected);
        services.AddBatchFlow();
        await using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<BatchJobRunOptions>().Should().BeSameAs(expected);
        provider.GetRequiredService<IBatchJobExecutionEngine>().Should().BeSameAs(engine);
        provider.GetRequiredService<TimeProvider>().Should().BeSameAs(clock);
    }

    [Fact]
    public async Task InMemory_checkpoint_should_survive_scopes_and_remain_concrete_registration()
    {
        var services = new ServiceCollection().AddBatchFlowInMemoryCheckpoint<long>();
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        InMemoryCheckpointStore<long> first;
        await using (var scope = provider.CreateAsyncScope())
        {
            first = scope.ServiceProvider.GetRequiredService<InMemoryCheckpointStore<long>>();
            await first.SaveAsync("orders", new StoredCheckpoint<long> { Value = 42L }, null, default);
        }

        await using var secondScope = provider.CreateAsyncScope();
        var second = secondScope.ServiceProvider.GetRequiredService<InMemoryCheckpointStore<long>>();
        second.Should().BeSameAs(first);
        (await second.LoadAsync("orders", default))!.Value.Should().Be(42L);
        provider.GetService<ICheckpointStore<long>>().Should().BeNull();
    }

    [Fact]
    public async Task Named_in_memory_stores_should_be_independent_and_reject_duplicates()
    {
        var services = new ServiceCollection().AddBatchFlowInMemoryCheckpoint<long>("first").AddBatchFlowInMemoryCheckpoint<long>("second");
        Action duplicate = () => services.AddBatchFlowInMemoryCheckpoint<long>("first");
        duplicate.Should().Throw<ArgumentException>().WithMessage("*уже зарегистрирован*");
        await using var provider = services.BuildServiceProvider();
        provider.GetRequiredKeyedService<InMemoryCheckpointStore<long>>("first")
            .Should().NotBeSameAs(provider.GetRequiredKeyedService<InMemoryCheckpointStore<long>>("second"));
        provider.GetService<InMemoryCheckpointStore<long>>().Should().BeNull();
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Registration_should_reject_blank_names_without_adding_descriptors(string name)
    {
        var services = new ServiceCollection();
        Action action = () => services.AddBatchFlowInMemoryCheckpoint<long>(name);
        action.Should().Throw<ArgumentException>();
        services.Should().BeEmpty();
    }

    [Fact]
    public void Common_extensions_should_reject_null_services()
    {
        Action engine = () => ((IServiceCollection)null!).AddBatchFlow();
        Action store = () => ((IServiceCollection)null!).AddBatchFlowInMemoryCheckpoint<long>();
        engine.Should().Throw<ArgumentNullException>();
        store.Should().Throw<ArgumentNullException>();
    }

    private sealed class CustomClock : TimeProvider;

    private sealed class CustomEngine : IBatchJobExecutionEngine
    {
        public ValueTask<BatchJobExecutionSummary<TCheckpoint>> ExecuteAsync<TItem, TCheckpoint>(
            BatchJobExecutionRequest<TItem, TCheckpoint> request, CancellationToken cancellationToken) => throw new NotSupportedException();
    }
}
