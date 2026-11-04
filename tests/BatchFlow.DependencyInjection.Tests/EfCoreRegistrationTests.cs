using BatchFlow;
using BatchFlow.Abstractions.Sources;
using BatchFlow.EntityFrameworkCore.DependencyInjection;
using BatchFlow.EntityFrameworkCore.Extensions;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace BatchFlow.DependencyInjection.Tests;

public sealed class EfCoreRegistrationTests
{
    [Fact]
    public async Task Source_registration_should_be_lazy_scoped_and_not_register_ambiguous_interface()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        var configured = 0;
        var services = new ServiceCollection();
        services.AddSingleton<IDbContextFactory<SourceTestDbContext>>(database.Factory);
        services.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>((_, context) =>
        {
            configured++;
            return database.CreateSourceOptions() with { Context = context };
        });
        configured.Should().Be(0);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        Action root = () => provider.GetRequiredService<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>();
        root.Should().Throw<InvalidOperationException>();
        await using var firstScope = provider.CreateAsyncScope();
        await using var secondScope = provider.CreateAsyncScope();
        var first = firstScope.ServiceProvider.GetRequiredService<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>();
        firstScope.ServiceProvider.GetRequiredService<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>().Should().BeSameAs(first);
        var second = secondScope.ServiceProvider.GetRequiredService<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>();
        second.Should().NotBeSameAs(first);
        configured.Should().Be(2);
        database.Factory.Contexts.Should().BeEmpty();
        firstScope.ServiceProvider.GetService<IBatchSource<SourceTestRow, long>>().Should().BeNull();

        (await EfCoreSourceTestData.ReadAllAsync(first)).Should().ContainSingle();
        database.Factory.Contexts.Should().ContainSingle().Which.IsDisposed.Should().BeTrue();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Native_factory_should_work_with_registered_source_and_FromDbContext(bool pooled)
    {
        await using var database = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 }, new SourceTestRow { Id = 2 });
        await using var context = new SourceTestDbContext(database.ContextOptions);
        var connectionString = context.Database.GetConnectionString();
        var services = new ServiceCollection();
        if (pooled)
        {
            services.AddPooledDbContextFactory<SourceTestDbContext>(builder => builder.UseSqlite(connectionString), poolSize: 2);
        }
        else
        {
            services.AddDbContextFactory<SourceTestDbContext>(builder => builder.UseSqlite(connectionString));
        }
        services.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>((_, factoryContext) =>
            database.CreateSourceOptions() with { Context = factoryContext });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var scope = provider.CreateAsyncScope();
        var source = scope.ServiceProvider.GetRequiredService<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>();
        var result = await BatchJob.Named("ef-native-di").FromDbContext(source).WithBatchSize(1)
            .RunEachAsync((_, _, _) => ValueTask.CompletedTask);

        result.SucceededItemCount.Should().Be(2);
        result.Exception.Should().BeNull();
    }

    [Fact]
    public async Task Null_configuration_result_should_fail_without_creating_context()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var services = new ServiceCollection();
        services.AddSingleton<IDbContextFactory<SourceTestDbContext>>(database.Factory);
        services.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>((_, _) => null!);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        Action resolve = () => scope.ServiceProvider.GetRequiredService<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>();

        resolve.Should().Throw<InvalidOperationException>().WithMessage("*null*");
        database.Factory.Contexts.Should().BeEmpty();
    }

    [Fact]
    public async Task Named_sources_should_use_explicit_keyed_factories_without_affecting_default_registration()
    {
        await using var first = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 1 });
        await using var second = await SqliteSourceDatabase.CreateAsync(new SourceTestRow { Id = 2 });
        var services = new ServiceCollection();
        services.AddKeyedSingleton<IDbContextFactory<SourceTestDbContext>>("db-one", first.Factory);
        services.AddKeyedSingleton<IDbContextFactory<SourceTestDbContext>>("db-two", second.Factory);
        services.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>((_, context) => first.CreateSourceOptions() with { Context = context }, "db-one", "one");
        services.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>((_, context) => second.CreateSourceOptions() with { Context = context }, "db-two", "two");
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await using var scope = provider.CreateAsyncScope();
        var one = scope.ServiceProvider.GetRequiredKeyedService<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>("one");
        var two = scope.ServiceProvider.GetRequiredKeyedService<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>("two");

        (await EfCoreSourceTestData.ReadAllAsync(one)).SelectMany(batch => batch.Items).Select(row => row.Id).Should().Equal(1L);
        (await EfCoreSourceTestData.ReadAllAsync(two)).SelectMany(batch => batch.Items).Select(row => row.Id).Should().Equal(2L);
        scope.ServiceProvider.GetService<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>().Should().BeNull();
    }

    [Fact]
    public async Task Source_configuration_should_use_current_scope_instead_of_capturing_root_dependencies()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var seen = new List<Guid>();
        var services = new ServiceCollection();
        services.AddScoped<ScopeMarker>();
        services.AddScoped<IDbContextFactory<SourceTestDbContext>>(_ => database.Factory);
        services.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>((provider, context) =>
        {
            seen.Add(provider.GetRequiredService<ScopeMarker>().Id);
            return database.CreateSourceOptions() with { Context = context };
        });
        await using var root = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        for (var index = 0; index < 2; index++)
        {
            await using var scope = root.CreateAsyncScope();
            _ = scope.ServiceProvider.GetRequiredService<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>();
        }

        seen.Should().HaveCount(2).And.OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task Missing_factory_and_configuration_rebinding_should_fail_without_creating_context()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var services = new ServiceCollection();
        services.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>((_, _) => database.CreateSourceOptions());
        await using (var provider = services.BuildServiceProvider())
        {
            await using var scope = provider.CreateAsyncScope();
            Action missing = () => scope.ServiceProvider.GetRequiredService<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>();
            missing.Should().Throw<InvalidOperationException>().WithMessage("*IDbContextFactory*");
        }

        services.AddSingleton<IDbContextFactory<SourceTestDbContext>>(database.Factory);
        await using var complete = services.BuildServiceProvider();
        await using var completeScope = complete.CreateAsyncScope();
        Action rebound = () => completeScope.ServiceProvider.GetRequiredService<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>();
        rebound.Should().Throw<ArgumentException>().WithMessage("*подменять фабрику*");
        database.Factory.Contexts.Should().BeEmpty();
    }

    [Fact]
    public async Task Duplicate_registration_should_not_override_existing_source()
    {
        await using var database = await SqliteSourceDatabase.CreateAsync();
        var services = new ServiceCollection();
        services.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>((_, context) => database.CreateSourceOptions() with { Context = context });
        var count = services.Count;
        Action duplicate = () => services.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>((_, _) => throw new InvalidOperationException());
        duplicate.Should().Throw<ArgumentException>().WithMessage("*уже зарегистрирован*");
        services.Should().HaveCount(count);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Registration_should_reject_blank_names(bool factoryName)
    {
        var services = new ServiceCollection();
        Action action = () => services.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>(
            (_, _) => throw new InvalidOperationException(), factoryName: factoryName ? " " : null, name: factoryName ? null : " ");
        action.Should().Throw<ArgumentException>();
        services.Should().BeEmpty();
    }

    private sealed class ScopeMarker
    {
        public Guid Id { get; } = Guid.NewGuid();
    }
}
