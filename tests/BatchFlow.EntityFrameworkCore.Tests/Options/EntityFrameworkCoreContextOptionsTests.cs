using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace BatchFlow.EntityFrameworkCore.Tests.Options;

public sealed class EntityFrameworkCoreContextOptionsTests
{
    [Fact]
    public void FromFactory_should_defer_creation_and_transfer_ownership()
    {
        var calls = 0;
        var options = EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(() =>
        {
            calls++;
            return new TestDbContext();
        });

        options.OwnsContext.Should().BeTrue();
        calls.Should().Be(0);
    }

    [Fact]
    public void FromContext_should_keep_external_ownership()
    {
        using var context = new TestDbContext();

        var options = EntityFrameworkCoreContextOptions<TestDbContext>.FromContext(context);

        options.OwnsContext.Should().BeFalse();
    }

    [Fact]
    public void Factory_methods_should_reject_null_dependencies()
    {
        Action efFactory = () => EntityFrameworkCoreContextOptions<TestDbContext>.FromDbContextFactory(null!);
        Action syncFactory = () => EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory((Func<TestDbContext>)null!);
        Action asyncFactory = () => EntityFrameworkCoreContextOptions<TestDbContext>.FromFactory(
            (Func<CancellationToken, ValueTask<TestDbContext>>)null!);
        Action context = () => EntityFrameworkCoreContextOptions<TestDbContext>.FromContext(null!);

        efFactory.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("contextFactory");
        syncFactory.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("contextFactory");
        asyncFactory.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("contextFactory");
        context.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("context");
    }

    [Fact]
    public async Task FromDbContextFactory_should_use_async_creation_and_forward_token()
    {
        using var cancellation = new CancellationTokenSource();
        var factory = new RecordingDbContextFactory();
        var source = new TestBatchSource(EntityFrameworkCoreContextOptions<TestDbContext>.FromDbContextFactory(factory));

        await source.ReadAllAsync(cancellationToken: cancellation.Token);

        factory.Token.Should().Be(cancellation.Token);
        factory.Context.DisposeCount.Should().Be(1);
    }

    private sealed class RecordingDbContextFactory : IDbContextFactory<TestDbContext>
    {
        public TestDbContext Context { get; } = new();

        public CancellationToken Token { get; private set; }

        public TestDbContext CreateDbContext() => throw new InvalidOperationException("Ожидается асинхронное создание.");

        public Task<TestDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default)
        {
            Token = cancellationToken;
            return Task.FromResult(Context);
        }
    }
}
