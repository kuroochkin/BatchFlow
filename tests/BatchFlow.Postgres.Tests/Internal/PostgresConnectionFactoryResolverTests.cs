using BatchFlow.Postgres.Internal;
using BatchFlow.Postgres.Options;
using FluentAssertions;
using Npgsql;
using Xunit;

namespace BatchFlow.Postgres.Tests.Internal;

public sealed class PostgresConnectionFactoryResolverTests
{
    [Theory]
    [InlineData(PostgresConnectionMode.ConnectionFactory)]
    [InlineData(PostgresConnectionMode.ExistingConnection)]
    public async Task OpenConnectionAsync_should_dispose_owned_connection_when_open_fails(PostgresConnectionMode mode)
    {
        await using var connection = new NpgsqlConnection();
        var factory = PostgresConnectionFactoryResolver.Create(CreateOwnedOptions(connection, mode));

        var action = async () => await factory.OpenConnectionAsync(CancellationToken.None);

        await action.Should().ThrowExactlyAsync<InvalidOperationException>();
        await AssertConnectionDisposedAsync(connection);
    }

    [Theory]
    [InlineData(PostgresConnectionMode.ConnectionFactory)]
    [InlineData(PostgresConnectionMode.ExistingConnection)]
    public async Task OpenConnectionAsync_should_dispose_owned_connection_when_cancelled(PostgresConnectionMode mode)
    {
        await using var connection = new NpgsqlConnection();
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();
        var factory = PostgresConnectionFactoryResolver.Create(CreateOwnedOptions(connection, mode));

        var action = async () => await factory.OpenConnectionAsync(cancellationSource.Token);

        var exception = await action.Should().ThrowAsync<OperationCanceledException>();
        exception.Which.CancellationToken.Should().Be(cancellationSource.Token);
        await AssertConnectionDisposedAsync(connection);
    }

    [Fact]
    public async Task OpenConnectionAsync_should_dispose_connection_when_factory_returns_it_after_cancellation()
    {
        await using var connection = new NpgsqlConnection();
        using var cancellationSource = new CancellationTokenSource();
        var options = PostgresConnectionOptions.FromConnectionFactory(token =>
        {
            token.Should().Be(cancellationSource.Token);
            cancellationSource.Cancel();
            return ValueTask.FromResult(connection);
        });
        var factory = PostgresConnectionFactoryResolver.Create(options);

        var action = async () => await factory.OpenConnectionAsync(cancellationSource.Token);

        var exception = await action.Should().ThrowAsync<OperationCanceledException>();
        exception.Which.CancellationToken.Should().Be(cancellationSource.Token);
        await AssertConnectionDisposedAsync(connection);
    }

    [Fact]
    public async Task OpenConnectionAsync_should_preserve_factory_exception()
    {
        var expectedException = new InvalidOperationException("Ошибка пользовательской фабрики.");
        var options = PostgresConnectionOptions.FromConnectionFactory(
            _ => ValueTask.FromException<NpgsqlConnection>(expectedException));
        var factory = PostgresConnectionFactoryResolver.Create(options);

        var action = async () => await factory.OpenConnectionAsync(CancellationToken.None);

        var exception = await action.Should().ThrowExactlyAsync<InvalidOperationException>();
        exception.Which.Should().BeSameAs(expectedException);
    }

    [Fact]
    public async Task OpenConnectionAsync_should_not_dispose_closed_external_connection()
    {
        await using var connection = new NpgsqlConnection();
        var options = PostgresConnectionOptions.FromConnection(connection, PostgresConnectionOwnership.External);
        var factory = PostgresConnectionFactoryResolver.Create(options);

        var action = async () => await factory.OpenConnectionAsync(CancellationToken.None);

        await action.Should().ThrowExactlyAsync<InvalidOperationException>()
            .WithMessage("*External*");

        var openAction = () => connection.OpenAsync(CancellationToken.None);
        await openAction.Should().ThrowExactlyAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task OpenConnectionAsync_should_not_dispose_external_connection_when_cancelled()
    {
        await using var connection = new NpgsqlConnection();
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();
        var options = PostgresConnectionOptions.FromConnection(connection, PostgresConnectionOwnership.External);
        var factory = PostgresConnectionFactoryResolver.Create(options);

        var action = async () => await factory.OpenConnectionAsync(cancellationSource.Token);

        var exception = await action.Should().ThrowAsync<OperationCanceledException>();
        exception.Which.CancellationToken.Should().Be(cancellationSource.Token);

        var openAction = () => connection.OpenAsync(CancellationToken.None);
        await openAction.Should().ThrowExactlyAsync<InvalidOperationException>();
    }

    private static PostgresConnectionOptions CreateOwnedOptions(NpgsqlConnection connection, PostgresConnectionMode mode) =>
        mode switch
        {
            PostgresConnectionMode.ConnectionFactory => PostgresConnectionOptions.FromConnectionFactory(() => connection),
            PostgresConnectionMode.ExistingConnection => PostgresConnectionOptions.FromConnection(
                connection,
                PostgresConnectionOwnership.AdapterOwned),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };

    private static async Task AssertConnectionDisposedAsync(NpgsqlConnection connection)
    {
        var action = () => connection.OpenAsync(CancellationToken.None);
        await action.Should().ThrowExactlyAsync<ObjectDisposedException>();
    }
}
