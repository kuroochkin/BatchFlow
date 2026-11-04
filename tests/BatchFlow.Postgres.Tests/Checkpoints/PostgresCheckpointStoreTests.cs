using BatchFlow.Postgres.Checkpoints;
using BatchFlow.Postgres.Options;
using FluentAssertions;
using Npgsql;
using Xunit;

namespace BatchFlow.Postgres.Tests.Checkpoints;

public sealed class PostgresCheckpointStoreTests
{
    [Fact]
    public async Task Constructor_should_reject_adapter_owned_existing_connection_without_disposing_it()
    {
        await using var connection = new NpgsqlConnection();
        var options = new PostgresCheckpointStoreOptions
        {
            Connection = PostgresConnectionOptions.FromConnection(connection, PostgresConnectionOwnership.AdapterOwned),
        };

        var action = () => new PostgresCheckpointStore<long>(options);

        var exception = action.Should().Throw<ArgumentException>()
            .WithMessage("*AdapterOwned*");
        exception.Which.ParamName.Should().Be("options");

        var openAction = () => connection.OpenAsync(CancellationToken.None);
        await openAction.Should().ThrowExactlyAsync<InvalidOperationException>();
    }

    [Fact]
    public void Constructor_should_accept_external_existing_connection_without_opening_it()
    {
        using var connection = new NpgsqlConnection();
        var options = new PostgresCheckpointStoreOptions
        {
            Connection = PostgresConnectionOptions.FromConnection(connection, PostgresConnectionOwnership.External),
        };

        var store = new PostgresCheckpointStore<long>(options);

        store.Options.Should().BeSameAs(options);
        connection.State.Should().Be(System.Data.ConnectionState.Closed);
    }
}
