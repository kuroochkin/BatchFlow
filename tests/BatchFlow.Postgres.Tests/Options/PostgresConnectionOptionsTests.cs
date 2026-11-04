using BatchFlow.Postgres.Options;
using FluentAssertions;
using Npgsql;
using Xunit;

namespace BatchFlow.Postgres.Tests.Options;

public sealed class PostgresConnectionOptionsTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(2)]
    public void FromConnection_should_reject_unknown_ownership(int ownership)
    {
        using var connection = new NpgsqlConnection();
        var action = () => PostgresConnectionOptions.FromConnection(connection, (PostgresConnectionOwnership)ownership);
        action.Should().Throw<ArgumentOutOfRangeException>().Which.ParamName.Should().Be("ownership");
    }

    [Fact]
    public void FromConnectionString_should_create_connection_string_mode()
    {
        var options = PostgresConnectionOptions.FromConnectionString(ConnectionString);

        options.Mode.Should().Be(PostgresConnectionMode.ConnectionString);
        options.ConnectionString.Should().Be(ConnectionString);
        options.DataSource.Should().BeNull();
        options.ConnectionFactory.Should().BeNull();
    }

    [Fact]
    public void FromDataSource_should_create_data_source_mode()
    {
        using var dataSource = NpgsqlDataSource.Create(ConnectionString);

        var options = PostgresConnectionOptions.FromDataSource(dataSource);

        options.Mode.Should().Be(PostgresConnectionMode.DataSource);
        options.DataSource.Should().BeSameAs(dataSource);
        options.ConnectionString.Should().BeNull();
        options.ConnectionFactory.Should().BeNull();
    }

    [Fact]
    public void FromConnectionFactory_should_create_factory_mode()
    {
        var options = PostgresConnectionOptions.FromConnectionFactory(
            () => new NpgsqlConnection(ConnectionString));

        options.Mode.Should().Be(PostgresConnectionMode.ConnectionFactory);
        options.ConnectionFactory.Should().NotBeNull();
        options.ConnectionString.Should().BeNull();
        options.DataSource.Should().BeNull();
    }

    [Fact]
    public void FromConnection_should_create_existing_connection_mode()
    {
        using var connection = new NpgsqlConnection(ConnectionString);

        var options = PostgresConnectionOptions.FromConnection(
            connection,
            PostgresConnectionOwnership.External);

        options.Mode.Should().Be(PostgresConnectionMode.ExistingConnection);
        options.Connection.Should().BeSameAs(connection);
        options.ConnectionOwnership.Should().Be(PostgresConnectionOwnership.External);
        options.ConnectionFactory.Should().BeNull();
        options.ConnectionString.Should().BeNull();
        options.DataSource.Should().BeNull();
    }

    [Fact]
    public void FromConnectionString_should_reject_blank_value()
    {
        var action = () => PostgresConnectionOptions.FromConnectionString(" ");

        action.Should().Throw<ArgumentException>();
    }

    private const string ConnectionString =
        "Host=localhost;Port=5432;Database=batchflow;Username=batchflow;Password=batchflow";
}
