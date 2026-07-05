using BatchFlow.Postgres.Options;
using FluentAssertions;
using Npgsql;
using Xunit;

namespace BatchFlow.Postgres.Tests.Options;

public sealed class PostgresConnectionOptionsTests
{
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
    public void FromConnectionString_should_reject_blank_value()
    {
        var action = () => PostgresConnectionOptions.FromConnectionString(" ");

        action.Should().Throw<ArgumentException>();
    }

    private const string ConnectionString =
        "Host=localhost;Port=5432;Database=batchflow;Username=batchflow;Password=batchflow";
}
