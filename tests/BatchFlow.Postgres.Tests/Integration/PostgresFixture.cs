using System.Globalization;
using BatchFlow.Postgres.Internal;
using Npgsql;
using Xunit;

namespace BatchFlow.Postgres.Tests.Integration;

public sealed class PostgresFixture : IAsyncLifetime
{
    internal const string ConnectionVariable = "BATCHFLOW_POSTGRES_TESTS";
    private NpgsqlDataSource? _dataSource;

    public string SchemaName { get; } = $"batchflow_test_{Guid.NewGuid():N}";
    public NpgsqlDataSource DataSource => _dataSource ?? throw new InvalidOperationException("Тестовая БД не настроена.");
    public string ConnectionString { get; private set; } = string.Empty;
    public string ReadItemsSql { get; }
    public string ReadLateItemsSql { get; }
    public string InsertLateItemSql { get; }
    public string ReadSnapshotItemsSql { get; }
    public string InsertSnapshotItemSql { get; }

    public PostgresFixture()
    {
        ReadItemsSql = Format(PostgresIntegrationQueries.ReadItems);
        ReadLateItemsSql = Format(PostgresIntegrationQueries.ReadLateItems);
        InsertLateItemSql = Format(PostgresIntegrationQueries.InsertLateItem);
        ReadSnapshotItemsSql = Format(PostgresIntegrationQueries.ReadSnapshotItems);
        InsertSnapshotItemSql = Format(PostgresIntegrationQueries.InsertSnapshotItem);
    }

    public async Task InitializeAsync()
    {
        var value = Environment.GetEnvironmentVariable(ConnectionVariable);
        if (string.IsNullOrWhiteSpace(value))
        {
            return;
        }

        var builder = new NpgsqlConnectionStringBuilder(value);
        if (builder.Database?.EndsWith("_tests", StringComparison.Ordinal) is not true)
        {
            throw new InvalidOperationException("Интеграционные тесты разрешены только в отдельной БД с суффиксом _tests.");
        }

        ConnectionString = builder.ConnectionString;
        _dataSource = NpgsqlDataSource.Create(ConnectionString);
        try
        {
            await using var command = DataSource.CreateCommand(Format(PostgresIntegrationQueries.Initialize));
            await command.ExecuteNonQueryAsync();
        }
        catch
        {
            await DisposeAsync();
            throw;
        }
    }

    public async Task DisposeAsync()
    {
        if (_dataSource is null)
        {
            return;
        }

        try
        {
            await using var command = _dataSource.CreateCommand(Format(PostgresIntegrationQueries.DropSchema));
            await command.ExecuteNonQueryAsync();
        }
        finally
        {
            await _dataSource.DisposeAsync();
            _dataSource = null;
        }
    }

    private string Format(string template) =>
        string.Format(CultureInfo.InvariantCulture, template, PostgresSqlIdentifier.Quote(SchemaName));
}

internal sealed class PostgresFactAttribute : FactAttribute
{
    public PostgresFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(PostgresFixture.ConnectionVariable)))
        {
            Skip = $"Задайте {PostgresFixture.ConnectionVariable} для тестовой PostgreSQL-БД.";
        }
    }
}

internal sealed class PostgresTheoryAttribute : TheoryAttribute
{
    public PostgresTheoryAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(PostgresFixture.ConnectionVariable)))
        {
            Skip = $"Задайте {PostgresFixture.ConnectionVariable} для тестовой PostgreSQL-БД.";
        }
    }
}
