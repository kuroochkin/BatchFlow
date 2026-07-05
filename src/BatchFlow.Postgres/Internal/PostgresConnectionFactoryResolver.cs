using System.Data;
using BatchFlow.Postgres.Options;
using Npgsql;

namespace BatchFlow.Postgres.Internal;

internal static class PostgresConnectionFactoryResolver
{
    public static IPostgresConnectionFactory Create(PostgresConnectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        return options.Mode switch
        {
            PostgresConnectionMode.ConnectionString => new ConnectionStringPostgresConnectionFactory(options.ConnectionString!),
            PostgresConnectionMode.DataSource => new DataSourcePostgresConnectionFactory(options.DataSource!),
            PostgresConnectionMode.ConnectionFactory => new DelegatePostgresConnectionFactory(options.ConnectionFactory!),
            _ => throw new ArgumentOutOfRangeException(nameof(options), options.Mode, "Неизвестный режим подключения PostgreSQL."),
        };
    }

    private sealed class ConnectionStringPostgresConnectionFactory(string connectionString) : IPostgresConnectionFactory
    {
        private readonly string _connectionString = connectionString;

        public async ValueTask<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
        {
            var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);
            return connection;
        }
    }

    private sealed class DataSourcePostgresConnectionFactory(NpgsqlDataSource dataSource) : IPostgresConnectionFactory
    {
        private readonly NpgsqlDataSource _dataSource = dataSource;

        public ValueTask<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken) =>
            _dataSource.OpenConnectionAsync(cancellationToken);
    }

    private sealed class DelegatePostgresConnectionFactory(
        Func<CancellationToken, ValueTask<NpgsqlConnection>> connectionFactory) : IPostgresConnectionFactory
    {
        private readonly Func<CancellationToken, ValueTask<NpgsqlConnection>> _connectionFactory = connectionFactory;

        public async ValueTask<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
        {
            var connection = await _connectionFactory(cancellationToken);
            ArgumentNullException.ThrowIfNull(connection);

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(cancellationToken);
            }

            return connection;
        }
    }
}
