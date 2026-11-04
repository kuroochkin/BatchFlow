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
            PostgresConnectionMode.ExistingConnection => new ExistingConnectionPostgresConnectionFactory(
                options.Connection!,
                options.ConnectionOwnership ?? PostgresConnectionOwnership.External),
            _ => throw new ArgumentOutOfRangeException(nameof(options), options.Mode, "Неизвестный режим подключения PostgreSQL."),
        };
    }

    private static async ValueTask<IPostgresConnectionLease> OpenOwnedConnectionAsync(
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync(cancellationToken);
            }

            return new OwnedPostgresConnectionLease(connection);
        }
        catch
        {
            try
            {
                await connection.DisposeAsync();
            }
            catch
            {
                // Ошибка освобождения не должна подменять исходную ошибку открытия или отмену.
            }

            throw;
        }
    }

    private sealed class ConnectionStringPostgresConnectionFactory(string connectionString) : IPostgresConnectionFactory
    {
        private readonly string _connectionString = connectionString;

        public ValueTask<IPostgresConnectionLease> OpenConnectionAsync(CancellationToken cancellationToken) =>
            OpenOwnedConnectionAsync(new NpgsqlConnection(_connectionString), cancellationToken);
    }

    private sealed class DataSourcePostgresConnectionFactory(NpgsqlDataSource dataSource) : IPostgresConnectionFactory
    {
        private readonly NpgsqlDataSource _dataSource = dataSource;

        public async ValueTask<IPostgresConnectionLease> OpenConnectionAsync(CancellationToken cancellationToken) =>
            new OwnedPostgresConnectionLease(await _dataSource.OpenConnectionAsync(cancellationToken));
    }

    private sealed class DelegatePostgresConnectionFactory(
        Func<CancellationToken, ValueTask<NpgsqlConnection>> connectionFactory) : IPostgresConnectionFactory
    {
        private readonly Func<CancellationToken, ValueTask<NpgsqlConnection>> _connectionFactory = connectionFactory;

        public async ValueTask<IPostgresConnectionLease> OpenConnectionAsync(CancellationToken cancellationToken)
        {
            var connection = await _connectionFactory(cancellationToken);
            ArgumentNullException.ThrowIfNull(connection);

            return await OpenOwnedConnectionAsync(connection, cancellationToken);
        }
    }

    private sealed class ExistingConnectionPostgresConnectionFactory(
        NpgsqlConnection connection,
        PostgresConnectionOwnership ownership) : IPostgresConnectionFactory
    {
        private readonly NpgsqlConnection _connection = connection;
        private readonly PostgresConnectionOwnership _ownership = ownership;

        public ValueTask<IPostgresConnectionLease> OpenConnectionAsync(CancellationToken cancellationToken)
        {
            if (_ownership == PostgresConnectionOwnership.AdapterOwned)
            {
                return OpenOwnedConnectionAsync(_connection, cancellationToken);
            }

            cancellationToken.ThrowIfCancellationRequested();

            if (_connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException(
                    "Для режима External готовое NpgsqlConnection должно быть уже открыто к моменту запуска операции.");
            }

            return ValueTask.FromResult<IPostgresConnectionLease>(new BorrowedPostgresConnectionLease(_connection));
        }
    }

    private sealed class OwnedPostgresConnectionLease(NpgsqlConnection connection) : IPostgresConnectionLease
    {
        public NpgsqlConnection Connection { get; } = connection;

        public ValueTask DisposeAsync() => Connection.DisposeAsync();
    }

    private sealed class BorrowedPostgresConnectionLease(NpgsqlConnection connection) : IPostgresConnectionLease
    {
        public NpgsqlConnection Connection { get; } = connection;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
