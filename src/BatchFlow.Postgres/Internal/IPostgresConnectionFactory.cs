using Npgsql;

namespace BatchFlow.Postgres.Internal;

internal interface IPostgresConnectionFactory
{
    ValueTask<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken);
}