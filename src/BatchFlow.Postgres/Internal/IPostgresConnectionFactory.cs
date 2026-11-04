using Npgsql;

namespace BatchFlow.Postgres.Internal;

internal interface IPostgresConnectionFactory
{
    ValueTask<IPostgresConnectionLease> OpenConnectionAsync(CancellationToken cancellationToken);
}
