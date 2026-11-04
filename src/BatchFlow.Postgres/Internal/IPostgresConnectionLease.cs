using Npgsql;

namespace BatchFlow.Postgres.Internal;

internal interface IPostgresConnectionLease : IAsyncDisposable
{
    NpgsqlConnection Connection { get; }
}
