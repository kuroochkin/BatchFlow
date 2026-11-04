namespace BatchFlow.Postgres.Tests.Integration;

internal static class PostgresIntegrationQueries
{
    internal const string Initialize = """
        create schema {0};
        create table {0}.items (id bigint primary key, occurred_at timestamptz not null, payload jsonb not null);
        insert into {0}.items
        select id, '2026-11-04 12:00:00+00'::timestamptz, jsonb_build_object('id', id)
        from generate_series(1, 10) as id;
        create table {0}.late_items (id bigint primary key);
        create table {0}.snapshot_items (id bigint primary key);
        insert into {0}.snapshot_items values (1), (2);
        """;
    internal const string ReadItems = """
        select id, occurred_at, payload from {0}.items
        where (@resume_checkpoint is null or id > @resume_checkpoint)
        order by id limit @batch_size
        """;
    internal const string ReadLateItems = """
        select id from {0}.late_items
        where (@resume_checkpoint is null or id > @resume_checkpoint)
        order by id limit @batch_size
        """;
    internal const string InsertLateItem = "insert into {0}.late_items (id) values (@id)";
    internal const string InsertSnapshotItem = "insert into {0}.snapshot_items values (3)";
    internal const string ReadSnapshotItems = """
        select id from {0}.snapshot_items
        where (@resume_checkpoint is null or id > @resume_checkpoint)
        order by id limit @batch_size
        """;
    internal const string DropSchema = "drop schema if exists {0} cascade";
    internal const string Sleep = "select pg_sleep(30)";
    internal const string IsSleeping = "select exists (select 1 from pg_stat_activity where pid = @pid and wait_event = 'PgSleep')";
    internal const string Ping = "select 42";
}
