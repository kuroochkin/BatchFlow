using System.Globalization;
using Npgsql;

internal static class PostgresSampleQueries
{
    public const string Initialize = """
        CREATE SCHEMA {0};
        CREATE TABLE {0}.orders (id bigint PRIMARY KEY, number text NOT NULL);
        CREATE TABLE {0}.exports (mode text NOT NULL, id bigint NOT NULL, number text NOT NULL, PRIMARY KEY (mode, id));
        INSERT INTO {0}.orders (id, number) VALUES (1, 'A-001'), (2, 'A-002'), (3, 'A-003');
        """;

    public const string Read = """
        SELECT id, number FROM {0}.orders
        WHERE (@resume_checkpoint IS NULL OR id > @resume_checkpoint)
        ORDER BY id LIMIT @batch_size
        """;

    public const string Upsert = """
        INSERT INTO {0}.exports (mode, id, number) VALUES (@mode, @id, @number)
        ON CONFLICT (mode, id) DO UPDATE SET number = EXCLUDED.number
        """;

    public const string Count = "SELECT count(*) FROM {0}.exports";

    public const string Cleanup = "DROP SCHEMA IF EXISTS {0} CASCADE";
}

internal sealed class PreparedPostgresSampleQueries
{
    public PreparedPostgresSampleQueries(string schema)
    {
        using var builder = new NpgsqlCommandBuilder();
        var quoted = builder.QuoteIdentifier(schema);
        Initialize = Format(PostgresSampleQueries.Initialize, quoted);
        Read = Format(PostgresSampleQueries.Read, quoted);
        Upsert = Format(PostgresSampleQueries.Upsert, quoted);
        Count = Format(PostgresSampleQueries.Count, quoted);
        Cleanup = Format(PostgresSampleQueries.Cleanup, quoted);
    }

    public string Initialize { get; }
    public string Read { get; }
    public string Upsert { get; }
    public string Count { get; }
    public string Cleanup { get; }

    private static string Format(string template, string quotedSchema) => string.Format(CultureInfo.InvariantCulture, template, quotedSchema);
}
