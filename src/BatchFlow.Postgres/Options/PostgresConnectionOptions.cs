using Npgsql;

namespace BatchFlow.Postgres.Options;

/// <summary>
/// Описывает, как PostgreSQL-адаптер должен получать подключения к базе данных.
/// Один экземпляр опций использует ровно один способ подключения.
/// </summary>
public sealed class PostgresConnectionOptions
{
    private PostgresConnectionOptions(
        PostgresConnectionMode mode,
        string? connectionString,
        NpgsqlDataSource? dataSource,
        Func<CancellationToken, ValueTask<NpgsqlConnection>>? connectionFactory)
    {
        Mode = mode;
        ConnectionString = connectionString;
        DataSource = dataSource;
        ConnectionFactory = connectionFactory;
    }

    /// <summary>
    /// Возвращает выбранный режим подключения.
    /// </summary>
    public PostgresConnectionMode Mode { get; }

    /// <summary>
    /// Возвращает строку подключения, если выбран режим <see cref="PostgresConnectionMode.ConnectionString" />.
    /// </summary>
    public string? ConnectionString { get; }

    /// <summary>
    /// Возвращает подготовленный <c>NpgsqlDataSource</c>, если выбран режим <see cref="PostgresConnectionMode.DataSource" />.
    /// </summary>
    public NpgsqlDataSource? DataSource { get; }

    /// <summary>
    /// Возвращает пользовательскую фабрику подключений, если выбран режим <see cref="PostgresConnectionMode.ConnectionFactory" />.
    /// Фабрика должна возвращать отдельный экземпляр подключения, которым адаптер может безопасно владеть в рамках одной операции.
    /// </summary>
    public Func<CancellationToken, ValueTask<NpgsqlConnection>>? ConnectionFactory { get; }

    /// <summary>
    /// Создает опции подключения на основе строки подключения PostgreSQL.
    /// </summary>
    /// <param name="connectionString">Строка подключения к PostgreSQL.</param>
    /// <returns>Сконфигурированные опции подключения.</returns>
    public static PostgresConnectionOptions FromConnectionString(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        return new PostgresConnectionOptions(
            PostgresConnectionMode.ConnectionString,
            connectionString,
            dataSource: null,
            connectionFactory: null);
    }

    /// <summary>
    /// Создает опции подключения на основе готового <c>NpgsqlDataSource</c>.
    /// Этот способ удобен, когда приложению нужен единый управляемый пул подключений.
    /// </summary>
    /// <param name="dataSource">Подготовленный источник подключений PostgreSQL.</param>
    /// <returns>Сконфигурированные опции подключения.</returns>
    public static PostgresConnectionOptions FromDataSource(NpgsqlDataSource dataSource)
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        return new PostgresConnectionOptions(
            PostgresConnectionMode.DataSource,
            connectionString: null,
            dataSource,
            connectionFactory: null);
    }

    /// <summary>
    /// Создает опции подключения на основе синхронной фабрики <c>NpgsqlConnection</c>.
    /// Фабрика должна возвращать отдельный экземпляр подключения, который адаптер сможет открыть и затем закрыть.
    /// </summary>
    /// <param name="connectionFactory">Фабрика экземпляров подключения PostgreSQL.</param>
    /// <returns>Сконфигурированные опции подключения.</returns>
    public static PostgresConnectionOptions FromConnectionFactory(Func<NpgsqlConnection> connectionFactory)
    {
        ArgumentNullException.ThrowIfNull(connectionFactory);

        return FromConnectionFactory(_ => ValueTask.FromResult(connectionFactory()));
    }

    /// <summary>
    /// Создает опции подключения на основе асинхронной фабрики <c>NpgsqlConnection</c>.
    /// Фабрика должна возвращать отдельный экземпляр подключения, который адаптер сможет открыть и затем закрыть.
    /// </summary>
    /// <param name="connectionFactory">Асинхронная фабрика экземпляров подключения PostgreSQL.</param>
    /// <returns>Сконфигурированные опции подключения.</returns>
    public static PostgresConnectionOptions FromConnectionFactory(
        Func<CancellationToken, ValueTask<NpgsqlConnection>> connectionFactory)
    {
        ArgumentNullException.ThrowIfNull(connectionFactory);

        return new PostgresConnectionOptions(
            PostgresConnectionMode.ConnectionFactory,
            connectionString: null,
            dataSource: null,
            connectionFactory);
    }
}
