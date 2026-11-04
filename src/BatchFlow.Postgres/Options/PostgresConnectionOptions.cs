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
        Func<CancellationToken, ValueTask<NpgsqlConnection>>? connectionFactory,
        NpgsqlConnection? connection,
        PostgresConnectionOwnership? connectionOwnership)
    {
        Mode = mode;
        ConnectionString = connectionString;
        DataSource = dataSource;
        ConnectionFactory = connectionFactory;
        Connection = connection;
        ConnectionOwnership = connectionOwnership;
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
    /// Возвращает готовое подключение, если выбран режим <see cref="PostgresConnectionMode.ExistingConnection" />.
    /// </summary>
    public NpgsqlConnection? Connection { get; }

    /// <summary>
    /// Возвращает модель владения готовым подключением, если выбран режим <see cref="PostgresConnectionMode.ExistingConnection" />.
    /// </summary>
    public PostgresConnectionOwnership? ConnectionOwnership { get; }

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
            connectionFactory: null,
            connection: null,
            connectionOwnership: null);
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
            connectionFactory: null,
            connection: null,
            connectionOwnership: null);
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
            connectionFactory,
            connection: null,
            connectionOwnership: null);
    }

    /// <summary>
    /// Создает опции подключения на основе уже созданного экземпляра <c>NpgsqlConnection</c>.
    /// Этот вариант нужен для сценариев, где жизненный цикл подключения контролируется отдельно от BatchFlow.
    /// </summary>
    /// <param name="connection">Готовый экземпляр подключения PostgreSQL.</param>
    /// <param name="ownership">Модель владения переданным подключением.</param>
    /// <returns>Сконфигурированные опции подключения.</returns>
    /// <remarks>
    /// В режиме <see cref="PostgresConnectionOwnership.External" /> вызывающий код заранее открывает соединение
    /// и самостоятельно освобождает его. Режим <see cref="PostgresConnectionOwnership.AdapterOwned" />
    /// подходит только для одного перечисления источника и не допускается в хранилище контрольных точек.
    /// Для повторных запусков с владением адаптера используйте фабрику, строку подключения или <c>NpgsqlDataSource</c>.
    /// </remarks>
    public static PostgresConnectionOptions FromConnection(
        NpgsqlConnection connection,
        PostgresConnectionOwnership ownership = PostgresConnectionOwnership.External)
    {
        ArgumentNullException.ThrowIfNull(connection);

        if (!Enum.IsDefined(ownership))
        {
            throw new ArgumentOutOfRangeException(nameof(ownership), ownership, "Неизвестная модель владения соединением.");
        }

        return new PostgresConnectionOptions(
            PostgresConnectionMode.ExistingConnection,
            connectionString: null,
            dataSource: null,
            connectionFactory: null,
            connection,
            ownership);
    }
}
