using System.Runtime.CompilerServices;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Postgres.Internal;
using BatchFlow.Postgres.Options;
using Npgsql;

namespace BatchFlow.Postgres.Sources;

/// <summary>
/// Представляет базовый класс для PostgreSQL-источников данных, читающих элементы пакетами.
/// Класс берет на себя открытие подключения и оставляет конкретную SQL-логику наследнику.
/// </summary>
/// <typeparam name="TItem">Тип элементов, возвращаемых источником.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, используемой для возобновления чтения.</typeparam>
public abstract class PostgresBatchSourceBase<TItem, TCheckpoint> : IBatchSource<TItem, TCheckpoint>
{
    private readonly IPostgresConnectionFactory _connectionFactory;

    /// <summary>
    /// Инициализирует PostgreSQL-источник с указанными параметрами подключения и выполнения SQL-команд.
    /// </summary>
    /// <param name="options">Параметры источника.</param>
    protected PostgresBatchSourceBase(PostgresBatchSourceOptions options)
        : this(options, CreateConnectionFactory(options))
    {
    }

    internal PostgresBatchSourceBase(
        PostgresBatchSourceOptions options,
        IPostgresConnectionFactory connectionFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Connection);
        ArgumentNullException.ThrowIfNull(connectionFactory);

        if (options.CommandTimeoutSeconds is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Таймаут SQL-команды должен быть положительным числом секунд.");
        }

        Options = options;
        _connectionFactory = connectionFactory;
    }

    private static IPostgresConnectionFactory CreateConnectionFactory(PostgresBatchSourceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Connection);
        return PostgresConnectionFactoryResolver.Create(options.Connection);
    }

    /// <summary>
    /// Возвращает параметры PostgreSQL-источника.
    /// </summary>
    protected PostgresBatchSourceOptions Options { get; }

    /// <summary>
    /// Для PostgreSQL-источников по умолчанию предполагается поддержка возобновления чтения.
    /// </summary>
    public virtual bool SupportsResume => true;

    /// <inheritdoc />
    public async IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> ReadAsync(
        BatchSourceContext<TCheckpoint> context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(context.RequestedBatchSize);
        cancellationToken.ThrowIfCancellationRequested();

        await using var connectionLease = await _connectionFactory.OpenConnectionAsync(cancellationToken);
        var connection = connectionLease.Connection;

        await foreach (var batch in ReadCoreAsync(connection, context, cancellationToken).WithCancellation(cancellationToken))
        {
            ValidateBatch(batch);
            yield return batch;
        }
    }

    /// <summary>
    /// Выполняет фактическое чтение данных из PostgreSQL через уже открытое подключение.
    /// Наследник должен возвращать только непустые пакеты.
    /// </summary>
    /// <param name="connection">Уже открытое подключение к PostgreSQL.</param>
    /// <param name="context">Контекст чтения source-данных.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Последовательность пакетов данных.</returns>
    protected abstract IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> ReadCoreAsync(
        NpgsqlConnection connection,
        BatchSourceContext<TCheckpoint> context,
        CancellationToken cancellationToken);

    /// <summary>
    /// Создает SQL-команду с применением настроек таймаута из <see cref="Options" />.
    /// Метод удобно использовать в наследниках при построении запросов.
    /// </summary>
    /// <param name="connection">Открытое подключение к PostgreSQL.</param>
    /// <param name="commandText">Текст SQL-команды.</param>
    /// <returns>Сконфигурированная SQL-команда.</returns>
    protected virtual NpgsqlCommand CreateCommand(NpgsqlConnection connection, string commandText)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentException.ThrowIfNullOrWhiteSpace(commandText);

        var command = connection.CreateCommand();
        command.CommandText = commandText;

        if (Options.CommandTimeoutSeconds is { } timeoutSeconds)
        {
            command.CommandTimeout = timeoutSeconds;
        }

        return command;
    }

    /// <summary>
    /// Выполняет минимальную проверку возвращаемого пакета.
    /// Если наследник возвращает пустой пакет, это трактуется как ошибка реализации.
    /// </summary>
    /// <param name="batch">Пакет, возвращенный PostgreSQL-источником.</param>
    protected virtual void ValidateBatch(SourceBatch<TItem, TCheckpoint> batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentNullException.ThrowIfNull(batch.Items);

        if (batch.Items.Count == 0)
        {
            throw new InvalidOperationException("PostgreSQL-источник вернул пустой пакет. Источник должен завершить последовательность, а не отдавать пустые наборы данных.");
        }
    }
}
