namespace BatchFlow.Postgres.Options;

/// <summary>
/// Содержит параметры PostgreSQL-хранилища контрольных точек.
/// </summary>
public sealed record PostgresCheckpointStoreOptions
{
    /// <summary>
    /// Определяет, как хранилище будет получать подключения к PostgreSQL.
    /// </summary>
    /// <remarks>
    /// Готовое соединение допускается только с владением <see cref="PostgresConnectionOwnership.External" />:
    /// хранилище выполняет несколько отдельных операций и не может освобождать один экземпляр после каждой из них.
    /// Для независимых подключений используйте строку подключения, <c>NpgsqlDataSource</c> или фабрику,
    /// возвращающую новый экземпляр при каждом вызове.
    /// </remarks>
    public required PostgresConnectionOptions Connection { get; init; }

    /// <summary>
    /// Имя схемы, в которой располагается таблица контрольных точек.
    /// По умолчанию используется схема <c>batchflow</c>.
    /// </summary>
    public string SchemaName { get; init; } = "batchflow";

    /// <summary>
    /// Имя таблицы, в которой сохраняются контрольные точки.
    /// По умолчанию используется таблица <c>checkpoints</c>.
    /// </summary>
    public string TableName { get; init; } = "checkpoints";

    /// <summary>
    /// Ограничение времени выполнения SQL-команды в секундах.
    /// Если значение не задано, используется настройка драйвера по умолчанию.
    /// </summary>
    public int? CommandTimeoutSeconds { get; init; }
}
