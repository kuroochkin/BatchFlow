namespace BatchFlow.Postgres.Options;

/// <summary>
/// Содержит общие параметры PostgreSQL-источника данных для пакетного чтения.
/// </summary>
public sealed record PostgresBatchSourceOptions
{
    /// <summary>
    /// Определяет, как источник будет получать подключения к PostgreSQL.
    /// </summary>
    public required PostgresConnectionOptions Connection { get; init; }

    /// <summary>
    /// Ограничение времени выполнения SQL-команды в секундах.
    /// Если значение не задано, используется настройка драйвера по умолчанию.
    /// </summary>
    public int? CommandTimeoutSeconds { get; init; }
}
