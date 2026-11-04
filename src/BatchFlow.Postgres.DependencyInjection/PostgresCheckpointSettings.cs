namespace BatchFlow.Postgres.DependencyInjection;

/// <summary>
/// Данные конфигурации таблицы checkpoint без подключений, SQL и исполняемых делегатов.
/// При регистрации из IConfiguration создается неизменяемый снимок этих настроек.
/// </summary>
public sealed record PostgresCheckpointSettings
{
    /// <summary>Схема таблицы checkpoint.</summary>
    public string SchemaName { get; init; } = "batchflow";

    /// <summary>Имя таблицы checkpoint.</summary>
    public string TableName { get; init; } = "checkpoints";

    /// <summary>Положительный таймаут SQL-команды в секундах; null оставляет настройку драйвера.</summary>
    public int? CommandTimeoutSeconds { get; init; }

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(SchemaName);
        ArgumentException.ThrowIfNullOrWhiteSpace(TableName);

        if (CommandTimeoutSeconds is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(CommandTimeoutSeconds), "Таймаут должен быть положительным.");
        }
    }
}
