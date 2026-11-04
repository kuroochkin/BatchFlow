using System.Globalization;
using System.Text.Json;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Postgres.Internal;
using BatchFlow.Postgres.Options;
using Npgsql;
using NpgsqlTypes;

namespace BatchFlow.Postgres.Checkpoints;

/// <summary>
/// Сохраняет и загружает контрольные точки BatchFlow в таблице PostgreSQL.
/// Реализация использует оптимистичную конкуренцию через <c>concurrency_token</c> и хранит значение checkpoint в формате JSON.
/// </summary>
/// <typeparam name="TCheckpoint">Тип значения контрольной точки.</typeparam>
public sealed class PostgresCheckpointStore<TCheckpoint> : ICheckpointStore<TCheckpoint>
{
    private static readonly IReadOnlyDictionary<string, string> EmptyMetadata =
        new Dictionary<string, string>(StringComparer.Ordinal);

    private readonly IPostgresConnectionFactory _connectionFactory;
    private readonly IPostgresCheckpointSerializer<TCheckpoint> _serializer;
    private readonly string _ensureStorageCommandText;
    private readonly string _loadCommandText;
    private readonly string _insertCommandText;
    private readonly string _updateCommandText;

    /// <summary>
    /// Инициализирует PostgreSQL-хранилище контрольных точек.
    /// </summary>
    /// <param name="options">Параметры подключения и структуры хранения.</param>
    /// <param name="serializer">Сериализатор значения контрольной точки. Если значение не задано, используется JSON-сериализация по умолчанию.</param>
    /// <exception cref="ArgumentException">
    /// Передано готовое соединение с владением <see cref="PostgresConnectionOwnership.AdapterOwned" />
    /// или пустое имя схемы либо таблицы. Для повторных операций используйте строку подключения,
    /// <c>NpgsqlDataSource</c>, фабрику либо готовое соединение с внешним владением.
    /// </exception>
    public PostgresCheckpointStore(
        PostgresCheckpointStoreOptions options,
        IPostgresCheckpointSerializer<TCheckpoint>? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Connection);

        if (options.Connection.Mode == PostgresConnectionMode.ExistingConnection &&
            options.Connection.ConnectionOwnership == PostgresConnectionOwnership.AdapterOwned)
        {
            throw new ArgumentException(
                "Checkpoint store не поддерживает готовое соединение с владением AdapterOwned: " +
                "оно освобождается после первой операции и не подходит для повторных загрузок и сохранений. " +
                "Используйте строку подключения, NpgsqlDataSource, фабрику или готовое соединение с владением External.",
                nameof(options));
        }

        if (options.CommandTimeoutSeconds is <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "Таймаут SQL-команды должен быть положительным числом секунд.");
        }

        if (string.IsNullOrWhiteSpace(options.SchemaName))
        {
            throw new ArgumentException("Имя схемы не может быть пустым.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.TableName))
        {
            throw new ArgumentException("Имя таблицы не может быть пустым.", nameof(options));
        }

        Options = options;
        _serializer = serializer ?? new JsonPostgresCheckpointSerializer<TCheckpoint>();
        _connectionFactory = PostgresConnectionFactoryResolver.Create(options.Connection);
        var quotedSchemaName = PostgresSqlIdentifier.Quote(options.SchemaName);
        var qualifiedTableName = $"{quotedSchemaName}.{PostgresSqlIdentifier.Quote(options.TableName)}";

        _ensureStorageCommandText = string.Format(
            CultureInfo.InvariantCulture,
            PostgresCheckpointQueries.EnsureStorage,
            quotedSchemaName,
            qualifiedTableName);
        _loadCommandText = string.Format(CultureInfo.InvariantCulture, PostgresCheckpointQueries.Load, qualifiedTableName);
        _insertCommandText = string.Format(CultureInfo.InvariantCulture, PostgresCheckpointQueries.Insert, qualifiedTableName);
        _updateCommandText = string.Format(CultureInfo.InvariantCulture, PostgresCheckpointQueries.Update, qualifiedTableName);
    }

    /// <summary>
    /// Возвращает параметры PostgreSQL-хранилища контрольных точек.
    /// </summary>
    public PostgresCheckpointStoreOptions Options { get; }

    /// <summary>
    /// Создает схему и таблицу хранения контрольных точек, если они еще не существуют.
    /// Метод выполняется явно и не вызывается автоматически внутри <see cref="LoadAsync" /> или <see cref="SaveAsync" />.
    /// </summary>
    /// <param name="cancellationToken">Токен отмены.</param>
    public async ValueTask EnsureStorageAsync(CancellationToken cancellationToken = default)
    {
        await using var connectionLease = await _connectionFactory.OpenConnectionAsync(cancellationToken);
        var connection = connectionLease.Connection;
        await using var command = CreateCommand(connection, _ensureStorageCommandText);

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<StoredCheckpoint<TCheckpoint>?> LoadAsync(
        string checkpointId,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);

        try
        {
            await using var connectionLease = await _connectionFactory.OpenConnectionAsync(cancellationToken);
            var connection = connectionLease.Connection;
            await using var command = CreateCommand(connection, _loadCommandText);

            command.Parameters.AddWithValue("checkpoint_id", NpgsqlDbType.Text, checkpointId);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return new StoredCheckpoint<TCheckpoint>
            {
                Value = _serializer.Deserialize(reader.GetFieldValue<string>(0)),
                SequenceNumber = reader.IsDBNull(1) ? null : reader.GetInt64(1),
                ConcurrencyToken = reader.GetString(2),
                SavedAt = reader.GetFieldValue<DateTimeOffset>(3),
                Metadata = DeserializeMetadata(reader.GetFieldValue<string>(4)),
            };
        }
        catch (PostgresException ex) when (IsStorageMissing(ex))
        {
            throw CreateStorageMissingException(ex);
        }
    }

    /// <inheritdoc />
    public async ValueTask<StoredCheckpoint<TCheckpoint>> SaveAsync(
        string checkpointId,
        StoredCheckpoint<TCheckpoint> checkpoint,
        string? expectedConcurrencyToken,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);
        ArgumentNullException.ThrowIfNull(checkpoint);

        var savedCheckpoint = checkpoint with
        {
            ConcurrencyToken = Guid.NewGuid().ToString("N"),
            SavedAt = checkpoint.SavedAt == default ? DateTimeOffset.UtcNow : checkpoint.SavedAt,
            Metadata = CloneMetadata(checkpoint.Metadata),
        };

        try
        {
            await using var connectionLease = await _connectionFactory.OpenConnectionAsync(cancellationToken);
            var connection = connectionLease.Connection;
            await using var command = CreateCommand(
                connection,
                expectedConcurrencyToken is null
                    ? _insertCommandText
                    : _updateCommandText);

            command.Parameters.AddWithValue("checkpoint_id", NpgsqlDbType.Text, checkpointId);
            command.Parameters.AddWithValue("checkpoint_value", NpgsqlDbType.Jsonb, _serializer.Serialize(savedCheckpoint.Value));
            command.Parameters.AddWithValue(
                "sequence_number",
                NpgsqlDbType.Bigint,
                savedCheckpoint.SequenceNumber is { } sequenceNumber
                    ? sequenceNumber
                    : DBNull.Value);
            command.Parameters.AddWithValue("concurrency_token", NpgsqlDbType.Text, savedCheckpoint.ConcurrencyToken);
            command.Parameters.AddWithValue("saved_at", NpgsqlDbType.TimestampTz, savedCheckpoint.SavedAt);
            command.Parameters.AddWithValue("metadata", NpgsqlDbType.Jsonb, SerializeMetadata(savedCheckpoint.Metadata));

            if (expectedConcurrencyToken is not null)
            {
                command.Parameters.AddWithValue(
                    "expected_concurrency_token",
                    NpgsqlDbType.Text,
                    expectedConcurrencyToken);
            }

            var result = await command.ExecuteScalarAsync(cancellationToken);

            if (result is null || result is DBNull)
            {
                throw new CheckpointConflictException(checkpointId);
            }

            return savedCheckpoint;
        }
        catch (PostgresException ex) when (IsStorageMissing(ex))
        {
            throw CreateStorageMissingException(ex);
        }
    }

    private NpgsqlCommand CreateCommand(NpgsqlConnection connection, string commandText)
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

    private static bool IsStorageMissing(PostgresException exception) =>
        string.Equals(exception.SqlState, PostgresErrorCodes.InvalidSchemaName, StringComparison.Ordinal) ||
        string.Equals(exception.SqlState, PostgresErrorCodes.UndefinedTable, StringComparison.Ordinal);

    private static InvalidOperationException CreateStorageMissingException(PostgresException exception) =>
        new(
            "Хранилище checkpoint в PostgreSQL не инициализировано. Создайте таблицу вручную или вызовите EnsureStorageAsync перед использованием адаптера.",
            exception);

    private static IReadOnlyDictionary<string, string> CloneMetadata(IReadOnlyDictionary<string, string>? metadata)
    {
        if (metadata is null || metadata.Count == 0)
        {
            return EmptyMetadata;
        }

        return new Dictionary<string, string>(metadata, StringComparer.Ordinal);
    }

    private static string SerializeMetadata(IReadOnlyDictionary<string, string>? metadata) =>
        JsonSerializer.Serialize(CloneMetadata(metadata));

    private static IReadOnlyDictionary<string, string> DeserializeMetadata(string payload)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(payload);

        var metadata = JsonSerializer.Deserialize<Dictionary<string, string>>(payload);
        return metadata is null || metadata.Count == 0
            ? EmptyMetadata
            : new Dictionary<string, string>(metadata, StringComparer.Ordinal);
    }
}
