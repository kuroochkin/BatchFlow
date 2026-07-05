using System.Text.Json;

namespace BatchFlow.Postgres.Checkpoints;

/// <summary>
/// Сериализует контрольную точку в JSON и обратно.
/// Эта реализация подходит как дефолтный вариант для большинства сценариев,
/// когда checkpoint можно безопасно представить через <see cref="JsonSerializer" />.
/// </summary>
/// <typeparam name="TCheckpoint">Тип значения контрольной точки.</typeparam>
public sealed class JsonPostgresCheckpointSerializer<TCheckpoint> : IPostgresCheckpointSerializer<TCheckpoint>
{
    private readonly JsonSerializerOptions _serializerOptions;

    /// <summary>
    /// Инициализирует JSON-сериализатор контрольных точек.
    /// </summary>
    /// <param name="serializerOptions">Пользовательские JSON-настройки. Если значение не задано, используются настройки по умолчанию.</param>
    public JsonPostgresCheckpointSerializer(JsonSerializerOptions? serializerOptions = null)
    {
        _serializerOptions = serializerOptions ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
    }

    /// <inheritdoc />
    public string Serialize(TCheckpoint checkpoint) =>
        JsonSerializer.Serialize(checkpoint, _serializerOptions);

    /// <inheritdoc />
    public TCheckpoint Deserialize(string payload)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(payload);
        return JsonSerializer.Deserialize<TCheckpoint>(payload, _serializerOptions)!;
    }
}