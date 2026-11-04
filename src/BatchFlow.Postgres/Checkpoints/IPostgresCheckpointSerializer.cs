namespace BatchFlow.Postgres.Checkpoints;

/// <summary>
/// Определяет сериализацию значения контрольной точки для хранения в PostgreSQL.
/// </summary>
/// <typeparam name="TCheckpoint">Тип значения контрольной точки.</typeparam>
public interface IPostgresCheckpointSerializer<TCheckpoint>
{
    /// <summary>
    /// Преобразует значение контрольной точки в строковое представление для сохранения в базе данных.
    /// </summary>
    /// <param name="checkpoint">Значение контрольной точки.</param>
    /// <returns>Сериализованное представление контрольной точки.</returns>
    string Serialize(TCheckpoint checkpoint);

    /// <summary>
    /// Восстанавливает значение контрольной точки из строкового представления, считанного из базы данных.
    /// </summary>
    /// <param name="payload">Сериализованное представление контрольной точки.</param>
    /// <returns>Десериализованное значение контрольной точки.</returns>
    TCheckpoint Deserialize(string payload);
}
