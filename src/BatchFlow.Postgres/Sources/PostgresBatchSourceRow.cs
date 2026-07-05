namespace BatchFlow.Postgres.Sources;

/// <summary>
/// Представляет одну строку результата PostgreSQL-запроса после материализации.
/// Содержит и пользовательскую модель, и checkpoint, который соответствует этой строке.
/// </summary>
/// <typeparam name="TItem">Тип материализованной пользовательской модели.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, продвигающей чтение.</typeparam>
public readonly record struct PostgresBatchSourceRow<TItem, TCheckpoint>(TItem Item, TCheckpoint Checkpoint);
