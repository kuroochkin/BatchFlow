using BatchFlow.Builders;
using BatchFlow.Postgres.Checkpoints;
using BatchFlow.Postgres.Sources;

namespace BatchFlow.Postgres.Extensions;

/// <summary>
/// Добавляет PostgreSQL-специфичные sugar-методы в fluent API BatchFlow.
/// </summary>
public static class BatchJobPostgresBuilderExtensions
{
    /// <summary>
    /// Выбирает PostgreSQL-источник данных и делает намерение конфигурации более явным на уровне fluent API.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, возвращаемых источником.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, используемой источником.</typeparam>
    /// <param name="stage">Стадия выбора источника данных.</param>
    /// <param name="source">PostgreSQL-источник данных.</param>
    /// <returns>Стадию builder для дальнейшей настройки задачи.</returns>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> FromPostgres<TItem, TCheckpoint>(
        this IBatchJobSourceSelectionStage stage,
        PostgresBatchSource<TItem, TCheckpoint> source)
    {
        ArgumentNullException.ThrowIfNull(stage);
        ArgumentNullException.ThrowIfNull(source);
        return stage.From(source);
    }

    /// <summary>
    /// Подключает PostgreSQL-хранилище контрольных точек, используя имя задачи как checkpoint-идентификатор.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, обрабатываемых задачей.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
    /// <param name="builder">Текущая стадия fluent-конфигурации.</param>
    /// <param name="checkpointStore">PostgreSQL-хранилище контрольных точек.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> WithPostgresCheckpoint<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        PostgresCheckpointStore<TCheckpoint> checkpointStore)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(checkpointStore);
        return builder.WithCheckpoint(checkpointStore);
    }

    /// <summary>
    /// Подключает PostgreSQL-хранилище контрольных точек и явно задает идентификатор checkpoint-потока.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, обрабатываемых задачей.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
    /// <param name="builder">Текущая стадия fluent-конфигурации.</param>
    /// <param name="checkpointId">Стабильный идентификатор потока контрольных точек.</param>
    /// <param name="checkpointStore">PostgreSQL-хранилище контрольных точек.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> WithPostgresCheckpoint<TItem, TCheckpoint>(
        this IResumableBatchJobBuilder<TItem, TCheckpoint> builder,
        string checkpointId,
        PostgresCheckpointStore<TCheckpoint> checkpointStore)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);
        ArgumentNullException.ThrowIfNull(checkpointStore);
        return builder.WithCheckpoint(checkpointId, checkpointStore);
    }

    /// <summary>
    /// Подключает PostgreSQL-хранилище контрольных точек к уже выбранному режиму обработки,
    /// используя имя задачи как checkpoint-идентификатор.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, обрабатываемых задачей.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
    /// <param name="builder">Текущая стадия fluent-конфигурации.</param>
    /// <param name="checkpointStore">PostgreSQL-хранилище контрольных точек.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    public static IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithPostgresCheckpoint<TItem, TCheckpoint>(
        this IResumableBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        PostgresCheckpointStore<TCheckpoint> checkpointStore)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(checkpointStore);
        return builder.WithCheckpoint(checkpointStore);
    }

    /// <summary>
    /// Подключает PostgreSQL-хранилище контрольных точек к уже выбранному режиму обработки
    /// и явно задает идентификатор checkpoint-потока.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, обрабатываемых задачей.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
    /// <param name="builder">Текущая стадия fluent-конфигурации.</param>
    /// <param name="checkpointId">Стабильный идентификатор потока контрольных точек.</param>
    /// <param name="checkpointStore">PostgreSQL-хранилище контрольных точек.</param>
    /// <returns>Текущую стадию builder для продолжения настройки.</returns>
    public static IResumableBatchJobReadyBuilder<TItem, TCheckpoint> WithPostgresCheckpoint<TItem, TCheckpoint>(
        this IResumableBatchJobReadyBuilder<TItem, TCheckpoint> builder,
        string checkpointId,
        PostgresCheckpointStore<TCheckpoint> checkpointStore)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);
        ArgumentNullException.ThrowIfNull(checkpointStore);
        return builder.WithCheckpoint(checkpointId, checkpointStore);
    }
}
