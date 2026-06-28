namespace BatchFlow.Abstractions.Checkpoints;

/// <summary>
/// Определяет постоянное хранилище контрольных точек, позволяющее задаче возобновить обработку после прерывания.
/// </summary>
/// <typeparam name="TCheckpoint">Тип значения контрольной точки, понятного источнику данных.</typeparam>
public interface ICheckpointStore<TCheckpoint>
{
    /// <summary>
    /// Загружает последнюю сохраненную контрольную точку по указанному идентификатору, если она существует.
    /// </summary>
    ValueTask<StoredCheckpoint<TCheckpoint>?> LoadAsync(string checkpointId, CancellationToken cancellationToken);

    /// <summary>
    /// Сохраняет последнюю подтвержденную контрольную точку по указанному идентификатору.
    /// </summary>
    /// <param name="checkpointId">Логический идентификатор потока контрольных точек.</param>
    /// <param name="checkpoint">Контрольная точка, которую нужно сохранить.</param>
    /// <param name="expectedConcurrencyToken">
    /// Токен оптимистичной конкуренции, полученный вместе с ранее загруженной контрольной точкой.
    /// Если токен не совпадает, реализация должна отклонить устаревшую запись.
    /// </param>
    /// <param name="cancellationToken">Токен отмены</param>
    /// <returns>Контрольную точку в том виде, в котором она была сохранена реализацией, включая обновленный токен конкуренции.</returns>
    /// <exception cref="CheckpointConflictException">
    /// Выбрасывается, если сохранить контрольную точку нельзя, потому что уже существует более новая версия.
    /// </exception>
    ValueTask<StoredCheckpoint<TCheckpoint>> SaveAsync(
        string checkpointId,
        StoredCheckpoint<TCheckpoint> checkpoint,
        string? expectedConcurrencyToken,
        CancellationToken cancellationToken);
}
