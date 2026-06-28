namespace BatchFlow.Abstractions.Execution;

/// <summary>
/// Описывает один запуск пакетной задачи.
/// </summary>
public sealed record JobExecutionContext
{
    /// <summary>
    /// Наименование задачи.
    /// </summary>
    public required string JobName { get; init; }

    /// <summary>
    /// Эффективный идентификатор, используемый движком для загрузки и сохранения контрольных точек задачи.
    /// Если задача выполняется без checkpoint store, значение может отсутствовать.
    /// </summary>
    public string? CheckpointId { get; init; }

    /// <summary>
    /// Уникальный идентификатор текущего запуска.
    /// </summary>
    public required string RunId { get; init; }

    /// <summary>
    /// Время запуска задачи по всемирному времени.
    /// </summary>
    public DateTimeOffset StartedAt { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Размер пакета.
    /// </summary>
    public int BatchSize { get; init; }

    /// <summary>
    /// Степень параллелизма задачи.
    /// </summary>
    public int DegreeOfParallelism { get; init; } = 1;
}
