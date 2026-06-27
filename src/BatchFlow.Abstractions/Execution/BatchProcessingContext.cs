namespace BatchFlow.Abstractions.Execution;

/// <summary>
/// Содержит контекст выполнения, необходимый для обработки пакета.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, соответствующей текущему пакету.</typeparam>
public sealed record BatchProcessingContext<TCheckpoint>
{
    /// <summary>
    /// Контекст выполнения текущей задачи.
    /// </summary>
    public required JobExecutionContext Job { get; init; }

    /// <summary>
    /// Возрастающий порядковый номер пакета в рамках текущего запуска.
    /// </summary>
    public required long BatchSequence { get; init; }

    /// <summary>
    /// Контрольную точка, соответствующая текущему пакету.
    /// </summary>
    public required TCheckpoint BatchCheckpoint { get; init; }

    /// <summary>
    /// Метаданные источника, связанные с текущим пакетом.
    /// </summary>
    public IReadOnlyDictionary<string, string> SourceMetadata { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// Номер попытки обработки текущего пакета
    /// </summary>
    public int Attempt { get; init; } = 1;

    /// <summary>
    /// Количество элементов в текущем пакете.
    /// </summary>
    public int ItemCount { get; init; }
}
