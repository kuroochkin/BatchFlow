namespace BatchFlow.Abstractions.Execution;

/// <summary>
/// Содержит контекст выполнения, необходимый для обработки отдельного элемента.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, соответствующей текущему пакету.</typeparam>
public sealed record ItemProcessingContext<TCheckpoint>
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
    /// Контрольную точка, соответствующую текущему пакету.
    /// </summary>
    public required TCheckpoint BatchCheckpoint { get; init; }

    /// <summary>
    /// Метаданные источника, связанные с текущим пакетом.
    /// </summary>
    public IReadOnlyDictionary<string, string> SourceMetadata { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// Индекс текущего элемента внутри пакета, начиная с нуля.
    /// </summary>
    public required int ItemIndex { get; init; }

    /// <summary>
    /// Общее количество элементов в текущем пакете.
    /// </summary>
    public int BatchItemCount { get; init; }

    /// <summary>
    /// Номер попытки обработки текущего элемента.
    /// </summary>
    public int Attempt { get; init; } = 1;
}
