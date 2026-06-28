namespace BatchFlow.Abstractions.Results;

/// <summary>
/// Представляет агрегированный результат обработки пакета.
/// Ошибки, которые должны прерывать выполнение, выражаются исключениями, а этот тип используется
/// для исходов, которые можно безопасно зафиксировать в прогрессе обработки.
/// </summary>
public sealed record BatchProcessingResult
{
    /// <summary>
    /// Общее количество элементов в обработанном пакете.
    /// </summary>
    public required int ItemCount { get; init; }

    /// <summary>
    /// Количество элементов, обработанных успешно.
    /// </summary>
    public required int SucceededCount { get; init; }

    /// <summary>
    /// Количество элементов, завершившихся окончательным пропуском.
    /// </summary>
    public int SkippedCount { get; init; }

    /// <summary>
    /// Необязательные сведения о проблемах, возникших внутри пакета.
    /// </summary>
    public IReadOnlyList<BatchProcessingIssue> Issues { get; init; } = [];

    /// <summary>
    /// Необязательное текстовое пояснение к результату обработки.
    /// </summary>
    public string? Reason { get; init; }

    /// <summary>
    /// Количество элементов, для которых в результате явно указан окончательный исход.
    /// </summary>
    public int AccountedItemCount => SucceededCount + SkippedCount;

    /// <summary>
    /// Признак того, что для каждого элемента в пакете известен окончательный исход.
    /// </summary>
    public bool IsFullyAccountedFor => AccountedItemCount == ItemCount;

    /// <summary>
    /// Создает результат, соответствующий полностью успешной обработке пакета.
    /// </summary>
    public static BatchProcessingResult AllSucceeded(int itemCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(itemCount);

        return new BatchProcessingResult
        {
            ItemCount = itemCount,
            SucceededCount = itemCount,
        };
    }

    /// <summary>
    /// Создает результат, который можно безопасно зафиксировать и который содержит
    /// смешанные исходы: часть элементов обработана успешно, часть пропущена.
    /// </summary>
    public static BatchProcessingResult Completed(
        int itemCount,
        int succeededCount,
        int skippedCount,
        IReadOnlyList<BatchProcessingIssue>? issues = null,
        string? reason = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(itemCount);
        ArgumentOutOfRangeException.ThrowIfNegative(succeededCount);
        ArgumentOutOfRangeException.ThrowIfNegative(skippedCount);

        if (succeededCount + skippedCount != itemCount)
        {
            throw new ArgumentException(
                "Сумма успешно обработанных и пропущенных элементов должна совпадать с общим количеством элементов.",
                nameof(itemCount));
        }

        return new BatchProcessingResult
        {
            ItemCount = itemCount,
            SucceededCount = succeededCount,
            SkippedCount = skippedCount,
            Issues = issues ?? [],
            Reason = reason,
        };
    }
}
