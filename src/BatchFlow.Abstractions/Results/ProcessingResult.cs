namespace BatchFlow.Abstractions.Results;

/// <summary>
/// Результат обработки элемента или пакета.
/// </summary>
public sealed record ProcessingResult
{
    /// <summary>
    /// Исход шага обработки.
    /// </summary>
    public required ProcessingDisposition Disposition { get; init; }

    /// <summary>
    /// Необязательный код результата.
    /// </summary>
    public string? Code { get; init; }

    /// <summary>
    /// Необязательное текстовое пояснение результата.
    /// </summary>
    public string? Reason { get; init; }

    /// <summary>
    /// Необязательные структурированные метаданные результата.
    /// </summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// Создает результат успешной обработки.
    /// </summary>
    public static ProcessingResult Success(string? reason = null) =>
        new()
        {
            Disposition = ProcessingDisposition.Succeeded,
            Reason = reason,
        };

    /// <summary>
    /// Создает результат обработки, завершившейся пропуском.
    /// </summary>
    public static ProcessingResult Skipped(
        string? reason = null,
        string? code = null,
        IReadOnlyDictionary<string, string>? metadata = null) =>
        new()
        {
            Disposition = ProcessingDisposition.Skipped,
            Code = code,
            Reason = reason,
            Metadata = metadata ?? new Dictionary<string, string>(),
        };
}
