namespace BatchFlow.Abstractions.Results;

/// <summary>
/// Представляет проблему, возникшую при обработке элемента или пакета в целом.
/// </summary>
public sealed record BatchProcessingIssue
{
    /// <summary>
    /// Индекс затронутого элемента, начиная с нуля, если проблема относится к конкретному элементу.
    /// </summary>
    public int? ItemIndex { get; init; }

    /// <summary>
    /// Необязательный код проблемы.
    /// </summary>
    public string? Code { get; init; }

    /// <summary>
    /// Необязательное понятное описание проблемы.
    /// </summary>
    public string? Message { get; init; }
}
