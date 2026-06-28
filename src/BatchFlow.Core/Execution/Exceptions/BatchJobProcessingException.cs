namespace BatchFlow.Core.Execution.Exceptions;

/// <summary>
/// Представляет ошибку выполнения обработки, дополненную техническими сведениями для runtime.
/// </summary>
internal sealed class BatchJobProcessingException(
    string message,
    int attempt,
    int? itemIndex,
    Exception? innerException = null) : Exception(message, innerException)
{
    public int Attempt { get; } = attempt;

    public int? ItemIndex { get; } = itemIndex;
}
