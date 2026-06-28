using BatchFlow.Abstractions.Observability;

namespace BatchFlow.Core.Execution.Exceptions;

/// <summary>
/// Представляет управляемое завершение задачи, инициированное политикой обработки ошибок.
/// </summary>
internal sealed class BatchJobTerminationException(
    JobCompletionStatus status,
    string message,
    int attempt,
    int? itemIndex,
    Exception innerException) : Exception(message, innerException)
{
    public JobCompletionStatus Status { get; } = status;

    public int Attempt { get; } = attempt;

    public int? ItemIndex { get; } = itemIndex;
}
