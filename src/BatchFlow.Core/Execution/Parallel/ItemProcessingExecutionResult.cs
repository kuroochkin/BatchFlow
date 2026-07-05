using BatchFlow.Abstractions.Results;

namespace BatchFlow.Core.Execution.Parallel;

/// <summary>
/// Представляет итог обработки одного элемента внутри batch-а.
/// </summary>
internal sealed record ItemProcessingExecutionResult(
    ProcessingDisposition Disposition,
    int AttemptCount,
    BatchProcessingIssue? Issue);
