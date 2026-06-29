using BatchFlow.Abstractions.Results;

namespace BatchFlow.Core.Execution.Models;

/// <summary>
/// Содержит итог обработки пакета вместе с метаданными выполнения, важными для observability и runtime.
/// </summary>
internal sealed record ProcessingExecutionResult(BatchProcessingResult Result, int AttemptCount);
