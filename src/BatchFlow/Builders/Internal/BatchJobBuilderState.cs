using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Failures;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Core.Processing;

namespace BatchFlow.Builders.Internal;

internal sealed record BatchJobBuilderState<TItem, TCheckpoint>
{
    public required string JobName { get; init; }

    public required IBatchSource<TItem, TCheckpoint> Source { get; init; }

    public int BatchSize { get; init; } = 100;

    public int DegreeOfParallelism { get; init; } = 1;

    public string? CheckpointId { get; init; }

    public ICheckpointStore<TCheckpoint>? CheckpointStore { get; init; }

    public IFailurePolicy<TItem, TCheckpoint>? FailurePolicy { get; init; }

    public BatchRetryOptions? RetryOptions { get; init; }

    public BatchFailureTerminalAction? TerminalFailureAction { get; init; }

    public BatchJobProcessingMode<TItem, TCheckpoint>? ProcessingMode { get; init; }
}
