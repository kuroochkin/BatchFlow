using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Failures;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;
using BatchFlow.Core.Jobs;
using BatchFlow.Core.Processing;
using BatchFlow.Processing;

namespace BatchFlow.Builders.Internal;

internal sealed class BatchJobBuilder<TItem, TCheckpoint> :
    IBatchJobBuilder<TItem, TCheckpoint>,
    IResumableBatchJobBuilder<TItem, TCheckpoint>,
    IBatchJobReadyBuilder<TItem, TCheckpoint>,
    IResumableBatchJobReadyBuilder<TItem, TCheckpoint>
{
    private readonly BatchJobBuilderState<TItem, TCheckpoint> _state;

    public BatchJobBuilder(BatchJobBuilderState<TItem, TCheckpoint> state)
    {
        _state = state ?? throw new ArgumentNullException(nameof(state));
    }

    public BatchJobDefinition<TItem, TCheckpoint> Build()
    {
        BatchJobBuilderValidation.ValidateBuildState(_state);

        return new BatchJobDefinition<TItem, TCheckpoint>
        {
            JobName = _state.JobName,
            CheckpointId = _state.CheckpointId,
            Source = _state.Source,
            ProcessingMode = _state.ProcessingMode!,
            BatchSize = _state.BatchSize,
            DegreeOfParallelism = _state.DegreeOfParallelism,
            PrefetchCapacity = _state.PrefetchCapacity,
            CheckpointStore = _state.CheckpointStore,
            FailurePolicy = BuildFailurePolicy(),
        };
    }

    IBatchJobBuilder<TItem, TCheckpoint> IBatchJobBuilder<TItem, TCheckpoint>.WithBatchSize(int batchSize) =>
        WithBatchSizeCore(batchSize);

    IResumableBatchJobBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.WithBatchSize(int batchSize) =>
        WithBatchSizeCore(batchSize);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobReadyBuilder<TItem, TCheckpoint>.WithBatchSize(int batchSize) =>
        WithBatchSizeCore(batchSize);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.WithBatchSize(int batchSize) =>
        WithBatchSizeCore(batchSize);

    IBatchJobBuilder<TItem, TCheckpoint> IBatchJobBuilder<TItem, TCheckpoint>.WithDegreeOfParallelism(int degreeOfParallelism) =>
        WithDegreeOfParallelismCore(degreeOfParallelism);

    IResumableBatchJobBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.WithDegreeOfParallelism(int degreeOfParallelism) =>
        WithDegreeOfParallelismCore(degreeOfParallelism);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobReadyBuilder<TItem, TCheckpoint>.WithDegreeOfParallelism(int degreeOfParallelism) =>
        WithDegreeOfParallelismCore(degreeOfParallelism);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.WithDegreeOfParallelism(int degreeOfParallelism) =>
        WithDegreeOfParallelismCore(degreeOfParallelism);

    IBatchJobBuilder<TItem, TCheckpoint> IBatchJobBuilder<TItem, TCheckpoint>.WithPrefetchCapacity(int prefetchCapacity) =>
        WithPrefetchCapacityCore(prefetchCapacity);

    IResumableBatchJobBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.WithPrefetchCapacity(int prefetchCapacity) =>
        WithPrefetchCapacityCore(prefetchCapacity);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobReadyBuilder<TItem, TCheckpoint>.WithPrefetchCapacity(int prefetchCapacity) =>
        WithPrefetchCapacityCore(prefetchCapacity);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.WithPrefetchCapacity(int prefetchCapacity) =>
        WithPrefetchCapacityCore(prefetchCapacity);

    IBatchJobBuilder<TItem, TCheckpoint> IBatchJobBuilder<TItem, TCheckpoint>.WithFailurePolicy(
        IFailurePolicy<TItem, TCheckpoint> failurePolicy) =>
        WithFailurePolicyCore(failurePolicy);

    IBatchJobBuilder<TItem, TCheckpoint> IBatchJobBuilder<TItem, TCheckpoint>.WithRetryOptions(
        BatchRetryOptions options) =>
        WithRetryOptionsCore(options);

    IBatchJobBuilder<TItem, TCheckpoint> IBatchJobBuilder<TItem, TCheckpoint>.WithTerminalFailureAction(
        BatchFailureTerminalAction terminalAction) =>
        WithTerminalFailureActionCore(terminalAction);

    IResumableBatchJobBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.WithFailurePolicy(
        IFailurePolicy<TItem, TCheckpoint> failurePolicy) =>
        WithFailurePolicyCore(failurePolicy);

    IResumableBatchJobBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.WithRetryOptions(
        BatchRetryOptions options) =>
        WithRetryOptionsCore(options);

    IResumableBatchJobBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.WithTerminalFailureAction(
        BatchFailureTerminalAction terminalAction) =>
        WithTerminalFailureActionCore(terminalAction);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobReadyBuilder<TItem, TCheckpoint>.WithFailurePolicy(
        IFailurePolicy<TItem, TCheckpoint> failurePolicy) =>
        WithFailurePolicyCore(failurePolicy);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobReadyBuilder<TItem, TCheckpoint>.WithRetryOptions(
        BatchRetryOptions options) =>
        WithRetryOptionsCore(options);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobReadyBuilder<TItem, TCheckpoint>.WithTerminalFailureAction(
        BatchFailureTerminalAction terminalAction) =>
        WithTerminalFailureActionCore(terminalAction);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.WithFailurePolicy(
        IFailurePolicy<TItem, TCheckpoint> failurePolicy) =>
        WithFailurePolicyCore(failurePolicy);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.WithRetryOptions(
        BatchRetryOptions options) =>
        WithRetryOptionsCore(options);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.WithTerminalFailureAction(
        BatchFailureTerminalAction terminalAction) =>
        WithTerminalFailureActionCore(terminalAction);

    IResumableBatchJobBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.WithCheckpoint(ICheckpointStore<TCheckpoint> checkpointStore) =>
        WithCheckpointCore(checkpointStore, checkpointId: null);

    IResumableBatchJobBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.WithCheckpoint(
        string checkpointId,
        ICheckpointStore<TCheckpoint> checkpointStore) =>
        WithCheckpointCore(checkpointStore, checkpointId);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.WithCheckpoint(
        ICheckpointStore<TCheckpoint> checkpointStore) =>
        WithCheckpointCore(checkpointStore, checkpointId: null);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.WithCheckpoint(
        string checkpointId,
        ICheckpointStore<TCheckpoint> checkpointStore) =>
        WithCheckpointCore(checkpointStore, checkpointId);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobBuilder<TItem, TCheckpoint>.ProcessEach(IItemProcessor<TItem, TCheckpoint> processor) =>
        ProcessEachCore(processor);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.ProcessEach(
        IItemProcessor<TItem, TCheckpoint> processor) =>
        ProcessEachCore(processor);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobReadyBuilder<TItem, TCheckpoint>.ProcessEach(
        IItemProcessor<TItem, TCheckpoint> processor) =>
        ProcessEachCore(processor);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.ProcessEach(
        IItemProcessor<TItem, TCheckpoint> processor) =>
        ProcessEachCore(processor);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobBuilder<TItem, TCheckpoint>.ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler) =>
        ProcessEachCore(handler);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler) =>
        ProcessEachCore(handler);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobReadyBuilder<TItem, TCheckpoint>.ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler) =>
        ProcessEachCore(handler);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler) =>
        ProcessEachCore(handler);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobBuilder<TItem, TCheckpoint>.ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler) =>
        ProcessEachCore(handler);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler) =>
        ProcessEachCore(handler);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobReadyBuilder<TItem, TCheckpoint>.ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler) =>
        ProcessEachCore(handler);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.ProcessEach(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler) =>
        ProcessEachCore(handler);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobBuilder<TItem, TCheckpoint>.ProcessBatch(IBatchProcessor<TItem, TCheckpoint> processor) =>
        ProcessBatchCore(processor);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.ProcessBatch(
        IBatchProcessor<TItem, TCheckpoint> processor) =>
        ProcessBatchCore(processor);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobReadyBuilder<TItem, TCheckpoint>.ProcessBatch(
        IBatchProcessor<TItem, TCheckpoint> processor) =>
        ProcessBatchCore(processor);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.ProcessBatch(
        IBatchProcessor<TItem, TCheckpoint> processor) =>
        ProcessBatchCore(processor);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobBuilder<TItem, TCheckpoint>.ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler) =>
        ProcessBatchCore(handler);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler) =>
        ProcessBatchCore(handler);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobReadyBuilder<TItem, TCheckpoint>.ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler) =>
        ProcessBatchCore(handler);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler) =>
        ProcessBatchCore(handler);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobBuilder<TItem, TCheckpoint>.ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler) =>
        ProcessBatchCore(handler);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobBuilder<TItem, TCheckpoint>.ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler) =>
        ProcessBatchCore(handler);

    IBatchJobReadyBuilder<TItem, TCheckpoint> IBatchJobReadyBuilder<TItem, TCheckpoint>.ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler) =>
        ProcessBatchCore(handler);

    IResumableBatchJobReadyBuilder<TItem, TCheckpoint> IResumableBatchJobReadyBuilder<TItem, TCheckpoint>.ProcessBatch(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler) =>
        ProcessBatchCore(handler);

    private BatchJobBuilder<TItem, TCheckpoint> WithBatchSizeCore(int batchSize)
    {
        BatchJobBuilderValidation.ValidateBatchSize(batchSize);
        return WithState(_state with { BatchSize = batchSize });
    }

    private BatchJobBuilder<TItem, TCheckpoint> WithDegreeOfParallelismCore(int degreeOfParallelism)
    {
        BatchJobBuilderValidation.ValidateDegreeOfParallelism(degreeOfParallelism);
        return WithState(_state with { DegreeOfParallelism = degreeOfParallelism });
    }

    private BatchJobBuilder<TItem, TCheckpoint> WithPrefetchCapacityCore(int prefetchCapacity)
    {
        BatchJobBuilderValidation.ValidatePrefetchCapacity(prefetchCapacity);
        return WithState(_state with { PrefetchCapacity = prefetchCapacity });
    }

    private BatchJobBuilder<TItem, TCheckpoint> WithFailurePolicyCore(
        IFailurePolicy<TItem, TCheckpoint> failurePolicy)
    {
        ArgumentNullException.ThrowIfNull(failurePolicy);
        return WithState(_state with
        {
            FailurePolicy = failurePolicy,
            RetryOptions = null,
            TerminalFailureAction = null,
        });
    }

    private BatchJobBuilder<TItem, TCheckpoint> WithRetryOptionsCore(BatchRetryOptions options)
    {
        BatchJobBuilderValidation.ValidateRetryOptions(options);

        return WithState(_state with
        {
            FailurePolicy = null,
            RetryOptions = options,
        });
    }

    private BatchJobBuilder<TItem, TCheckpoint> WithTerminalFailureActionCore(
        BatchFailureTerminalAction terminalAction)
    {
        BatchJobBuilderValidation.ValidateTerminalFailureAction(terminalAction);

        return WithState(_state with
        {
            FailurePolicy = null,
            TerminalFailureAction = terminalAction,
        });
    }

    private BatchJobBuilder<TItem, TCheckpoint> WithCheckpointCore(
        ICheckpointStore<TCheckpoint> checkpointStore,
        string? checkpointId)
    {
        ArgumentNullException.ThrowIfNull(checkpointStore);
        BatchJobBuilderValidation.ValidateCheckpointingSupport(_state.Source);

        return WithState(
            _state with
            {
                CheckpointId = checkpointId is null
                    ? null
                    : BatchJobBuilderValidation.ValidateCheckpointId(checkpointId),
                CheckpointStore = checkpointStore,
            });
    }

    private BatchJobBuilder<TItem, TCheckpoint> ProcessEachCore(IItemProcessor<TItem, TCheckpoint> processor)
    {
        ArgumentNullException.ThrowIfNull(processor);

        return WithState(
            _state with
            {
                ProcessingMode = new ItemProcessorRegistration<TItem, TCheckpoint>(processor),
            });
    }

    private BatchJobBuilder<TItem, TCheckpoint> ProcessEachCore(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return ProcessEachCore(new DelegateItemProcessor<TItem, TCheckpoint>(handler));
    }

    private BatchJobBuilder<TItem, TCheckpoint> ProcessEachCore(
        Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        async ValueTask<ProcessingResult> AdaptedHandler(
            TItem item,
            ItemProcessingContext<TCheckpoint> context,
            CancellationToken cancellationToken)
        {
            await handler(item, context, cancellationToken);
            return ProcessingResult.Success();
        }

        return ProcessEachCore(AdaptedHandler);
    }

    private BatchJobBuilder<TItem, TCheckpoint> ProcessBatchCore(IBatchProcessor<TItem, TCheckpoint> processor)
    {
        ArgumentNullException.ThrowIfNull(processor);

        return WithState(
            _state with
            {
                ProcessingMode = new BatchProcessorRegistration<TItem, TCheckpoint>(processor),
            });
    }

    private BatchJobBuilder<TItem, TCheckpoint> ProcessBatchCore(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return ProcessBatchCore(new DelegateBatchProcessor<TItem, TCheckpoint>(handler));
    }

    private BatchJobBuilder<TItem, TCheckpoint> ProcessBatchCore(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        async ValueTask<BatchProcessingResult> AdaptedHandler(
            IReadOnlyList<TItem> items,
            BatchProcessingContext<TCheckpoint> context,
            CancellationToken cancellationToken)
        {
            await handler(items, context, cancellationToken);
            return BatchProcessingResult.AllSucceeded(items.Count);
        }

        return ProcessBatchCore(AdaptedHandler);
    }

    private BatchJobBuilder<TItem, TCheckpoint> WithState(BatchJobBuilderState<TItem, TCheckpoint> state) =>
        new(state);

    private IFailurePolicy<TItem, TCheckpoint>? BuildFailurePolicy()
    {
        if (_state.FailurePolicy is not null)
        {
            return _state.FailurePolicy;
        }

        if (_state.RetryOptions is null && _state.TerminalFailureAction is null)
        {
            return null;
        }

        var retryOptions = _state.RetryOptions ?? new BatchRetryOptions();
        var terminalAction = _state.TerminalFailureAction ?? BatchFailureTerminalAction.Stop;

        return new FixedRetryFailurePolicy<TItem, TCheckpoint>(retryOptions, terminalAction);
    }
}
