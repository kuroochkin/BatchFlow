using BatchFlow.Abstractions.Sources;

namespace BatchFlow.Builders.Internal;

internal sealed class BatchJobSourceSelectionStage(string jobName) : IBatchJobSourceSelectionStage
{
    private readonly string _jobName = BatchJobBuilderValidation.ValidateJobName(jobName);

    public IBatchJobBuilder<TItem, NoCheckpoint> From<TItem>(IBatchSource<TItem> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new BatchJobBuilder<TItem, NoCheckpoint>(
            new BatchJobBuilderState<TItem, NoCheckpoint>
            {
                JobName = _jobName,
                Source = source,
            });
    }

    public IResumableBatchJobBuilder<TItem, TCheckpoint> From<TItem, TCheckpoint>(IBatchSource<TItem, TCheckpoint> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new BatchJobBuilder<TItem, TCheckpoint>(
            new BatchJobBuilderState<TItem, TCheckpoint>
            {
                JobName = _jobName,
                Source = source,
            });
    }
}
