using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;

namespace BatchFlow.Processing;

internal sealed class DelegateBatchProcessor<TItem, TCheckpoint>(
    Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler) : IBatchProcessor<TItem, TCheckpoint>
{
    private readonly Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> _handler = handler ?? throw new ArgumentNullException(nameof(handler));

    public ValueTask<BatchProcessingResult> ProcessAsync(
        IReadOnlyList<TItem> items,
        BatchProcessingContext<TCheckpoint> context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(context);
        return _handler(items, context, cancellationToken);
    }
}