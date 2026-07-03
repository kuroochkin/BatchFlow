using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;

namespace BatchFlow.Processing;

internal sealed class DelegateItemProcessor<TItem, TCheckpoint>(
    Func<TItem, ItemProcessingContext<TCheckpoint>, 
    CancellationToken, ValueTask<ProcessingResult>> handler) : IItemProcessor<TItem, TCheckpoint>
{
    private readonly Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> _handler = 
        handler ?? throw new ArgumentNullException(nameof(handler));

    public ValueTask<ProcessingResult> ProcessAsync(
        TItem item,
        ItemProcessingContext<TCheckpoint> context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        return _handler(item, context, cancellationToken);
    }
}
