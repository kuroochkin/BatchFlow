using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;

namespace BatchFlow.Testing.Processing;

/// <summary>
/// Представляет поэлементный обработчик, основанный на делегате.
/// </summary>
/// <typeparam name="TItem">Тип обрабатываемого элемента.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
public sealed class DelegateItemProcessor<TItem, TCheckpoint> : IItemProcessor<TItem, TCheckpoint>
{
    private readonly Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> _handler;

    /// <summary>
    /// Создает поэлементный обработчик.
    /// </summary>
    public DelegateItemProcessor(Func<TItem, ItemProcessingContext<TCheckpoint>, CancellationToken, ValueTask<ProcessingResult>> handler)
    {
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
    }

    /// <inheritdoc />
    public ValueTask<ProcessingResult> ProcessAsync(
        TItem item,
        ItemProcessingContext<TCheckpoint> context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        return _handler(item, context, cancellationToken);
    }
}
