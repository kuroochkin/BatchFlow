using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;

namespace BatchFlow.Testing.Processing;

/// <summary>
/// Представляет тестовый batch-обработчик, основанный на делегате.
/// </summary>
/// <typeparam name="TItem">Тип элементов, входящих в пакет.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
public sealed class DelegateBatchProcessor<TItem, TCheckpoint> : IBatchProcessor<TItem, TCheckpoint>
{
    private readonly Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> _handler;

    /// <summary>
    /// Создает тестовый batch-обработчик.
    /// </summary>
    public DelegateBatchProcessor(
        Func<IReadOnlyList<TItem>, BatchProcessingContext<TCheckpoint>, CancellationToken, ValueTask<BatchProcessingResult>> handler)
    {
        _handler = handler ?? throw new ArgumentNullException(nameof(handler));
    }

    /// <inheritdoc />
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
