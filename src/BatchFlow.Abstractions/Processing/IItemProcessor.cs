using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Results;

namespace BatchFlow.Abstractions.Processing;

/// <summary>
/// Определяет обработчик, который принимает один элемент из пакета.
/// </summary>
/// <typeparam name="TItem">Тип обрабатываемого элемента.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с текущим пакетом.</typeparam>
public interface IItemProcessor<TItem, TCheckpoint>
{
    /// <summary>
    /// Обрабатывает элемент и возвращает логический результат его обработки.
    /// </summary>
    ValueTask<ProcessingResult> ProcessAsync(
        TItem item,
        ItemProcessingContext<TCheckpoint> context,
        CancellationToken cancellationToken);
}