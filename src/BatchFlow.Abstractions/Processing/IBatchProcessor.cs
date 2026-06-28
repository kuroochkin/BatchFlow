using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Results;

namespace BatchFlow.Abstractions.Processing;

/// <summary>
/// Определяет обработчик, который принимает пакет целиком и возвращает агрегированный результат.
/// </summary>
/// <typeparam name="TItem">Тип элементов, входящих в пакет.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с пакетом.</typeparam>
public interface IBatchProcessor<TItem, TCheckpoint>
{
    /// <summary>
    /// Обрабатывает пакет и возвращает итог, который может содержать смешанные окончательные исходы,
    /// например сочетание успешно обработанных и пропущенных элементов.
    /// </summary>
    ValueTask<BatchProcessingResult> ProcessAsync(
        IReadOnlyList<TItem> items,
        BatchProcessingContext<TCheckpoint> context,
        CancellationToken cancellationToken);
}
