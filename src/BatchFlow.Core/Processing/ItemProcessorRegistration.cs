using BatchFlow.Abstractions.Processing;

namespace BatchFlow.Core.Processing;

/// <summary>
/// Представляет режим обработки, при котором движок вызывает обработчик для каждого элемента по отдельности.
/// </summary>
/// <typeparam name="TItem">Тип обрабатываемого элемента.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с пакетом.</typeparam>
public sealed record ItemProcessorRegistration<TItem, TCheckpoint> : BatchJobProcessingMode<TItem, TCheckpoint>
{
    /// <summary>
    /// Создает режим обработки, который вызывает обработчик для каждого элемента пакета отдельно.
    /// </summary>
    /// <param name="processor">Поэлементный обработчик.</param>
    public ItemProcessorRegistration(IItemProcessor<TItem, TCheckpoint> processor)
    {
        Processor = processor ?? throw new ArgumentNullException(nameof(processor));
    }

    /// <summary>
    /// Обработчик, который вызывается для каждого элемента пакета отдельно.
    /// </summary>
    public IItemProcessor<TItem, TCheckpoint> Processor { get; }
}
