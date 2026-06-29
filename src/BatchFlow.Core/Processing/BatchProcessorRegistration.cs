using BatchFlow.Abstractions.Processing;

namespace BatchFlow.Core.Processing;

/// <summary>
/// Представляет режим обработки, при котором движок передает обработчику весь пакет целиком.
/// </summary>
/// <typeparam name="TItem">Тип элементов, входящих в пакет.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с пакетом.</typeparam>
public sealed record BatchProcessorRegistration<TItem, TCheckpoint> : BatchJobProcessingMode<TItem, TCheckpoint>
{
    /// <summary>
    /// Создает режим обработки, который передает обработчику весь пакет целиком.
    /// </summary>
    /// <param name="processor">Обработчик пакета.</param>
    public BatchProcessorRegistration(IBatchProcessor<TItem, TCheckpoint> processor)
    {
        Processor = processor ?? throw new ArgumentNullException(nameof(processor));
    }

    /// <summary>
    /// Обработчик, который принимает пакет целиком.
    /// </summary>
    public IBatchProcessor<TItem, TCheckpoint> Processor { get; }
}
