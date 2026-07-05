using BatchFlow.Abstractions.Sources;

namespace BatchFlow.Core.Execution.Buffering;

/// <summary>
/// Представляет batch, которому runtime уже присвоил порядковый номер внутри текущего запуска.
/// </summary>
/// <typeparam name="TItem">Тип элементов внутри пакета.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
internal sealed record BufferedSourceBatch<TItem, TCheckpoint>
{
    /// <summary>
    /// Порядковый номер batch-а в рамках текущего запуска.
    /// </summary>
    public required long Sequence { get; init; }

    /// <summary>
    /// Пакет, считанный из источника данных.
    /// </summary>
    public required SourceBatch<TItem, TCheckpoint> Batch { get; init; }
}
