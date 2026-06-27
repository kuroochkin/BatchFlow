using BatchFlow.Abstractions.Checkpoints;

namespace BatchFlow.Abstractions.Execution;

/// <summary>
/// Содержит параметры и состояние, необходимые источнику для чтения данных пакетами.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, который "понимает" источник данных.</typeparam>
public sealed record BatchSourceContext<TCheckpoint>
{
    /// <summary>
    /// Контекст выполнения текущей задачи.
    /// </summary>
    public required JobExecutionContext Job { get; init; }

    /// <summary>
    /// Последняя сохраненная контрольная точка, от которой нужно возобновить чтение.
    /// Значение может отсутствовать как при первом запуске, так и для источников без поддержки возобновления.
    /// </summary>
    public StoredCheckpoint<TCheckpoint>? ResumeFrom { get; init; }

    /// <summary>
    /// Размер пакета. Источник может вернуть меньше элементов, если это обусловлено особенностями его работы.
    /// </summary>
    public required int RequestedBatchSize { get; init; }
}
