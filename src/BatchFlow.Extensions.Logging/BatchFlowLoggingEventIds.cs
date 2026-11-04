using Microsoft.Extensions.Logging;

namespace BatchFlow.Extensions.Logging;

/// <summary>
/// Содержит стабильные идентификаторы лог-событий, которые публикует logging adapter BatchFlow.
/// </summary>
public static class BatchFlowLoggingEventIds
{
    /// <summary>
    /// Идентификатор лога о старте job.
    /// </summary>
    public static readonly EventId JobStarted = new(10_000, nameof(JobStarted));

    /// <summary>
    /// Идентификатор лога о старте batch-а.
    /// </summary>
    public static readonly EventId BatchStarted = new(10_010, nameof(BatchStarted));

    /// <summary>
    /// Идентификатор лога об успешном завершении batch-а.
    /// </summary>
    public static readonly EventId BatchCompleted = new(10_020, nameof(BatchCompleted));

    /// <summary>
    /// Идентификатор лога об ошибке batch-а.
    /// </summary>
    public static readonly EventId BatchFailed = new(10_030, nameof(BatchFailed));

    /// <summary>
    /// Идентификатор лога о сохранении checkpoint.
    /// </summary>
    public static readonly EventId CheckpointSaved = new(10_040, nameof(CheckpointSaved));

    /// <summary>
    /// Идентификатор лога о завершении job.
    /// </summary>
    public static readonly EventId JobCompleted = new(10_050, nameof(JobCompleted));

    /// <summary>Идентификатор лога о разрешенной повторной попытке обработки.</summary>
    public static readonly EventId ProcessingRetry = new(10_060, nameof(ProcessingRetry));

    /// <summary>Идентификатор лога об окончательном пропуске элемента или части пакета.</summary>
    public static readonly EventId ProcessingSkipped = new(10_070, nameof(ProcessingSkipped));
}
