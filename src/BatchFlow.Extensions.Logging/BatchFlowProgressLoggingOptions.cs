using BatchFlow.Abstractions.Observability;
using Microsoft.Extensions.Logging;

namespace BatchFlow.Extensions.Logging;

/// <summary>
/// Определяет параметры логирования runtime-событий BatchFlow.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с логируемой задачей.</typeparam>
public sealed class BatchFlowProgressLoggingOptions<TCheckpoint>
{
    /// <summary>
    /// Уровень лога для события старта job.
    /// </summary>
    public LogLevel JobStartedLogLevel { get; set; } = LogLevel.Information;

    /// <summary>
    /// Уровень лога для события старта batch-а.
    /// По умолчанию используется <see cref="LogLevel.Debug"/>, чтобы не шуметь в production-логах.
    /// </summary>
    public LogLevel BatchStartedLogLevel { get; set; } = LogLevel.Debug;

    /// <summary>
    /// Уровень лога для события успешного завершения batch-а.
    /// </summary>
    public LogLevel BatchCompletedLogLevel { get; set; } = LogLevel.Information;

    /// <summary>
    /// Уровень лога для события ошибки batch-а.
    /// </summary>
    public LogLevel BatchFailedLogLevel { get; set; } = LogLevel.Error;

    /// <summary>Уровень лога о решении повторить обработку после ошибки.</summary>
    public LogLevel ProcessingRetryLogLevel { get; set; } = LogLevel.Warning;

    /// <summary>Уровень лога об окончательном пропуске.</summary>
    public LogLevel ProcessingSkippedLogLevel { get; set; } = LogLevel.Warning;

    /// <summary>
    /// Уровень лога для события сохранения checkpoint.
    /// По умолчанию используется <see cref="LogLevel.Debug"/>, чтобы не перегружать лог каждую подтвержденную фиксацию.
    /// </summary>
    public LogLevel CheckpointSavedLogLevel { get; set; } = LogLevel.Debug;

    /// <summary>
    /// Возвращает функцию, которая определяет уровень лога для финального события завершения job.
    /// </summary>
    public Func<JobCompletionStatus, LogLevel> JobCompletedLogLevelSelector { get; set; } =
        static status => status switch
        {
            JobCompletionStatus.Succeeded => LogLevel.Information,
            JobCompletionStatus.CompletedWithSkips => LogLevel.Warning,
            JobCompletionStatus.Cancelled => LogLevel.Warning,
            JobCompletionStatus.Suspended => LogLevel.Warning,
            JobCompletionStatus.Failed => LogLevel.Error,
            _ => LogLevel.Information,
        };

    /// <summary>
    /// Определяет, нужно ли включать значение checkpoint в логи.
    /// По умолчанию выключено, чтобы не утекали чувствительные или слишком объемные значения resume-токенов.
    /// </summary>
    public bool IncludeCheckpointValue { get; set; }

    /// <summary>
    /// Определяет, нужно ли включать source metadata в логи batch-событий.
    /// По умолчанию выключено, чтобы не раздувать лог лишними деталями.
    /// </summary>
    public bool IncludeSourceMetadata { get; set; }

    /// <summary>
    /// Позволяет переопределить форматирование значения checkpoint перед записью в лог.
    /// </summary>
    public Func<TCheckpoint, string>? CheckpointFormatter { get; set; }

    /// <summary>
    /// Позволяет переопределить форматирование source metadata перед записью в лог.
    /// </summary>
    public Func<IReadOnlyDictionary<string, string>, string>? SourceMetadataFormatter { get; set; }
}
