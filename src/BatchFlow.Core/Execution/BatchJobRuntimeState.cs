using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Observability;

namespace BatchFlow.Core.Execution;

/// <summary>
/// Хранит изменяемое состояние одного запуска задачи внутри движка выполнения.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, с которой работает текущая задача.</typeparam>
internal sealed class BatchJobRuntimeState<TCheckpoint>
{
    /// <summary>
    /// Создает контейнер изменяемого runtime-состояния для одного запуска пакетной задачи.
    /// </summary>
    /// <param name="job">Контекст текущего запуска задачи.</param>
    /// <param name="timeProvider">Источник времени, используемый движком выполнения.</param>
    public BatchJobRuntimeState(JobExecutionContext job, TimeProvider timeProvider)
    {
        Job = job;
        TimeProvider = timeProvider;
    }

    /// <summary>
    /// Контекст текущего запуска задачи.
    /// </summary>
    public JobExecutionContext Job { get; }

    /// <summary>
    /// Источник времени, привязанный к текущему запуску.
    /// </summary>
    public TimeProvider TimeProvider { get; }

    /// <summary>
    /// Контрольная точка, загруженная при старте выполнения, если она существовала.
    /// </summary>
    public StoredCheckpoint<TCheckpoint>? ResumeFrom { get; set; }

    /// <summary>
    /// Последняя контрольная точка, которую движок успешно сохранил в ходе текущего запуска.
    /// </summary>
    public StoredCheckpoint<TCheckpoint>? LastPersistedCheckpoint { get; set; }

    /// <summary>
    /// Порядковый номер последнего прочитанного пакета в рамках текущего запуска.
    /// </summary>
    public long BatchSequence { get; set; }

    /// <summary>
    /// Количество пакетов, достигших окончательного состояния обработки.
    /// </summary>
    public long ProcessedBatchCount { get; set; }

    /// <summary>
    /// Накопленное количество элементов, обработанных успешно.
    /// </summary>
    public long SucceededItemCount { get; set; }

    /// <summary>
    /// Накопленное количество элементов, завершившихся окончательным пропуском.
    /// </summary>
    public long SkippedItemCount { get; set; }

    /// <summary>
    /// Текущий или итоговый статус выполнения задачи.
    /// </summary>
    public JobCompletionStatus Status { get; set; } = JobCompletionStatus.Succeeded;

    /// <summary>
    /// Пояснение к текущему или итоговому состоянию выполнения, если оно задано.
    /// </summary>
    public string? Reason { get; set; }

    /// <summary>
    /// Исключение, которым завершился запуск, если выполнение дошло до ошибочного исхода.
    /// </summary>
    public Exception? Exception { get; set; }
}
