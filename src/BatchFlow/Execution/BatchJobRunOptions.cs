using BatchFlow.Core.Execution;

namespace BatchFlow;

/// <summary>
/// Определяет дополнительные параметры запуска пакетной задачи через публичный API.
/// </summary>
public sealed record BatchJobRunOptions
{
    /// <summary>
    /// Явный идентификатор запуска, если его нужно задать извне.
    /// Если значение не указано, движок выполнения сгенерирует идентификатор самостоятельно.
    /// </summary>
    public string? RunId { get; init; }

    /// <summary>
    /// Источник времени, который должен использоваться во время выполнения задачи.
    /// </summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>
    /// Пользовательский движок выполнения, если запуск должен идти не через стандартный runtime.
    /// Если значение не указано, будет создан стандартный <see cref="BatchJobExecutionEngine"/>.
    /// </summary>
    public IBatchJobExecutionEngine? ExecutionEngine { get; init; }
}
