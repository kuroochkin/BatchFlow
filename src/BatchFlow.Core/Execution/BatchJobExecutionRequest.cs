using BatchFlow.Core.Jobs;

namespace BatchFlow.Core.Execution;

/// <summary>
/// Описывает запрос на выполнение пакетной задачи.
/// </summary>
/// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, используемой источником данных.</typeparam>
public sealed record BatchJobExecutionRequest<TItem, TCheckpoint>
{
    /// <summary>
    /// Описание задачи (ее настройки), которое должно быть выполнено.
    /// </summary>
    public required BatchJobDefinition<TItem, TCheckpoint> Job { get; init; }

    /// <summary>
    /// Идентификатор запуска, если он задан извне.
    /// Если значение не указано, движок создаст его самостоятельно.
    /// </summary>
    public string? RunId { get; init; }

    /// <summary>
    /// Источник времени, используемый во время выполнения задачи.
    /// </summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
