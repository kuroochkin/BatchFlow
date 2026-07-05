using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Failures;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Core.Processing;

namespace BatchFlow.Core.Jobs;

/// <summary>
/// Описывает пакетную задачу (настройки), которую должен выполнить движок.
/// </summary>
/// <typeparam name="TItem">Тип элементов, которые обрабатывает задача.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, используемой источником данных.</typeparam>
public sealed record BatchJobDefinition<TItem, TCheckpoint>
{
    /// <summary>
    /// Имя задачи.
    /// </summary>
    public required string JobName { get; init; }

    /// <summary>
    /// Идентификатор потока контрольных точек.
    /// Если значение не указано, для сохранения контрольных точек используется имя задачи.
    /// </summary>
    public string? CheckpointId { get; init; }

    /// <summary>
    /// Источник данных, из которого задача читает элементы пакетами.
    /// </summary>
    public required IBatchSource<TItem, TCheckpoint> Source { get; init; }

    /// <summary>
    /// Режим обработки, которым должен пользоваться движок.
    /// </summary>
    public required BatchJobProcessingMode<TItem, TCheckpoint> ProcessingMode { get; init; }

    /// <summary>
    /// Размер пакета, который задача запрашивает у источника.
    /// </summary>
    public int BatchSize { get; init; } = 100;

    /// <summary>
    /// Возвращает настроенную степень параллелизма.
    /// В текущем движке значение валидируется и сохраняется в контексте, но фактическая обработка пока остается последовательной.
    /// </summary>
    public int DegreeOfParallelism { get; init; } = 1;

    /// <summary>
    /// Количество пакетов, которое runtime может предварительно прочитать и удерживать
    /// в ограниченной очереди между чтением и обработкой.
    /// Значение должно быть положительным и используется для организации backpressure.
    /// </summary>
    public int PrefetchCapacity { get; init; } = 1;

    /// <summary>
    /// Хранилище контрольных точек, если задача должна сохранять прогресс.
    /// </summary>
    public ICheckpointStore<TCheckpoint>? CheckpointStore { get; init; }

    /// <summary>
    /// Политика обработки ошибок, если она задана.
    /// </summary>
    public IFailurePolicy<TItem, TCheckpoint>? FailurePolicy { get; init; }

    /// <summary>
    /// Список наблюдателей, которые должны получать события выполнения.
    /// </summary>
    public IReadOnlyList<IExecutionObserver<TCheckpoint>> Observers { get; init; } = [];

    /// <summary>
    /// Идентификатор потока контрольных точек, который будет использован движком.
    /// </summary>
    public string ResolveCheckpointId() => string.IsNullOrWhiteSpace(CheckpointId) ? JobName : CheckpointId;
}
