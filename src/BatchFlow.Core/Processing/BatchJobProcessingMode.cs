namespace BatchFlow.Core.Processing;

/// <summary>
/// Определяет режим обработки пакетной задачи.
/// </summary>
/// <typeparam name="TItem">Тип элементов, которые обрабатываются задачей.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с обработкой.</typeparam>
public abstract record BatchJobProcessingMode<TItem, TCheckpoint>;
