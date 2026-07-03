using BatchFlow.Abstractions.Sources;

namespace BatchFlow.Builders;

/// <summary>
/// Определяет первую стадию fluent-сборки пакетной задачи, на которой выбирается источник данных.
/// </summary>
public interface IBatchJobSourceSelectionStage
{
    /// <summary>
    /// Выбирает источник данных, который читает пакеты, но не поддерживает возобновление обработки по контрольной точке.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, возвращаемых источником.</typeparam>
    /// <param name="source">Источник данных.</param>
    /// <returns>Стадию builder для настройки параметров выполнения и выбора способа обработки.</returns>
    IBatchJobBuilder<TItem, NoCheckpoint> From<TItem>(IBatchSource<TItem> source);

    /// <summary>
    /// Выбирает источник данных, который может возвращать пакеты с типизированной контрольной точкой.
    /// </summary>
    /// <typeparam name="TItem">Тип элементов, возвращаемых источником.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки, используемой источником.</typeparam>
    /// <param name="source">Источник данных.</param>
    /// <returns>Стадию builder для настройки параметров выполнения, checkpoint store и выбора способа обработки.</returns>
    IResumableBatchJobBuilder<TItem, TCheckpoint> From<TItem, TCheckpoint>(IBatchSource<TItem, TCheckpoint> source);
}
