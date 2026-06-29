using BatchFlow.Abstractions.Execution;

namespace BatchFlow.Abstractions.Sources;

/// <summary>
/// Определяет источник данных, который возвращает элементы пакетами.
/// Источник может как поддерживать возобновление чтения по контрольной точке, так и работать без этой возможности.
/// </summary>
/// <typeparam name="TItem">Тип элементов, которые возвращает источник данных.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, понятный источнику данных.</typeparam>
public interface IBatchSource<TItem, TCheckpoint>
{
    /// <summary>
    /// Возвращает признак того, что источник умеет возобновлять чтение по контрольной точке.
    /// </summary>
    bool SupportsResume => true;

    /// <summary>
    /// Читает пакеты из источника, начиная с позиции, заданной в контексте возобновления.
    /// Источник не должен возвращать пустые пакеты: отсутствие данных выражается завершением последовательности.
    /// </summary>
    IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> ReadAsync(
        BatchSourceContext<TCheckpoint> context,
        CancellationToken cancellationToken);
}

/// <summary>
/// Определяет источник данных, который возвращает элементы пакетами, но не поддерживает возобновление чтения после перезапуска.
/// Для таких источников в качестве типа контрольной точки используется <see cref="NoCheckpoint"/>.
/// </summary>
/// <typeparam name="TItem">Тип элементов, которые возвращает источник данных.</typeparam>
public interface IBatchSource<TItem> : IBatchSource<TItem, NoCheckpoint>
{
    /// <summary>
    /// Для источников без контрольных точек возобновление чтения недоступно.
    /// </summary>
    bool IBatchSource<TItem, NoCheckpoint>.SupportsResume => false;
}
