namespace BatchFlow.Abstractions.Sources;

/// <summary>
/// Обозначает отсутствие поддерживаемой контрольной точки у источника данных.
/// Используется для источников, которые читают данные пакетами, но не умеют возобновлять чтение после перезапуска.
/// </summary>
public readonly record struct NoCheckpoint
{
    /// <summary>
    /// Возвращает стандартное значение, обозначающее отсутствие контрольной точки.
    /// </summary>
    public static NoCheckpoint Value => default;
}
