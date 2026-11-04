using BatchFlow.Builders;
using BatchFlow.EntityFrameworkCore.Sources;
using Microsoft.EntityFrameworkCore;

namespace BatchFlow.EntityFrameworkCore.Extensions;

/// <summary>
/// Подключает источники EF Core к стандартному контуру выполнения BatchFlow.
/// </summary>
public static class BatchJobEntityFrameworkCoreBuilderExtensions
{
    /// <summary>
    /// Выбирает источник EF Core, не создавая контекст и не выполняя запросов до запуска задачи.
    /// </summary>
    /// <typeparam name="TContext">Тип контекста приложения.</typeparam>
    /// <typeparam name="TItem">Тип элементов источника.</typeparam>
    /// <typeparam name="TCheckpoint">Тип контрольной точки источника.</typeparam>
    /// <param name="stage">Стадия выбора источника.</param>
    /// <param name="source">Реализация источника поверх EF Core.</param>
    /// <returns>Стандартный builder для выбора обработчика и параметров выполнения.</returns>
    /// <remarks>
    /// Хранилище контрольных точек подключается обычным WithCheckpoint. Если источник не поддерживает
    /// восстановление, builder отклонит подключение хранилища независимо от типа контрольной точки.
    /// </remarks>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> FromEntityFrameworkCore<TContext, TItem, TCheckpoint>(
        this IBatchJobSourceSelectionStage stage,
        EntityFrameworkCoreBatchSourceBase<TContext, TItem, TCheckpoint> source)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(stage);
        ArgumentNullException.ThrowIfNull(source);

        return stage.From(source);
    }
}
