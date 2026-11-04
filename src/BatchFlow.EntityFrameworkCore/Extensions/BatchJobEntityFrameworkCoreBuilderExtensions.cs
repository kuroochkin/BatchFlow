using BatchFlow.Builders;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using Microsoft.EntityFrameworkCore;

namespace BatchFlow.EntityFrameworkCore.Extensions;

/// <summary>
/// Подключает источники EF Core к стандартному контуру выполнения BatchFlow.
/// </summary>
public static class BatchJobEntityFrameworkCoreBuilderExtensions
{
    /// <summary>
    /// Подключает готовый keyset-источник EF Core с отдельным контекстом на каждую порцию.
    /// </summary>
    /// <typeparam name="TContext">Тип контекста приложения.</typeparam>
    /// <typeparam name="TItem">Ссылочный тип сущности или модели результата.</typeparam>
    /// <typeparam name="TCheckpoint">Тип неизменяемой позиции, не допускающий <see langword="null" />.</typeparam>
    /// <param name="stage">Стадия выбора источника.</param>
    /// <param name="source">Источник с коротким временем жизни контекста.</param>
    /// <returns>Стандартный builder с поддержкой контрольных точек.</returns>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> FromEntityFrameworkCore<TContext, TItem, TCheckpoint>(
        this IBatchJobSourceSelectionStage stage,
        EfCoreBatchSource<TContext, TItem, TCheckpoint> source)
        where TContext : DbContext
        where TItem : class
        where TCheckpoint : notnull
    {
        ArgumentNullException.ThrowIfNull(stage);
        ArgumentNullException.ThrowIfNull(source);
        return stage.From(source);
    }

    /// <summary>
    /// Создает keyset-источник по настройкам и подключает его без наследования базового класса.
    /// Контексты и запросы не создаются до запуска задачи.
    /// </summary>
    /// <typeparam name="TContext">Тип контекста приложения.</typeparam>
    /// <typeparam name="TItem">Ссылочный тип сущности или модели результата.</typeparam>
    /// <typeparam name="TCheckpoint">Тип неизменяемой позиции, не допускающий <see langword="null" />.</typeparam>
    /// <param name="stage">Стадия выбора источника.</param>
    /// <param name="options">Настройки запроса, стабильной сортировки и keyset-перехода.</param>
    /// <returns>Стандартный builder с поддержкой контрольных точек.</returns>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> FromEntityFrameworkCore<TContext, TItem, TCheckpoint>(
        this IBatchJobSourceSelectionStage stage,
        EfCoreBatchSourceOptions<TContext, TItem, TCheckpoint> options)
        where TContext : DbContext
        where TItem : class
        where TCheckpoint : notnull
    {
        ArgumentNullException.ThrowIfNull(stage);
        ArgumentNullException.ThrowIfNull(options);
        return stage.From(new EfCoreBatchSource<TContext, TItem, TCheckpoint>(options));
    }

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
