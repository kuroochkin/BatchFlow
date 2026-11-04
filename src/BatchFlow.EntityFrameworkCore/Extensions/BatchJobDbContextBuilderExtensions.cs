using BatchFlow.Builders;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using Microsoft.EntityFrameworkCore;

namespace BatchFlow.EntityFrameworkCore.Extensions;

/// <summary>
/// Подключает EF Core к builder через готовый источник, фабрику или явную конфигурацию DbContext.
/// </summary>
public static class BatchJobDbContextBuilderExtensions
{
    /// <summary>
    /// Подключает готовый EF-источник, в том числе полученный через конструкторный DI.
    /// </summary>
    /// <typeparam name="TContext">Тип контекста.</typeparam>
    /// <typeparam name="TItem">Ссылочный тип результата.</typeparam>
    /// <typeparam name="TCheckpoint">Тип позиции, отличный от null.</typeparam>
    /// <param name="stage">Стадия выбора источника.</param>
    /// <param name="source">Настроенный источник.</param>
    /// <returns>Стандартный builder с поддержкой checkpoint.</returns>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> FromDbContext<TContext, TItem, TCheckpoint>(
        this IBatchJobSourceSelectionStage stage, EfCoreBatchSource<TContext, TItem, TCheckpoint> source)
        where TContext : DbContext where TItem : class where TCheckpoint : notnull =>
        stage.FromEntityFrameworkCore(source);

    /// <summary>
    /// Создает и подключает источник из полных типизированных options, не обращаясь к БД.
    /// </summary>
    /// <typeparam name="TContext">Тип контекста.</typeparam>
    /// <typeparam name="TItem">Ссылочный тип результата.</typeparam>
    /// <typeparam name="TCheckpoint">Тип позиции, отличный от null.</typeparam>
    /// <param name="stage">Стадия выбора источника.</param>
    /// <param name="options">Настройки чтения.</param>
    /// <returns>Стандартный builder с поддержкой checkpoint.</returns>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> FromDbContext<TContext, TItem, TCheckpoint>(
        this IBatchJobSourceSelectionStage stage, EfCoreBatchSourceOptions<TContext, TItem, TCheckpoint> options)
        where TContext : DbContext where TItem : class where TCheckpoint : notnull =>
        stage.FromEntityFrameworkCore(options);

    /// <summary>
    /// Подключает фабрику EF Core и функцию настройки keyset-чтения. Контекст при сборке не создается.
    /// </summary>
    /// <typeparam name="TContext">Тип контекста.</typeparam>
    /// <typeparam name="TItem">Ссылочный тип результата.</typeparam>
    /// <typeparam name="TCheckpoint">Тип позиции, отличный от null.</typeparam>
    /// <param name="stage">Стадия выбора источника.</param>
    /// <param name="factory">Явно переданная фабрика, обычно внедренная в приложение через DI.</param>
    /// <param name="configure">Возвращает options с переданным Context.</param>
    /// <returns>Builder для настройки и запуска job.</returns>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> FromDbContext<TContext, TItem, TCheckpoint>(
        this IBatchJobSourceSelectionStage stage,
        IDbContextFactory<TContext> factory,
        Func<EntityFrameworkCoreContextOptions<TContext>, EfCoreBatchSourceOptions<TContext, TItem, TCheckpoint>> configure)
        where TContext : DbContext where TItem : class where TCheckpoint : notnull
    {
        ArgumentNullException.ThrowIfNull(stage);
        return stage.From(factory.CreateBatchSource(configure));
    }

    /// <summary>
    /// Подключает контролируемую фабрику контекстов и функцию настройки чтения.
    /// </summary>
    /// <typeparam name="TContext">Тип контекста.</typeparam>
    /// <typeparam name="TItem">Ссылочный тип результата.</typeparam>
    /// <typeparam name="TCheckpoint">Тип позиции, отличный от null.</typeparam>
    /// <param name="stage">Стадия выбора источника.</param>
    /// <param name="context">Настройки синхронной или асинхронной фабрики.</param>
    /// <param name="configure">Возвращает options с переданным Context.</param>
    /// <returns>Builder для настройки job.</returns>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> FromDbContext<TContext, TItem, TCheckpoint>(
        this IBatchJobSourceSelectionStage stage,
        EntityFrameworkCoreContextOptions<TContext> context,
        Func<EntityFrameworkCoreContextOptions<TContext>, EfCoreBatchSourceOptions<TContext, TItem, TCheckpoint>> configure)
        where TContext : DbContext where TItem : class where TCheckpoint : notnull
    {
        ArgumentNullException.ThrowIfNull(stage);
        return stage.From(context.CreateBatchSource(configure));
    }

    /// <summary>
    /// Настраивает DbContextOptions один раз и использует явную функцию создания нового контекста на каждую порцию.
    /// Не применяет reflection, не строит DI-контейнер и не включает пул неявно.
    /// </summary>
    /// <typeparam name="TContext">Тип контекста.</typeparam>
    /// <typeparam name="TItem">Ссылочный тип результата.</typeparam>
    /// <typeparam name="TCheckpoint">Тип позиции, отличный от null.</typeparam>
    /// <param name="stage">Стадия выбора источника.</param>
    /// <param name="configureContext">Настройка провайдера и параметров EF Core.</param>
    /// <param name="createContext">Создание контекста из подготовленных options; владение передается source.</param>
    /// <param name="configureSource">Настройка чтения с переданным Context.</param>
    /// <returns>Builder для выполнения job.</returns>
    public static IResumableBatchJobBuilder<TItem, TCheckpoint> FromDbContext<TContext, TItem, TCheckpoint>(
        this IBatchJobSourceSelectionStage stage,
        Action<DbContextOptionsBuilder<TContext>> configureContext,
        Func<DbContextOptions<TContext>, TContext> createContext,
        Func<EntityFrameworkCoreContextOptions<TContext>, EfCoreBatchSourceOptions<TContext, TItem, TCheckpoint>> configureSource)
        where TContext : DbContext where TItem : class where TCheckpoint : notnull
    {
        ArgumentNullException.ThrowIfNull(stage);
        ArgumentNullException.ThrowIfNull(configureContext);
        ArgumentNullException.ThrowIfNull(createContext);
        ArgumentNullException.ThrowIfNull(configureSource);
        var builder = new DbContextOptionsBuilder<TContext>();
        configureContext(builder);
        var options = builder.Options;
        return stage.FromDbContext(EntityFrameworkCoreContextOptions<TContext>.FromFactory(() => createContext(options)), configureSource);
    }
}
