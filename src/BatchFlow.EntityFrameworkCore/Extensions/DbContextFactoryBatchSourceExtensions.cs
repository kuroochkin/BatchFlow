using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using Microsoft.EntityFrameworkCore;

namespace BatchFlow.EntityFrameworkCore.Extensions;

/// <summary>
/// Создает EF-источник из явно переданной фабрики и неизменяемой конфигурации запроса.
/// </summary>
public static class DbContextFactoryBatchSourceExtensions
{
    /// <summary>
    /// Настраивает источник без создания контекста. Фабрика принадлежит приложению;
    /// каждый полученный контекст освобождается источником на границе порции.
    /// </summary>
    /// <typeparam name="TContext">Тип контекста.</typeparam>
    /// <typeparam name="TItem">Ссылочный тип результата.</typeparam>
    /// <typeparam name="TCheckpoint">Тип позиции, отличный от null.</typeparam>
    /// <param name="factory">Фабрика EF Core, в том числе с пулом.</param>
    /// <param name="configure">Возвращает options, используя переданный объект Context без его подмены.</param>
    /// <returns>Настроенный источник; запросы начнутся при чтении.</returns>
    public static EfCoreBatchSource<TContext, TItem, TCheckpoint> CreateBatchSource<TContext, TItem, TCheckpoint>(
        this IDbContextFactory<TContext> factory,
        Func<EntityFrameworkCoreContextOptions<TContext>, EfCoreBatchSourceOptions<TContext, TItem, TCheckpoint>> configure)
        where TContext : DbContext
        where TItem : class
        where TCheckpoint : notnull
    {
        ArgumentNullException.ThrowIfNull(factory);
        return EntityFrameworkCoreContextOptions<TContext>.FromDbContextFactory(factory).CreateBatchSource(configure);
    }

    /// <summary>
    /// Создает источник из контролируемой синхронной или асинхронной фабрики контекстов.
    /// Готовый внешний контекст отклоняется конструктором source.
    /// </summary>
    /// <typeparam name="TContext">Тип контекста.</typeparam>
    /// <typeparam name="TItem">Ссылочный тип результата.</typeparam>
    /// <typeparam name="TCheckpoint">Тип позиции, отличный от null.</typeparam>
    /// <param name="context">Настройки получения контекста.</param>
    /// <param name="configure">Возвращает options с тем же объектом Context.</param>
    /// <returns>Источник с коротким временем жизни контекста.</returns>
    public static EfCoreBatchSource<TContext, TItem, TCheckpoint> CreateBatchSource<TContext, TItem, TCheckpoint>(
        this EntityFrameworkCoreContextOptions<TContext> context,
        Func<EntityFrameworkCoreContextOptions<TContext>, EfCoreBatchSourceOptions<TContext, TItem, TCheckpoint>> configure)
        where TContext : DbContext
        where TItem : class
        where TCheckpoint : notnull
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(configure);
        var options = configure(context) ?? throw new InvalidOperationException("Настройка EF-источника вернула null.");

        if (!ReferenceEquals(options.Context, context))
        {
            throw new ArgumentException("Настройка EF-источника должна использовать переданный Context, а не подменять фабрику.", nameof(configure));
        }

        return new EfCoreBatchSource<TContext, TItem, TCheckpoint>(options);
    }
}
