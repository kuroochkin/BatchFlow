using BatchFlow.EntityFrameworkCore.Extensions;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using BatchFlow.Extensions.DependencyInjection.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace BatchFlow.EntityFrameworkCore.DependencyInjection;

/// <summary>
/// Регистрирует scoped-источники EF Core поверх фабрики, которую настраивает приложение.
/// </summary>
public static class BatchFlowEfCoreServiceCollectionExtensions
{
    /// <summary>
    /// Регистрирует конкретный EfCoreBatchSource как scoped-сервис. IDbContextFactory должен быть зарегистрирован
    /// приложением через штатный AddDbContextFactory/AddPooledDbContextFactory или собственную реализацию.
    /// </summary>
    /// <typeparam name="TContext">Тип контекста.</typeparam>
    /// <typeparam name="TItem">Ссылочный тип результата.</typeparam>
    /// <typeparam name="TCheckpoint">Тип позиции, отличный от null.</typeparam>
    /// <param name="services">Коллекция сервисов.</param>
    /// <param name="configure">Строит options с переданным Context, используя зависимости текущего scope.</param>
    /// <param name="factoryName">Необязательное имя IDbContextFactory; null выбирает обычную регистрацию.</param>
    /// <param name="name">Необязательное имя keyed-сервиса; null означает обычную регистрацию.</param>
    /// <returns>Ту же коллекцию сервисов.</returns>
    /// <remarks>
    /// Фабрика и source не создаются при регистрации. Интерфейс IBatchSource не регистрируется неявно.
    /// Scope должен жить до завершения job; singleton-worker создает scope на отдельный запуск.
    /// Повторная регистрация того же конкретного типа и имени отклоняется.
    /// </remarks>
    public static IServiceCollection AddBatchFlowEfCoreSource<TContext, TItem, TCheckpoint>(
        this IServiceCollection services,
        Func<IServiceProvider, EntityFrameworkCoreContextOptions<TContext>, EfCoreBatchSourceOptions<TContext, TItem, TCheckpoint>> configure,
        string? factoryName = null, string? name = null)
        where TContext : DbContext where TItem : class where TCheckpoint : notnull
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        BatchFlowServiceRegistration.ValidateName(factoryName);
        BatchFlowServiceRegistration.AddScoped(services, provider =>
            BatchFlowServiceRegistration.Resolve<IDbContextFactory<TContext>>(provider, factoryName)
                .CreateBatchSource(context => configure(provider, context)), name);
        return services;
    }
}
