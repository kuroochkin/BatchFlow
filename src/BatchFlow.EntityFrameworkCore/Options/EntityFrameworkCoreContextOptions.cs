using Microsoft.EntityFrameworkCore;

namespace BatchFlow.EntityFrameworkCore.Options;

/// <summary>
/// Определяет, откуда источник получает контекст EF Core и кто освобождает его после чтения.
/// Настройки не создают контекст до начала перечисления источника.
/// </summary>
/// <typeparam name="TContext">Тип контекста приложения с уже настроенным провайдером базы данных.</typeparam>
public sealed class EntityFrameworkCoreContextOptions<TContext> where TContext : DbContext
{
    private EntityFrameworkCoreContextOptions(
        Func<CancellationToken, ValueTask<TContext>>? factory,
        TContext? context)
    {
        Factory = factory;
        Context = context;
    }

    /// <summary>
    /// Показывает, освобождает ли источник контекст. Для фабрик значение равно <see langword="true" />,
    /// для готового контекста - <see langword="false" />.
    /// </summary>
    public bool OwnsContext => Factory is not null;

    internal Func<CancellationToken, ValueTask<TContext>>? Factory { get; }

    internal TContext? Context { get; }

    /// <summary>
    /// Использует фабрику EF Core, в том числе фабрику с пулом контекстов.
    /// Каждый созданный контекст освобождается источником; сама фабрика остается во владении приложения.
    /// </summary>
    /// <param name="contextFactory">
    /// Фабрика отдельного контекста на каждое обращение. EfCoreBatchSource обращается к ней на каждую порцию,
    /// а EntityFrameworkCoreBatchSourceBase - на каждое перечисление.
    /// </param>
    /// <returns>Настройки получения контекста через фабрику EF Core.</returns>
    public static EntityFrameworkCoreContextOptions<TContext> FromDbContextFactory(
        IDbContextFactory<TContext> contextFactory)
    {
        ArgumentNullException.ThrowIfNull(contextFactory);

        return FromFactory(async token => await contextFactory.CreateDbContextAsync(token).ConfigureAwait(false));
    }

    /// <summary>
    /// Использует синхронную фабрику контекстов. Источник освобождает каждый возвращенный экземпляр.
    /// </summary>
    /// <param name="contextFactory">Фабрика нового контекста; не должна возвращать общий экземпляр из DI.</param>
    /// <returns>Настройки получения контекста через пользовательскую фабрику.</returns>
    public static EntityFrameworkCoreContextOptions<TContext> FromFactory(Func<TContext> contextFactory)
    {
        ArgumentNullException.ThrowIfNull(contextFactory);

        return FromFactory(_ => ValueTask.FromResult(contextFactory()));
    }

    /// <summary>
    /// Использует асинхронную фабрику контекстов с поддержкой отмены.
    /// Владение возвращенным контекстом передается источнику даже при отмене во время создания.
    /// </summary>
    /// <param name="contextFactory">
    /// Фабрика отдельного контекста. Если создание завершилось исключением, фабрика сама освобождает
    /// ресурсы, которые не успела передать источнику.
    /// </param>
    /// <returns>Настройки получения контекста через асинхронную фабрику.</returns>
    public static EntityFrameworkCoreContextOptions<TContext> FromFactory(
        Func<CancellationToken, ValueTask<TContext>> contextFactory)
    {
        ArgumentNullException.ThrowIfNull(contextFactory);

        return new EntityFrameworkCoreContextOptions<TContext>(contextFactory, context: null);
    }

    /// <summary>
    /// Использует готовый контекст без передачи владения. Источник не освобождает его
    /// и запрещает одновременные перечисления через один экземпляр источника.
    /// </summary>
    /// <param name="context">Контекст, жизненным циклом которого управляет приложение.</param>
    /// <returns>Настройки использования внешнего контекста.</returns>
    /// <remarks>
    /// Приложение не должно обращаться к этому контексту из обработчиков или других источников,
    /// пока идет чтение. Для независимых и параллельных запусков используйте фабрику.
    /// </remarks>
    public static EntityFrameworkCoreContextOptions<TContext> FromContext(TContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        return new EntityFrameworkCoreContextOptions<TContext>(factory: null, context);
    }
}
