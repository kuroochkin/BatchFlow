using System.Runtime.CompilerServices;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;
using BatchFlow.EntityFrameworkCore.Internal;
using BatchFlow.EntityFrameworkCore.Options;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;

namespace BatchFlow.EntityFrameworkCore.Sources;

/// <summary>
/// Читает данные через EF Core порциями со строгим keyset-продолжением.
/// Создает и освобождает контекст на каждую порцию до передачи материализованного пакета runtime.
/// </summary>
/// <typeparam name="TContext">Тип контекста приложения.</typeparam>
/// <typeparam name="TItem">Ссылочный тип сущности или модели результата без ленивой загрузки.</typeparam>
/// <typeparam name="TCheckpoint">Тип неизменяемой позиции, не допускающий <see langword="null" />.</typeparam>
/// <remarks>
/// Источник не удерживает контекст во время обработки пакета, не открывает общую транзакцию на job
/// и не обеспечивает согласованный снимок изменяющейся таблицы. Ключи порядка и фильтры должны быть стабильными.
/// Для проверки границы читается не больше RequestedBatchSize + 1 элементов; дополнительный элемент
/// не выдается и при продолжении читается снова. Checkpoint сохраняет только runtime после обработки.
/// Курсор каждого перечисления независим. При совместном использовании источника фабрика, делегаты
/// конфигурации и сравниватель должны допускать параллельные вызовы.
/// </remarks>
public sealed class EfCoreBatchSource<TContext, TItem, TCheckpoint> : IBatchSource<TItem, TCheckpoint>
    where TContext : DbContext
    where TItem : class
    where TCheckpoint : notnull
{
    private readonly EfCoreBatchSourceOptions<TContext, TItem, TCheckpoint> _options;
    private readonly EntityFrameworkCoreContextProvider<TContext> _contextProvider;

    /// <summary>
    /// Проверяет конфигурацию без создания контекста или выполнения запроса.
    /// </summary>
    /// <param name="options">Настройки запроса, полного порядка и контрольной точки.</param>
    /// <exception cref="ArgumentException">Передан внешний контекст вместо фабрики.</exception>
    public EfCoreBatchSource(EfCoreBatchSourceOptions<TContext, TItem, TCheckpoint> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Context);
        ArgumentNullException.ThrowIfNull(options.QueryFactory);
        ArgumentNullException.ThrowIfNull(options.ApplyStableOrdering);
        ArgumentNullException.ThrowIfNull(options.ResumePredicateFactory);
        ArgumentNullException.ThrowIfNull(options.CheckpointSelector);
        ArgumentNullException.ThrowIfNull(options.CheckpointComparer);

        if (!options.Context.OwnsContext)
        {
            throw new ArgumentException(
                "EfCoreBatchSource требует фабрику контекстов: готовый внешний DbContext несовместим с коротким временем жизни порции.", nameof(options));
        }

        _options = options;
        _contextProvider = new EntityFrameworkCoreContextProvider<TContext>(options.Context);
    }

    /// <summary>
    /// Источник использует обязательный keyset-предикат и поддерживает продолжение со строгой границы checkpoint.
    /// </summary>
    public bool SupportsResume => true;

    /// <summary>
    /// Читает непустые пакеты, начиная строго после ResumeFrom. Контекст освобождается до каждого yield return,
    /// включая последнюю и пустую порцию; первичная ошибка чтения или отмена не подменяется ошибкой освобождения.
    /// </summary>
    /// <param name="context">Контекст запуска, сохраненная позиция и размер пакета от 1 до Int32.MaxValue - 1.</param>
    /// <param name="cancellationToken">Отмена создания контекстов, выполнения запросов и проверки результатов.</param>
    /// <returns>Материализованные пакеты в заданном порядке, без пустых пакетов.</returns>
    /// <exception cref="InvalidOperationException">
    /// Запрос принадлежит другому контексту, нарушена форма запроса или сортировки,
    /// получены элементы или позиции со значением null либо checkpoint не продвигается строго в заданном порядке.
    /// </exception>
    public async IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> ReadAsync(
        BatchSourceContext<TCheckpoint> context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(context.RequestedBatchSize);

        if (context.RequestedBatchSize == int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(context), "Размер пакета должен оставлять место для проверки следующего элемента.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        var hasCheckpoint = context.ResumeFrom is not null;
        var checkpoint = hasCheckpoint ? context.ResumeFrom!.Value : default!;

        if (hasCheckpoint && checkpoint is null)
        {
            throw new InvalidOperationException("Сохраненная позиция EF-источника не может быть null.");
        }

        ReadPageResult page;

        do
        {
            page = await ReadPageAsync(hasCheckpoint, checkpoint, context.RequestedBatchSize, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();

            if (page.Batch is { } batch)
            {
                checkpoint = batch.Checkpoint;
                hasCheckpoint = true;
                yield return batch;
            }
        }
        while (page.HasMore);
    }

    private async ValueTask<ReadPageResult> ReadPageAsync(
        bool hasCheckpoint,
        TCheckpoint checkpoint,
        int batchSize,
        CancellationToken cancellationToken)
    {
        var lease = await _contextProvider.AcquireAsync(cancellationToken).ConfigureAwait(false);
        var readFailed = false;

        try
        {
            var dbContext = lease.Context;
            dbContext.ChangeTracker.LazyLoadingEnabled = false;
            dbContext.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;
            var query = CreateQuery(dbContext, hasCheckpoint, checkpoint);
            var items = await query.AsNoTracking().Take(batchSize + 1).ToListAsync(cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();

            var page = CreatePage(items, hasCheckpoint, checkpoint, batchSize, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            return page;
        }
        catch
        {
            readFailed = true;
            throw;
        }
        finally
        {
            try
            {
                await lease.DisposeAsync().ConfigureAwait(false);
            }
            catch when (readFailed)
            {
                // Сохраняем ошибку запроса, проверки или отмену вместо вторичной ошибки освобождения.
            }
            catch (Exception cleanupFailure) when (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException("Чтение порции EF Core было отменено при освобождении контекста.", cleanupFailure, cancellationToken);
            }
        }
    }

    private IQueryable<TItem> CreateQuery(TContext dbContext, bool hasCheckpoint, TCheckpoint checkpoint)
    {
        var query = _options.QueryFactory(dbContext)
            ?? throw new InvalidOperationException("Фабрика EF-запроса вернула null.");

        // Scoped-провайдер EF позволяет проверить принадлежность запроса, в том числе после DTO-проекции.
        if (!ReferenceEquals(query.Provider, dbContext.GetService<IAsyncQueryProvider>()))
        {
            throw new InvalidOperationException("EF-запрос должен принадлежать контексту, переданному в QueryFactory, без замены провайдера запроса.");
        }

        EfCoreQueryShapeValidator.ValidateBaseQuery(query.Expression);

        if (hasCheckpoint)
        {
            var predicate = _options.ResumePredicateFactory(checkpoint)
                ?? throw new InvalidOperationException("Фабрика предиката продолжения вернула null.");
            query = query.Where(predicate);
        }

        var ordered = _options.ApplyStableOrdering(query)
            ?? throw new InvalidOperationException("Правило стабильной сортировки вернуло null.");
        EfCoreQueryShapeValidator.ValidateOrdering(ordered.Expression, query.Expression);

        if (!ReferenceEquals(ordered.Provider, query.Provider))
        {
            throw new InvalidOperationException("Правило стабильной сортировки не должно заменять провайдер EF-запроса.");
        }

        return ordered;
    }

    private ReadPageResult CreatePage(
        List<TItem> items,
        bool hasCheckpoint,
        TCheckpoint checkpoint,
        int batchSize,
        CancellationToken cancellationToken)
    {
        if (items.Count == 0)
        {
            return default;
        }

        var previous = checkpoint;
        var lastIncluded = default(TCheckpoint)!;

        for (var index = 0; index < items.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var item = items[index]
                ?? throw new InvalidOperationException("EF-запрос вернул null вместо элемента.");
            var current = _options.CheckpointSelector(item);

            if (current is null)
            {
                throw new InvalidOperationException("CheckpointSelector не должен возвращать null.");
            }

            if (hasCheckpoint && _options.CheckpointComparer.Compare(previous, current) >= 0)
            {
                throw new InvalidOperationException(
                    "Checkpoint должен строго продвигаться в порядке сортировки. Проверьте уникальность всех ключей, предикат продолжения и сравниватель.");
            }

            previous = current;
            hasCheckpoint = true;

            if (index < batchSize)
            {
                lastIncluded = current;
            }
        }

        var hasMore = items.Count > batchSize;

        if (hasMore)
        {
            items.RemoveAt(batchSize);
        }

        return new ReadPageResult(new SourceBatch<TItem, TCheckpoint>
        {
            Items = items.AsReadOnly(),
            Checkpoint = lastIncluded,
        }, hasMore);
    }

    private readonly record struct ReadPageResult(SourceBatch<TItem, TCheckpoint>? Batch, bool HasMore);
}
