using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;
using BatchFlow.EntityFrameworkCore.Internal;
using BatchFlow.EntityFrameworkCore.Options;
using Microsoft.EntityFrameworkCore;

namespace BatchFlow.EntityFrameworkCore.Sources;

/// <summary>
/// Управляет контекстом EF Core на время одного перечисления источника и проверяет выдаваемые пакеты.
/// Запросы и способ продвижения контрольной точки определяет наследник.
/// </summary>
/// <typeparam name="TContext">Тип настроенного контекста приложения.</typeparam>
/// <typeparam name="TItem">Тип материализованных элементов, передаваемых обработчикам.</typeparam>
/// <typeparam name="TCheckpoint">Тип позиции, понятной реализации источника.</typeparam>
/// <remarks>
/// Контекст принадлежит только чтению и не передается обработчикам. Для запросов только на чтение
/// используйте AsNoTracking или проекцию в независимые модели. Базовый класс не меняет настройки
/// контекста, не открывает транзакцию, не вызывает SaveChanges и не сохраняет контрольные точки.
/// </remarks>
public abstract class EntityFrameworkCoreBatchSourceBase<TContext, TItem, TCheckpoint> : IBatchSource<TItem, TCheckpoint>
    where TContext : DbContext
{
    private readonly EntityFrameworkCoreContextProvider<TContext> _contextProvider;

    /// <summary>
    /// Настраивает способ получения контекста без его немедленного создания.
    /// </summary>
    /// <param name="contextOptions">Настройки фабрики или внешнего контекста.</param>
    protected EntityFrameworkCoreBatchSourceBase(EntityFrameworkCoreContextOptions<TContext> contextOptions)
    {
        ArgumentNullException.ThrowIfNull(contextOptions);
        _contextProvider = new EntityFrameworkCoreContextProvider<TContext>(contextOptions);
    }

    /// <summary>
    /// Определяет, учитывает ли реализация ResumeFrom при чтении. Само использование EF Core
    /// не дает поддержки восстановления: наследник должен явно указать свою гарантию.
    /// </summary>
    public abstract bool SupportsResume { get; }

    /// <summary>
    /// Получает контекст и читает пакеты до завершения, ошибки, отмены или освобождения перечислителя.
    /// Контекст из фабрики освобождается при любом из этих исходов; внешний контекст остается у приложения.
    /// </summary>
    /// <param name="context">Параметры чтения, включая размер пакета и сохраненную позицию.</param>
    /// <param name="cancellationToken">Отмена создания контекста и последующих операций чтения.</param>
    /// <returns>Последовательность непустых пакетов размером не больше запрошенного.</returns>
    /// <remarks>
    /// Ошибка чтения или проверки пакета, включая отмену, сохраняется даже при сбое освобождения ресурсов.
    /// Если чтение не завершилось ошибкой, возвращается первая ошибка освобождения перечислителя или контекста.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// Источник без поддержки восстановления получил сохраненную позицию, внешний контекст уже занят,
    /// фабрика не вернула контекст или наследник нарушил контракт пакетов.
    /// </exception>
    public async IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> ReadAsync(
        BatchSourceContext<TCheckpoint> context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(context.RequestedBatchSize);
        cancellationToken.ThrowIfCancellationRequested();

        if (!SupportsResume && context.ResumeFrom is not null)
        {
            throw new InvalidOperationException("Источник EF Core не поддерживает чтение с сохраненной позиции.");
        }

        var lease = await _contextProvider.AcquireAsync(cancellationToken).ConfigureAwait(false);
        IAsyncEnumerator<SourceBatch<TItem, TCheckpoint>>? enumerator = null;
        var readFailed = false;

        try
        {
            try
            {
                var batches = ReadCoreAsync(lease.Context, context, cancellationToken)
                    ?? throw new InvalidOperationException("Источник EF Core вернул null вместо последовательности пакетов.");
                enumerator = batches.GetAsyncEnumerator(cancellationToken)
                    ?? throw new InvalidOperationException("Источник EF Core вернул null вместо перечислителя пакетов.");
            }
            catch
            {
                readFailed = true;
                throw;
            }

            SourceBatch<TItem, TCheckpoint>? batch;

            do
            {
                // yield return нельзя размещать внутри try/catch: отмечаем сбой до выдачи пакета.
                try
                {
                    batch = await ReadNextBatchAsync(enumerator, context.RequestedBatchSize, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    readFailed = true;
                    throw;
                }

                if (batch is not null)
                {
                    yield return batch;
                }
            }
            while (batch is not null);
        }
        finally
        {
            await DisposeReadResourcesAsync(enumerator, lease, readFailed).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Читает данные через предоставленный контекст. Наследник полностью материализует пакет до выдачи,
    /// учитывает RequestedBatchSize и ResumeFrom, передает токен в операции EF Core и завершает
    /// последовательность при отсутствии данных. Выданные коллекции больше не изменяются.
    /// </summary>
    /// <param name="dbContext">Контекст только для текущего перечисления; освобождать его в наследнике нельзя.</param>
    /// <param name="context">Параметры чтения и сохраненная позиция.</param>
    /// <param name="cancellationToken">Токен отмены операций EF Core.</param>
    /// <returns>Полностью материализованные пакеты без отложенных запросов и ленивой загрузки.</returns>
    protected abstract IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> ReadCoreAsync(
        TContext dbContext,
        BatchSourceContext<TCheckpoint> context,
        CancellationToken cancellationToken);

    private static async ValueTask<SourceBatch<TItem, TCheckpoint>?> ReadNextBatchAsync(
        IAsyncEnumerator<SourceBatch<TItem, TCheckpoint>> enumerator,
        int requestedBatchSize,
        CancellationToken cancellationToken)
    {
        if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
        {
            return null;
        }

        cancellationToken.ThrowIfCancellationRequested();
        var batch = enumerator.Current;
        ValidateBatch(batch, requestedBatchSize);
        return batch;
    }

    private static async ValueTask DisposeReadResourcesAsync(
        IAsyncEnumerator<SourceBatch<TItem, TCheckpoint>>? enumerator,
        EntityFrameworkCoreContextLease<TContext> lease,
        bool suppressCleanupErrors)
    {
        ExceptionDispatchInfo? cleanupFailure = null;

        if (enumerator is not null)
        {
            try
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (!suppressCleanupErrors)
                {
                    cleanupFailure = ExceptionDispatchInfo.Capture(ex);
                }
            }
        }

        try
        {
            await lease.DisposeAsync().ConfigureAwait(false);
        }
        catch when (suppressCleanupErrors || cleanupFailure is not null)
        {
            // Исходная ошибка чтения или первого освобождения важнее последующих ошибок очистки.
        }

        cleanupFailure?.Throw();
    }

    private static void ValidateBatch(SourceBatch<TItem, TCheckpoint> batch, int requestedBatchSize)
    {
        if (batch is null || batch.Items is null || batch.Items.Count == 0)
        {
            throw new InvalidOperationException(
                "Источник EF Core должен возвращать непустые пакеты. При отсутствии данных завершите последовательность.");
        }

        if (batch.Items.Count > requestedBatchSize)
        {
            throw new InvalidOperationException("Источник EF Core вернул пакет больше запрошенного размера.");
        }
    }
}
