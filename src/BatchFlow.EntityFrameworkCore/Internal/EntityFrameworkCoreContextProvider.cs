using BatchFlow.EntityFrameworkCore.Options;
using Microsoft.EntityFrameworkCore;

namespace BatchFlow.EntityFrameworkCore.Internal;

internal sealed class EntityFrameworkCoreContextProvider<TContext>(
    EntityFrameworkCoreContextOptions<TContext> options) where TContext : DbContext
{
    private int _externalContextInUse;

    public async ValueTask<EntityFrameworkCoreContextLease<TContext>> AcquireAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (options.Factory is not { } factory)
        {
            if (Interlocked.CompareExchange(ref _externalContextInUse, 1, 0) != 0)
            {
                throw new InvalidOperationException(
                    "Источник с внешним DbContext уже используется. Для параллельных запусков передайте фабрику контекстов.");
            }

            return new EntityFrameworkCoreContextLease<TContext>(
                options.Context!, ownsContext: false, () => Interlocked.Exchange(ref _externalContextInUse, 0));
        }

        var context = await factory(cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("Фабрика EF Core вернула null вместо контекста.");

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new EntityFrameworkCoreContextLease<TContext>(context, ownsContext: true);
        }
        catch
        {
            try
            {
                await context.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Ошибка освобождения не должна подменять отмену после создания контекста.
            }

            throw;
        }
    }
}
