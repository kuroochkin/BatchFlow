using Microsoft.EntityFrameworkCore;

namespace BatchFlow.EntityFrameworkCore.Internal;

internal sealed class EntityFrameworkCoreContextLease<TContext>(
    TContext context,
    bool ownsContext,
    Action? release = null) : IAsyncDisposable where TContext : DbContext
{
    private int _disposed;

    public TContext Context { get; } = context;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            if (ownsContext)
            {
                await Context.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            release?.Invoke();
        }
    }
}
