using Microsoft.EntityFrameworkCore;

namespace BatchFlow.EntityFrameworkCore.Tests.TestDoubles;

internal sealed class TestDbContext : DbContext
{
    public int DisposeCount { get; private set; }

    public Exception? DisposeException { get; init; }

    public override async ValueTask DisposeAsync()
    {
        DisposeCount++;
        await base.DisposeAsync();

        if (DisposeException is { } exception)
        {
            throw exception;
        }
    }
}
