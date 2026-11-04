using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;

namespace BatchFlow.EntityFrameworkCore.Tests.TestDoubles;

internal sealed class TestBatchSource(
    EntityFrameworkCoreContextOptions<TestDbContext> options,
    Func<TestDbContext, BatchSourceContext<long>, CancellationToken, IAsyncEnumerable<SourceBatch<int, long>>>? read = null,
    bool supportsResume = true) : EntityFrameworkCoreBatchSourceBase<TestDbContext, int, long>(options)
{
    public override bool SupportsResume => supportsResume;

    protected override IAsyncEnumerable<SourceBatch<int, long>> ReadCoreAsync(
        TestDbContext dbContext,
        BatchSourceContext<long> context,
        CancellationToken cancellationToken) =>
        read is null ? Batches() : read(dbContext, context, cancellationToken);

    public static BatchSourceContext<long> CreateContext(int batchSize = 2) => new()
    {
        Job = new JobExecutionContext { JobName = "ef-source-test", RunId = "test-run" },
        RequestedBatchSize = batchSize,
    };

    public static SourceBatch<int, long> CreateBatch(params int[] items) => new()
    {
        Items = items,
        Checkpoint = items.Length == 0 ? 0L : items[^1],
    };

    public static async IAsyncEnumerable<SourceBatch<int, long>> Batches(params SourceBatch<int, long>[] batches)
    {
        await Task.CompletedTask;

        foreach (var batch in batches)
        {
            yield return batch;
        }
    }

    public async Task<IReadOnlyList<SourceBatch<int, long>>> ReadAllAsync(
        BatchSourceContext<long>? context = null,
        CancellationToken cancellationToken = default)
    {
        var batches = new List<SourceBatch<int, long>>();

        await foreach (var batch in ReadAsync(context ?? CreateContext(), cancellationToken))
        {
            batches.Add(batch);
        }

        return batches;
    }
}
