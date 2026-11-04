using System.Runtime.CompilerServices;
using BatchFlow;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Checkpoints;
using BatchFlow.EntityFrameworkCore.DependencyInjection;
using BatchFlow.EntityFrameworkCore.Extensions;
using BatchFlow.EntityFrameworkCore.Sources;
using BatchFlow.Extensions.DependencyInjection;
using BatchFlow.Extensions.Logging;
using BatchFlow.Postgres.Checkpoints;
using BatchFlow.Postgres.DependencyInjection;
using BatchFlow.Postgres.Sources;
using BatchFlow.Testing.Observability;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NpgsqlTypes;

var store = new InMemoryCheckpointStore<long>();
var observer = new RecordingExecutionObserver<long>();
var source = new NumberSource();
var each = BatchJob.Named("package-each").From(source).WithBatchSize(2).WithDegreeOfParallelism(2)
    .WithCheckpoint(store).WithRetry(1).WithObserver(observer).WithProgressLogging(NullLogger.Instance);
var result = await each.RunEachAsync((item, context, _) =>
{
    if (item == 2 && context.Attempt == 1)
    {
        throw new TimeoutException("Проверка retry установленного NuGet-пакета.");
    }

    return ValueTask.CompletedTask;
});
Check(result.Status == JobCompletionStatus.Succeeded && result.SucceededItemCount == 3 && result.LastPersistedCheckpoint?.Value == 3, "each/retry/checkpoint");
var resumed = await each.RunEachAsync((_, _, _) => ValueTask.FromException(new InvalidOperationException("Подтвержденные данные прочитаны повторно.")));
Check(resumed.Status == JobCompletionStatus.Succeeded && resumed.ProcessedBatchCount == 0 && resumed.ResumeFrom?.Value == 3, "resume");
Check(observer.Events.Count > 0, "события и пакет Testing");
var batch = await BatchJob.Named("package-batch").From(source).WithBatchSize(2).WithDegreeOfParallelism(2)
    .RunBatchAsync((_, _, _) => ValueTask.CompletedTask);
Check(batch.Status == JobCompletionStatus.Succeeded && batch.SucceededItemCount == 3, "batch");

var connectionString = new SqliteConnectionStringBuilder
{
    DataSource = "batchflow-packages-" + Guid.NewGuid().ToString("N"),
    Mode = SqliteOpenMode.Memory,
    Cache = SqliteCacheMode.Shared,
    Pooling = false,
}.ToString();
await using var anchor = new SqliteConnection(connectionString);
await anchor.OpenAsync();
var services = new ServiceCollection().AddBatchFlow();
services.AddDbContextFactory<SmokeContext>(options => options.UseSqlite(connectionString));
services.AddBatchFlowEfCoreSource<SmokeContext, SmokeRow, long>((_, context) => new()
{
    Context = context,
    QueryFactory = db => db.Rows,
    ApplyStableOrdering = query => query.OrderBy(row => row.Id),
    ResumePredicateFactory = checkpoint => row => row.Id > checkpoint,
    CheckpointSelector = row => row.Id,
    CheckpointComparer = Comparer<long>.Default,
});
services.AddBatchFlowPostgresDataSource("Host=127.0.0.1;Port=1;Database=batchflow_tests;Username=batchflow", name: "sql");
services.AddBatchFlowPostgresSource<int, long>((_, connection) => new()
{
    Connection = connection,
    CommandText = "select @batch_size, @resume_checkpoint where false",
    ResumeCheckpointParameterType = NpgsqlDbType.Bigint,
    RowMaterializer = (_, _) => ValueTask.FromResult(new PostgresBatchSourceRow<int, long>(0, 0)),
}, dataSourceName: "sql");
services.AddBatchFlowPostgresCheckpoint<long>(provider => new()
{
    Connection = BatchFlow.Postgres.Options.PostgresConnectionOptions.FromDataSource(provider.GetRequiredKeyedService<Npgsql.NpgsqlDataSource>("sql")),
});
await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
var factory = provider.GetRequiredService<IDbContextFactory<SmokeContext>>();
await using (var setup = await factory.CreateDbContextAsync())
{
    await setup.Database.EnsureCreatedAsync();
    setup.Rows.AddRange(new SmokeRow { Id = 1 }, new SmokeRow { Id = 2 });
    await setup.SaveChangesAsync();
}

await using (var scope = provider.CreateAsyncScope())
{
    var efSource = scope.ServiceProvider.GetRequiredService<EfCoreBatchSource<SmokeContext, SmokeRow, long>>();
    var runOptions = scope.ServiceProvider.GetRequiredService<BatchJobRunOptions>();
    var efResult = await BatchJob.Named("package-ef").FromDbContext(efSource).WithBatchSize(1)
        .RunEachAsync((_, _, _) => ValueTask.CompletedTask, runOptions);
    Check(efResult.Status == JobCompletionStatus.Succeeded && efResult.SucceededItemCount == 2, "EF Core и DI");
    _ = scope.ServiceProvider.GetRequiredService<PostgresBatchSource<int, long>>();
    _ = scope.ServiceProvider.GetRequiredService<PostgresCheckpointStore<long>>();
}

Console.WriteLine("NuGet smoke test пройден: each, batch, retry, resume, события, logging, EF/SQLite и DI обоих адаптеров.");

static void Check(bool condition, string feature)
{
    if (!condition)
    {
        throw new InvalidOperationException("Не пройдена проверка: " + feature);
    }
}

internal sealed class SmokeContext(DbContextOptions<SmokeContext> options) : DbContext(options)
{
    public DbSet<SmokeRow> Rows => Set<SmokeRow>();
}

internal sealed class SmokeRow
{
    public long Id { get; set; }
}

internal sealed class NumberSource : IBatchSource<int, long>
{
    public async IAsyncEnumerable<SourceBatch<int, long>> ReadAsync(BatchSourceContext<long> context, [EnumeratorCancellation] CancellationToken token)
    {
        var last = context.ResumeFrom?.Value ?? 0;
        foreach (var items in new[] { 1, 2, 3 }.Where(item => item > last).Chunk(context.RequestedBatchSize))
        {
            token.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return new SourceBatch<int, long> { Items = items, Checkpoint = items[^1] };
        }
    }
}
