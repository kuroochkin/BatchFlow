using BatchFlow;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Checkpoints;
using BatchFlow.Core.Execution.Models;
using BatchFlow.EntityFrameworkCore.Extensions;
using BatchFlow.EntityFrameworkCore.Options;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

var connectionString = new SqliteConnectionStringBuilder
{
    DataSource = $"batchflow-example-{Guid.NewGuid():N}",
    Mode = SqliteOpenMode.Memory,
    Cache = SqliteCacheMode.Shared,
    Pooling = false,
}.ToString();
await using var anchor = new SqliteConnection(connectionString);
await anchor.OpenAsync();
var dbOptions = new DbContextOptionsBuilder<OrdersDbContext>().UseSqlite(connectionString).Options;
var factory = new PooledDbContextFactory<OrdersDbContext>(dbOptions, poolSize: 4);

await using (var setup = await factory.CreateDbContextAsync())
{
    await setup.Database.EnsureCreatedAsync();
    setup.Orders.AddRange(
        new OrderRow { Id = 1, Number = "A-001" },
        new OrderRow { Id = 2, Number = "A-002" },
        new OrderRow { Id = 3, Number = "A-003" });
    await setup.SaveChangesAsync();
}

var sourceOptions = new EfCoreBatchSourceOptions<OrdersDbContext, OrderDto, long>
{
    Context = EntityFrameworkCoreContextOptions<OrdersDbContext>.FromDbContextFactory(factory),
    QueryFactory = db => db.Orders.Where(order => order.ExportReady)
        .Select(order => new OrderDto { Id = order.Id, Number = order.Number }),
    ApplyStableOrdering = query => query.OrderBy(order => order.Id),
    ResumePredicateFactory = checkpoint => order => order.Id > checkpoint,
    CheckpointSelector = order => order.Id,
    CheckpointComparer = Comparer<long>.Default,
};

var job = BatchJob.Named("ef-orders-export")
    .FromEntityFrameworkCore(sourceOptions)
    .WithBatchSize(2)
    .WithDegreeOfParallelism(2)
    .WithCheckpoint("ef-orders-export-v1", new InMemoryCheckpointStore<long>())
    .ProcessEach((order, _, token) =>
    {
        token.ThrowIfCancellationRequested();
        Console.WriteLine($"Экспорт {order.Number}");
        return ValueTask.CompletedTask;
    })
    .Build();

var first = await job.RunAsync();
EnsureSuccess(first);
Console.WriteLine($"Первый запуск: {first.Status}, обработано {first.SucceededItemCount}, checkpoint {first.LastPersistedCheckpoint?.Value}");

var resumed = await job.RunAsync();
EnsureSuccess(resumed);
Console.WriteLine($"Повторный запуск: {resumed.Status}, обработано {resumed.SucceededItemCount}, старт с checkpoint {resumed.ResumeFrom?.Value}");

static void EnsureSuccess(BatchJobExecutionSummary<long> summary)
{
    if (summary.Status == JobCompletionStatus.Cancelled)
    {
        throw new OperationCanceledException(summary.Reason);
    }

    if (summary.Status != JobCompletionStatus.Succeeded)
    {
        throw new InvalidOperationException($"Задача завершилась со статусом {summary.Status}: {summary.Reason}", summary.Exception);
    }
}

internal sealed class OrdersDbContext(DbContextOptions<OrdersDbContext> options) : DbContext(options)
{
    public DbSet<OrderRow> Orders => Set<OrderRow>();
}

internal sealed class OrderRow
{
    public long Id { get; set; }

    public required string Number { get; set; }

    public bool ExportReady { get; set; } = true;
}

internal sealed record OrderDto
{
    public required long Id { get; init; }

    public required string Number { get; init; }
}
