using BatchFlow.Checkpoints;
using BatchFlow.EntityFrameworkCore.DependencyInjection;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.Extensions.DependencyInjection;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

using var shutdown = new CancellationTokenSource();
ConsoleCancelEventHandler onCancel = (_, args) =>
{
    args.Cancel = true;
    shutdown.Cancel();
};
Console.CancelKeyPress += onCancel;

try
{
    var connectionString = new SqliteConnectionStringBuilder
    {
        DataSource = $"batchflow-example-{Guid.NewGuid():N}",
        Mode = SqliteOpenMode.Memory,
        Cache = SqliteCacheMode.Shared,
        Pooling = false,
    }.ToString();
    await using var anchor = new SqliteConnection(connectionString);
    await anchor.OpenAsync(shutdown.Token);
    var services = new ServiceCollection();
    services.AddBatchFlow().AddBatchFlowInMemoryCheckpoint<long>();
    services.AddPooledDbContextFactory<OrdersDbContext>(options => options.UseSqlite(connectionString), poolSize: 4);
    services.AddBatchFlowEfCoreSource<OrdersDbContext, OrderDto, long>((_, context) => new EfCoreBatchSourceOptions<OrdersDbContext, OrderDto, long>
    {
        Context = context,
        QueryFactory = db => db.Orders.Where(order => order.ExportReady)
            .Select(order => new OrderDto { Id = order.Id, Number = order.Number }),
        ApplyStableOrdering = query => query.OrderBy(order => order.Id),
        ResumePredicateFactory = checkpoint => order => order.Id > checkpoint,
        CheckpointSelector = order => order.Id,
        CheckpointComparer = Comparer<long>.Default,
    });
    services.AddScoped<EfCoreSampleRunner>();
    await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });

    await using (var setup = await provider.GetRequiredService<IDbContextFactory<OrdersDbContext>>().CreateDbContextAsync(shutdown.Token))
    {
        await setup.Database.EnsureCreatedAsync(shutdown.Token);
        setup.Orders.AddRange(
            new OrderRow { Id = 1, Number = "A-001" },
            new OrderRow { Id = 2, Number = "A-002" },
            new OrderRow { Id = 3, Number = "A-003" });
        await setup.SaveChangesAsync(shutdown.Token);
    }

    // В worker service такой scope создается на один запуск, а не хранится в singleton-worker.
    for (var pass = 1; pass <= 2; pass++)
    {
        await using var scope = provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<EfCoreSampleRunner>().RunAsync(pass, shutdown.Token);
    }

    await using var verification = await provider.GetRequiredService<IDbContextFactory<OrdersDbContext>>().CreateDbContextAsync(shutdown.Token);
    var count = await verification.Exports.CountAsync(shutdown.Token);
    if (count != 6)
    {
        throw new InvalidOperationException($"Ожидалось 6 результатов без дубликатов, получено {count}.");
    }

    Console.WriteLine($"Результаты each и batch: {count}, без дубликатов.");
}
catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
{
    Console.Error.WriteLine("Пример отменен.");
    Environment.ExitCode = 130;
}
finally
{
    Console.CancelKeyPress -= onCancel;
}
