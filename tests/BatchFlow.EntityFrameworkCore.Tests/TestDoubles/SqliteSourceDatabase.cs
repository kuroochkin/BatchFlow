using System.Collections.Concurrent;
using System.Data.Common;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;
using BatchFlow.EntityFrameworkCore.Options;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace BatchFlow.EntityFrameworkCore.Tests.TestDoubles;

internal sealed class SqliteSourceDatabase : IAsyncDisposable
{
    private readonly SqliteConnection _anchor;

    private SqliteSourceDatabase(SqliteConnection anchor, DbContextOptions<SourceTestDbContext> options, SourceCommandRecorder commands)
    {
        _anchor = anchor;
        ContextOptions = options;
        Commands = commands;
        Factory = new RecordingSourceContextFactory(options);
    }

    public DbContextOptions<SourceTestDbContext> ContextOptions { get; }

    public SourceCommandRecorder Commands { get; }

    public RecordingSourceContextFactory Factory { get; }

    public static async Task<SqliteSourceDatabase> CreateAsync(params SourceTestRow[] rows)
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = $"batchflow-source-{Guid.NewGuid():N}",
            Mode = SqliteOpenMode.Memory,
            Cache = SqliteCacheMode.Shared,
            Pooling = false,
        }.ToString();
        var anchor = new SqliteConnection(connectionString);

        try
        {
            await anchor.OpenAsync();
            var commands = new SourceCommandRecorder();
            var options = new DbContextOptionsBuilder<SourceTestDbContext>()
                .UseSqlite(connectionString)
                .AddInterceptors(commands)
                .Options;
            await using (var setup = new SourceTestDbContext(options))
            {
                await setup.Database.EnsureCreatedAsync();
                setup.Rows.AddRange(rows);
                await setup.SaveChangesAsync();
            }

            commands.Executed.Clear();
            return new SqliteSourceDatabase(anchor, options, commands);
        }
        catch
        {
            await anchor.DisposeAsync();
            throw;
        }
    }

    public EfCoreBatchSourceOptions<SourceTestDbContext, SourceTestRow, long> CreateSourceOptions() => new()
    {
        Context = EntityFrameworkCoreContextOptions<SourceTestDbContext>.FromDbContextFactory(Factory),
        QueryFactory = context => context.Rows,
        ApplyStableOrdering = query => query.OrderBy(row => row.Id),
        ResumePredicateFactory = checkpoint => row => row.Id > checkpoint,
        CheckpointSelector = row => row.Id,
        CheckpointComparer = Comparer<long>.Default,
    };

    public ValueTask DisposeAsync() => _anchor.DisposeAsync();
}

internal sealed class RecordingSourceContextFactory(DbContextOptions<SourceTestDbContext> options) : IDbContextFactory<SourceTestDbContext>
{
    public ConcurrentQueue<SourceTestDbContext> Contexts { get; } = new();

    public ConcurrentQueue<CancellationToken> Tokens { get; } = new();

    public Action<SourceTestDbContext>? ConfigureContext { get; set; }

    public Func<CancellationToken, Task>? BeforeCreateAsync { get; set; }

    public SourceTestDbContext CreateDbContext()
    {
        var context = new SourceTestDbContext(options);
        ConfigureContext?.Invoke(context);
        Contexts.Enqueue(context);
        return context;
    }

    public async Task<SourceTestDbContext> CreateDbContextAsync(CancellationToken cancellationToken = default)
    {
        Tokens.Enqueue(cancellationToken);

        if (BeforeCreateAsync is { } beforeCreate)
        {
            await beforeCreate(cancellationToken);
        }

        return CreateDbContext();
    }
}

internal sealed class SourceTestDbContext(DbContextOptions<SourceTestDbContext> options) : DbContext(options)
{
    public DbSet<SourceTestRow> Rows => Set<SourceTestRow>();

    public bool IsDisposed { get; private set; }

    public int DisposeCount { get; private set; }

    public int TrackedAtDispose { get; private set; }

    public Exception? DisposeFailure { get; set; }

    public Func<Task>? BeforeDisposeAsync { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SourceTestRow>().Property(row => row.Id).ValueGeneratedNever();
        modelBuilder.Entity<SourceTestRow>().HasIndex(row => new { row.Bucket, row.Id });
    }

    public override async ValueTask DisposeAsync()
    {
        DisposeCount++;
        TrackedAtDispose = ChangeTracker.Entries().Count();

        if (BeforeDisposeAsync is { } beforeDispose)
        {
            await beforeDispose();
        }

        await base.DisposeAsync();
        IsDisposed = true;

        if (DisposeFailure is { } failure)
        {
            throw failure;
        }
    }
}

internal sealed class SourceTestRow
{
    public long Id { get; set; }

    public int Bucket { get; set; }

    public string? Label { get; set; }

    public bool Enabled { get; set; } = true;
}

internal sealed class SourceCommandRecorder : DbCommandInterceptor
{
    public ConcurrentQueue<(string Sql, object?[] Parameters)> Executed { get; } = new();

    public Func<CancellationToken, Task>? BeforeExecuteAsync { get; set; }

    public override async ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<DbDataReader> result,
        CancellationToken cancellationToken = default)
    {
        Executed.Enqueue((command.CommandText, command.Parameters.Cast<DbParameter>().Select(parameter => parameter.Value).ToArray()));

        if (BeforeExecuteAsync is { } beforeExecute)
        {
            await beforeExecute(cancellationToken);
        }

        return result;
    }
}

internal static class EfCoreSourceTestData
{
    public static BatchSourceContext<TCheckpoint> Context<TCheckpoint>(
        int batchSize = 2,
        StoredCheckpoint<TCheckpoint>? resume = null) => new()
    {
        Job = new JobExecutionContext { JobName = "ef-keyset-source", RunId = "test-run" },
        RequestedBatchSize = batchSize,
        ResumeFrom = resume,
    };

    public static async Task<List<SourceBatch<TItem, TCheckpoint>>> ReadAllAsync<TItem, TCheckpoint>(
        IBatchSource<TItem, TCheckpoint> source,
        int batchSize = 2,
        StoredCheckpoint<TCheckpoint>? resume = null,
        CancellationToken cancellationToken = default)
    {
        var result = new List<SourceBatch<TItem, TCheckpoint>>();

        await foreach (var batch in source.ReadAsync(Context(batchSize, resume), cancellationToken))
        {
            result.Add(batch);
        }

        return result;
    }
}
