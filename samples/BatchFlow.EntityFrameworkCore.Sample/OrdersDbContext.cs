using Microsoft.EntityFrameworkCore;

internal sealed class OrdersDbContext(DbContextOptions<OrdersDbContext> options) : DbContext(options)
{
    public DbSet<OrderRow> Orders => Set<OrderRow>();

    public DbSet<ExportRow> Exports => Set<ExportRow>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ExportRow>().HasKey(row => new { row.Mode, row.Id });
    }
}

internal sealed class OrderRow
{
    public long Id { get; set; }

    public required string Number { get; set; }

    public bool ExportReady { get; set; } = true;
}

internal sealed class ExportRow
{
    public required string Mode { get; set; }

    public long Id { get; set; }

    public required string Number { get; set; }
}

internal sealed record OrderDto
{
    public required long Id { get; init; }

    public required string Number { get; init; }
}
