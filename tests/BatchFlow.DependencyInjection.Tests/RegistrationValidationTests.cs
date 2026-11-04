using BatchFlow.EntityFrameworkCore.DependencyInjection;
using BatchFlow.EntityFrameworkCore.Options;
using BatchFlow.EntityFrameworkCore.Sources;
using BatchFlow.EntityFrameworkCore.Tests.TestDoubles;
using BatchFlow.Postgres.DependencyInjection;
using BatchFlow.Postgres.Options;
using BatchFlow.Postgres.Sources;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

namespace BatchFlow.DependencyInjection.Tests;

public sealed class RegistrationValidationTests
{
    private const string ConnectionString = "Host=127.0.0.1;Database=batchflow_tests";

    [Fact]
    public async Task Adapter_extensions_should_validate_null_collection_before_invoking_factories()
    {
        await using var dataSource = NpgsqlDataSource.Create(ConnectionString);
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Database:ConnectionString"] = ConnectionString,
            ["Checkpoint:TableName"] = "progress",
        }).Build();
        IServiceCollection missing = null!;
        Action[] actions =
        [
            () => missing.AddBatchFlowPostgresDataSource(ConnectionString),
            () => missing.AddBatchFlowPostgresDataSource(dataSource),
            () => missing.AddBatchFlowPostgresDataSource(config.GetSection("Database")),
            () => missing.AddBatchFlowPostgresSource<int, long>(_ => throw new InvalidOperationException()),
            () => missing.AddBatchFlowPostgresSource<int, long>((_, _) => throw new InvalidOperationException()),
            () => missing.AddBatchFlowPostgresCheckpoint<long>(_ => throw new InvalidOperationException()),
            () => missing.AddBatchFlowPostgresCheckpoint<long>(config.GetSection("Checkpoint")),
            () => missing.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>((_, _) => throw new InvalidOperationException()),
        ];

        foreach (var action in actions)
        {
            action.Should().Throw<ArgumentNullException>().WithParameterName("services");
        }
    }

    [Fact]
    public void Adapter_extensions_should_reject_null_configuration_and_missing_sections_without_adding_services()
    {
        var services = new ServiceCollection();
        var empty = new ConfigurationBuilder().Build();
        Action[] nulls =
        [
            () => services.AddBatchFlowPostgresDataSource((string)null!),
            () => services.AddBatchFlowPostgresDataSource((NpgsqlDataSource)null!),
            () => services.AddBatchFlowPostgresDataSource((IConfigurationSection)null!),
            () => services.AddBatchFlowPostgresSource<int, long>((Func<IServiceProvider, PostgresBatchSourceOptions<int, long>>)null!),
            () => services.AddBatchFlowPostgresSource<int, long>((Func<IServiceProvider, PostgresConnectionOptions, PostgresBatchSourceOptions<int, long>>)null!),
            () => services.AddBatchFlowPostgresCheckpoint<long>((Func<IServiceProvider, PostgresCheckpointStoreOptions>)null!),
            () => services.AddBatchFlowPostgresCheckpoint<long>((IConfigurationSection)null!),
            () => services.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>(null!),
            () => services.AddBatchFlowPostgresDataSource(empty.GetSection("Database")),
            () => services.AddBatchFlowPostgresCheckpoint<long>(empty.GetSection("Checkpoint")),
        ];

        foreach (var action in nulls)
        {
            action.Should().Throw<ArgumentException>();
            services.Should().BeEmpty();
        }
    }

    [Fact]
    public void Blank_adapter_names_should_fail_without_invoking_callbacks_or_adding_services()
    {
        var services = new ServiceCollection();
        Action[] actions =
        [
            () => services.AddBatchFlowPostgresDataSource(ConnectionString, name: " "),
            () => services.AddBatchFlowPostgresSource<int, long>(_ => throw new InvalidOperationException(), name: " "),
            () => services.AddBatchFlowPostgresSource<int, long>((_, _) => throw new InvalidOperationException(), dataSourceName: " "),
            () => services.AddBatchFlowPostgresCheckpoint<long>(_ => throw new InvalidOperationException(), name: " "),
        ];

        foreach (var action in actions)
        {
            action.Should().Throw<ArgumentException>();
            services.Should().BeEmpty();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Duplicate_guard_should_also_preserve_registrations_made_by_application(bool named)
    {
        var services = new ServiceCollection();
        if (named)
        {
            services.AddKeyedScoped<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>("source", (_, _) => throw new InvalidOperationException());
        }
        else
        {
            services.AddScoped<EfCoreBatchSource<SourceTestDbContext, SourceTestRow, long>>(_ => throw new InvalidOperationException());
        }

        var original = services.Single();
        Action duplicate = () => services.AddBatchFlowEfCoreSource<SourceTestDbContext, SourceTestRow, long>(
            (_, _) => throw new InvalidOperationException(), name: named ? "source" : null);
        duplicate.Should().Throw<ArgumentException>();
        services.Should().ContainSingle().Which.Should().BeSameAs(original);
    }
}
