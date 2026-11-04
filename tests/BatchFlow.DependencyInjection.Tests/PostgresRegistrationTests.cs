using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Postgres.Checkpoints;
using BatchFlow.Postgres.DependencyInjection;
using BatchFlow.Postgres.Options;
using BatchFlow.Postgres.Sources;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace BatchFlow.DependencyInjection.Tests;

public sealed class PostgresRegistrationTests
{
    private const string ConnectionString = "Host=127.0.0.1;Port=1;Database=batchflow_tests;Username=batchflow;Timeout=1";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Container_should_dispose_only_owned_data_source(bool named)
    {
        var services = new ServiceCollection();
        var configured = 0;
        var name = named ? "pool" : null;
        services.AddBatchFlowPostgresDataSource(ConnectionString, _ => configured++, name);
        configured.Should().Be(0);
        var provider = services.BuildServiceProvider();
        var source = Resolve<NpgsqlDataSource>(provider, name);
        Resolve<NpgsqlDataSource>(provider, name).Should().BeSameAs(source);
        configured.Should().Be(1);
        await provider.DisposeAsync();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        Func<Task> disposed = async () => { await using var connection = await source.OpenConnectionAsync(cancellation.Token); };
        await disposed.Should().ThrowAsync<ObjectDisposedException>();

        await using var external = NpgsqlDataSource.Create(ConnectionString);
        var borrowedServices = new ServiceCollection().AddBatchFlowPostgresDataSource(external, name);
        await using (var borrowed = borrowedServices.BuildServiceProvider())
        {
            Resolve<NpgsqlDataSource>(borrowed, name).Should().BeSameAs(external);
        }

        Func<Task> alive = async () => { await using var connection = await external.OpenConnectionAsync(cancellation.Token); };
        await alive.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Registration_should_bind_snapshots_and_construct_source_and_store_without_database_io()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Database:ConnectionString"] = ConnectionString,
            ["Checkpoint:SchemaName"] = "first_schema",
            ["Checkpoint:TableName"] = "progress",
            ["Checkpoint:CommandTimeoutSeconds"] = "17",
        }).Build();
        var services = new ServiceCollection();
        services.AddBatchFlowPostgresDataSource(config.GetSection("Database"));
        services.AddBatchFlowPostgresCheckpoint<long>(config.GetSection("Checkpoint"));
        services.AddBatchFlowPostgresSource<int, long>((_, connection) => SourceOptions(connection));
        config["Database:ConnectionString"] = "Host=changed;Database=other_tests";
        config["Checkpoint:SchemaName"] = "changed_schema";
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
        await using var scope = provider.CreateAsyncScope();
        var dataSource = provider.GetRequiredService<NpgsqlDataSource>();
        var store = scope.ServiceProvider.GetRequiredService<PostgresCheckpointStore<long>>();
        var source = scope.ServiceProvider.GetRequiredService<PostgresBatchSource<int, long>>();

        new NpgsqlConnectionStringBuilder(dataSource.ConnectionString).Database.Should().Be("batchflow_tests");
        store.Options.SchemaName.Should().Be("first_schema");
        store.Options.TableName.Should().Be("progress");
        store.Options.CommandTimeoutSeconds.Should().Be(17);
        store.Options.Connection.DataSource.Should().BeSameAs(dataSource);
        scope.ServiceProvider.GetRequiredService<PostgresBatchSource<int, long>>().Should().BeSameAs(source);
        scope.ServiceProvider.GetService<IBatchSource<int, long>>().Should().BeNull();
        scope.ServiceProvider.GetService<ICheckpointStore<long>>().Should().BeNull();
    }

    [Fact]
    public async Task Sources_and_stores_should_be_scoped_and_support_multiple_explicit_pools()
    {
        var services = new ServiceCollection();
        services.AddBatchFlowPostgresDataSource(ConnectionString, name: "one");
        services.AddBatchFlowPostgresDataSource(ConnectionString, name: "two");
        services.AddBatchFlowPostgresSource<int, long>((_, connection) => SourceOptions(connection), "one", "source-one");
        services.AddBatchFlowPostgresSource<int, long>((_, connection) => SourceOptions(connection), "two", "source-two");
        services.AddBatchFlowPostgresCheckpoint<long>(provider => new PostgresCheckpointStoreOptions
        {
            Connection = PostgresConnectionOptions.FromDataSource(provider.GetRequiredKeyedService<NpgsqlDataSource>("one")),
        });
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        Action root = () => provider.GetRequiredService<PostgresCheckpointStore<long>>();
        root.Should().Throw<InvalidOperationException>();
        await using var first = provider.CreateAsyncScope();
        await using var second = provider.CreateAsyncScope();
        first.ServiceProvider.GetRequiredKeyedService<PostgresBatchSource<int, long>>("source-one")
            .Should().NotBeSameAs(second.ServiceProvider.GetRequiredKeyedService<PostgresBatchSource<int, long>>("source-one"));
        first.ServiceProvider.GetRequiredService<PostgresCheckpointStore<long>>()
            .Should().NotBeSameAs(second.ServiceProvider.GetRequiredService<PostgresCheckpointStore<long>>());
        first.ServiceProvider.GetRequiredKeyedService<PostgresBatchSource<int, long>>("source-two").Should().NotBeNull();
        first.ServiceProvider.GetService<PostgresBatchSource<int, long>>().Should().BeNull();
    }

    [Fact]
    public async Task Data_source_callback_should_not_replace_explicit_connection()
    {
        var services = new ServiceCollection().AddBatchFlowPostgresDataSource(ConnectionString);
        services.AddBatchFlowPostgresSource<int, long>((_, _) => SourceOptions(PostgresConnectionOptions.FromConnectionString(ConnectionString)));
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        Action action = () => scope.ServiceProvider.GetRequiredService<PostgresBatchSource<int, long>>();
        action.Should().Throw<ArgumentException>().WithMessage("*подменять пул*");
    }

    [Fact]
    public async Task Direct_registration_should_allow_fresh_connection_factory_without_invoking_it_on_resolution()
    {
        var calls = 0;
        var services = new ServiceCollection();
        services.AddBatchFlowPostgresSource<int, long>(_ => SourceOptions(PostgresConnectionOptions.FromConnectionFactory(() =>
        {
            calls++;
            return new NpgsqlConnection(ConnectionString);
        })));
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        scope.ServiceProvider.GetRequiredService<PostgresBatchSource<int, long>>().Should().NotBeNull();
        calls.Should().Be(0);
    }

    [Theory]
    [InlineData(PostgresConnectionOwnership.External)]
    [InlineData(PostgresConnectionOwnership.AdapterOwned)]
    public async Task Scoped_registrations_should_reject_single_existing_connection(PostgresConnectionOwnership ownership)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        var services = new ServiceCollection();
        services.AddBatchFlowPostgresSource<int, long>(_ => SourceOptions(PostgresConnectionOptions.FromConnection(connection, ownership)));
        services.AddBatchFlowPostgresCheckpoint<long>(_ => new PostgresCheckpointStoreOptions
        {
            Connection = PostgresConnectionOptions.FromConnection(connection, ownership),
        });
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        Action source = () => scope.ServiceProvider.GetRequiredService<PostgresBatchSource<int, long>>();
        Action store = () => scope.ServiceProvider.GetRequiredService<PostgresCheckpointStore<long>>();
        source.Should().Throw<ArgumentException>().WithMessage("*Готовые соединения*");
        store.Should().Throw<ArgumentException>().WithMessage("*Готовые соединения*");
    }

    [Fact]
    public void Duplicate_registrations_should_fail_without_mutating_existing_descriptors()
    {
        var services = new ServiceCollection().AddBatchFlowPostgresDataSource(ConnectionString);
        services.AddBatchFlowPostgresSource<int, long>(_ => SourceOptions(PostgresConnectionOptions.FromConnectionString(ConnectionString)));
        services.AddBatchFlowPostgresCheckpoint<long>(_ => new PostgresCheckpointStoreOptions { Connection = PostgresConnectionOptions.FromConnectionString(ConnectionString) });
        var count = services.Count;
        Action dataSource = () => services.AddBatchFlowPostgresDataSource(ConnectionString);
        Action source = () => services.AddBatchFlowPostgresSource<int, long>(_ => throw new InvalidOperationException());
        Action store = () => services.AddBatchFlowPostgresCheckpoint<long>(_ => throw new InvalidOperationException());
        dataSource.Should().Throw<ArgumentException>();
        source.Should().Throw<ArgumentException>();
        store.Should().Throw<ArgumentException>();
        services.Should().HaveCount(count);
    }

    [Theory]
    [InlineData("SchemaName", " ")]
    [InlineData("TableName", "")]
    [InlineData("CommandTimeoutSeconds", "0")]
    [InlineData("CommandTimeoutSeconds", "invalid")]
    [InlineData("Unexpected", "value")]
    public void Checkpoint_binding_should_reject_invalid_values_before_registering_service(string key, string value)
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["Checkpoint:" + key] = value }).Build();
        var services = new ServiceCollection();
        Action action = () => services.AddBatchFlowPostgresCheckpoint<long>(config.GetSection("Checkpoint"));
        if (key == "Unexpected" || value == "invalid")
        {
            action.Should().Throw<InvalidOperationException>();
        }
        else
        {
            action.Should().Throw<ArgumentException>();
        }
        services.Should().BeEmpty();
    }

    [Fact]
    public async Task Separately_registered_serializer_should_be_owned_and_disposed_by_its_scope()
    {
        var services = new ServiceCollection();
        services.AddScoped<DisposableSerializer>();
        services.AddBatchFlowPostgresCheckpoint<long>(_ => new PostgresCheckpointStoreOptions
        {
            Connection = PostgresConnectionOptions.FromConnectionString(ConnectionString),
        }, serializerFactory: provider => provider.GetRequiredService<DisposableSerializer>());
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        DisposableSerializer serializer;
        await using (var scope = provider.CreateAsyncScope())
        {
            _ = scope.ServiceProvider.GetRequiredService<PostgresCheckpointStore<long>>();
            serializer = scope.ServiceProvider.GetRequiredService<DisposableSerializer>();
            serializer.DisposeCount.Should().Be(0);
        }

        serializer.DisposeCount.Should().Be(1);
    }

    [Theory]
    [InlineData("source")]
    [InlineData("store")]
    [InlineData("serializer")]
    public async Task Registration_should_reject_null_results_from_factories(string result)
    {
        var services = new ServiceCollection();
        services.AddBatchFlowPostgresSource<int, long>((Func<IServiceProvider, PostgresBatchSourceOptions<int, long>>)(_ => null!));
        services.AddBatchFlowPostgresCheckpoint<long>(_ => result == "store" ? null! : new PostgresCheckpointStoreOptions
        {
            Connection = PostgresConnectionOptions.FromConnectionString(ConnectionString),
        }, serializerFactory: _ => null!);
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        Action action = result == "source"
            ? () => scope.ServiceProvider.GetRequiredService<PostgresBatchSource<int, long>>()
            : () => scope.ServiceProvider.GetRequiredService<PostgresCheckpointStore<long>>();
        action.Should().Throw<InvalidOperationException>().WithMessage("*null*");
    }

    private static T Resolve<T>(IServiceProvider provider, string? name) where T : notnull =>
        name is null ? provider.GetRequiredService<T>() : provider.GetRequiredKeyedService<T>(name);

    private sealed class DisposableSerializer : IPostgresCheckpointSerializer<long>, IDisposable
    {
        public int DisposeCount { get; private set; }
        public string Serialize(long checkpoint) => throw new NotSupportedException();
        public long Deserialize(string payload) => throw new NotSupportedException();
        public void Dispose() => DisposeCount++;
    }

    private static PostgresBatchSourceOptions<int, long> SourceOptions(PostgresConnectionOptions connection) => new()
    {
        Connection = connection,
        CommandText = "select @batch_size, @resume_checkpoint where false",
        ResumeCheckpointParameterType = NpgsqlDbType.Bigint,
        RowMaterializer = (_, _) => ValueTask.FromResult(new PostgresBatchSourceRow<int, long>(0, 0L)),
    };
}
