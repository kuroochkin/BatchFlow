using BatchFlow.Extensions.DependencyInjection.Internal;
using BatchFlow.Postgres.Checkpoints;
using BatchFlow.Postgres.Options;
using BatchFlow.Postgres.Sources;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace BatchFlow.Postgres.DependencyInjection;

/// <summary>
/// Добавляет явные DI-регистрации PostgreSQL-пулов, источников и checkpoint store.
/// </summary>
public static class BatchFlowPostgresServiceCollectionExtensions
{
    /// <summary>
    /// Регистрирует singleton NpgsqlDataSource, который создается при первом разрешении и освобождается контейнером.
    /// Подключение к БД при регистрации и разрешении сервиса не открывается.
    /// </summary>
    /// <param name="services">Коллекция сервисов.</param>
    /// <param name="connectionString">Строка подключения; не выводится библиотекой в лог.</param>
    /// <param name="configure">Необязательная настройка пула до Build.</param>
    /// <param name="name">Необязательное имя keyed-сервиса.</param>
    /// <returns>Ту же коллекцию сервисов.</returns>
    public static IServiceCollection AddBatchFlowPostgresDataSource(this IServiceCollection services,
        string connectionString, Action<NpgsqlDataSourceBuilder>? configure = null, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        _ = new NpgsqlConnectionStringBuilder(connectionString);
        BatchFlowServiceRegistration.AddSingleton(services, _ =>
        {
            var builder = new NpgsqlDataSourceBuilder(connectionString);
            configure?.Invoke(builder);
            return builder.Build();
        }, name);
        return services;
    }

    /// <summary>
    /// Регистрирует уже созданный пул без передачи владения: контейнер не освобождает переданный экземпляр.
    /// Повторная регистрация того же типа и имени отклоняется без изменения существующего сервиса.
    /// </summary>
    /// <param name="services">Коллекция сервисов.</param>
    /// <param name="dataSource">Пул, которым продолжает владеть приложение.</param>
    /// <param name="name">Необязательное имя keyed-сервиса.</param>
    /// <returns>Ту же коллекцию сервисов.</returns>
    public static IServiceCollection AddBatchFlowPostgresDataSource(this IServiceCollection services, NpgsqlDataSource dataSource, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(dataSource);
        BatchFlowServiceRegistration.AddInstance(services, dataSource, name);
        return services;
    }

    /// <summary>
    /// Считывает ConnectionString из секции один раз и регистрирует пул. Изменения конфигурации после регистрации
    /// не меняют работающий пул; автоматического hot reload нет. Остальные параметры задаются через configure.
    /// </summary>
    /// <param name="services">Коллекция сервисов.</param>
    /// <param name="configuration">Секция с ключом ConnectionString.</param>
    /// <param name="configure">Настройка пула.</param>
    /// <param name="name">Необязательное имя keyed-сервиса.</param>
    /// <returns>Ту же коллекцию сервисов.</returns>
    public static IServiceCollection AddBatchFlowPostgresDataSource(this IServiceCollection services,
        IConfigurationSection configuration, Action<NpgsqlDataSourceBuilder>? configure = null, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        var connectionString = configuration["ConnectionString"];
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return services.AddBatchFlowPostgresDataSource(connectionString, configure, name);
    }

    /// <summary>
    /// Регистрирует scoped-источник из полной конфигурации. Для повторных запусков допустимы строка подключения,
    /// пул или фабрика новых соединений; готовое соединение в DI-регистрации отклоняется.
    /// </summary>
    /// <typeparam name="TItem">Тип элемента.</typeparam>
    /// <typeparam name="TCheckpoint">Тип позиции.</typeparam>
    /// <param name="services">Коллекция сервисов.</param>
    /// <param name="optionsFactory">Конфигурация с зависимостями текущего scope.</param>
    /// <param name="name">Необязательное имя keyed-сервиса.</param>
    /// <returns>Ту же коллекцию сервисов.</returns>
    public static IServiceCollection AddBatchFlowPostgresSource<TItem, TCheckpoint>(this IServiceCollection services,
        Func<IServiceProvider, PostgresBatchSourceOptions<TItem, TCheckpoint>> optionsFactory, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(optionsFactory);
        BatchFlowServiceRegistration.AddScoped(services, provider =>
        {
            var options = optionsFactory(provider) ?? throw new InvalidOperationException("Фабрика настроек PostgreSQL-source вернула null.");
            ValidateConnection(options.Connection);
            return new PostgresBatchSource<TItem, TCheckpoint>(options);
        }, name);
        return services;
    }

    /// <summary>
    /// Регистрирует scoped-источник поверх явно выбранного NpgsqlDataSource из DI.
    /// configure должен использовать переданный объект Connection, а не подменять подключение.
    /// </summary>
    /// <typeparam name="TItem">Тип элемента.</typeparam>
    /// <typeparam name="TCheckpoint">Тип позиции.</typeparam>
    /// <param name="services">Коллекция сервисов.</param>
    /// <param name="configure">Настройка SQL и материализации с зависимостями текущего scope.</param>
    /// <param name="dataSourceName">Имя пула; null выбирает обычный NpgsqlDataSource.</param>
    /// <param name="name">Необязательное имя source.</param>
    /// <returns>Ту же коллекцию сервисов.</returns>
    public static IServiceCollection AddBatchFlowPostgresSource<TItem, TCheckpoint>(this IServiceCollection services,
        Func<IServiceProvider, PostgresConnectionOptions, PostgresBatchSourceOptions<TItem, TCheckpoint>> configure,
        string? dataSourceName = null, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        BatchFlowServiceRegistration.ValidateName(dataSourceName);
        return services.AddBatchFlowPostgresSource(provider =>
        {
            var connection = PostgresConnectionOptions.FromDataSource(BatchFlowServiceRegistration.Resolve<NpgsqlDataSource>(provider, dataSourceName));
            var options = configure(provider, connection) ?? throw new InvalidOperationException("Настройка PostgreSQL-source вернула null.");

            if (!ReferenceEquals(options.Connection, connection))
            {
                throw new ArgumentException("Настройка source должна использовать переданный Connection, а не подменять пул.", nameof(configure));
            }

            return options;
        }, name);
    }

    /// <summary>
    /// Регистрирует конкретный checkpoint store как scoped-сервис. Не выполняет DDL и не регистрирует alias ICheckpointStore.
    /// </summary>
    /// <typeparam name="TCheckpoint">Тип позиции.</typeparam>
    /// <param name="services">Коллекция сервисов.</param>
    /// <param name="optionsFactory">Настройки подключения и таблицы с зависимостями текущего scope.</param>
    /// <param name="name">Необязательное имя keyed-сервиса.</param>
    /// <param name="serializerFactory">
    /// Получение сериализатора без передачи владения store; освобождаемые реализации регистрируйте отдельно в DI.
    /// null оставляет JSON-сериализацию адаптера.
    /// </param>
    /// <returns>Ту же коллекцию сервисов.</returns>
    public static IServiceCollection AddBatchFlowPostgresCheckpoint<TCheckpoint>(this IServiceCollection services,
        Func<IServiceProvider, PostgresCheckpointStoreOptions> optionsFactory, string? name = null,
        Func<IServiceProvider, IPostgresCheckpointSerializer<TCheckpoint>>? serializerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(optionsFactory);
        BatchFlowServiceRegistration.AddScoped(services, provider =>
        {
            var options = optionsFactory(provider) ?? throw new InvalidOperationException("Фабрика настроек checkpoint store вернула null.");
            ValidateConnection(options.Connection);
            var serializer = serializerFactory is null ? null : serializerFactory(provider)
                ?? throw new InvalidOperationException("Фабрика сериализатора вернула null.");
            return new PostgresCheckpointStore<TCheckpoint>(options, serializer);
        }, name);
        return services;
    }

    /// <summary>
    /// Создает snapshot настроек схемы, таблицы и таймаута из конфигурации и использует выбранный пул DI.
    /// Неизвестные ключи отклоняются. Создание таблицы остается явным действием приложения.
    /// </summary>
    /// <typeparam name="TCheckpoint">Тип позиции.</typeparam>
    /// <param name="services">Коллекция сервисов.</param>
    /// <param name="configuration">Секция с SchemaName, TableName и CommandTimeoutSeconds.</param>
    /// <param name="dataSourceName">Имя пула; null выбирает обычную регистрацию.</param>
    /// <param name="name">Необязательное имя store.</param>
    /// <param name="serializerFactory">Получение сериализатора без передачи владения; его время жизни задает приложение.</param>
    /// <returns>Ту же коллекцию сервисов.</returns>
    public static IServiceCollection AddBatchFlowPostgresCheckpoint<TCheckpoint>(this IServiceCollection services,
        IConfigurationSection configuration, string? dataSourceName = null, string? name = null,
        Func<IServiceProvider, IPostgresCheckpointSerializer<TCheckpoint>>? serializerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        BatchFlowServiceRegistration.ValidateName(dataSourceName);
        var settings = configuration.Get<PostgresCheckpointSettings>(binder => binder.ErrorOnUnknownConfiguration = true)
            ?? throw new ArgumentException("Секция настроек checkpoint store отсутствует или пуста.", nameof(configuration));
        settings.Validate();
        return services.AddBatchFlowPostgresCheckpoint(provider => new PostgresCheckpointStoreOptions
        {
            Connection = PostgresConnectionOptions.FromDataSource(BatchFlowServiceRegistration.Resolve<NpgsqlDataSource>(provider, dataSourceName)),
            SchemaName = settings.SchemaName,
            TableName = settings.TableName,
            CommandTimeoutSeconds = settings.CommandTimeoutSeconds,
        }, name, serializerFactory);
    }

    private static void ValidateConnection(PostgresConnectionOptions connection)
    {
        ArgumentNullException.ThrowIfNull(connection);

        if (connection.Mode == PostgresConnectionMode.ExistingConnection)
        {
            throw new ArgumentException("DI-регистрация требует пул, строку подключения или фабрику новых соединений. Готовые соединения поддерживаются только при явном ручном владении.", nameof(connection));
        }
    }
}
