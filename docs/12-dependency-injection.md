# Dependency Injection и готовые примеры

[Оглавление](README.md) · [EF Core source](11-ef-core-source.md) · [Примеры](../samples/README.md)

## Задача и принятые решения

DI-слой связывает существующие адаптеры с приложением, а не создает второй runtime. Он не открывает БД, не выполняет миграции, не запускает job и не вызывает `BuildServiceProvider` внутри регистрации.

Разделение на необязательные пакеты сохраняет границы зависимостей:

| Проект / пакет | Что добавляет |
| --- | --- |
| `BatchFlow.Extensions.DependencyInjection` | `AddBatchFlow`, DI-настройки запуска, singleton in-memory checkpoint |
| `BatchFlow.Postgres.DependencyInjection` | Регистрации Npgsql-пулов, scoped source и checkpoint store, привязку данных конфигурации |
| `BatchFlow.EntityFrameworkCore.DependencyInjection` | Scoped source поверх штатного `IDbContextFactory<TContext>` |

Базовые адаптеры не требуют этих пакетов. EF DI не подключает PostgreSQL, PostgreSQL DI не подключает EF. Пакеты адаптеров транзитивно подключают общий DI-пакет. В текущем репозитории используйте `ProjectReference` на нужный проект; эти инструкции не подразумевают, что пакеты уже опубликованы в NuGet.

Расширения находятся в пространствах имен соответствующих пакетов. Для общего `AddBatchFlow` нужен `using BatchFlow.Extensions.DependencyInjection;`, для PostgreSQL - `using BatchFlow.Postgres.DependencyInjection;`, для EF - `using BatchFlow.EntityFrameworkCore.DependencyInjection;`.

## Время жизни и владение

| Сервис | Регистрация | Кто освобождает ресурс |
| --- | --- | --- |
| `IBatchJobExecutionEngine` | Singleton по умолчанию | У стандартного движка нет ресурсов на уровне экземпляра; состояние отдельное на запуск |
| `TimeProvider` | Singleton по умолчанию | По правилам регистрации приложения |
| `BatchJobRunOptions` | Transient | Не владеет внедренными engine/часами |
| `InMemoryCheckpointStore<TCheckpoint>` | Singleton | Состояние общее в процессе, но не сохраняется после его завершения |
| Созданный через строку `NpgsqlDataSource` | Singleton | DI-контейнер |
| Переданный готовый `NpgsqlDataSource` | Singleton, заимствованный | Приложение, после завершения всех использующих его scope |
| `PostgresBatchSource`, `PostgresCheckpointStore`, `EfCoreBatchSource` | Scoped | Scope ограничивает зависимости; открытые контексты и соединения освобождают адаптеры |
| `IDbContextFactory<TContext>` | Как зарегистрировано приложением | По правилам EF/приложения; BatchFlow не освобождает фабрику |

Scoped source может захватывать настройки арендатора и другие зависимости текущего scope. Scope должен жить до окончания job, включая producer и handlers. Один `DbContext`/`NpgsqlConnection` нельзя разделять между параллельными обработчиками. Для записи создавайте отдельный контекст или получайте соединение из пула в обработчике.

Для `EfCoreBatchSource` фабрика создает контекст на каждую порцию; `DisposeAsync` выполняется до выдачи данных. При использовании пула это возврат контекста для повторного использования, а не уничтожение экземпляра. Пользовательское состояние pooled-контекста, например tenant ID, нельзя считать автоматически перенастроенным на каждый запуск. Эти правила согласованы с [руководством DI](https://learn.microsoft.com/en-us/dotnet/core/extensions/dependency-injection-guidelines) и [документацией EF о пулах](https://learn.microsoft.com/en-us/ef/core/performance/advanced-performance-topics#dbcontext-pooling).

## Общая регистрация

```csharp
services.AddBatchFlow();
services.AddBatchFlowInMemoryCheckpoint<long>();
```

`AddBatchFlow` идемпотентен и не заменяет ранее зарегистрированные engine, часы или `BatchJobRunOptions`. Остальные методы отклоняют повтор конкретного типа и имени: ошибка конфигурации не должна молча заменять другую job.

Статические методы `RunEachAsync`/`RunBatchAsync` по-прежнему работают без контейнера. Чтобы использовать пользовательские engine и часы из DI, внедрите `BatchJobRunOptions` и передайте их явно:

```csharp
var summary = await builder.RunEachAsync(handler, runOptions, cancellationToken);
```

Не задавайте постоянный `RunId` в общих настройках: по умолчанию runtime создает уникальный идентификатор на запуск. Если нужна настройка конкретного запуска, используйте `runOptions with { RunId = ... }`.

## PostgreSQL

Ниже `SourceOptions` - функция приложения, возвращающая `PostgresBatchSourceOptions<OrderDto, long>` с SQL, материализатором, типом параметра resume и проверкой строгого продвижения. Полная реализация находится в [SQL-примере](../samples/BatchFlow.Postgres.Sample/Program.cs).

```csharp
services.AddBatchFlow();
services.AddBatchFlowPostgresDataSource(configuration.GetSection("BatchFlow:Database"));
services.AddBatchFlowPostgresSource<OrderDto, long>((_, connection) => SourceOptions(connection));
services.AddBatchFlowPostgresCheckpoint<long>(configuration.GetSection("BatchFlow:Checkpoint"));
services.AddScoped<OrdersExport>();
```

Данные конфигурации:

```json
{
  "BatchFlow": {
    "Database": { "ConnectionString": "<из защищенного источника конфигурации>" },
    "Checkpoint": {
      "SchemaName": "batchflow",
      "TableName": "checkpoints",
      "CommandTimeoutSeconds": 30
    }
  }
}
```

Строку подключения не коммитьте в репозиторий и не выводите в логи. Ее можно передать непосредственно в `AddBatchFlowPostgresDataSource(connectionString, builder => ...)`. Для существующего пула есть перегрузка `AddBatchFlowPostgresDataSource(dataSource)` без передачи владения контейнеру.

При двухаргументном configure source необходимо сохранить переданный `Connection`, а не создать другое подключение. Вариант `AddBatchFlowPostgresSource<TItem, TCheckpoint>(provider => options)` позволяет явно выбрать строку подключения, пул или фабрику **нового** соединения. Аналогичная фабрика options есть у `AddBatchFlowPostgresCheckpoint<TCheckpoint>`. Готовое соединение в DI отклоняется: оно не подходит для повторных/конкурентных запусков. Ручной сценарий с внешним соединением остается доступен в базовом адаптере.

Checkpoint store разрешается как `PostgresCheckpointStore<TCheckpoint>`. `EnsureStorageAsync` вызывается явно при подготовке приложения, либо таблица создается миграцией. Обычное разрешение source/store не открывает соединение и не создает таблицы. Пользовательский `IPostgresCheckpointSerializer<TCheckpoint>` передается через явный `serializerFactory`; DI не выбирает произвольный сериализатор неявно. Store не владеет сериализатором: освобождаемую реализацию зарегистрируйте отдельно, например `services.AddScoped<MySerializer>()`, и передайте `serializerFactory: provider => provider.GetRequiredService<MySerializer>()`. Тогда ее освобождает scope; не создавайте скрытый disposable-ресурс внутри callback без собственного управления его временем жизни.

Привязка `IConfigurationSection` выполняется один раз **при регистрации**. Изменение конфигурации после этого не меняет pool или checkpoint-настройки. Таймаут должен быть положительным; неизвестные поля секции checkpoint отклоняются. SQL и делегаты остаются в типизированных options, а не в JSON. `IOptionsMonitor` и горячая смена подключения не поддерживаются: такая смена посреди resume требует отдельного контракта.

## EF Core и FromDbContext

Приложение регистрирует собственную фабрику стандартными средствами EF. BatchFlow не переопределяет ее, не выбирает провайдер и не выполняет миграции:

```csharp
services.AddBatchFlow();
services.AddDbContextFactory<OrdersDbContext>(options => options.UseSqlite(connectionString));
services.AddBatchFlowInMemoryCheckpoint<long>();
services.AddBatchFlowEfCoreSource<OrdersDbContext, OrderDto, long>((_, context) => new()
{
    Context = context,
    QueryFactory = db => db.Orders.Where(order => order.ExportReady)
        .Select(order => new OrderDto { Id = order.Id, Number = order.Number }),
    ApplyStableOrdering = query => query.OrderBy(order => order.Id),
    ResumePredicateFactory = checkpoint => order => order.Id > checkpoint,
    CheckpointSelector = order => order.Id,
    CheckpointComparer = Comparer<long>.Default,
});
services.AddScoped<OrdersExport>();
```

Можно заменить `AddDbContextFactory` на `AddPooledDbContextFactory` или собственную scoped/keyed фабрику. Обычный `AddDbContext` сам по себе не регистрирует нужный `IDbContextFactory`. Configure использует зависимости текущего scope и обязан сохранить переданный объект `Context`.

В `OrdersExport` внедрите конкретный `EfCoreBatchSource<OrdersDbContext, OrderDto, long>`, store и `BatchJobRunOptions`, затем используйте:

```csharp
var summary = await BatchJob.Named("orders-export-v1")
    .FromDbContext(source)
    .WithBatchSize(500)
    .WithCheckpoint(checkpoints)
    .RunEachAsync(handler, runOptions, cancellationToken);

if (summary.Status != JobCompletionStatus.Succeeded)
{
    throw new InvalidOperationException($"{summary.Status}: {summary.Reason}", summary.Exception);
}
```

Полный [EF-runner](../samples/BatchFlow.EntityFrameworkCore.Sample/EfCoreSampleRunner.cs) отдельно обрабатывает отмену, настраивает retry и показывает также `RunBatchAsync`. In-memory checkpoint в примере переживает смену scope, **не перезапуск процесса**. Для постоянного resume внедрите постоянное хранилище; отдельного EF checkpoint store сейчас нет.

Для использования без DI доступны пять способов `FromDbContext`:

```csharp
BatchJob.Named("orders").FromDbContext(source);
BatchJob.Named("orders").FromDbContext(sourceOptions);
BatchJob.Named("orders").FromDbContext(factory, ConfigureSource);
BatchJob.Named("orders").FromDbContext(contextFactoryOptions, ConfigureSource);
BatchJob.Named("orders").FromDbContext<OrdersDbContext, OrderDto, long>(
    options => options.UseSqlite(connectionString),
    options => new OrdersDbContext(options),
    ConfigureSource);
```

Здесь `ConfigureSource` принимает `EntityFrameworkCoreContextOptions<OrdersDbContext>` и возвращает `EfCoreBatchSourceOptions<OrdersDbContext, OrderDto, long>` с этим `Context`. В последнем варианте DB-options настраиваются один раз при сборке, а конструктор вызывается на каждую порцию. Явная функция создания поддерживает нестандартные конструкторы без reflection, скрытого контейнера и неявного пула. Для асинхронного создания используйте `contextFactoryOptions` с асинхронной фабрикой. Отдельно создать source можно через `factory.CreateBatchSource(ConfigureSource)`.

`FromEntityFrameworkCore` остается совместимым прежним именем. Сортировка, keyset/resume и требования к изменяющимся данным не меняются; они описаны в [руководстве source](11-ef-core-source.md). Упрощение подключения не делает offset-пагинацию безопасной для restart.

## Несколько источников и BackgroundService

Методы принимают необязательный строковый `name`: это ключ штатного .NET DI. `null` означает обычную регистрацию, пустое имя запрещено. Отдельные `dataSourceName` и `factoryName` выбирают зависимость; они не обязаны совпадать с именем source/store.

```csharp
services.AddBatchFlowPostgresDataSource(connectionString, name: "reporting");
services.AddBatchFlowPostgresSource<OrderDto, long>(
    (_, connection) => SourceOptions(connection), dataSourceName: "reporting", name: "orders");
```

Конструктор может использовать `[FromKeyedServices("orders")] PostgresBatchSource<OrderDto, long> source`. Для EF имена фабрик регистрирует приложение через `AddKeyedSingleton`/`AddKeyedScoped<IDbContextFactory<TContext>>`.

Автоматических регистраций `IBatchSource<TItem, TCheckpoint>` и `ICheckpointStore<TCheckpoint>` нет: одинаковые типы элементов/позиций не означают одинаковую БД или job. Нужный alias можно явно добавить в приложении. `name` в DI не является `CheckpointId`; checkpoint ID задается в builder и должен быть стабильным и изолированным по job/арендатору.

В ASP.NET Core source внедряется в scoped-сервис запроса. В `BackgroundService` нельзя внедрить scoped source в singleton-конструктор: внедрите `IServiceScopeFactory` и создайте scope на запуск, а не на каждый элемент. Пример тела одного запуска, где `OrdersExport.RunAsync` ожидает окончания job:

```csharp
await using var scope = scopeFactory.CreateAsyncScope();
var export = scope.ServiceProvider.GetRequiredService<OrdersExport>();
await export.RunAsync(stoppingToken);
```

Не возвращайте fire-and-forget задачу за пределы scope. Не запускайте одновременно две job с одним checkpoint ID: оптимистичная конкуренция store обнаруживает конфликт, но не заменяет распределенную блокировку. Scoped-сервисы в обработчиках также должны поддерживать выбранный DOP.

## Валидация и проверка

Null-аргументы, пустые имена, повторы регистраций и некорректные bound-настройки отклоняются при регистрации. Callback options выполняется при первом разрешении source в каждом scope, поэтому отсутствие фабрики/пула, возвращенный `null` и подмена `Context`/`Connection` обнаруживаются тогда. `ValidateOnBuild` не выполняет фабричные делегаты: для ранней проверки явно разрешите сервисы внутри startup-scope. SQL/модель/доступность БД проверяются только при реальной операции, а не при разрешении.

Критерии проверены тестами: ленивые регистрации, singleton/scoped/transient, сохранение пользовательских сервисов, keyed-изоляция, отсутствие неоднозначных aliases, владение пулом, native EF-фабрики и SQLite-запросы. Реальные PostgreSQL-тесты проверяют `RunEachAsync`/`RunBatchAsync`, новый scope для resume, keyed/unkeyed сервисы, сериализатор и работоспособность заимствованного пула после освобождения контейнера. Команды: [интеграционные тесты](../tests/BatchFlow.Postgres.Tests/Integration/README.md).

Полные демонстрационные программы лежат в [samples](../samples/README.md). Они проверяют итоговый статус и количество результатов, моделируют transient-ошибку и доказывают отсутствие повторной обработки при втором запуске. Пример не заменяет тестирование SQL/индексов, транзакций и EF-провайдера конкретного приложения.
