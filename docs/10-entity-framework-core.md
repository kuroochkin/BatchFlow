# Контексты и точки расширения EF Core

[Оглавление](README.md)

Для готового `EfCoreBatchSource` без наследования и с коротким контекстом на порцию начните с [руководства по keyset-чтению](11-ef-core-source.md). Этот раздел описывает общий слой подключения и `EntityFrameworkCoreBatchSourceBase` для собственных реализаций.

## Задача и ценность

Пакет `BatchFlow.EntityFrameworkCore` позволяет приложению использовать собственную EF-модель и способ настройки подключения, не связывая `Core`, `Abstractions` или PostgreSQL-адаптер с ORM. Это альтернативный путь чтения, а не обертка над `BatchFlow.Postgres`.

Общий слой пакета определяет получение и владение `DbContext` и предоставляет базовый класс для собственных источников. Готовое чтение по LINQ/keyset реализовано отдельно в `EfCoreBatchSource`. EF-хранилища checkpoint пока нет.

## Принятые решения

| Вопрос | Решение |
| --- | --- |
| Зависимости | `Abstractions`, `Core`, фасад `BatchFlow` и `Microsoft.EntityFrameworkCore` 9.0.15 |
| Платформа | Существующая `net9.0`; обновление SDK не входит в задачу |
| Провайдер и подключение | Настраивает приложение; пакет не ссылается на Npgsql, SqlClient или EF Relational |
| Основной способ получения контекста | `IDbContextFactory<TContext>`, включая фабрики с пулом |
| Пользовательские фабрики | `Func<TContext>` и `Func<CancellationToken, ValueTask<TContext>>` |
| Готовый контекст | Только внешнее владение в базовом классе; готовый `EfCoreBatchSource` требует фабрику |
| Время жизни | У базового класса - на перечисление; у `EfCoreBatchSource` - на одну порцию |
| Поддержка восстановления | У наследника базового класса - явный `SupportsResume`; у готового source - обязательная keyset-конфигурация |
| Контрольные точки | Любой существующий `ICheckpointStore<TCheckpoint>` через обычный `WithCheckpoint` |
| Оркестрация | Только существующий execution engine, без повторной реализации retry и coordinator |

EF Core может транзитивно подключать logging и DI-зависимости. Они остаются в графе EF-пакета и не становятся зависимостями `Core` или `Abstractions`. Адаптер сам не регистрирует службы и не использует service locator.

## Способы подключения

Добавьте ссылку на `src/BatchFlow.EntityFrameworkCore/BatchFlow.EntityFrameworkCore.csproj`. Ниже `OrdersDbContext` - контекст приложения, а `dbContextFactory` - уже настроенный `IDbContextFactory<OrdersDbContext>`:

```csharp
using BatchFlow.EntityFrameworkCore.Options;

var fromEfFactory = EntityFrameworkCoreContextOptions<OrdersDbContext>
    .FromDbContextFactory(dbContextFactory);

var fromDelegate = EntityFrameworkCoreContextOptions<OrdersDbContext>
    .FromFactory(() => new OrdersDbContext(dbContextOptions));

var fromAsyncDelegate = EntityFrameworkCoreContextOptions<OrdersDbContext>
    .FromFactory(async token => await dbContextFactory.CreateDbContextAsync(token));

await using var dbContext = new OrdersDbContext(dbContextOptions);
var fromExisting = EntityFrameworkCoreContextOptions<OrdersDbContext>
    .FromContext(dbContext);
```

Это альтернативные настройки, а не четыре обязательных шага. Последний вариант с готовым контекстом поддерживает только базовый класс собственных источников, не `EfCoreBatchSource`. `dbContextOptions` содержит выбранный EF-провайдер и строку подключения. Передавать в универсальный EF-пакет голую строку подключения недостаточно: без провайдера и способа создания пользовательского контекста она не определяет доступ к БД.

## Подключение источника

Когда приложение реализовало `OrdersSource : EntityFrameworkCoreBatchSourceBase<OrdersDbContext, OrderDto, long>`, настроило строгое возрастание уникального `Id` и учет `ResumeFrom`, оно использует обычный runtime:

```csharp
using BatchFlow;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Checkpoints;
using BatchFlow.EntityFrameworkCore.Extensions;

var source = new OrdersSource(fromEfFactory);
var checkpointStore = new InMemoryCheckpointStore<long>();

var summary = await BatchJob
    .Named("orders-export")
    .FromEntityFrameworkCore(source)
    .WithBatchSize(500)
    .WithDegreeOfParallelism(4)
    .WithCheckpoint(checkpointStore)
    .RunEachAsync((order, context, token) => ExportAsync(order, token), cancellationToken);

if (summary.Status == JobCompletionStatus.Cancelled)
{
    throw new OperationCanceledException(summary.Reason, cancellationToken);
}

if (summary.Status != JobCompletionStatus.Succeeded)
{
    throw new InvalidOperationException(
        $"Задача завершилась со статусом {summary.Status}: {summary.Reason}",
        summary.Exception);
}
```

Фрагмент предполагает пользовательские `OrdersSource`, `OrderDto` и `ExportAsync`, возвращающий `ValueTask`; это не готовые типы пакета. Создание builder не обращается к БД. `WithDegreeOfParallelism(4)` не разрешает делить контекст чтения между обработчиками. Для записи каждый обработчик получает отдельный контекст из собственной фабрики. In-memory store переживает повторный запуск в процессе, но не перезапуск процесса.

Проверять результат обязательно: runtime может вернуть `Failed` без исключения из `RunEachAsync`. В этом примере полным успехом считается только `Succeeded`: `CompletedWithSkips` и `Suspended` также требуют внимания. Приложение может явно разрешить пропуски или отдельно обработать приостановку; считать их успехом автоматически не следует. Внешняя отмена сохраняет свою семантику через `OperationCanceledException`.

## Контракт наследника

`ReadCoreAsync(dbContext, context, token)` получает контекст только для текущего перечисления. Реализация должна ограничить запрос через `RequestedBatchSize`, полностью материализовать очередной пакет и освободить reader до `yield return`. Для окончания чтения завершается последовательность: пустой пакет не выдается.

Для чтения без накопления tracked-сущностей используйте `AsNoTracking` или проекцию в независимую модель. Не возвращайте `IQueryable`, ленивые коллекции, прокси с lazy loading или модели, которым для дальнейшей работы нужен живой контекст. Базовый класс намеренно не меняет `ChangeTracker`, параметры подключения и внешние транзакции.

Если `SupportsResume` равен `true`, наследник действительно применяет `context.ResumeFrom` и возвращает позицию, соответствующую пакету. Если восстановления нет, укажите `false`: подключение checkpoint store отклонит builder, а прямая передача сохраненной позиции в source приведет к ошибке до создания контекста.

## Отмена и ресурсы

Источник создает контекст только при первом чтении перечислителя. Токен метода и токен перечислителя объединяются механизмом асинхронного итератора и передаются в фабрику и `ReadCoreAsync`. Наследник передает этот токен в операции EF Core. Отмена кооперативная: базовый класс не способен прервать запрос, который игнорирует токен.

Если фабрика вернула контекст после запроса отмены, источник освобождает его до передачи управления вызывающему коду. Ошибка такого освобождения не подменяет отмену. Если фабрика сама бросила исключение до возврата контекста, очистка ее внутренних ресурсов остается ее обязанностью.

Если ошибка возникла при создании перечислителя, чтении или проверке пакета, последующие ошибки освобождения перечислителя и контекста не подменяют первоначальное исключение, включая отмену. Оба ресурса все равно получают попытку освобождения. Если первичной ошибки чтения не было, наружу передается первая ошибка очистки: сначала перечислителя, затем контекста. Это правило относится к ресурсам адаптера; исключение, скрытое собственной реализацией `ReadCoreAsync`, базовый класс восстановить не может.

Внешний контекст не освобождается ни при успехе, ни при ошибке или отмене. Защита одного экземпляра source от одновременных перечислений снимается при освобождении перечислителя. Как и для любого `IAsyncEnumerable`, вызывающий код обязан освобождать перечислитель; `await foreach` делает это автоматически.

## Проверка и критерии приемки

- Новый пакет и отдельный тестовый проект включены в solution в папки `src` и `tests`.
- Общие настройки платформы, nullable и XML-документации наследуются из `Directory.Build.props`.
- Контекст не создается при конфигурации; фабричные контексты освобождаются, внешний контекст не освобождается.
- Повторные запуски с фабрикой получают независимые контексты; конкурентное чтение через один source с внешним контекстом отклоняется.
- Проверяются отмена, ранний выход, ошибки фабрики и чтения, неправильные пакеты и размер пакета.
- Источник подключается к стандартному builder и проходит повторный запуск с checkpoint.
- `Core` и `Abstractions` не получают ссылок на EF Core; PostgreSQL-адаптер не изменяется.

Модульные тесты lifecycle используют настоящий `DbContext` без провайдера, проверяя в том числе первичную ошибку при двойном сбое очистки. Дополнительно категория `EntityFrameworkCoreIntegration` использует настоящий `PooledDbContextFactory` и SQLite в памяти: проверяет запрос, возврат контекста в пул и его повторное использование после успеха, раннего выхода, отмены и ошибки чтения. Эти тесты не требуют внешней БД и не пропускаются по переменным окружения; SQLite-провайдер подключен только к тестовому проекту.

Отдельные проверки готового `EfCoreBatchSource` описаны в [руководстве](11-ef-core-source.md). SQLite-проверки не доказывают поведение других EF-провайдеров, внешних транзакций или отмены долгого SQL-запроса: для контролируемой отмены используются управляемые ожидания. Запуск:

```sh
dotnet test tests/BatchFlow.EntityFrameworkCore.Tests/BatchFlow.EntityFrameworkCore.Tests.csproj
```

## Следующий этап

Готовый источник реализован как `EfCoreBatchSource`, а не как наследник этого базового класса. Он поддерживает обязательный стабильный порядок, составной checkpoint и контекст на порцию. Базовый класс сохраняет прежний lifetime на перечисление для совместимости с собственными реализациями.

Нужны проверки на реальном EF-провайдере: корректность фильтров после resume, индексы и план SQL, отмена запросов, возврат контекстов и соединений в пулы. Поддержку EF-хранилища checkpoint, его модель и миграции следует проектировать отдельной задачей. Адаптер не обещает согласованный снимок таблицы, общую транзакцию source/handler/checkpoint или exactly-once.

## Источники решений

- [Microsoft: жизненный цикл DbContext, фабрики и ограничения конкурентности](https://learn.microsoft.com/en-us/ef/core/dbcontext-configuration/).
- [Microsoft: контексты с пулом](https://learn.microsoft.com/en-us/ef/core/performance/advanced-performance-topics#dbcontext-pooling).
- [Microsoft: уникальная сортировка и keyset pagination](https://learn.microsoft.com/en-us/ef/core/querying/pagination).
- [NuGet: зависимость Microsoft.EntityFrameworkCore 9.0.15](https://www.nuget.org/packages/Microsoft.EntityFrameworkCore/9.0.15).
