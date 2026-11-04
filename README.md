# BatchFlow

BatchFlow помогает запускать пакетную обработку в .NET: читать источник порциями, обрабатывать элементы или пакеты, повторять неудачные вызовы и сохранять позицию для следующего запуска.

Приложение определяет источник и бизнес-обработчик. Библиотека управляет очередью, параллелизмом и продвижением контрольной точки. PostgreSQL и логирование подключаются отдельными проектами.

## Текущее состояние

Репозиторий собирается для `net9.0`. Реализованы fluent API, два режима обработки, фиксированный retry, stop/skip, приостановка через пользовательскую политику, контрольные точки, события, `ILogger` и PostgreSQL-адаптер. [EF Core source](docs/11-ef-core-source.md) читает по LINQ/keyset, поддерживает типизированные options, стабильный составной порядок и отдельный контекст на порцию. [Необязательные DI-пакеты](docs/12-dependency-injection.md) регистрируют runtime, PostgreSQL и EF Core с явными правилами владения. `BatchFlow.Extensions.Diagnostics` пока содержит только каркас проекта.

Это развивающаяся библиотека. Известные ограничения и рекомендации перед эксплуатацией описаны в [разборе проекта](docs/08-review.md). Наличие проекта с именем пакета не подтверждает его публикацию в NuGet; инструкции ниже используют исходники из этого репозитория.

## Подключение

Для сборки репозитория нужен .NET 9 SDK, совместимый с [global.json](global.json). Из корня репозитория можно создать пример:

```sh
dotnet new console --framework net9.0 --output samples/FirstJob
dotnet add samples/FirstJob/FirstJob.csproj reference src/BatchFlow/BatchFlow.csproj
```

Для собственного приложения укажите путь к `src/BatchFlow/BatchFlow.csproj` в `ProjectReference`. Фасад подтянет `Core` и `Abstractions` транзитивно. Для PostgreSQL дополнительно подключите `BatchFlow.Postgres`, для логов - `BatchFlow.Extensions.Logging` и провайдер логирования приложения.

Для чтения через `DbContext` подключите `BatchFlow.EntityFrameworkCore`. EF-провайдер и подключение настраиваются в приложении; этот пакет не зависит от `BatchFlow.Postgres`. `.FromDbContext(...)` принимает source, типизированные options, фабрику или configure-delegate: [руководство](docs/12-dependency-injection.md). Прежнее имя `.FromEntityFrameworkCore(...)` сохраняется. Исполняемый пример без внешней БД: `dotnet run --project samples/BatchFlow.EntityFrameworkCore.Sample`.

Для ASP.NET Core и worker services доступны `BatchFlow.EntityFrameworkCore.DependencyInjection` и `BatchFlow.Postgres.DependencyInjection`: отдельные пакеты не смешивают ORM и SQL-зависимости. [Два полных примера](samples/README.md) показывают DI, checkpoint, retry, `RunEachAsync`, `RunBatchAsync` и повторный запуск без дублей. SQL-пример запускается только на тестовой БД.

## Первый запуск

Ниже полный `Program.cs`, которому не нужны внешние сервисы. Источник демонстрационный: заказы уже находятся в памяти и отсортированы по уникальному возрастающему `Id`.

```csharp
using System.Runtime.CompilerServices;
using BatchFlow;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Results;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Checkpoints;

Order[] orders =
[
    new(1, "A-001", 1250m),
    new(2, "A-002", 990m),
    new(3, "A-003", 1875m),
];

var store = new InMemoryCheckpointStore<long>();
var source = new OrderSource(orders);

var summary = await BatchJob
    .Named("orders-export")
    .From(source)
    .WithBatchSize(2)
    .WithDegreeOfParallelism(1)
    .WithPrefetchCapacity(1)
    .WithCheckpoint("orders-export-v1", store)
    .WithRetry(2, TimeSpan.FromMilliseconds(100))
    .RunEachAsync((order, context, token) =>
    {
        token.ThrowIfCancellationRequested();
        Console.WriteLine($"{order.Number}: {order.Amount}; попытка {context.Attempt}");
        return ValueTask.FromResult(ProcessingResult.Success());
    });

Console.WriteLine($"{summary.Status}: успешно {summary.SucceededItemCount}, пропущено {summary.SkippedItemCount}");

if (summary.Status is not JobCompletionStatus.Succeeded)
{
    throw new InvalidOperationException(summary.Reason, summary.Exception);
}

internal sealed record Order(long Id, string Number, decimal Amount);

internal sealed class OrderSource(IReadOnlyList<Order> orders) : IBatchSource<Order, long>
{
    public async IAsyncEnumerable<SourceBatch<Order, long>> ReadAsync(
        BatchSourceContext<long> context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(context.RequestedBatchSize);
        var lastId = context.ResumeFrom?.Value ?? 0L;

        foreach (var items in orders.Where(order => order.Id > lastId).Chunk(context.RequestedBatchSize))
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();

            yield return new SourceBatch<Order, long>
            {
                Items = items,
                Checkpoint = items[^1].Id,
            };
        }
    }
}
```

Запуск: `dotnet run --project samples/FirstJob`. Ожидаемый результат: три заказа, два пакета, статус `Succeeded` и последняя контрольная точка `3`.

`RunEachAsync` сразу выполняет задачу. Для отложенного запуска используйте `.ProcessEach(...).Build()`, затем `job.RunAsync()`. Создавать движок вручную не требуется.

## Что нужно учесть

- Проверяйте `summary.Status`: ошибки выполнения возвращаются как `Failed`, внешняя отмена после проверки конфигурации - как `Cancelled`. Ошибки конфигурации выбрасываются до запуска. Перед возвратом engine ожидает producer и всех обработчиков.
- При сбое между бизнес-операцией и сохранением checkpoint элементы могут обработаться повторно. Обработчик должен переносить повторный вызов, например использовать upsert или ключ дедупликации.
- `Skip` окончателен: пропущенный элемент учитывается при продвижении checkpoint и не возвращается автоматически при следующем запуске.
- `InMemoryCheckpointStore` хранит прогресс только в текущем экземпляре внутри процесса. Для восстановления после перезапуска приложения нужно постоянное хранилище.
- `WithDegreeOfParallelism(4)` в item-режиме означает четыре элемента внутри одного пакета, в batch-режиме - четыре пакета. Это не обещание выделенных потоков для CPU-вычислений.
- Общее окно неподтвержденных пакетов ограничено `PrefetchCapacity + DegreeOfParallelism` в batch-режиме и `PrefetchCapacity + 1` в item-режиме. Место освобождается после подтверждения; размер элементов в байтах этим не ограничивается. Подробнее в [описании надежности](docs/09-runtime-hardening.md).

## Документация

Начните с [оглавления](docs/README.md) или выберите нужный сценарий:

| Документ | Содержание |
| --- | --- |
| [Сборка и запуск](docs/01-pipeline.md) | Builder, item/batch, контексты, результаты, параллелизм |
| [Контрольные точки](docs/02-checkpoints.md) | Resume, повторная обработка, конкуренция, граница подтверждения |
| [Ошибки и отмена](docs/03-errors-and-cancellation.md) | Retry, stop, skip, suspend, `CancellationToken` |
| [PostgreSQL](docs/04-postgres.md) | Полный пример SQL, материализация, составной ключ, подключения |
| [EF Core](docs/11-ef-core-source.md) | LINQ/keyset, options, короткий DbContext и ограничения восстановления |
| [Dependency Injection](docs/12-dependency-injection.md) | Регистрация адаптеров, FromDbContext, scoped lifetime и владение ресурсами |
| [Логи и события](docs/05-observability.md) | `WithProgressLogging`, observer, уровни и ограничения |
| [Расширение и тесты](docs/06-extending-and-testing.md) | Собственный source, processor, store и тестирование |
| [Архитектура и стиль](docs/07-architecture-and-style.md) | Порядок изучения, зависимости, соглашения кода |
| [Разбор и улучшения](docs/08-review.md) | Подтвержденные проблемы и направления оптимизации |

## Проверка репозитория

```sh
dotnet restore BatchFlow.sln
dotnet build BatchFlow.sln --no-restore
dotnet test BatchFlow.sln --no-restore
dotnet run --project src/ConsoleTest/ConsoleTest.csproj
```

`ConsoleTest` показывает retry, skip, resume и логирование на данных в памяти. Помимо модульных тестов поддерживается [интеграционный набор PostgreSQL](tests/BatchFlow.Postgres.Tests/Integration/README.md). Для него нужна отдельная тестовая БД и переменная `BATCHFLOW_POSTGRES_TESTS`; без нее категория явно пропускается.
