# Логирование и события выполнения

[Оглавление](README.md) · [Далее: расширение и тесты](06-extending-and-testing.md)

## Подключение ILogger

Logging-адаптер зависит от `Microsoft.Extensions.Logging.Abstractions`; конкретный провайдер выбирает приложение. Для консольного примера из README добавьте ссылки:

```sh
dotnet add samples/FirstJob/FirstJob.csproj reference src/BatchFlow.Extensions.Logging/BatchFlow.Extensions.Logging.csproj
dotnet add samples/FirstJob/FirstJob.csproj package Microsoft.Extensions.Logging.Console --version 9.0.15
```

Добавьте импорты к `Program.cs`:

```csharp
using BatchFlow.Extensions.Logging;
using Microsoft.Extensions.Logging;
```

Этот фрагмент использует `source` из README и создает собственный console logger:

```csharp
using var loggerFactory = LoggerFactory.Create(logging =>
    logging.SetMinimumLevel(LogLevel.Debug).AddSimpleConsole(console =>
    {
        console.SingleLine = true;
        console.TimestampFormat = "HH:mm:ss ";
    }));

var logger = loggerFactory.CreateLogger("OrdersExport");

var summary = await BatchJob
    .Named("orders-export")
    .From(source)
    .WithProgressLogging(logger, options =>
    {
        options.BatchStartedLogLevel = LogLevel.Debug;
        options.BatchCompletedLogLevel = LogLevel.Information;
        options.CheckpointSavedLogLevel = LogLevel.Debug;
        options.IncludeCheckpointValue = false;
        options.IncludeSourceMetadata = false;
    })
    .RunEachAsync((_, _, _) => ValueTask.CompletedTask);
```

В приложении с хостом передайте уже настроенный `ILogger<T>`. `WithProgressLogging` доступен до и после выбора обработчика, а также для собранной `BatchJobDefinition`. Каждый вызов добавляет нового observer: повторная регистрация дает дублирующиеся логи.

## События и уровни

| Событие | EventId | Уровень по умолчанию |
| --- | --- | --- |
| `JobStartedEvent` | `10000` | `Information` |
| `BatchStartedEvent` | `10010` | `Debug` |
| `BatchCompletedEvent` | `10020` | `Information` |
| `BatchFailedEvent` | `10030` | `Error` |
| `CheckpointSavedEvent` | `10040` | `Debug` |
| `JobCompletedEvent` | `10050` | Зависит от статуса |
| `ProcessingRetryEvent` | `10060` | `Warning` |
| `ProcessingSkippedEvent` | `10070` | `Warning` |

Завершение с `Succeeded` логируется как `Information`, с пропусками, отменой или приостановкой - как `Warning`, с ошибкой - как `Error`. Это можно изменить через `JobCompletedLogLevelSelector`.

Checkpoint и source metadata по умолчанию скрыты. При включении можно задать `CheckpointFormatter` и `SourceMetadataFormatter`, например для маскировки. Эти функции вызываются в контуре выполнения, поэтому должны быть быстрыми и не выбрасывать исключений.

Все записи адаптера содержат структурированный `RunId`, включая batch, checkpoint, retry и skip. Уровни повторов и пропусков настраиваются через `ProcessingRetryLogLevel` и `ProcessingSkippedLogLevel`; `LogLevel.None` отключает соответствующую категорию.

## Собственный observer

Для простой реакции достаточно делегата. Пример использует `source` из README с checkpoint типа `long`:

```csharp
var summary = await BatchJob
    .Named("orders-export")
    .From(source)
    .WithObserver((evt, _) =>
    {
        if (evt is BatchFlow.Abstractions.Observability.BatchCompletedEvent<long> completed)
        {
            Console.WriteLine($"Пакет {completed.BatchSequence}: {completed.SucceededCount} успешных");
        }

        return ValueTask.CompletedTask;
    })
    .RunEachAsync((_, _, _) => ValueTask.CompletedTask);
```

Для переиспользуемой реализации применяйте `IExecutionObserver<TCheckpoint>`. `WithObservers(...)` сохраняет порядок регистрации. Runtime сериализует доставку событий в пределах одного запуска, в том числе при параллельной item-обработке, и последовательно ожидает observers. Медленный сетевой запрос из `OnEventAsync` задерживает обработку или сохранение следующих checkpoint. Общий observer разных одновременных запусков должен быть потокобезопасным.

## Точная семантика событий

`JobStartedEvent` публикуется после загрузки checkpoint. При ошибке загрузки стартового события не будет. `JobCompletedEvent` - попытка финального уведомления, а не гарантированная доставка в постоянный журнал.

При обычной обработке с checkpoint runtime сначала сохраняет позицию, затем публикует `BatchCompletedEvent` и `CheckpointSavedEvent`. При параллельной batch-обработке с checkpoint событие завершения также ждет непрерывной подтвержденной границы. Без store завершения могут публиковаться в порядке получения результатов.

`BatchStartedEvent` в параллельном режиме доставляется через очередь уведомлений: обработчик может уже работать до того, как observer получит событие. Поэтому время получения событий не подходит для точного измерения длительности handler без отдельного инструментирования.

`BatchFailedEvent` отражает окончательный сбой, остановивший обработку. `ProcessingRetryEvent` фиксирует решение о повторе до задержки; отмена еще может не позволить выполнить следующую попытку. `ProcessingSkippedEvent` содержит область (`Item` / `Batch`), `ItemIndex`, `SkippedCount`, `Attempt`, `Reason`, `Exception` и `Issues`. Для смешанного результата batch-handler публикуется одно агрегированное событие пропуска. Причины доступны также в `BatchCompletedEvent.Issues`.

В параллельном batch-режиме retry/skip проходят через тот же ограниченный канал уведомлений. Порядок событий одного worker сохраняется, но время доставки может отставать от обработки. Skip отражает результат handler, а не сохранение checkpoint. В item-режиме `BatchCompletedEvent.Attempt` - максимальная попытка среди элементов пакета, а не общее число вызовов.

## Ошибки наблюдателей

Исключение observer при обычной публикации приводит к `Failed`. Если checkpoint уже сохранен перед событием завершения пакета, он не откатывается. Ошибка одного observer прерывает доставку этого события следующим observers.

При финальном завершении каждый observer получает попытку доставки с `CancellationToken.None`. Его ошибки подавляются ради уже выбранного summary и не препятствуют остальным observers. Вторичный сбой при уведомлении о первоначальной ошибке также подавляется. Подробности отмены приведены в [руководстве по ошибкам](03-errors-and-cancellation.md).

Для best-effort диагностики перехватывайте ошибки внутри собственного observer и используйте ограниченную очередь со своей политикой переполнения, если доставка вынесена в фон. Не добавляйте неограниченную очередь событий: это перенесет проблему памяти в приложение.

`BatchFlow.Extensions.Diagnostics` пока не предоставляет готовых метрик, `ActivitySource` или интеграции с OpenTelemetry. Их можно построить над событиями, учитывая ограничения времени и состава данных выше.
