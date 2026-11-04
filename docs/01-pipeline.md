# Сборка и запуск задачи

[Оглавление](README.md) · [Далее: контрольные точки](02-checkpoints.md)

## Из чего состоит задача

Один запуск связывает источник `IBatchSource`, обработчик, параметры выполнения и необязательные checkpoint store, политику ошибок и наблюдателей. Пользовательский код запускает задачу из консольного приложения, сервиса или собственного планировщика.

```csharp
var builder = BatchJob.Named("orders-export").From(source);
var configured = builder.WithBatchSize(500).WithDegreeOfParallelism(4);
```

В этом фрагменте `source` - реализация `IBatchSource<Order, long>` из README. Вызовы builder создают новое состояние. Сохраняйте возвращаемое значение или стройте цепочку: `builder.WithBatchSize(500);` без присваивания не меняет `builder`.

## Выбор режима

| Режим | Что получает обработчик | Что означает параллелизм | Что повторяет retry |
| --- | --- | --- | --- |
| `RunEachAsync` | Один элемент | До N элементов текущего пакета | Только неудачный элемент |
| `RunBatchAsync` | `IReadOnlyList<TItem>` | До N разных пакетов | Пакет целиком |

В item-режиме следующий пакет поступает в обработку после завершения текущего. Чтение будущих пакетов идет отдельно, в пределах входной очереди. В batch-режиме пакеты могут завершаться в произвольном порядке; checkpoint координируется отдельно.

Обработчик вызывается конкурентно на одном экземпляре. Захваченные коллекции, клиенты и другие зависимости должны допускать такое использование. Не делите один `DbContext` или одно активное DB-соединение между параллельными обработчиками.

## Поэлементная обработка

Для полного контроля результата возвращайте `ProcessingResult`:

```csharp
var summary = await BatchJob
    .Named("orders-export")
    .From(source)
    .WithBatchSize(500)
    .RunEachAsync((order, context, token) =>
    {
        token.ThrowIfCancellationRequested();

        if (order.Amount <= 0)
        {
            return ValueTask.FromResult(
                ProcessingResult.Skipped("Неположительная сумма", code: "invalid-amount"));
        }

        Console.WriteLine($"{order.Number}, пакет {context.BatchSequence}");
        return ValueTask.FromResult(ProcessingResult.Success());
    });
```

Пропуск считается окончательным исходом. Возвращать неуспех через `ProcessingResult` нельзя: в нем только `Succeeded` и `Skipped`. Для сбоя выбрасывайте исключение.

Есть перегрузка с `ValueTask` без результата: нормальное завершение означает успех. Для существующего API, возвращающего `Task`, используйте `async`-лямбду с `await`; именованный метод с возвращаемым типом `Task` напрямую не соответствует `Func<..., ValueTask>`.

## Обработка пакета

Этот фрагмент использует тот же `source`:

```csharp
var summary = await BatchJob
    .Named("orders-bulk-export")
    .From(source)
    .WithBatchSize(500)
    .WithDegreeOfParallelism(2)
    .RunBatchAsync((items, context, token) =>
    {
        token.ThrowIfCancellationRequested();
        Console.WriteLine($"Пакет {context.BatchSequence}: {items.Count} заказов");
        return ValueTask.FromResult(BatchProcessingResult.AllSucceeded(items.Count));
    });
```

Для частичного пропуска верните `BatchProcessingResult.Completed(itemCount, succeededCount, skippedCount, issues, reason)`. Сумма успешных и пропущенных должна равняться фактическому количеству элементов. Индексы в `BatchProcessingIssue` начинаются с нуля. Результат с необъясненными элементами завершит задачу ошибкой.

В batch-режиме библиотека не знает, какие внешние операции уже выполнены до исключения. Для повтора всего пакета нужны транзакция на стороне назначения или идемпотентные операции.

## Настроить сейчас, запустить позже

```csharp
var job = BatchJob
    .Named("orders-export")
    .From(source)
    .WithBatchSize(500)
    .ProcessEach((order, _, token) =>
    {
        token.ThrowIfCancellationRequested();
        Console.WriteLine(order.Number);
        return ValueTask.CompletedTask;
    })
    .Build();

var summary = await job.RunAsync(new BatchJobRunOptions
{
    RunId = Guid.NewGuid().ToString("N"),
});
```

`ProcessEach` и `ProcessBatch` регистрируют обработчик, `Build` создает `BatchJobDefinition`, `RunAsync` выполняет ее. `RunEachAsync` сокращает цепочку `ProcessEach(...).RunAsync()`, `RunBatchAsync` - `ProcessBatch(...).RunAsync()`.

Описание задачи хранит ссылки на source, processor, observers и store. Его неизменяемость поверхностная: зависимости и вложенные коллекции не становятся потокобезопасными автоматически. Повторный запуск допустим, когда зависимости это поддерживают. Одновременные запуски с общим checkpoint ID требуют отдельной координации.

`BatchJobRunOptions` позволяет задать `RunId`, `TimeProvider` и собственный `IBatchJobExecutionEngine`. Подстановка времени сейчас влияет на отметки runtime, но не на задержки retry, использующие `Task.Delay`.

## Параметры и контексты

| Параметр | По умолчанию | Значение |
| --- | --- | --- |
| `BatchSize` | `100` | Запрашиваемый размер пакета |
| `DegreeOfParallelism` | `1` | Конкурентные вызовы в выбранном режиме |
| `PrefetchCapacity` | `1` | Пакеты во входной очереди |
| Retry | Не задан | Первый сбой обработчика останавливает задачу |
| Checkpoint store | Не задан | Прогресс между запусками не сохраняется |

Все три числовых параметра должны быть положительными. Общий контракт source допускает пакеты меньше запрошенного размера и не проверяет верхний предел. PostgreSQL-адаптер дополнительно отклоняет результат больше `RequestedBatchSize`.

Настраивайте параметры по измерениям своего сценария. Больший пакет уменьшает число запросов и checkpoint-сохранений, но увеличивает память и объем повторной обработки при сбое. Параллелизм полезен для перекрытия ожидания I/O, пока внешний сервис, БД и пул соединений выдерживают нагрузку. Большая очередь сглаживает колебания скорости чтения, но не ускоряет уже насыщенное назначение.

Начните с последовательного запуска и небольшой очереди, измерьте время чтения, обработки, сохранения checkpoint и потребление памяти. Затем меняйте один параметр за раз.

Общее окно неподтвержденных пакетов ограничено `PrefetchCapacity + DegreeOfParallelism` в batch-режиме и `PrefetchCapacity + 1` в item-режиме. Место резервируется перед чтением следующего пакета и освобождается после обработки, сохранения checkpoint и публикации подтверждения. Поэтому долгий ранний пакет не позволяет coordinator-у накопить всю выборку. Без checkpoint место освобождается после публикации завершения обработки. Очередь уведомлений также ограничена.

Это ограничение количества пакетов, а не байтов. Размер элементов, буферы самого source и история, которую сохраняет пользовательский observer, остаются ответственностью их владельцев.

Обработчик получает `Job`, `BatchSequence`, `BatchCheckpoint`, `SourceMetadata` и `Attempt`. Item-контекст также содержит `ItemIndex` и `BatchItemCount`, batch-контекст - `ItemCount`.

`BatchSequence` начинается с `1` в каждом запуске, `ItemIndex` - с `0` внутри пакета, `Attempt` - с `1`. `BatchCheckpoint` соответствует всему пакету: он не является индивидуальной позицией текущего элемента.

## Итог выполнения

`BatchJobExecutionSummary<TCheckpoint>` содержит статус, причину, исключение, счетчики, `ResumeFrom` и `LastPersistedCheckpoint`.

| Статус | Значение |
| --- | --- |
| `Succeeded` | Все учтенные элементы успешны |
| `CompletedWithSkips` | Есть окончательные пропуски |
| `Failed` | Сбой обработки, source, store или наблюдателя |
| `Cancelled` | Выполнение отменено; engine дождался producer и обработчиков |
| `Suspended` | Пользовательская политика запросила приостановку |

Счетчики относятся к пакетам, для которых runtime получил окончательный результат. Успехи из частично обработанного, затем упавшего пакета могут в них не попасть. Обработанные, но еще не подтвержденные пакеты могут входить в счетчики: число обработанных элементов не равно сохраненному прогрессу.

Если новых пакетов нет, `LastPersistedCheckpoint` остается `null`, даже когда на старте была контрольная точка. Для последней известной позиции используйте `summary.LastPersistedCheckpoint ?? summary.ResumeFrom`.
