# BatchFlowKit

Пакетная обработка для .NET 9: ограниченный параллелизм, backpressure, retry,
события прогресса и возобновление по контрольной точке.

Публичные имена NuGet-пакетов начинаются с **BatchFlowKit**. DLL, C#-типы и
пространства имен сохраняют **BatchFlow**. Это самостоятельный проект, не
связанный с ранее опубликованным пакетом `BatchFlow` другого автора.

## Установка

```sh
dotnet add package BatchFlowKit --version 0.1.0-preview.1
```

Это предварительный выпуск, API может изменяться. Основной пакет транзитивно
подключает `BatchFlowKit.Core` и `BatchFlowKit.Abstractions`.

## Пример без внешних сервисов

```csharp
using System.Runtime.CompilerServices;
using BatchFlow;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Sources;

var summary = await BatchJob.Named("example")
    .From(new NumberSource())
    .WithBatchSize(100)
    .WithDegreeOfParallelism(4)
    .RunEachAsync((number, _, token) =>
    {
        token.ThrowIfCancellationRequested();
        Console.WriteLine(number);
        return ValueTask.CompletedTask;
    });

if (summary.Status != JobCompletionStatus.Succeeded)
{
    throw new InvalidOperationException(summary.Reason, summary.Exception);
}

sealed class NumberSource : IBatchSource<int>
{
    public async IAsyncEnumerable<SourceBatch<int, NoCheckpoint>> ReadAsync(
        BatchSourceContext<NoCheckpoint> context,
        [EnumeratorCancellation] CancellationToken token)
    {
        foreach (var items in Enumerable.Range(1, 10).Chunk(context.RequestedBatchSize))
        {
            token.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return new SourceBatch<int, NoCheckpoint>
            {
                Items = items,
                Checkpoint = NoCheckpoint.Value,
            };
        }
    }
}
```

В item-режиме DOP ограничивает элементы внутри одного пакета; в batch-режиме
он ограничивает одновременно обрабатываемые пакеты. Создавать движок вручную
не нужно. Примерный источник выше не поддерживает восстановление позиции.

## Семейство пакетов

| Пакет | Назначение |
| --- | --- |
| `BatchFlowKit` | Пользовательский fluent API, запуск и in-memory checkpoint |
| `BatchFlowKit.Abstractions` | Минимальные контракты адаптеров и событий |
| `BatchFlowKit.Core` | Движок, очередь, workers и coordinator |
| `BatchFlowKit.Postgres` | SQL/keyset-source и PostgreSQL checkpoint store |
| `BatchFlowKit.EntityFrameworkCore` | LINQ/keyset-source через фабрику DbContext |
| `BatchFlowKit.Extensions.Logging` | Runtime-события в ILogger |
| `BatchFlowKit.Extensions.DependencyInjection` | Общие DI-регистрации |
| `BatchFlowKit.Postgres.DependencyInjection` | DI-регистрации PostgreSQL-адаптера |
| `BatchFlowKit.EntityFrameworkCore.DependencyInjection` | DI-регистрации EF-адаптера |
| `BatchFlowKit.Testing` | Инструменты проверки пользовательских задач |

EF-провайдер и соединения выбирает приложение. Регистрация в DI не выполняет
DDL и не открывает БД. Для DI-движка и часов передавайте внедренные
`BatchJobRunOptions` в перегрузку запуска явно.

## Гарантии и ограничения

- Проверяйте `summary.Status`: отсутствие исключения не доказывает успех.
- Handler должен переносить повторный вызов: exactly-once не гарантируется.
- In-memory checkpoint не переживает завершение процесса.
- Retry относится к обработчику, а не к чтению, сохранению checkpoint или observer.
- `Skip` окончателен и допускает продвижение checkpoint.
- Встроенных scheduler, distributed lock и отправки в dead-letter нет.
- Приложение отвечает за стабильный порядок, ключ и согласованность меняющейся выборки.

## Документация

- [Основное руководство](https://github.com/kuroochkin/BatchFlow/blob/master/README.md).
- [PostgreSQL](https://github.com/kuroochkin/BatchFlow/blob/master/docs/04-postgres.md).
- [EF Core](https://github.com/kuroochkin/BatchFlow/blob/master/docs/11-ef-core-source.md).
- [DI и владение ресурсами](https://github.com/kuroochkin/BatchFlow/blob/master/docs/12-dependency-injection.md).
- [Примеры](https://github.com/kuroochkin/BatchFlow/tree/master/samples).
- [Изменения](https://github.com/kuroochkin/BatchFlow/blob/master/CHANGELOG.md).

Лицензия MIT; полный текст включен в пакет как `LICENSE`.
