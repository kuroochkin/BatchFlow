# Свои адаптеры и тестирование

[Оглавление](README.md) · [Далее: архитектура и стиль](07-architecture-and-style.md)

## Собственный источник

Для возобновляемого чтения реализуйте `IBatchSource<TItem, TCheckpoint>`, для чтения без сохраненной позиции - `IBatchSource<TItem>`. Полный возобновляемый source есть в [README](../README.md).

Ниже полная реализация невозобновляемого источника чисел:

```csharp
using System.Runtime.CompilerServices;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;

internal sealed class NumberSource(IReadOnlyList<int> numbers) : IBatchSource<int>
{
    public async IAsyncEnumerable<SourceBatch<int, NoCheckpoint>> ReadAsync(
        BatchSourceContext<NoCheckpoint> context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(context.RequestedBatchSize);

        foreach (var items in numbers.Chunk(context.RequestedBatchSize))
        {
            cancellationToken.ThrowIfCancellationRequested();
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

Для реального I/O вместо демонстрационного `Task.Yield()` ожидайте чтение внешнего ресурса. Source возвращает непустые пакеты и завершает последовательность при отсутствии данных. У возобновляемого source checkpoint не должен быть `null`.

Runtime может читать вперед, пока обработчик работает с предыдущим пакетом. Каждый выданный пакет должен оставаться неизменным: нельзя повторно наполнять тот же `List<T>` или возвращать буфер в пул, пока consumer еще использует данные. `IReadOnlyList` ограничивает интерфейс доступа, но не запрещает владельцу менять коллекцию.

Ресурсы source открывайте внутри перечисления и освобождайте через `using` / `await using`, включая ранний выход и отмену. Не возвращайте `null` вместо `IAsyncEnumerable`.

## Свой обработчик

Делегаты подходят для короткой логики. Отдельный класс удобен для внедрения зависимостей и своего тестирования. Полная реализация для `NumberSource`:

```csharp
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Processing;
using BatchFlow.Abstractions.Results;
using BatchFlow.Abstractions.Sources;

internal sealed class PositiveNumberProcessor : IItemProcessor<int, NoCheckpoint>
{
    public ValueTask<ProcessingResult> ProcessAsync(
        int item,
        ItemProcessingContext<NoCheckpoint> context,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(item > 0
            ? ProcessingResult.Success()
            : ProcessingResult.Skipped("Число должно быть положительным"));
    }
}
```

Подключение: `BatchJob.Named("numbers").From(new NumberSource([1, -1, 2])).RunEachAsync(new PositiveNumberProcessor())`. Для пакетной логики используйте `IBatchProcessor<TItem, TCheckpoint>`.

Обработчик не получает отдельный экземпляр на worker автоматически. Lifetime зависимостей организует приложение. `AddBatchFlow` и отдельные DI-пакеты адаптеров описаны в [руководстве DI](12-dependency-injection.md). Собственные адаптеры можно зарегистрировать обычными средствами контейнера; время жизни их зависимостей остается ответственностью приложения.

## Свой checkpoint store

`ICheckpointStore<TCheckpoint>` требует двух методов:

- `LoadAsync` возвращает сохраненный `StoredCheckpoint` либо `null` при отсутствии записи.
- `SaveAsync` атомарно проверяет `expectedConcurrencyToken`, сохраняет новое значение и возвращает запись с обновленным токеном.

Несовпадение версии приводит к `CheckpointConflictException`. Создание с `expectedConcurrencyToken = null` не должно молча перезаписывать существующую версию. Возвращайте неотрицательный `SequenceNumber`, ненулевые `Value` и `Metadata`.

Store должен сохранять metadata, обрабатывать отмену и не менять переданный объект checkpoint. Если `Value` изменяемое, определите правила копирования. Запись в память процесса не дает восстановления после падения процесса.

## Тестовые инструменты

| Тип из BatchFlow.Testing | Что помогает проверить |
| --- | --- |
| `DelegateBatchSource` | Позицию старта, параметры чтения, ошибку source |
| `DelegateItemProcessor` / `DelegateBatchProcessor` | Контексты и исходы |
| `DelegateFailurePolicy` | Retry, stop, skip, suspend |
| `RecordingExecutionObserver` | Состав и порядок событий |
| `Checkpoints.InMemoryCheckpointStore` | Загрузки, сохранения, токены, искусственные ошибки |

Тестовая обертка store отличается от `BatchFlow.Checkpoints.InMemoryCheckpointStore` в фасаде. При использовании обоих пространств имен применяйте alias или полное имя.

## Пример теста восстановления

Полный xUnit-тест требует ссылок на `BatchFlow.Testing` и `xunit`. Source учитывает checkpoint; после второго запуска новых сохранений быть не должно.

```csharp
using System.Runtime.CompilerServices;
using BatchFlow;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Testing.Sources;
using Xunit;
using TestStore = BatchFlow.Testing.Checkpoints.InMemoryCheckpointStore<long>;

public sealed class ResumeExampleTests
{
    [Fact]
    public async Task Second_run_should_skip_confirmed_data()
    {
        var store = new TestStore();
        var source = new DelegateBatchSource<int, long>(ReadAsync);
        var processed = new List<int>();

        var job = BatchJob.Named("resume-example")
            .From(source)
            .WithBatchSize(2)
            .WithCheckpoint(store)
            .ProcessEach((item, _, _) =>
            {
                processed.Add(item);
                return ValueTask.CompletedTask;
            })
            .Build();

        var first = await job.RunAsync();
        var second = await job.RunAsync();

        Assert.Equal(JobCompletionStatus.Succeeded, first.Status);
        Assert.Equal(JobCompletionStatus.Succeeded, second.Status);
        Assert.Equal(new[] { 1, 2, 3 }, processed);
        Assert.Equal(0L, second.SucceededItemCount);
        Assert.Equal(3L, second.ResumeFrom!.Value);
        Assert.Null(second.LastPersistedCheckpoint);
        Assert.Equal(2, store.SaveCallCount);
    }

    private static async IAsyncEnumerable<SourceBatch<int, long>> ReadAsync(
        BatchSourceContext<long> context,
        [EnumeratorCancellation] CancellationToken token)
    {
        var last = context.ResumeFrom?.Value ?? 0L;
        foreach (var items in new[] { 1, 2, 3 }.Where(item => item > last).Chunk(context.RequestedBatchSize))
        {
            token.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return new SourceBatch<int, long>
            {
                Items = items,
                Checkpoint = items[^1],
            };
        }
    }
}
```

Коллекции `CapturedContexts`, `Events`, `SaveRequests` не предназначены для неограниченного накопления или независимых конкурентных запусков. Создавайте doubles на один тест/запуск. Для конкурентных сценариев используйте `TaskCompletionSource` и дедлайны вместо предположений о скорости машины.

## Сценарии проверки приложения

1. Успешный проход, пустой источник и последний короткий пакет.
2. Повторный запуск с тем же checkpoint ID.
3. Сбой после внешнего эффекта до checkpoint: повтор должен быть безопасным.
4. Retry элемента и целого пакета, включая частичное выполнение пакета.
5. Skip с последующим продвижением checkpoint.
6. Ошибку source, ошибку save и конфликт версии.
7. Отмену чтения, handler и задержки retry.
8. Завершение пакетов не по порядку и долгий первый пакет.
9. Ошибку observer и закрытие всех пользовательских ресурсов.

Unit-тесты с подставным reader не заменяют интеграционные проверки PostgreSQL. В репозитории есть [набор с настоящей БД](../tests/BatchFlow.Postgres.Tests/Integration/README.md): nullable-параметры, JSONB, временные типы, конфликт сохранений, DDL, отмена SQL и возврат соединений в пул. Бизнес-транзакции и запросы приложения проверяйте дополнительно.
