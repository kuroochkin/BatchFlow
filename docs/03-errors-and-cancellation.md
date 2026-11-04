# Ошибки, retry и отмена

[Оглавление](README.md) · [Далее: PostgreSQL](04-postgres.md)

## Как обработчик сообщает исход

Нормальное завершение делегата без результата означает успех. `ProcessingResult.Success()` означает успех элемента, `ProcessingResult.Skipped(...)` - окончательный пропуск. Исключение передает решение runtime и политике ошибок.

Без политики первый сбой обработчика останавливает задачу. Ошибка конфигурации обычно выбрасывается до запуска runtime; ошибка выполнения обычно попадает в `summary.Exception` со статусом `Failed`. Поэтому одного `try/catch` вокруг `RunAsync` недостаточно для определения успеха.

## Фиксированный retry

Фрагмент использует `source` из README:

```csharp
var job = BatchJob
    .Named("orders-export")
    .From(source)
    .WithRetry(new BatchRetryOptions
    {
        MaxRetryAttempts = 3,
        Delay = TimeSpan.FromMilliseconds(200),
        ShouldHandle = exception => exception is TimeoutException,
    })
    .WithStopOnFailure()
    .ProcessEach((order, _, token) =>
    {
        token.ThrowIfCancellationRequested();
        Console.WriteLine(order.Number);
        return ValueTask.CompletedTask;
    })
    .Build();
```

`MaxRetryAttempts = 3` означает до четырех вызовов: исходный и три повтора. `context.Attempt` принимает значения от `1`. Без `ShouldHandle` повторяются все исключения обработчика, кроме `OperationCanceledException`.

В item-режиме повторяется один элемент. В batch-режиме - весь пакет, включая уже выполненные перед исключением действия. Встроенная стратегия не добавляет экспоненциальную задержку и jitter.

Retry относится к вызовам обработчика. Чтение source, открытие соединения, материализация строки, сохранение checkpoint и события этим механизмом не повторяются. Некорректный результат обработчика также не следует трактовать как временный сбой для retry.

## Stop и skip

`.WithStopOnFailure()` завершает задачу с `Failed` после исчерпания повторов. `.WithSkipOnFailure()` завершает неудачный элемент или пакет пропуском и продолжает задачу.

`ShouldHandle` определяет только право на retry. Если предикат вернул `false`, terminal-действие применяется сразу. Сочетание `ShouldHandle = ex => ex is TimeoutException` и `WithSkipOnFailure()` также пропустит неповторяемые ошибки, например `InvalidOperationException`.

`WithRetry` и `WithSkipOnFailure` сохраняют настройки друг друга. Пользовательская `WithFailurePolicy` заменяет встроенную конфигурацию. Последующий вызов встроенных retry/terminal-методов убирает пользовательскую политику; это замена, а не композиция.

## Своя политика

Ниже полная реализация политики для типов из README. Ее подключают через `.WithFailurePolicy(new OrderFailurePolicy())`.

```csharp
using BatchFlow.Abstractions.Failures;

internal sealed class OrderFailurePolicy : IFailurePolicy<Order, long>
{
    public ValueTask<FailureDecision> DecideAsync(
        FailureContext<Order, long> context,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var decision = context.Exception is TimeoutException && context.Attempt < 3
            ? FailureDecision.Retry(TimeSpan.FromMilliseconds(200))
            : FailureDecision.Suspend("Нужна проверка оператора");

        return ValueTask.FromResult(decision);
    }
}
```

Политика получает `Scope` (`Item` или `Batch`), исключение, номер попытки, пакет и контекст. В item-режиме также доступны `FailedItem` и `ItemIndex`. При параллелизме политика должна переносить конкурентные вызовы.

`Suspend` возвращает итоговый статус `Suspended` без подтверждения неудачного пакета. Он не сохраняет живой процесс в памяти и не планирует продолжение. Новый запуск с тем же checkpoint ID повторит неподтвержденные данные.

`FailureDecision.DeadLetter()` пока завершает выполнение ошибкой о неподдерживаемом действии. Отправки данных в карантин нет. У пользовательской политики, которая постоянно возвращает `Retry`, нет общего автоматического лимита попыток: предел задает сама политика.

## Отмена

Передавайте `CancellationToken` из хоста в `RunAsync` / `RunEachAsync` / `RunBatchAsync` и далее во все операции ввода-вывода. В CPU-циклах проверяйте токен явно. Отмена кооперативная: библиотека не может принудительно завершить обработчик, игнорирующий токен.

Для запуска `job`, собранной выше:

```csharp
using var cancellation = new CancellationTokenSource();
ConsoleCancelEventHandler onCancel = (_, args) =>
{
    args.Cancel = true;
    cancellation.Cancel();
};
Console.CancelKeyPress += onCancel;

try
{
    var summary = await job.RunAsync(cancellation.Token);
    Console.WriteLine($"Задача завершилась: {summary.Status}");
}
finally
{
    Console.CancelKeyPress -= onCancel;
}
```

После проверки конфигурации runtime возвращает внешнюю отмену как `summary.Status = Cancelled`, в том числе при заранее отмененном токене. `OperationCanceledException` без отмены рабочего токена рассматривается как ошибка выполнения, а не как внешняя отмена. Ошибки конфигурации по-прежнему выбрасываются до запуска.

При остановке runtime отправляет отмену и ожидает producer и всех workers, прежде чем вернуть результат. Обработчик, игнорирующий токен, задерживает возврат: принудительного прерывания и фонового «забытого» handler нет. Внутренняя отмена остальных workers, ошибки callback-ов остановки и вторичных уведомлений не заменяют уже зафиксированную первичную ошибку.

`JobCompletedEvent` доставляется с `CancellationToken.None`: отмена рабочего запуска не прерывает финальное уведомление. Каждый observer получает попытку доставки; его исключение, включая собственную отмену, подавляется и не меняет summary. Наблюдатели должны завершать свои операции и самостоятельно ограничивать внешние I/O по времени.

## Сбор ошибок

Summary хранит одно итоговое исключение и общие счетчики, а не историю всех ошибок. `ProcessingRetryEvent` сообщает причину повтора, попытку и задержку; `ProcessingSkippedEvent` сообщает причину окончательного пропуска, исходное исключение политики, индекс элемента либо число пропущенных элементов пакета. Явный `ProcessingResult.Skipped` также порождает событие. `BatchCompletedEvent.Issues` сохраняет подробности текущего пакета.

Для аудита подключите observer и сохраняйте события во внешнем хранилище. Не копите всю историю задания в памяти. Событие пропуска не является подтверждением checkpoint и может повториться после восстановления. Гарантированного транзакционного error sink пока нет. `BatchFailedEvent` остается уведомлением об окончательном сбое, остановившем обработку.
