# BatchFlow: grooming и архитектурный план

## 1. Что это за продукт

`BatchFlow` - это библиотека для построения restartable batch jobs в .NET:

- читает данные порциями из источника;
- обрабатывает их последовательно или параллельно;
- делает retry по политике;
- сохраняет checkpoint;
- продолжает выполнение после падения;
- публикует прогресс, метрики и ошибки.

### Что входит в scope v1

- запуск одного batch-job из приложения или worker-сервиса;
- pull-based чтение данных из абстрактного источника;
- обработка по item и по batch;
- bounded parallelism;
- retry и terminal failure policy;
- checkpoint/resume;
- progress/error reporting;
- адаптерная модель через отдельные NuGet-пакеты.

### Что не входит в scope v1

- scheduler уровня cron;
- exactly-once гарантия;
- распределенные транзакции между source и target;
- полноценный оркестратор workflow;
- универсальная поддержка всех брокеров сообщений в первой итерации.

## 2. Главные архитектурные принципы

### 2.1 Core ничего не знает о технологиях

Ядро не зависит от MongoDB, PostgreSQL, RabbitMQ, файловой системы или HTTP-клиентов. Все внешние системы входят через контракты:

- `IBatchSource<TItem, TCheckpoint>`
- `ICheckpointStore<TCheckpoint>`
- `IItemProcessor<TItem>` или item delegate
- `IBatchProcessor<TItem>` или batch delegate
- `IFailurePolicy<TItem>`
- `IProgressObserver`
- `IErrorSink`

### 2.2 У библиотеки должен быть один runtime-контур

Вместо набора независимых helper-ов лучше иметь единый execution engine:

1. загрузить checkpoint;
2. открыть источник с позиции resume;
3. читать порции;
4. складывать их в bounded queue;
5. обрабатывать worker-ами;
6. собирать результаты;
7. двигать checkpoint только для подтвержденной последовательной границы;
8. публиковать метрики и итог выполнения.

### 2.3 Базовая гарантия выполнения

Для v1 стоит честно зафиксировать семантику:

- гарантия по умолчанию: `at-least-once`;
- пользовательский handler должен быть idempotent либо иметь deduplication;
- checkpoint означает: "все данные до этой границы достигли terminal outcome";
- exact-once объявляется как non-goal.

## 3. Самый важный системный вопрос: checkpoint и параллельность

Это ключевой узел всей библиотеки.

Если batch `#10` завершился раньше batch `#9`, нельзя просто сохранить "последний успешный". Нужен commit coordinator:

- reader присваивает каждой порции `sequence number`;
- worker завершает порцию с результатом;
- coordinator хранит completed set;
- checkpoint продвигается только по непрерывной подтвержденной последовательности.

Из этого следуют решения:

- checkpoint unit в v1 лучше делать на уровне batch, а не item;
- item-level parallelism внутри batch допускается, но batch считается commit-ready только когда все его items дошли до terminal state;
- skip после исчерпания retry должен явно влиять на commit semantics.

## 4. Предлагаемая модель API

Наружный API может оставаться fluent, но под ним должен собираться immutable job definition.

Пример направления:

```csharp
await BatchJob
    .From(source)
    .WithBatchSize(1000)
    .WithDegreeOfParallelism(4)
    .WithRetry(3)
    .WithCheckpoint("user-migration-v1", checkpointStore)
    .WithProgress(progressObserver)
    .OnFailure(failurePolicy)
    .RunEachAsync(async (item, context, token) =>
    {
        await target.UpdateAsync(item, token);
    }, cancellationToken);
```

Нужны две основные execution-модели:

- `RunEachAsync(...)`
- `RunBatchAsync(...)`

Это лучше, чем пытаться смешивать batch и item pipeline в одном delegate.

## 5. Основные абстракции

### 5.1 Источник данных

```csharp
public interface IBatchSource<TItem, TCheckpoint>
{
    IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> ReadAsync(
        BatchSourceContext<TCheckpoint> context,
        CancellationToken cancellationToken);
}
```

`SourceBatch` должен содержать:

- `Items`
- `Checkpoint`
- `Metadata`

Важно: `BatchSize` надо трактовать как requested size, а не как жесткую гарантию. Для SQL, Mongo, HTTP pagination и файлов это нормально, но не каждый источник умеет вернуть ровно N элементов.

### 5.2 Checkpoint store

Для v1 лучше начать с typed-модели:

```csharp
public interface ICheckpointStore<TCheckpoint>
{
    ValueTask<StoredCheckpoint<TCheckpoint>?> LoadAsync(
        string jobName,
        CancellationToken cancellationToken);

    ValueTask SaveAsync(
        string jobName,
        StoredCheckpoint<TCheckpoint> checkpoint,
        CancellationToken cancellationToken);
}
```

Позже можно добавить нетипизированный envelope-слой для более широкой экосистемы.

### 5.3 Обработчики

Два режима:

- item processor;
- batch processor.

Не нужно заставлять пользователя адаптировать batch в item или item в batch вручную.

### 5.4 Ошибки

Retry и terminal policy лучше разделить:

- retry отвечает за повторные попытки transient errors;
- failure policy отвечает, что делать после исчерпания retry: `stop`, `skip`, `dead-letter`, `suspend`.

### 5.5 Наблюдаемость

Core лучше проектировать не вокруг `ILogger`, а вокруг событий/observer-интерфейса:

- `JobStarted`
- `BatchStarted`
- `BatchSucceeded`
- `BatchFailed`
- `CheckpointSaved`
- `JobCompleted`

Тогда логирование, метрики и tracing вынесутся в optional packages.

## 6. Исполнительный runtime

### Рекомендуемый runtime-контур

- один reader;
- bounded `Channel` между reader и workers;
- N workers для обработки;
- один commit coordinator;
- один progress publisher.

Почему так:

- есть backpressure;
- есть явный контроль памяти;
- легко управлять graceful shutdown;
- checkpoint coordination становится детерминированной.

## 7. На какие проекты разбивать solution

### Базовые проекты

- `src/BatchFlow.Abstractions`
  - интерфейсы и базовые модели;
- `src/BatchFlow`
  - core runtime, builder, orchestration;
- `src/BatchFlow.Extensions.DependencyInjection`
  - регистрация через `IServiceCollection`;
- `src/BatchFlow.Extensions.Logging`
  - адаптация progress/events в `ILogger`;
- `src/BatchFlow.Extensions.Diagnostics`
  - `ActivitySource`, `Meter`, интеграция с OpenTelemetry;
- `src/BatchFlow.Testing`
  - fake source, fake checkpoint store, test helpers.

### Адаптеры

- `src/Adapters/BatchFlow.MongoDb`
- `src/Adapters/BatchFlow.PostgreSql`
- `src/Adapters/BatchFlow.SqlServer`
- `src/Adapters/BatchFlow.Files`
- `src/Adapters/BatchFlow.Http`
- `src/Adapters/BatchFlow.RabbitMq`

### Тесты

- `tests/BatchFlow.UnitTests`
- `tests/BatchFlow.ContractTests`
- `tests/BatchFlow.IntegrationTests`
- `tests/BatchFlow.AdapterTests.*`

### Примеры

- `samples/SqlToHttpMigration`
- `samples/MongoBackfill`
- `samples/FileImport`

## 8. Что брать в MVP, а что не тащить сразу

### MVP

- один core runtime;
- item и batch обработка;
- retry;
- skip/stop policy;
- in-memory checkpoint store;
- file/json checkpoint store;
- базовый logging adapter;
- 2-3 образцовых source adapter-а: `Files`, `Http`, `PostgreSql` или `MongoDb`.

### Не тащить в MVP

- RabbitMQ adapter;
- distributed locking;
- admin UI;
- DSL для multi-step pipelines;
- advanced partitioning по shard/tenant;
- exactly-once promises.

RabbitMQ лучше делать после pull-based источников, потому что у очередей совсем другая семантика подтверждения и повторной доставки.

## 9. Инструменты и стек

### Внутри core

- `IAsyncEnumerable<T>` для потока чтения;
- `System.Threading.Channels` для bounded queue и backpressure;
- `Polly` для retry/resilience;
- `System.Diagnostics.Metrics` и `ActivitySource` для observability;
- `System.Text.Json` для сериализации checkpoint state там, где это нужно;
- `TimeProvider` для тестируемых timeout/retry/delay сценариев.

### Интеграционные пакеты

- `Microsoft.Extensions.DependencyInjection.Abstractions`
- `Microsoft.Extensions.Options`
- `Microsoft.Extensions.Logging.Abstractions`
- `Microsoft.Extensions.Hosting.Abstractions`

### Качество

- `xUnit`
- `FluentAssertions`
- integration tests с real containers для DB adapters
- `BenchmarkDotNet` позже, когда появится стабильный core runtime

## 10. Продуктовый взгляд

### Для кого библиотека

- команды, которые пишут data migration jobs;
- команды, делающие backfill;
- интеграционные сервисы с checkpoint/resume;
- one-off jobs, которые быстро становятся "почти платформой".

### Почему ей будут пользоваться

- меньше одноразового кода;
- единая модель retry/checkpoint/progress;
- одинаковый operational experience между разными адаптерами;
- возможность наращивать адаптеры без раздувания core.

### Что будет главным продающим тезисом

Не "читает батчами", а:

"дает restartable, observable, adapter-friendly batch runtime для .NET".

## 11. Взгляд системного аналитика

До написания кода надо явно зафиксировать ответы на вопросы:

1. Что считается границей checkpoint: item, batch или source cursor?
2. Что происходит после частичного успеха batch?
3. Skip считается terminal success или error?
4. Можно ли продолжать job после N ошибок?
5. Нужен ли общий error budget на job?
6. Сохраняем ли мы только cursor или еще execution summary?
7. Обязана ли обработка быть ordered?
8. Как отличать transient error от business error?
9. Что считать completed run: без ошибок или без unhandled errors?
10. Может ли source гарантировать стабильный порядок при resume?

Пока эти ответы не зафиксированы, fluent API лучше не цементировать.

## 12. Взгляд разработчика

### Риски архитектуры

- если слишком рано сделать "универсальный" source contract, RabbitMQ сломает модель;
- если checkpoint не увязан с ordered commit, при параллельности будут дыры;
- если core привязать к `ILogger`, DI и конфигу, библиотека станет тяжелее и менее переносимой;
- если сразу добавить слишком много adapters, core останется недообкатанным.

### Что стоит сделать первым

1. Зафиксировать терминологию и guarantees.
2. Спроектировать минимальные контракты.
3. Реализовать in-memory runtime и checkpoint.
4. Написать contract tests для source/checkpoint adapters.
5. Проверить API на 2-3 реальных сценариях.

## 13. Предлагаемый roadmap

### Phase 0. Discovery

- терминология;
- guarantees;
- границы MVP;
- контракты и sequence diagram выполнения.

### Phase 1. Core runtime

- builder;
- execution engine;
- retry + failure policy;
- checkpoint coordinator;
- progress events.

### Phase 2. Developer experience

- DI;
- logging;
- diagnostics;
- testing kit;
- samples.

### Phase 3. First adapters

- Files;
- HTTP;
- SQL или Mongo.

### Phase 4. Advanced adapters

- RabbitMQ;
- richer checkpoint stores;
- partitioning;
- throttling/rate limiting;
- scheduled job integration.

## 14. Практическая рекомендация по target framework

Не стоит строить библиотеку только вокруг `net9.0`. Для NuGet-пакета разумнее взять `net8.0` как минимальную основную цель и не завязывать архитектуру на `net9.0`-specific runtime assumptions.

Причина простая: библиотека должна жить дольше одного STS-цикла, а не только внутри текущего шаблона репозитория.
