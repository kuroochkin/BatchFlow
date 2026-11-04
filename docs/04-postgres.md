# Чтение и checkpoint в PostgreSQL

[Оглавление](README.md) · [Далее: логи и события](05-observability.md)

## Подключение проекта

Из корня репозитория, после создания `samples/FirstJob` по инструкции README:

```sh
dotnet add samples/FirstJob/FirstJob.csproj reference src/BatchFlow.Postgres/BatchFlow.Postgres.csproj
```

`Npgsql` подключается через проект адаптера. Текущая версия в `BatchFlow.Postgres.csproj` - `10.0.3`; это версия репозитория, а не обещание использовать последний релиз драйвера.

## Полный пример

Пример читает заказы из одной таблицы и выполняет upsert в другую. Повторная обработка одного заказа обновляет ту же строку назначения. Данные, бизнес-запись и checkpoint сохраняются отдельными операциями: общей транзакции между ними нет.

Для локального воспроизведения подготовьте таблицы и данные:

```sql
create table public.batchflow_demo_orders
(
    id bigint primary key,
    number text not null,
    amount numeric(18, 2) not null
);

create table public.batchflow_demo_exported_orders
(
    id bigint primary key,
    number text not null,
    amount numeric(18, 2) not null
);

insert into public.batchflow_demo_orders (id, number, amount)
select id, 'A-' || id::text, id * 100.00
from generate_series(1, 10) as id;
```

Задайте переменную окружения `BATCHFLOW_POSTGRES` со строкой подключения к своей локальной БД. Ниже полный альтернативный `Program.cs`:

```csharp
using BatchFlow;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Postgres.Checkpoints;
using BatchFlow.Postgres.Extensions;
using BatchFlow.Postgres.Options;
using BatchFlow.Postgres.Sources;
using Npgsql;
using NpgsqlTypes;

var connectionString = Environment.GetEnvironmentVariable("BATCHFLOW_POSTGRES")
    ?? throw new InvalidOperationException("Задайте BATCHFLOW_POSTGRES");

await using var dataSource = NpgsqlDataSource.Create(connectionString);
var connectionOptions = PostgresConnectionOptions.FromDataSource(dataSource);

var checkpointStore = new PostgresCheckpointStore<long>(new PostgresCheckpointStoreOptions
{
    Connection = connectionOptions,
    CommandTimeoutSeconds = 30,
});

// Для локального примера; в развернутом приложении структуру обычно создает миграция.
await checkpointStore.EnsureStorageAsync();

var sourceOptions = new PostgresBatchSourceOptions<Order, long>
{
    Connection = connectionOptions,
    CommandTimeoutSeconds = 30,
    CommandText = """
        select id, number, amount
        from public.batchflow_demo_orders
        where (@resume_checkpoint is null or id > @resume_checkpoint)
        order by id
        limit @batch_size
        """,
    ResumeCheckpointParameterType = NpgsqlDbType.Bigint,
    RowMaterializer = (reader, token) =>
    {
        token.ThrowIfCancellationRequested();
        var id = reader.GetInt64(0);
        var number = reader.GetString(1);
        var amount = reader.GetDecimal(2);

        return ValueTask.FromResult(
            new PostgresBatchSourceRow<Order, long>(new Order(id, number, amount), id));
    },
};

var summary = await BatchJob
    .Named("postgres-orders-export")
    .FromPostgres(sourceOptions)
    .WithBatchSize(3)
    .WithDegreeOfParallelism(1)
    .WithPrefetchCapacity(1)
    .WithPostgresCheckpoint("postgres-orders-export-v1", checkpointStore)
    .WithRetry(new BatchRetryOptions
    {
        MaxRetryAttempts = 2,
        Delay = TimeSpan.FromMilliseconds(200),
        ShouldHandle = exception => exception is NpgsqlException { IsTransient: true },
    })
    .RunEachAsync(async (order, _, token) =>
    {
        await using var command = dataSource.CreateCommand("""
            insert into public.batchflow_demo_exported_orders (id, number, amount)
            values (@id, @number, @amount)
            on conflict (id) do update
            set number = excluded.number, amount = excluded.amount
            """);
        command.Parameters.AddWithValue("id", NpgsqlDbType.Bigint, order.Id);
        command.Parameters.AddWithValue("number", NpgsqlDbType.Text, order.Number);
        command.Parameters.AddWithValue("amount", NpgsqlDbType.Numeric, order.Amount);
        await command.ExecuteNonQueryAsync(token);
    });

Console.WriteLine($"{summary.Status}: {summary.SucceededItemCount} заказов");

if (summary.Status is not JobCompletionStatus.Succeeded)
{
    throw new InvalidOperationException(summary.Reason, summary.Exception);
}

internal sealed record Order(long Id, string Number, decimal Amount);
```

Первый успешный запуск обрабатывает десять заказов в четырех пакетах. Второй с тем же checkpoint ID не обрабатывает старые строки. Изменение уже пройденной строки не возвращает ее в выборку: этот пример предназначен для последовательного обхода, а не для синхронизации всех обновлений.

Source удерживает одно соединение на время перечисления, а target и store получают собственные соединения из того же пула. Пул должен вмещать их одновременную работу; при размере пула `1` пример может ждать соединение до таймаута. Объект `dataSource` нужно сохранять живым до завершения задачи.

## Требования к SQL

Источник повторяет заданный SQL; библиотека не добавляет `where`, `order by` или `limit` самостоятельно. Параметры передаются драйверу отдельно от текста запроса. Имена таблиц и колонок параметрами значений не подставляются.

Запрос должен соблюдать следующие условия:

1. Порядок устойчивый и уникальный, например `order by id` при уникальном `id`.
2. Фильтр продолжает чтение строго после checkpoint, например `id > @resume_checkpoint`.
3. Ограничение использует `@batch_size`, полученное из runtime.
4. Значение checkpoint последней материализованной строки соответствует этому порядку.
5. Условие и сортировка поддерживаются подходящим индексом.

Без уникального порядка `LIMIT` может давать непредсказуемые подмножества. Большой `OFFSET` заставляет сервер вычислять пропускаемые строки; поэтому здесь используется keyset. См. [документацию PostgreSQL о LIMIT/OFFSET](https://www.postgresql.org/docs/current/queries-limit.html).

Чтение заканчивается на пустом результате. Короткий пакет не завершает обход автоматически: source выполнит еще один запрос. Это позволяет не терять данные, если запрос по своей логике возвращает меньше запрошенного размера.

`EnforceCheckpointProgression` проверяет переход к каждой строке, включая первую после `ResumeFrom`. По умолчанию требуется только неравенство соседних позиций; это не доказывает возрастание. Для строгого порядка задайте `CheckpointProgressionValidator = (previous, next) => next > previous`. Для убывающего обхода используйте `<`, для составного cursor сравнивайте поля в том же порядке, что и SQL. Нарушение порядка завершает чтение до выдачи некорректного пакета. При `EnforceCheckpointProgression = false` обе проверки отключены. Анализ SQL и доказательство его корректности не выполняются.

## Контракт параметров

| Опция | По умолчанию | Назначение |
| --- | --- | --- |
| `CommandText` | Обязательна | SQL одной порции |
| `RowMaterializer` | Обязательна | Строка -> модель и checkpoint |
| `BatchSizeParameterName` | `batch_size` | Имя параметра размера |
| `BatchSizeParameterType` | `Integer` | Тип параметра размера |
| `ResumeCheckpointParameterName` | `resume_checkpoint` | Имя параметра позиции, `null` отключает стандартную привязку |
| `ResumeCheckpointParameterType` | Не задан | Тип позиции; нужен при стандартной передаче `NULL` |
| `BindResumeCheckpointWhenMissing` | `true` | Добавлять отсутствующую позицию как SQL `NULL` |
| `ResumeCheckpointValueFactory` | `ResumeFrom.Value` или `null` | Преобразование позиции в значение параметра |
| `ParameterBinder` | Не задан | Дополнительные параметры и составные checkpoint |
| `CommandTimeoutSeconds` | Не задан | Таймаут драйвера, явное значение должно быть больше нуля |
| `ReaderBehavior` | `SequentialAccess \| SingleResult` | Поведение reader; флаг `CloseConnection` запрещен |
| `EnforceCheckpointProgression` | `true` | Проверка перехода к каждой строке, включая границу resume |
| `CheckpointProgressionValidator` | Не задан | Пользовательская проверка порядка соседних позиций |

Стандартные параметры добавляются перед `ParameterBinder`. Если нужно изменить существующий, меняйте его по индексу/имени, а не добавляйте дубликат. Чтобы полностью задать параметры checkpoint вручную, установите `ResumeCheckpointParameterName = null`. Размер пакета source продолжает добавлять автоматически.

Binder получает исходный `SourceContext`, текущий `ResumeFrom`, `BatchSize` и `ReadIteration`, начиная с `0`. После первой порции `ResumeFrom` здесь является внутренней позицией чтения, которая может еще не быть сохранена runtime. Это не подтверждение обработки; у синтетической точки нет токена сохраненного checkpoint.

## Составной checkpoint

Если `created_at` не уникален, добавьте `id` в сортировку и в checkpoint. Фрагмент использует `connectionOptions` из полной программы и отдельные типы ниже:

```csharp
var compositeOptions = new PostgresBatchSourceOptions<TimedOrder, OrderCursor>
{
    Connection = connectionOptions,
    CommandText = """
        select created_at, id, number
        from public.timed_orders
        where (@resume_id is null or (created_at, id) > (@resume_created_at, @resume_id))
        order by created_at, id
        limit @batch_size
        """,
    ResumeCheckpointParameterName = null,
    ParameterBinder = (parameters, context) =>
    {
        var cursor = context.ResumeFrom?.Value;
        parameters.AddWithValue(
            "resume_created_at", NpgsqlDbType.TimestampTz,
            (object?)cursor?.CreatedAt ?? DBNull.Value);
        parameters.AddWithValue(
            "resume_id", NpgsqlDbType.Bigint,
            (object?)cursor?.Id ?? DBNull.Value);
    },
    RowMaterializer = (reader, _) =>
    {
        var createdAt = reader.GetFieldValue<DateTimeOffset>(0);
        var id = reader.GetInt64(1);
        var number = reader.GetString(2);
        return ValueTask.FromResult(new PostgresBatchSourceRow<TimedOrder, OrderCursor>(
            new TimedOrder(id, number), new OrderCursor(createdAt, id)));
    },
};
```

Типы для этого фрагмента:

```csharp
internal sealed record TimedOrder(long Id, string Number);
internal sealed record OrderCursor(DateTimeOffset CreatedAt, long Id);
```

Таблица `public.timed_orders` должна иметь `created_at timestamptz not null`, `id bigint` и `number text`, уникальную пару `(created_at, id)` и индекс по этой паре. `DateTimeOffset` для `timestamptz` в этих примерах используется с нулевым смещением UTC.

## Материализация и память

Materializer вызывается один раз для каждой читаемой строки. Он должен полностью скопировать нужные данные в пользовательскую модель. `DbDataReader`, stream поля или другие объекты, зависящие от открытого reader, передавать в обработчик нельзя: перед выдачей пакета reader уже освобожден.

По умолчанию включен `SequentialAccess`: читайте колонки в порядке `SELECT` и сохраняйте значение ключа в локальную переменную, вместо повторного чтения ранее пройденного поля. Для свободного доступа можно явно выбрать `ReaderBehavior = CommandBehavior.SingleResult`, оценив память на широких строках. Особенности режима описаны в [Npgsql Performance](https://www.npgsql.org/doc/performance.html).

Допускаются только `Default`, `SequentialAccess`, `SingleResult` и комбинация двух последних. `CloseConnection`, `SchemaOnly`, `SingleRow`, `KeyInfo` и неизвестные биты отклоняются при создании источника. Reader освобождается после каждой порции, но соединение должно оставаться открытым до конца перечисления. Его освобождением управляет модель подключения, а не reader.

Пакет материализуется в памяти целиком. Лимит задан количеством строк, а не байтами. Набор из тысячи больших JSON или бинарных полей может быть существенно тяжелее тысячи коротких записей.

Токен передается открытию соединения, выполнению SQL, `reader.ReadAsync` и materializer. Пользовательский binder синхронный; длительный ввод-вывод внутри него не подходит. Ошибка чтения или materializer завершает source и не повторяется `WithRetry` обработчика.

## Способы подключения

Рекомендуемый для приложения вариант - переиспользуемый `NpgsqlDataSource`, жизненным циклом которого управляет приложение. Он предоставляет пул и допускает совместное использование; см. [Npgsql Basic Usage](https://www.npgsql.org/doc/basic-usage.html).

| Способ | Открытие | Освобождение | Повторное использование |
| --- | --- | --- | --- |
| `FromConnectionString(string)` | Адаптер создает и открывает | Адаптер освобождает соединение | Новый экземпляр на операцию |
| `FromDataSource(NpgsqlDataSource)` | Через data source | Адаптер освобождает выданное соединение | Сам data source остается у приложения |
| `FromConnectionFactory(Func<NpgsqlConnection>)` | Адаптер открывает, если нужно | Адаптер освобождает | Фабрика должна выдавать отдельный экземпляр |
| Асинхронная `FromConnectionFactory` | Фабрика получает токен, адаптер открывает при необходимости | Адаптер освобождает | То же правило отдельного экземпляра |
| `FromConnection(connection, External)` | Уже должно быть открыто | Вызывающий код | Нельзя параллельно выполнять операции на том же соединении |
| `FromConnection(connection, AdapterOwned)` | Адаптер открывает при необходимости | Освобождает переданный экземпляр после перечисления | Одно перечисление source; для store запрещен |

Фабрику можно настроить так: `PostgresConnectionOptions.FromConnectionFactory(() => new NpgsqlConnection(connectionString))`. Асинхронный вариант принимает `Func<CancellationToken, ValueTask<NpgsqlConnection>>`.

`PostgresCheckpointStore` отклоняет готовое `AdapterOwned`-соединение в конструкторе с `ArgumentException`: один экземпляр, освобождаемый после операции, не подходит для последовательности `EnsureStorageAsync`, `LoadAsync` и повторных `SaveAsync`. При отказе конфигурации переданный ресурс не освобождается. Для store выбирайте data source, строку, фабрику или отдельное открытое соединение с владением `External`.

Для source готовое `AdapterOwned`-соединение поддерживает одно перечисление. Для повторных запусков используйте фабрику, строку или data source. При ошибке открытия или отмене полученное owned-соединение освобождается до возврата ошибки; исходное исключение открытия или отмены сохраняется. Если пользовательская фабрика выбросила исключение до возврата экземпляра, освобождение созданных ею ресурсов остается ее ответственностью.

`External` требует исключительного использования соединения на время чтения, в том числе во время prefetch. Не выполняйте на нем команды из handler или checkpoint store. Готового адаптера `DbContext` пока нет; передача соединения из ORM требует самостоятельного согласования lifetime, транзакции и отсутствия конкурентных обращений.

## Постоянный checkpoint store

По умолчанию используется таблица `batchflow.checkpoints`: `checkpoint_id` (`text`, PK), `checkpoint_value` (`jsonb`), `sequence_number` (`bigint`, nullable), `concurrency_token` (`text`), `saved_at` (`timestamptz`), `metadata` (`jsonb`). Названия схемы и таблицы меняются через `PostgresCheckpointStoreOptions`.

`EnsureStorageAsync` явно создает отсутствующие схему и таблицу; он не проверяет версию существующей структуры и не выполняет миграцию. В продакшене DDL можно выполнять миграциями, а приложению выдавать права на чтение и запись уже подготовленной таблицы.

При первом сохранении store делает `INSERT ... ON CONFLICT DO NOTHING`, при обновлении - `UPDATE ... WHERE concurrency_token = ...`. Отсутствие сохраненной строки означает `CheckpointConflictException`. Это атомарная проверка версии одной записи, а не блокировка выполнения всей job.

По умолчанию checkpoint сериализуется через `System.Text.Json`. Пользовательский `IPostgresCheckpointSerializer<TCheckpoint>` должен возвращать корректный JSON: колонка и параметр остаются `jsonb`. Поддерживайте совместимость сохраненного JSON при изменении типа checkpoint.

## Границы чтения

Адаптер не создает транзакцию со снимком данных на весь запуск. При обычном `Read Committed` следующие запросы могут увидеть новые коммиты. Поздно закоммиченная строка с ключом ниже уже пройденной позиции может не попасть в обработку; `bigserial` сам по себе не гарантирует порядок коммитов. Смена ключа или фильтра в процессе обхода также требует отдельной стратегии. См. [уровни изоляции PostgreSQL 17](https://www.postgresql.org/docs/17/transaction-iso.html).

Для миграции стабильного набора заранее определите границы и условия консистентности. Для постоянно изменяющихся данных может потребоваться снимок, staging-таблица или CDC. Установка одной верхней границы ключа сама по себе не устраняет поздние коммиты.

Выберите сценарий явно:

| Сценарий | Контракт |
| --- | --- |
| Стабильный набор | Уникальный неизменяемый ключ, фиксированный состав выборки; checkpoint возобновляет обход |
| Снимок одного запуска | Заранее открытое `External`-соединение и транзакция `RepeatableRead`, которыми управляет приложение; store использует отдельное соединение |
| Возобновление того же набора после падения | Материализованный staging/список ключей с устойчивым идентификатором; новый запуск не восстанавливает старый снимок автоматически |
| Непрерывные изменения | CDC/outbox либо иной протокол доставки изменений; keyset по ID не заменяет его |

Ограничение поздних коммитов воспроизводится постоянным интеграционным тестом: строка `12` видима раньше незакоммиченной `11`; после сохранения позиции `12` обычное продолжение уже не возвращается к `11`. Валидатор порядка не исправляет этот сценарий.

Помимо unit-тестов доступны проверки с реальной БД: [запуск интеграционных тестов](../tests/BatchFlow.Postgres.Tests/Integration/README.md). Индексы, планы и бизнес-транзакции конкретного приложения требуют собственных проверок.
