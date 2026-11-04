# Примеры

Полная программа без инфраструктуры находится в [корневом README](../README.md). Оба проекта ниже показывают DI, checkpoint, фиксированный retry только для transient-ошибок, `RunEachAsync` и `RunBatchAsync`. Источник и checkpoint изолированы от контекста/соединения записи обработчика.

Готовый консольный проект: `dotnet run --project src/ConsoleTest/ConsoleTest.csproj`. Он показывает in-memory source, retry, skip, checkpoint и `ILogger`.

## EF Core без внешней БД

```sh
dotnet run --project samples/BatchFlow.EntityFrameworkCore.Sample
```

[Program.cs](BatchFlow.EntityFrameworkCore.Sample/Program.cs) регистрирует `AddPooledDbContextFactory`, `AddBatchFlowEfCoreSource` и singleton in-memory checkpoint. [Runner](BatchFlow.EntityFrameworkCore.Sample/EfCoreSampleRunner.cs) использует `FromDbContext(source)`, получает отдельный контекст записи, а в batch-режиме создает транзакцию. SQLite находится в памяти, постоянные файлы/таблицы приложения не изменяются. DOP установлен в `1` из-за ограничений одновременной записи SQLite.

## PostgreSQL через SQL

Нужна **отдельная тестовая БД с суффиксом `_tests`** и правами создания/удаления схем. Подготовка одноразового PostgreSQL описана в [интеграционных тестах](../tests/BatchFlow.Postgres.Tests/Integration/README.md). Не используйте базу приложения.

```sh
export BATCHFLOW_POSTGRES_SAMPLE='Host=127.0.0.1;Port=55432;Database=batchflow_tests;Username=batchflow_test;Password=batchflow_test'
dotnet run --project samples/BatchFlow.Postgres.Sample
```

[Program.cs](BatchFlow.Postgres.Sample/Program.cs) регистрирует NpgsqlDataSource, SQL-source и PostgreSQL checkpoint store через DI. Пример создает уникальную схему `batchflow_sample_<guid>`, три исходные записи и таблицу назначения. `EnsureStorageAsync` вызывается явно. [SQL-константы](BatchFlow.Postgres.Sample/PostgresSampleQueries.cs) содержат keyset-запрос и идемпотентный UPSERT; динамический SQL подготавливается один раз с экранированным идентификатором схемы. [Runner](BatchFlow.Postgres.Sample/PostgresSampleRunner.cs) использует пул для отдельных соединений записи и транзакции batch; DOP равен `2`.

В `finally` пример удаляет **только свою схему**, даже после ошибки или Ctrl+C. При аварийном завершении процесса схема может остаться. PostgreSQL store постоянный, но демонстрация намеренно очищает его в конце: здесь resume проверяется между запусками job в разных scope, а не между запусками процесса. В приложении сохраняйте стабильные schema/table и checkpoint ID, не переносите демонстрационный cleanup.

## Ожидаемый результат

В обоих примерах первый проход каждого режима обрабатывает `3` элемента и сохраняет checkpoint `3`; тестовая `TimeoutException` показывает работу retry. Второй проход в новом scope обрабатывает `0`. В назначении остаются `6` строк: по три на каждый режим, без дублей. Каждый runner проверяет `summary.Status` и счетчики; ошибка приводит к ненулевому коду завершения, Ctrl+C - к `130`.

Это не exactly-once: handlers допускают повторную запись по тому же ключу. Они не меняют ключ/фильтр исходных записей, так как checkpoint обоих независимых job должен видеть одинаковую исходную выборку. Для меняющейся таблицы, поздних коммитов и бизнес-транзакций нужны правила приложения: [SQL](../docs/04-postgres.md), [EF](../docs/11-ef-core-source.md).

Проекты собираются отдельно от solution; `dotnet run` восстанавливает зависимости и собирает пример. [DI API и lifetimes](../docs/12-dependency-injection.md).

Команды корневого README также позволяют создать `samples/FirstJob` локально для знакомства с библиотекой.
