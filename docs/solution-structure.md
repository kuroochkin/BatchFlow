# Структура solution

## Каталоги

- `src` - исходный код библиотек и adapter-пакетов.
- `tests` - unit-, contract- и integration-тесты.
- `samples` - примеры использования библиотеки.
- `docs` - архитектурные и продуктовые документы.

## Проекты

- `BatchFlow.Abstractions`
  - базовые контракты и модели;
  - не зависит от runtime, фасада и инфраструктурных пакетов.
- `BatchFlow.Core`
  - execution engine, orchestration flow и runtime-модели выполнения;
  - зависит только от `BatchFlow.Abstractions`.
- `BatchFlow`
  - публичный фасад библиотеки, fluent/builder API и удобные точки входа для потребителя;
  - зависит от `BatchFlow.Core`.
- `BatchFlow.Extensions.DependencyInjection`
  - регистрация engine, часов, настроек запуска и in-memory checkpoint через `IServiceCollection`;
  - зависит от `BatchFlow.Core`, фасада и DI abstractions, не содержит Npgsql или EF.
- `BatchFlow.Postgres.DependencyInjection`
  - scoped/keyed регистрации source/store, singleton NpgsqlDataSource и снимок конфигурации;
  - зависит от `BatchFlow.Postgres`, общего DI-пакета и Configuration.Binder, без EF.
- `BatchFlow.EntityFrameworkCore.DependencyInjection`
  - scoped/keyed регистрация EF-source поверх штатной фабрики контекстов приложения;
  - зависит от `BatchFlow.EntityFrameworkCore` и общего DI-пакета, не выбирает EF-провайдер.
- `BatchFlow.Extensions.Logging`
  - адаптация runtime-событий в `ILogger`;
  - напрямую зависит от `BatchFlow`, получает `Core` транзитивно.
- `BatchFlow.Extensions.Diagnostics`
  - каркас будущих метрик и трассировки, готового API пока нет;
  - зависит от `BatchFlow.Core`.
- `BatchFlow.Postgres`
  - PostgreSQL-адаптеры для источников данных и хранения контрольных точек;
  - зависит от `BatchFlow.Abstractions`, `BatchFlow.Core`, `BatchFlow` и `Npgsql`.
- `BatchFlow.EntityFrameworkCore`
  - LINQ/keyset-source, фабрики контекстов, управление ресурсами и fluent-расширение;
  - зависит от `BatchFlow.Abstractions`, `BatchFlow.Core`, `BatchFlow` и `Microsoft.EntityFrameworkCore`;
  - освобождает контекст на каждую порцию готового source и не выбирает EF-провайдер; базовый класс для собственных источников сохраняет прежний lifetime.
- `BatchFlow.Testing`
  - тестовые реализации, fake-адаптеры и вспомогательные утилиты для сценариев библиотеки;
  - напрямую зависит от `BatchFlow`, `BatchFlow.Core` и `BatchFlow.Abstractions`.

## Примеры и проверки

`src/ConsoleTest` - исполняемый пример с in-memory source, retry, skip, checkpoint и логированием. `samples/BatchFlow.EntityFrameworkCore.Sample` и `samples/BatchFlow.Postgres.Sample` показывают полный цикл DI, retry, оба режима и resume. EF использует SQLite в памяти; PostgreSQL требует отдельную тестовую БД и создает только собственную временную схему. Оба sample-проекта собираются отдельно от solution.

`BatchFlow.Core.Tests` проверяет runtime, builder, retry, in-memory store и logging-адаптер. `BatchFlow.Postgres.Tests` проверяет options, source с подставным reader, serializer и fluent-расширения. В нем также есть категория `PostgresIntegration` для проверок с настоящей БД: [подготовка и запуск](../tests/BatchFlow.Postgres.Tests/Integration/README.md).

`BatchFlow.EntityFrameworkCore.Tests` проверяет готовый EF-source, options и базовые точки расширения: keyset и resume, сортировку, SQL-лимит, владение контекстами, отмену, первичный сбой при ошибках очистки и независимость запусков. Категория `EntityFrameworkCoreIntegration` использует настоящий EF-пул и запросы SQLite в памяти; внешний сервер БД не требуется. Поведение других EF-провайдеров и реальных схем требует отдельных проверок.

Общие настройки в `Directory.Build.props`: `net9.0`, nullable, implicit usings, генерация XML. Порядок изучения и соглашения кода: [архитектурное руководство](07-architecture-and-style.md). Использование библиотеки: [README](../README.md).

`BatchFlow.DependencyInjection.Tests` проверяет lifetimes, keyed-регистрации, snapshot binding, валидацию, native EF-фабрики и владение пулом без подключения к PostgreSQL. Реальный PostgreSQL DI/resume проверяется в категории `PostgresIntegration` существующего PostgreSQL-набора.

## Правила зависимостей

1. `BatchFlow.Abstractions` не должен зависеть от инфраструктурных библиотек и конкретных технологий хранения или транспорта.
2. `BatchFlow.Core` не должен зависеть от `Extensions.*`, adapter-пакетов, `ILogger`, DI, diagnostics и конкретных технологий.
3. `BatchFlow` остается тонким публичным фасадом поверх `BatchFlow.Core` и не должен превращаться в место для инфраструктурной логики.
4. Все интеграционные пакеты строятся поверх `BatchFlow.Core`, а не наоборот.
5. Адаптеры источников данных и checkpoint store должны жить в отдельных пакетах и ссылаться только на необходимые уровни ядра.
6. Тестовые проекты в `tests` должны ссылаться только на те проекты, которые они реально проверяют.
7. `BatchFlow.Postgres` отвечает только за PostgreSQL-специфичную интеграцию и не должен тянуть в себя EF Core, DI, logging и runtime-логику, не связанную с адаптером.
8. `BatchFlow.EntityFrameworkCore` отвечает за EF Core, не ссылается на `BatchFlow.Postgres` и не навязывает провайдер, миграции или транзакции приложения. EF-зависимости не добавляются в ядро.
9. DI-регистрации адаптеров находятся только в отдельных необязательных пакетах. Общий DI-пакет не ссылается на адаптеры; адаптеры не ссылаются на DI-пакеты. Пакет PostgreSQL DI не зависит от EF DI и наоборот.
