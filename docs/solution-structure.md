# Solution structure

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
  - интеграция с `IServiceCollection`;
  - зависит от `BatchFlow.Core` и не затягивает зависимость обратно в runtime.
- `BatchFlow.Extensions.Logging`
  - адаптация runtime-событий в `ILogger`;
  - зависит от `BatchFlow.Core`.
- `BatchFlow.Extensions.Diagnostics`
  - метрики и tracing поверх runtime;
  - зависит от `BatchFlow.Core`.
- `BatchFlow.Postgres`
  - PostgreSQL-адаптеры для источников данных и хранения контрольных точек;
  - зависит от `BatchFlow.Abstractions`, `BatchFlow.Core`, `BatchFlow` и `Npgsql`.
- `BatchFlow.Testing`
  - тестовые реализации, fake-адаптеры и вспомогательные утилиты для сценариев библиотеки;
  - зависит от `BatchFlow.Core` и `BatchFlow.Abstractions`.

## Правила зависимостей

1. `BatchFlow.Abstractions` не должен зависеть от инфраструктурных библиотек и конкретных технологий хранения или транспорта.
2. `BatchFlow.Core` не должен зависеть от `Extensions.*`, adapter-пакетов, `ILogger`, DI, diagnostics и конкретных технологий.
3. `BatchFlow` остается тонким публичным фасадом поверх `BatchFlow.Core` и не должен превращаться в место для инфраструктурной логики.
4. Все интеграционные пакеты строятся поверх `BatchFlow.Core`, а не наоборот.
5. Адаптеры источников данных и checkpoint store должны жить в отдельных пакетах и ссылаться только на необходимые уровни ядра.
6. Тестовые проекты в `tests` должны ссылаться только на те проекты, которые они реально проверяют.
7. `BatchFlow.Postgres` отвечает только за PostgreSQL-специфичную интеграцию и не должен тянуть в себя EF Core, DI, logging и runtime-логику, не связанную с адаптером.
