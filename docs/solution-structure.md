# Solution structure

## Каталоги

- `src` - исходный код библиотек и adapter-пакетов.
- `tests` - unit-, contract- и integration-тесты.
- `samples` - примеры использования библиотеки.
- `docs` - архитектурные и продуктовые документы.

## Проекты

- `BatchFlow.Abstractions`
  - базовые контракты и модели;
  - не зависит от core и инфраструктурных пакетов.
- `BatchFlow`
  - основной runtime, orchestration и builder API;
  - зависит только от `BatchFlow.Abstractions`.
- `BatchFlow.Extensions.DependencyInjection`
  - интеграция с `IServiceCollection`;
  - зависит от `BatchFlow`.
- `BatchFlow.Extensions.Logging`
  - адаптация runtime-событий в `ILogger`;
  - зависит от `BatchFlow`.
- `BatchFlow.Extensions.Diagnostics`
  - метрики и tracing поверх runtime;
  - зависит от `BatchFlow`.
- `BatchFlow.Testing`
  - тестовые реализации и вспомогательные утилиты;
  - зависит от `BatchFlow` и `BatchFlow.Abstractions`.

## Правила зависимостей

1. `BatchFlow.Abstractions` не должен зависеть от инфраструктурных библиотек и конкретных технологий хранения или транспорта.
2. `BatchFlow` не должен зависеть от `Extensions.*`, adapter-пакетов, `ILogger`, DI или diagnostics.
3. Все интеграционные пакеты строятся поверх `BatchFlow`, а не наоборот.
4. Адаптеры источников данных и checkpoint store должны жить в отдельных пакетах и ссылаться только на необходимые уровни core.
5. Тестовые проекты в `tests` должны ссылаться только на те проекты, которые они реально проверяют.
