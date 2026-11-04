# Архитектура, порядок изучения и стиль

[Оглавление](README.md) · [Далее: разбор проекта](08-review.md)

## Слои

`Abstractions` задает контракты. `Core` выполняет их. `BatchFlow` предоставляет fluent API поверх runtime. Инфраструктурные проекты реализуют контракты и подключают свои удобные методы к фасаду.

| Проект | Ответственность | Зависимости проектов |
| --- | --- | --- |
| `BatchFlow.Abstractions` | Источники, обработчики, checkpoint, ошибки, события, модели | Нет |
| `BatchFlow.Core` | Execution engine, очереди, workers, coordinator | `Abstractions` |
| `BatchFlow` | Builder, запуск, политика ошибок, in-memory store | `Core`, `Abstractions` |
| `BatchFlow.Postgres` | SQL-чтение и PostgreSQL checkpoint | `Abstractions`, `Core`, `BatchFlow` |
| `BatchFlow.EntityFrameworkCore` | LINQ/keyset-source и жизненный цикл `DbContext` | `Abstractions`, `Core`, `BatchFlow` |
| `BatchFlow.Extensions.Logging` | Runtime-события -> `ILogger` | `BatchFlow` |
| `BatchFlow.Testing` | Doubles и фиксация вызовов | `Abstractions`, `Core`, `BatchFlow` |
| `BatchFlow.Extensions.DependencyInjection` | Пока каркас | `Core` |
| `BatchFlow.Extensions.Diagnostics` | Пока каркас | `Core` |

`Npgsql` находится в PostgreSQL-проекте, logging abstractions - в logging-проекте. Core не должен ссылаться на них. `ConsoleTest` - исполняемый пример, а не обязательный пакет пользователя.

`Microsoft.EntityFrameworkCore` находится только в EF-адаптере. Пакет не выбирает провайдер БД и не использует PostgreSQL-адаптер. Готовый `EfCoreBatchSource` реализует `IBatchSource` напрямую и освобождает контекст на каждую порцию; `EntityFrameworkCoreBatchSourceBase` сохраняет lifetime на перечисление для собственных наследников. [Контракты и ограничения](11-ef-core-source.md).

## Путь одного запуска

1. `BatchJob.Named` создает стадию выбора source.
2. `From` выбирает возобновляемую или невозобновляемую ветку builder.
3. `With...` возвращает новое состояние, `Process...` регистрирует обработчик.
4. `Build` создает `BatchJobDefinition`; `Run...` использует стандартный или переданный engine.
5. Engine проверяет конфигурацию, создает контекст и загружает checkpoint.
6. `BufferedBatchChannel` перечисляет source и присваивает пакетам номера текущего запуска.
7. Последовательный контур либо batch-workers обрабатывают данные. Item-workers работают внутри одного пакета.
8. Runtime сохраняет подтвержденную позицию, публикует события и возвращает summary.

У `PostgresBatchSourceBase` общий lifetime соединения и проверка непустых пакетов. `PostgresBatchSource` реализует повторные SQL-запросы, параметризацию и материализацию. `ReadNextBatchAsync` освобождает command и reader до выдачи материализованного пакета. Connection остается у source до завершения перечисления.

Отдельный base-класс уже является частью публичного API. Его развитие стоит связывать с реальными дополнительными source; само наличие базового класса не означает, что нужны новые типы под каждый вариант SQL.

## Порядок изучения исходников

1. `BatchFlow.Abstractions/Sources`, `Execution`, `Results`: данные, контексты и исходы.
2. `Checkpoints`, `Failures`, `Observability`: сохранение, решения по ошибкам и события.
3. `BatchFlow/BatchJob.cs`, `Builders/IBatchJobSourceSelectionStage.cs` и остальные builder-интерфейсы: пользовательский API.
4. `Builders/Internal/BatchJobBuilderState`, `BatchJobBuilderValidation`, `BatchJobBuilder`: неизменяемое конфигурирование.
5. `BatchFlow/Execution`, `Failures`: удобные запуски и встроенная политика.
6. `BatchFlow.Core/Jobs`, `Processing`, `Execution/Models`: входы движка.
7. `BatchJobExecutionEngine.ExecuteAsync`, затем последовательный и параллельный контуры.
8. `Execution/Buffering`, `Execution/Parallel`: producer, notifications и coordinator.
9. `BatchFlow.Postgres/Options`, `Sources`, `Internal`, `Checkpoints`, `Extensions`: адаптер.
10. Logging-адаптер и тесты рядом с изученным поведением.
11. `EfCoreBatchSourceOptions`, `EfCoreQueryShapeValidator`, `EfCoreBatchSource`, затем fluent-перегрузки и SQLite-тесты: готовый EF-адаптер. Старый базовый класс изучается отдельно.

## Существующий код-стайл

Это описание наблюдаемых соглашений и ориентир для новых изменений. `.editorconfig`, зафиксированного набора analyzer-правил и автоматической проверки форматирования пока нет.

- File-scoped namespaces, отступ в четыре пробела, скобки на отдельных строках.
- `PascalCase` для типов, методов и свойств; `camelCase` для параметров и переменных; `_camelCase` для приватных полей.
- `I` для интерфейсов, `TItem` / `TCheckpoint` для generic-параметров, `Async` для асинхронных методов.
- XML публичного API на русском с описанием смысла, ограничений и владения ресурсами. Идентификаторы и названия технологий сохраняются.
- Records с `required` / `init` для данных и настроек; закрытые классы для конечных реализаций, отдельные extension-классы для sugar API.
- `ValueTask` в контрактах, `IAsyncEnumerable` для source, `CancellationToken` последним параметром.
- Ранние проверки через `ThrowIfNull`, `ThrowIfNullOrWhiteSpace` и проверки диапазонов.
- Внутренние механизмы остаются `internal`; тесты получают доступ через `InternalsVisibleTo`.
- Понятные приватные методы для сложных веток и блоки в объемных `switch case`.

Есть неоднородность переносов сигнатур, порядка `using`, терминологии XML и `ConfigureAwait(false)`. В тестах смешаны имена `ExecuteAsync_With...` и `Build_should_...`. Новые изменения должны учитывать соседний файл; унификацию удобнее делать отдельной задачей с автоматической проверкой.

`while (true)` само по себе не является ошибкой. В обсужденном стиле команды предпочтительнее явное условие завершения, если оно делает алгоритм понятнее. Для новых циклов сначала определяйте условие остановки; не заменяйте алгоритм фиктивным флагом только ради формы записи.

## Развитие библиотеки

Добавляя поведение runtime, сначала определите семантику успеха, пропуска, сбоя, отмены и сохранения прогресса. Затем добавляйте контракт, реализацию и тест наблюдаемого результата. Драйвер БД, логгер и DI-контейнер остаются за пределами Core.

Оптимизации проверяйте на воспроизводимом сценарии. Уменьшение аллокаций не должно усложнять обработку ошибок или размывать границу checkpoint. Приоритетные проблемы перечислены в [разборе проекта](08-review.md).
