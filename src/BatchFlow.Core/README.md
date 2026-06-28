# BatchFlow.Core

Runtime-слой библиотеки `BatchFlow`.

Содержит:

- описание пакетной задачи через `BatchJobDefinition<TItem, TCheckpoint>`;
- execution engine через `IBatchJobExecutionEngine` и `BatchJobExecutionEngine`;
- runtime-модели выполнения и итогов запуска;
- режимы обработки по пакету и по элементу.

Проект зависит только от `BatchFlow.Abstractions` и не должен зависеть от:

- `ILogger`;
- `IServiceCollection`;
- OpenTelemetry;
- конкретных баз данных, брокеров сообщений, файловых API и HTTP-клиентов.

Текущее состояние проекта:

- checkpoint/resume поддерживается только для источников, которые явно умеют возобновляться;
- source-адаптеры не должны возвращать пустые пакеты; завершение чтения выражается окончанием последовательности;
- execution engine пока выполняет обработку последовательно;
- retry, skip, stop и suspend уже входят в каркас runtime;
- dead-letter сценарии пока не реализованы и будут добавлены отдельным инкрементом.
