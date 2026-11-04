# Изменения BatchFlowKit

## 0.1.0-preview.1

Первый кандидат на публичный предварительный выпуск. Наличие этой записи не
означает, что версия уже опубликована в NuGet.org.

- Fluent API `RunEachAsync`, `RunBatchAsync`, отложенные `Build`/`RunAsync`.
- Ограниченная очередь, параллелизм и непрерывное подтверждение checkpoint.
- Retry обработчиков, stop/skip и пользовательские политики ошибок.
- In-memory и PostgreSQL checkpoint store, SQL/keyset-источник PostgreSQL.
- EF Core keyset-source с отдельным контекстом на порцию и `FromDbContext`.
- Runtime-события, ILogger, необязательные DI-пакеты и тестовые инструменты.
- Имена NuGet `BatchFlowKit.*`, неизменные C#-пространства имен `BatchFlow.*`.
- MIT, XML-документация и отдельные пакеты символов.

Целевая платформа: `net9.0`. Exactly-once, scheduler, distributed lock и
встроенная отправка в dead-letter не реализованы. Diagnostics остается
каркасом и не включается в выпуск. До стабильной версии возможны изменения API.
