# Тесты

Текущие проверки выполняются командой `dotnet test BatchFlow.sln` из корня репозитория.

Существующий контур:

- `BatchFlow.Core.Tests` - execution engine, builder, retry, checkpoint coordination, in-memory store и логирование;
- `BatchFlow.Postgres.Tests` - options, materialization, keyset-переходы, serializer и fluent-расширения;
- `BatchFlow.EntityFrameworkCore.Tests` - фабрики и владение `DbContext`, LINQ/keyset и составные позиции, SQL-лимит, короткий контекст на порцию, отмена, первичный сбой, конкурентные чтения и restart; категория `EntityFrameworkCoreIntegration` проверяет настоящий EF-пул и запросы SQLite в памяти без внешней БД;
- `BatchFlow.Testing` в `src` - переиспользуемые doubles для тестов библиотеки и приложений.

Модульные PostgreSQL-тесты используют подставной reader. В том же проекте есть отдельная категория `PostgresIntegration` для настоящей БД: [подготовка и запуск](BatchFlow.Postgres.Tests/Integration/README.md). Без переменной `BATCHFLOW_POSTGRES_TESTS` эта категория явно пропускается. Core содержит управляемые конкурентные регрессии и стресс-проверки ограниченного окна на 2000 пакетах; это не нагрузочный benchmark. Как тестировать пользовательские задачи: [руководство](../docs/06-extending-and-testing.md).
