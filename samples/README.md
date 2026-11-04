# Примеры

Полная программа без инфраструктуры находится в [корневом README](../README.md), вариант с SQL и PostgreSQL checkpoint store - в [PostgreSQL-руководстве](../docs/04-postgres.md).

Готовый консольный проект: `dotnet run --project src/ConsoleTest/ConsoleTest.csproj`. Он показывает in-memory source, retry, skip, checkpoint и `ILogger`.

`dotnet run --project samples/BatchFlow.EntityFrameworkCore.Sample` запускает настоящий keyset-source через EF Core и SQLite в памяти. Пример показывает типизированные options, фабрику с пулом, короткий контекст на порцию и повторный запуск с checkpoint. Внешняя БД не нужна; пример не создает постоянных файлов и таблиц приложения. Его проект собирается отдельно от основного solution.

Команды корневого README также позволяют создать `samples/FirstJob` локально для знакомства с библиотекой.
