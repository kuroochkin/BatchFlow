# Примеры

Полная программа без инфраструктуры находится в [корневом README](../README.md), вариант с SQL и PostgreSQL checkpoint store - в [PostgreSQL-руководстве](../docs/04-postgres.md).

Готовый консольный проект: `dotnet run --project src/ConsoleTest/ConsoleTest.csproj`. Он показывает in-memory source, retry, skip, checkpoint и `ILogger`.

В этой папке пока нет самостоятельных исполняемых проектов. Команды README создают `samples/FirstJob` локально для знакомства с библиотекой.
