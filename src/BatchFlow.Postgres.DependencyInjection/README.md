# BatchFlow.Postgres.DependencyInjection

Необязательные расширения `IServiceCollection` поверх PostgreSQL-адаптера и общего DI-пакета, без EF Core.

- `AddBatchFlowPostgresDataSource` регистрирует singleton pool: контейнер владеет созданным из строки пулом, но не переданным экземпляром.
- `AddBatchFlowPostgresSource` и `AddBatchFlowPostgresCheckpoint` регистрируют конкретные scoped-сервисы, поддерживают явно именованные зависимости и keyed-регистрации.
- Привязка конфигурации сохраняет снимок строки подключения и `PostgresCheckpointSettings`, без горячей смены подключения.

Регистрации не открывают БД и не выполняют DDL. Готовое соединение в DI отклоняется; используйте пул, строку или фабрику новых соединений. `EnsureStorageAsync` остается явной операцией приложения. Source/store не получают автоматические interface aliases.

[Руководство и правила владения](../../docs/12-dependency-injection.md) · [Исполняемый SQL-пример](../../samples/BatchFlow.Postgres.Sample/Program.cs).
