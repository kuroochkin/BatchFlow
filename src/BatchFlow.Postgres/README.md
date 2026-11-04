# BatchFlow.Postgres

Инфраструктурный пакет PostgreSQL для `BatchFlow`.

Полный пример SQL, подключений и восстановления: [руководство по PostgreSQL](../../docs/04-postgres.md). Известные ограничения и необходимые интеграционные проверки: [разбор проекта](../../docs/08-review.md).

Необязательный `BatchFlow.Postgres.DependencyInjection` добавляет регистрации пула, source/store и snapshot binding, не меняя зависимости этого адаптера. [DI и lifetimes](../../docs/12-dependency-injection.md) · [Полная исполняемая программа](../../samples/BatchFlow.Postgres.Sample/Program.cs).

Содержит:

- базовые точки расширения для источников данных поверх PostgreSQL;
- `PostgresBatchSource<TItem, TCheckpoint>` для чтения пакетов по SQL-запросу;
- реализацию `checkpoint store` на PostgreSQL;
- унифицированную модель подключения через `connection string`, `NpgsqlDataSource` или фабрику `NpgsqlConnection`;
- поддержку работы с готовым `NpgsqlConnection` с явной моделью владения ресурсом;
- sugar-методы для fluent-конфигурации pipeline.

Готовое соединение с владением `AdapterOwned` допускается только для одного перечисления source. Checkpoint store отклоняет его при создании: для повторных операций используйте строку подключения, `NpgsqlDataSource`, фабрику или отдельное открытое соединение с владением `External`. Флаг `ReaderBehavior.CloseConnection` запрещен, поскольку соединение должно оставаться открытым между порциями.

Пакет отвечает за:

- интеграцию с `Npgsql`;
- управление подключением на уровне адаптера;
- SQL-специфичные детали чтения и хранения checkpoint-данных;
- удобные публичные входные точки для потребителя библиотеки.

Пакет не отвечает за:

- выполнение batch job и orchestration flow;
- retry, terminal-политики и checkpoint coordination;
- привязку к `ILogger`, `IServiceCollection` и OpenTelemetry;
- EF Core и ORM-специфичную модель доступа к данным.

Базовая структура проекта:

- `Sources` - PostgreSQL-источники и общие базовые классы для чтения данных;
- `Checkpoints` - хранение контрольных точек в PostgreSQL;
- `Options` - опции подключения и конфигурации адаптера;
- `Extensions` - fluent/sugar-методы для потребителя пакета;
- `Internal` - внутренние вспомогательные механизмы, которые не должны становиться частью public API.
