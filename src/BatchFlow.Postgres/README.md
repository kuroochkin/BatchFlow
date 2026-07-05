# BatchFlow.Postgres

Инфраструктурный пакет PostgreSQL для `BatchFlow`.

Содержит:

- базовые точки расширения для источников данных поверх PostgreSQL;
- реализацию `checkpoint store` на PostgreSQL;
- унифицированную модель подключения через `connection string`, `NpgsqlDataSource` или фабрику `NpgsqlConnection`;
- sugar-методы для fluent-конфигурации pipeline.

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
