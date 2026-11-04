# Интеграционные тесты PostgreSQL

Тесты используют настоящий Npgsql и сервер PostgreSQL, без fake reader и дополнительных тестовых NuGet-зависимостей. Они входят в `BatchFlow.Postgres.Tests` с категорией `PostgresIntegration`.

Без `BATCHFLOW_POSTGRES_TESTS` интеграционные тесты явно пропускаются. Заданная, но недоступная БД приводит к ошибке, а не к ложному успешному результату. Для защиты от случайного запуска на базе приложения имя тестовой БД должно заканчиваться на `_tests`.

## Локальный запуск

Из корня репозитория, используя отдельный временный контейнер:

```sh
docker run --detach --rm --name batchflow-postgres-tests \
  -e POSTGRES_USER=batchflow_test \
  -e POSTGRES_PASSWORD=batchflow_test \
  -e POSTGRES_DB=batchflow_tests \
  -p 127.0.0.1:55432:5432 postgres:17
```

Дождитесь готовности сервера:

```sh
docker exec batchflow-postgres-tests pg_isready -U batchflow_test -d batchflow_tests
```

Затем задайте подключение и выполните проверки:

```sh
export BATCHFLOW_POSTGRES_TESTS='Host=127.0.0.1;Port=55432;Database=batchflow_tests;Username=batchflow_test;Password=batchflow_test'
dotnet test BatchFlow.sln --no-restore
dotnet test tests/BatchFlow.Postgres.Tests/BatchFlow.Postgres.Tests.csproj --no-restore --filter Category=PostgresIntegration
```

Fixture создает уникальную схему `batchflow_test_<guid>`, а после тестов удаляет только ее. Пользователю тестовой БД нужны права создания и удаления собственных схем и таблиц. Тесты не удаляют БД и не очищают чужие таблицы. После аварийного завершения процесса собственная схема может остаться; одноразовый контейнер позволяет убрать ее вместе с тестовой БД:

```sh
docker stop batchflow-postgres-tests
```

## Что проверяется

- Пакетный SQL и resume через строку, data source, фабрику, готовое External- и AdapterOwned-соединение.
- Материализация `jsonb` и `timestamptz` при `SequentialAccess`.
- DDL, загрузка и повторные сохранения store через все разрешенные режимы подключения.
- JSON checkpoint, метаданные, UTC-время и конфликт конкурентных обновлений с одинаковым токеном.
- Полный запуск engine с PostgreSQL checkpoint store и повторный запуск без повторной обработки старых строк.
- Отмена действительно выполняющегося `pg_sleep`, освобождение owned-соединения и повторное получение соединения из пула размером один.
- Возврат соединения в пул после досрочного окончания перечисления.
- Освобождение соединения при ошибке аутентификации с сохранением исключения драйвера.
- Ограничение поздних коммитов при обычном keyset-обходе в Read Committed.
- Стабильный снимок между порциями при внешнем соединении с транзакцией RepeatableRead и видимость новых строк после ее завершения.

В Core отдельно поддерживаются регрессии остановки, первичной ошибки, отмены на границах source/handler/retry/save и стресс-проверки 2000 пакетов при DOP 1, 2 и 8. Это не benchmark и не доказательство поведения при любом сетевом разрыве или аварии PostgreSQL. Показатели throughput и памяти в байтах здесь не измеряются.
