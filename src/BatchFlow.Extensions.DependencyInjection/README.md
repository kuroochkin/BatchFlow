# BatchFlow.Extensions.DependencyInjection

Общий необязательный DI-слой без Npgsql, EF Core и конкретного хранилища БД.

`services.AddBatchFlow()` добавляет стандартный singleton engine, `TimeProvider` и transient `BatchJobRunOptions`, сохраняя пользовательские регистрации. Передавайте `runOptions` в `RunEachAsync`/`RunBatchAsync` явно: статические методы не используют глобальный контейнер.

`services.AddBatchFlowInMemoryCheckpoint<TCheckpoint>(name)` регистрирует конкретный singleton store, переживающий смену scope, но не перезапуск процесса. `name` необязателен; повтор того же типа/имени отклоняется.

Адаптеры регистрируются отдельными пакетами `BatchFlow.Postgres.DependencyInjection` и `BatchFlow.EntityFrameworkCore.DependencyInjection`. [Время жизни, API и примеры](../../docs/12-dependency-injection.md).
