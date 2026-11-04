# BatchFlow.EntityFrameworkCore.DependencyInjection

Необязательная регистрация scoped `EfCoreBatchSource` через `AddBatchFlowEfCoreSource<TContext, TItem, TCheckpoint>`, без Npgsql и выбора EF-провайдера.

Приложение предварительно регистрирует `IDbContextFactory<TContext>` через штатный `AddDbContextFactory`, `AddPooledDbContextFactory` или собственную реализацию. Configure получает текущий scope и настройки выбранной фабрики; переданный `Context` нельзя подменять. `factoryName` выбирает keyed-фабрику, `name` задает ключ source.

Никаких скрытых миграций, контекстов на весь runtime и service locator внутри builder. В пользовательском сервисе внедрите конкретный source и подключите его через `.FromDbContext(source)`. Scope должен существовать до окончания job.

[Руководство](../../docs/12-dependency-injection.md) · [Полный EF-пример](../../samples/BatchFlow.EntityFrameworkCore.Sample/Program.cs).
