using BatchFlow.Checkpoints;
using BatchFlow.Core.Execution;
using BatchFlow.Extensions.DependencyInjection.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace BatchFlow.Extensions.DependencyInjection;

/// <summary>
/// Регистрирует общие компоненты BatchFlow без зависимостей от конкретных адаптеров.
/// </summary>
public static class BatchFlowServiceCollectionExtensions
{
    /// <summary>
    /// Добавляет singleton-движок, TimeProvider и transient-настройки запуска. Повторный вызов безопасен;
    /// ранее зарегистрированные пользовательские реализации сохраняются.
    /// </summary>
    /// <param name="services">Коллекция сервисов приложения.</param>
    /// <returns>Ту же коллекцию для продолжения регистрации.</returns>
    /// <remarks>
    /// Статические методы Run не обращаются к DI автоматически. Передайте полученный из DI
    /// BatchJobRunOptions в перегрузку запуска, чтобы использовать зарегистрированные движок и часы.
    /// </remarks>
    public static IServiceCollection AddBatchFlow(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<IBatchJobExecutionEngine, BatchJobExecutionEngine>();
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddTransient(provider => new BatchJobRunOptions
        {
            ExecutionEngine = provider.GetRequiredService<IBatchJobExecutionEngine>(),
            TimeProvider = provider.GetRequiredService<TimeProvider>(),
        });
        return services;
    }

    /// <summary>
    /// Регистрирует конкретное in-memory хранилище как singleton, чтобы checkpoint переживал смену DI-scope.
    /// Данные не переживают завершение процесса. Повторная регистрация того же типа и имени отклоняется.
    /// </summary>
    /// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
    /// <param name="services">Коллекция сервисов.</param>
    /// <param name="name">Необязательный ключ DI; null означает обычную регистрацию.</param>
    /// <returns>Ту же коллекцию сервисов.</returns>
    public static IServiceCollection AddBatchFlowInMemoryCheckpoint<TCheckpoint>(this IServiceCollection services, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        BatchFlowServiceRegistration.AddSingleton(services, _ => new InMemoryCheckpointStore<TCheckpoint>(), name);
        return services;
    }
}
