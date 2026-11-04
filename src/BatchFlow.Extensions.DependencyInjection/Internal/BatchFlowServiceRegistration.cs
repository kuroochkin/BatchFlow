using Microsoft.Extensions.DependencyInjection;

namespace BatchFlow.Extensions.DependencyInjection.Internal;

internal static class BatchFlowServiceRegistration
{
    public static void AddScoped<TService>(IServiceCollection services, Func<IServiceProvider, TService> factory, string? name)
        where TService : class =>
        Add(services, name is null
            ? ServiceDescriptor.Scoped(factory)
            : ServiceDescriptor.KeyedScoped<TService>(name, (provider, _) => factory(provider)), name);

    public static void AddSingleton<TService>(IServiceCollection services, Func<IServiceProvider, TService> factory, string? name)
        where TService : class =>
        Add(services, name is null
            ? ServiceDescriptor.Singleton(factory)
            : ServiceDescriptor.KeyedSingleton<TService>(name, (provider, _) => factory(provider)), name);

    public static void AddInstance<TService>(IServiceCollection services, TService instance, string? name)
        where TService : class =>
        Add(services, name is null
            ? ServiceDescriptor.Singleton(instance)
            : ServiceDescriptor.KeyedSingleton(name, instance), name);

    public static TService Resolve<TService>(IServiceProvider provider, string? name) where TService : notnull =>
        name is null ? provider.GetRequiredService<TService>() : provider.GetRequiredKeyedService<TService>(name);

    public static void ValidateName(string? name)
    {
        if (name is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
        }
    }

    private static void Add(IServiceCollection services, ServiceDescriptor descriptor, string? name)
    {
        ValidateName(name);

        if (services.Any(existing => existing.ServiceType == descriptor.ServiceType
            && existing.IsKeyedService == descriptor.IsKeyedService
            && (!existing.IsKeyedService || Equals(existing.ServiceKey, descriptor.ServiceKey))))
        {
            throw new ArgumentException($"Сервис {descriptor.ServiceType} с именем '{name ?? "по умолчанию"}' уже зарегистрирован. Используйте другое имя или удалите повторную регистрацию.", nameof(services));
        }

        services.Add(descriptor);
    }
}
