namespace BatchFlow.Core.Execution.Exceptions;

/// <summary>
/// Представляет ошибку, возникшую при публикации runtime-события одному из наблюдателей.
/// </summary>
internal sealed class BatchJobObserverException(Type observerType, Type eventType, Exception innerException)
    : Exception($"Наблюдатель '{observerType.FullName}' завершился исключением при обработке события '{eventType.FullName}'.",
        innerException)
{
    public Type ObserverType { get; } = observerType;

    public Type EventType { get; } = eventType;
}
