using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;

namespace BatchFlow.Testing.Sources;

/// <summary>
/// Представляет тестовый источник данных, основанный на делегате.
/// </summary>
/// <typeparam name="TItem">Тип элементов, которые возвращает источник.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
public sealed class DelegateBatchSource<TItem, TCheckpoint> : IBatchSource<TItem, TCheckpoint>
{
    private readonly Func<BatchSourceContext<TCheckpoint>, CancellationToken, IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>>> _readAsync;
    private readonly List<BatchSourceContext<TCheckpoint>> _capturedContexts = [];

    /// <summary>
    /// Создает тестовый источник данных.
    /// </summary>
    /// <param name="readAsync">Делегат чтения пакетов.</param>
    /// <param name="supportsResume">Признак поддержки возобновления чтения по контрольной точке.</param>
    public DelegateBatchSource(
        Func<BatchSourceContext<TCheckpoint>, CancellationToken, IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>>> readAsync,
        bool supportsResume = true)
    {
        _readAsync = readAsync ?? throw new ArgumentNullException(nameof(readAsync));
        SupportsResume = supportsResume;
    }

    /// <summary>
    /// Возвращает все контексты чтения, с которыми источник был вызван в тесте.
    /// </summary>
    public IReadOnlyList<BatchSourceContext<TCheckpoint>> CapturedContexts => _capturedContexts;

    /// <inheritdoc />
    public bool SupportsResume { get; }

    /// <inheritdoc />
    public IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> ReadAsync(
        BatchSourceContext<TCheckpoint> context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);

        _capturedContexts.Add(context);
        return _readAsync(context, cancellationToken);
    }
}
