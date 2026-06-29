using BatchFlow.Abstractions.Checkpoints;

namespace BatchFlow.Testing.Checkpoints;

/// <summary>
/// Представляет тестовое in-memory хранилище контрольных точек с возможностью наблюдать вызовы загрузки и сохранения.
/// </summary>
/// <typeparam name="TCheckpoint">Тип значения контрольной точки.</typeparam>
public sealed class InMemoryCheckpointStore<TCheckpoint> : ICheckpointStore<TCheckpoint>
{
    private readonly Dictionary<string, StoredCheckpoint<TCheckpoint>> _storage = new(StringComparer.Ordinal);
    private readonly List<CheckpointSaveRequest<TCheckpoint>> _saveRequests = [];
    private readonly List<StoredCheckpoint<TCheckpoint>> _savedCheckpoints = [];

    /// <summary>
    /// Пользовательская логика загрузки контрольной точки, если тесту нужно переопределить стандартное поведение.
    /// </summary>
    public Func<string, CancellationToken, ValueTask<StoredCheckpoint<TCheckpoint>?>>? OnLoadAsync { get; init; }

    /// <summary>
    /// Пользовательская логика сохранения контрольной точки, если тесту нужно переопределить стандартное поведение.
    /// </summary>
    public Func<CheckpointSaveRequest<TCheckpoint>, CancellationToken, ValueTask<StoredCheckpoint<TCheckpoint>>>? OnSaveAsync { get; init; }

    /// <summary>
    /// Количество вызовов загрузки контрольной точки.
    /// </summary>
    public int LoadCallCount { get; private set; }

    /// <summary>
    /// Количество вызовов сохранения контрольной точки.
    /// </summary>
    public int SaveCallCount => _saveRequests.Count;

    /// <summary>
    /// Возвращает историю запросов на сохранение контрольных точек.
    /// </summary>
    public IReadOnlyList<CheckpointSaveRequest<TCheckpoint>> SaveRequests => _saveRequests;

    /// <summary>
    /// Возвращает историю фактически сохраненных значений контрольных точек.
    /// </summary>
    public IReadOnlyList<StoredCheckpoint<TCheckpoint>> SavedCheckpoints => _savedCheckpoints;

    /// <summary>
    /// Помещает контрольную точку в тестовое хранилище до начала сценария.
    /// </summary>
    /// <param name="checkpointId">Логический идентификатор потока контрольных точек.</param>
    /// <param name="checkpoint">Контрольная точка, которую нужно считать уже сохраненной.</param>
    public void Seed(string checkpointId, StoredCheckpoint<TCheckpoint> checkpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);
        ArgumentNullException.ThrowIfNull(checkpoint);

        _storage[checkpointId] = checkpoint;
    }

    /// <summary>
    /// Пытается вернуть текущее сохраненное состояние по указанному идентификатору.
    /// </summary>
    public bool TryGet(string checkpointId, out StoredCheckpoint<TCheckpoint>? checkpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);

        return _storage.TryGetValue(checkpointId, out checkpoint);
    }

    /// <inheritdoc />
    public async ValueTask<StoredCheckpoint<TCheckpoint>?> LoadAsync(string checkpointId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);

        LoadCallCount++;

        if (OnLoadAsync is not null)
        {
            return await OnLoadAsync(checkpointId, cancellationToken);
        }

        return _storage.TryGetValue(checkpointId, out var checkpoint)
            ? checkpoint
            : null;
    }

    /// <inheritdoc />
    public async ValueTask<StoredCheckpoint<TCheckpoint>> SaveAsync(
        string checkpointId,
        StoredCheckpoint<TCheckpoint> checkpoint,
        string? expectedConcurrencyToken,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);
        ArgumentNullException.ThrowIfNull(checkpoint);

        var request = new CheckpointSaveRequest<TCheckpoint>
        {
            CheckpointId = checkpointId,
            Checkpoint = checkpoint,
            ExpectedConcurrencyToken = expectedConcurrencyToken,
        };

        _saveRequests.Add(request);

        if (OnSaveAsync is not null)
        {
            var saved = await OnSaveAsync(request, cancellationToken);
            _storage[checkpointId] = saved;
            _savedCheckpoints.Add(saved);
            return saved;
        }

        if (_storage.TryGetValue(checkpointId, out var current) &&
            !string.Equals(current.ConcurrencyToken, expectedConcurrencyToken, StringComparison.Ordinal))
        {
            throw new CheckpointConflictException(checkpointId);
        }

        if (!_storage.ContainsKey(checkpointId) && expectedConcurrencyToken is not null)
        {
            throw new CheckpointConflictException(checkpointId);
        }

        var savedCheckpoint = checkpoint with
        {
            ConcurrencyToken = Guid.NewGuid().ToString("N"),
        };

        _storage[checkpointId] = savedCheckpoint;
        _savedCheckpoints.Add(savedCheckpoint);
        return savedCheckpoint;
    }
}
