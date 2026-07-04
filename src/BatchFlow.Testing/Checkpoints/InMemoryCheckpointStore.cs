using BatchFlow.Abstractions.Checkpoints;

namespace BatchFlow.Testing.Checkpoints;

/// <summary>
/// Представляет тестовое in-memory хранилище контрольных точек с возможностью наблюдать вызовы загрузки и сохранения.
/// </summary>
/// <typeparam name="TCheckpoint">Тип значения контрольной точки.</typeparam>
public sealed class InMemoryCheckpointStore<TCheckpoint> : ICheckpointStore<TCheckpoint>
{
    private readonly BatchFlow.Checkpoints.InMemoryCheckpointStore<TCheckpoint> _innerStore = new();
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
        _innerStore.Seed(checkpointId, checkpoint);
    }

    /// <summary>
    /// Пытается вернуть текущее сохраненное состояние по указанному идентификатору.
    /// </summary>
    public bool TryGet(string checkpointId, out StoredCheckpoint<TCheckpoint>? checkpoint)
    {
        return _innerStore.TryGet(checkpointId, out checkpoint);
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

        return await _innerStore.LoadAsync(checkpointId, cancellationToken);
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
            _innerStore.Seed(checkpointId, saved);
            _savedCheckpoints.Add(saved);
            return saved;
        }

        var savedCheckpoint = await _innerStore.SaveAsync(
            checkpointId,
            checkpoint,
            expectedConcurrencyToken,
            cancellationToken);

        _savedCheckpoints.Add(savedCheckpoint);
        return savedCheckpoint;
    }
}
