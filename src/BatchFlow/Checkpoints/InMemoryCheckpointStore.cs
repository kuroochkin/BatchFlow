using BatchFlow.Abstractions.Checkpoints;

namespace BatchFlow.Checkpoints;

/// <summary>
/// Представляет in-memory хранилище контрольных точек, подходящее для локальной разработки,
/// примеров и сценариев без внешней инфраструктуры.
/// </summary>
/// <typeparam name="TCheckpoint">Тип значения контрольной точки.</typeparam>
public sealed class InMemoryCheckpointStore<TCheckpoint> : ICheckpointStore<TCheckpoint>
{
    private readonly Dictionary<string, StoredCheckpoint<TCheckpoint>> _storage = new(StringComparer.Ordinal);
    private readonly Lock _syncRoot = new();

    /// <summary>
    /// Помещает контрольную точку в хранилище до начала выполнения сценария.
    /// Метод полезен для локального воспроизведения restart-сценариев и примеров использования.
    /// </summary>
    /// <param name="checkpointId">Логический идентификатор потока контрольных точек.</param>
    /// <param name="checkpoint">Контрольная точка, которую нужно считать уже сохраненной.</param>
    public void Seed(string checkpointId, StoredCheckpoint<TCheckpoint> checkpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);
        ArgumentNullException.ThrowIfNull(checkpoint);

        lock (_syncRoot)
        {
            _storage[checkpointId] = CloneCheckpoint(checkpoint);
        }
    }

    /// <summary>
    /// Пытается вернуть контрольную точку без асинхронного API.
    /// Метод удобен для локальной диагностики и примеров.
    /// </summary>
    /// <param name="checkpointId">Логический идентификатор потока контрольных точек.</param>
    /// <param name="checkpoint">Найденная контрольная точка или <see langword="null" />, если запись отсутствует.</param>
    /// <returns><see langword="true" />, если точка найдена; иначе <see langword="false" />.</returns>
    public bool TryGet(string checkpointId, out StoredCheckpoint<TCheckpoint>? checkpoint)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);

        lock (_syncRoot)
        {
            if (_storage.TryGetValue(checkpointId, out var storedCheckpoint))
            {
                checkpoint = CloneCheckpoint(storedCheckpoint);
                return true;
            }
        }

        checkpoint = null;
        return false;
    }

    /// <inheritdoc />
    public ValueTask<StoredCheckpoint<TCheckpoint>?> LoadAsync(string checkpointId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_syncRoot)
        {
            return ValueTask.FromResult<StoredCheckpoint<TCheckpoint>?>(
                _storage.TryGetValue(checkpointId, out var checkpoint)
                    ? CloneCheckpoint(checkpoint)
                    : null);
        }
    }

    /// <inheritdoc />
    public ValueTask<StoredCheckpoint<TCheckpoint>> SaveAsync(
        string checkpointId,
        StoredCheckpoint<TCheckpoint> checkpoint,
        string? expectedConcurrencyToken,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);
        ArgumentNullException.ThrowIfNull(checkpoint);
        cancellationToken.ThrowIfCancellationRequested();

        lock (_syncRoot)
        {
            if (_storage.TryGetValue(checkpointId, out var current))
            {
                if (!string.Equals(current.ConcurrencyToken, expectedConcurrencyToken, StringComparison.Ordinal))
                {
                    throw new CheckpointConflictException(checkpointId);
                }
            }
            else if (expectedConcurrencyToken is not null)
            {
                throw new CheckpointConflictException(checkpointId);
            }

            var savedCheckpoint = CloneCheckpoint(
                checkpoint with
                {
                    ConcurrencyToken = Guid.NewGuid().ToString("N"),
                });

            _storage[checkpointId] = savedCheckpoint;
            return ValueTask.FromResult(CloneCheckpoint(savedCheckpoint));
        }
    }

    private static StoredCheckpoint<TCheckpoint> CloneCheckpoint(StoredCheckpoint<TCheckpoint> checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);

        return checkpoint with
        {
            Metadata = CloneMetadata(checkpoint.Metadata),
        };
    }

    private static IReadOnlyDictionary<string, string> CloneMetadata(IReadOnlyDictionary<string, string>? metadata)
    {
        if (metadata is null || metadata.Count == 0)
        {
            return new Dictionary<string, string>(StringComparer.Ordinal);
        }

        return new Dictionary<string, string>(metadata, StringComparer.Ordinal);
    }
}
