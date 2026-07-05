using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Core.Execution.Models;

namespace BatchFlow.Core.Execution.Parallel;

/// <summary>
/// Координирует безопасное продвижение checkpoint в parallel batch.
/// Хранит out-of-order завершения, отслеживает границу сбоя и разрешает
/// коммит только по непрерывной подтвержденной последовательности батчей.
/// </summary>
/// <typeparam name="TItem">Тип элементов внутри batch-а.</typeparam>
/// <typeparam name="TCheckpoint">Тип значения checkpoint.</typeparam>
internal sealed class ParallelBatchCheckpointCoordinator<TItem, TCheckpoint>
{
    private readonly BatchJobRuntimeState<TCheckpoint> _runtime;
    private readonly Func<BatchWorkerCompletedNotification<TItem, TCheckpoint>, CancellationToken, ValueTask<StoredCheckpoint<TCheckpoint>>> _persistCheckpointAsync;
    private readonly Func<BatchWorkerCompletedNotification<TItem, TCheckpoint>, CancellationToken, ValueTask> _publishBatchCompletedAsync;
    private readonly Func<StoredCheckpoint<TCheckpoint>, CancellationToken, ValueTask> _publishCheckpointSavedAsync;
    private readonly SortedDictionary<long, BatchWorkerCompletedNotification<TItem, TCheckpoint>> _completedBatches = [];

    private long _nextCheckpointSequence = 1;
    private long? _stopBeforeSequence;

    /// <summary>
    /// Создает coordinator, который будет продвигать checkpoint и публиковать подтвержденные исходы батчей
    /// </summary>
    public ParallelBatchCheckpointCoordinator(
        BatchJobRuntimeState<TCheckpoint> runtime,
        Func<BatchWorkerCompletedNotification<TItem, TCheckpoint>, CancellationToken, ValueTask<StoredCheckpoint<TCheckpoint>>> persistCheckpointAsync,
        Func<BatchWorkerCompletedNotification<TItem, TCheckpoint>, CancellationToken, ValueTask> publishBatchCompletedAsync,
        Func<StoredCheckpoint<TCheckpoint>, CancellationToken, ValueTask> publishCheckpointSavedAsync)
    {
        _runtime = runtime ?? throw new ArgumentNullException(nameof(runtime));
        _persistCheckpointAsync = persistCheckpointAsync ?? throw new ArgumentNullException(nameof(persistCheckpointAsync));
        _publishBatchCompletedAsync = publishBatchCompletedAsync ?? throw new ArgumentNullException(nameof(publishBatchCompletedAsync));
        _publishCheckpointSavedAsync = publishCheckpointSavedAsync ?? throw new ArgumentNullException(nameof(publishCheckpointSavedAsync));
    }

    /// <summary>
    /// Следующий номер batch-а, который coordinator ожидает подтвердить и закоммитить.
    /// </summary>
    public long NextCheckpointSequence => _nextCheckpointSequence;

    /// <summary>
    /// Количество завершенных batch-ей, которые пока нельзя закоммитить из-за разрыва последовательности.
    /// </summary>
    public int BufferedCompletionCount => _completedBatches.Count;

    /// <summary>
    /// Верхняя граница последовательности, после которой коммиты больше запрещены из-за terminal-сбоя.
    /// </summary>
    public long? StopBeforeSequence => _stopBeforeSequence;

    /// <summary>
    /// Пытается принять завершенный batch в окно ожидания коммита.
    /// Возвращает <see langword="false" />, если batch уже лежит за границей terminal-сбоя и больше не может
    /// участвовать в продвижении checkpoint.
    /// </summary>
    public bool TryTrackCompletion(BatchWorkerCompletedNotification<TItem, TCheckpoint> completedBatch)
    {
        ArgumentNullException.ThrowIfNull(completedBatch);

        if (_stopBeforeSequence is not null && completedBatch.BufferedBatch.Sequence >= _stopBeforeSequence.Value)
        {
            return false;
        }

        _completedBatches[completedBatch.BufferedBatch.Sequence] = completedBatch;
        return true;
    }

    /// <summary>
    /// Регистрирует terminal-сбой и запрещает дальнейшее продвижение checkpoint начиная с указанного номера batch-а.
    /// </summary>
    public void RegisterTerminalFailure(long failedBatchSequence)
    {
        if (failedBatchSequence <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(failedBatchSequence));
        }

        _stopBeforeSequence = _stopBeforeSequence is null
            ? failedBatchSequence
            : Math.Min(_stopBeforeSequence.Value, failedBatchSequence);

        PruneUncommittableCompletions();
    }

    /// <summary>
    /// Пытается продвинуть checkpoint настолько далеко, насколько это разрешает непрерывная подтвержденная последовательность.
    /// </summary>
    public async ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        while (_completedBatches.TryGetValue(_nextCheckpointSequence, out var completedBatch)
               && (_stopBeforeSequence is null || _nextCheckpointSequence < _stopBeforeSequence.Value))
        {
            _runtime.BatchSequence = completedBatch.BufferedBatch.Sequence;
            _runtime.LastPersistedCheckpoint = await _persistCheckpointAsync(completedBatch, cancellationToken);

            var persistedCheckpoint = _runtime.LastPersistedCheckpoint
                ?? throw new InvalidOperationException(
                    "Coordinator не получил сохраненную контрольную точку после подтвержденного коммита batch-а.");

            await _publishBatchCompletedAsync(completedBatch, cancellationToken);
            await _publishCheckpointSavedAsync(persistedCheckpoint, cancellationToken);

            _completedBatches.Remove(_nextCheckpointSequence);
            _nextCheckpointSequence++;
        }
    }

    private void PruneUncommittableCompletions()
    {
        if (_stopBeforeSequence is null || _completedBatches.Count == 0)
        {
            return;
        }

        var rejectedSequences = _completedBatches.Keys
            .Where(sequence => sequence >= _stopBeforeSequence.Value)
            .ToArray();

        foreach (var rejectedSequence in rejectedSequences)
        {
            _completedBatches.Remove(rejectedSequence);
        }
    }
}