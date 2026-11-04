using System.Threading.Channels;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Core.Jobs;
using BatchFlow.Core.Processing;

namespace BatchFlow.Core.Execution.Buffering;

/// <summary>
/// Представляет bounded channel между чтением batch-ей из источника и их дальнейшей обработкой.
/// </summary>
/// <typeparam name="TItem">Тип элементов внутри пакета.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
internal sealed class BufferedBatchChannel<TItem, TCheckpoint> : IAsyncDisposable
{
    private readonly CancellationTokenSource _producerCancellationSource;
    private readonly Task _producerTask;
    private readonly SemaphoreSlim _inFlightSlots;
    private bool _isCompleted;

    private BufferedBatchChannel(
        ChannelReader<BufferedSourceBatch<TItem, TCheckpoint>> reader,
        Task producerTask,
        CancellationTokenSource producerCancellationSource,
        SemaphoreSlim inFlightSlots)
    {
        Reader = reader;
        _producerTask = producerTask;
        _producerCancellationSource = producerCancellationSource;
        _inFlightSlots = inFlightSlots;
    }

    /// <summary>
    /// Reader bounded channel-а, из которого coordinator или worker-ы получают подготовленные batch-и.
    /// </summary>
    public ChannelReader<BufferedSourceBatch<TItem, TCheckpoint>> Reader { get; }

    /// <summary>
    /// Запускает producer, который читает batch-и из источника и публикует их в bounded channel.
    /// Фабрика последовательности получает рабочий токен producer, отменяемый и при внутренней остановке.
    /// </summary>
    public static BufferedBatchChannel<TItem, TCheckpoint> Start(
        BatchJobDefinition<TItem, TCheckpoint> job,
        Func<CancellationToken, IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>>> readBatches,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(readBatches);

        var producerCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var channel = Channel.CreateBounded<BufferedSourceBatch<TItem, TCheckpoint>>(
            new BoundedChannelOptions(job.PrefetchCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = true,
                AllowSynchronousContinuations = false,
            });

        var workerCount = job.ProcessingMode is BatchProcessorRegistration<TItem, TCheckpoint>
            ? job.DegreeOfParallelism
            : 1;

        var windowSize = (int)Math.Min(int.MaxValue, (long)job.PrefetchCapacity + workerCount);
        var inFlightSlots = new SemaphoreSlim(windowSize, windowSize);
        var producerTask = ProduceAsync(readBatches, channel.Writer, inFlightSlots, producerCancellationSource.Token);

        return new BufferedBatchChannel<TItem, TCheckpoint>(
            channel.Reader,
            producerTask,
            producerCancellationSource,
            inFlightSlots);
    }

    /// <summary>
    /// Освобождает место в общем окне только после подтверждения обработки и сохранения checkpoint, если он включен.
    /// </summary>
    public void ConfirmBatch() => _inFlightSlots.Release();

    /// <summary>
    /// Завершает работу producer-а и освобождает связанные ресурсы.
    /// </summary>
    /// <param name="suppressProducerException">
    /// Если <see langword="true" />, ошибки producer-а не будут проброшены вызывающему коду,
    /// потому что первичная причина завершения уже известна на стороне consumer-а.
    /// </param>
    public async ValueTask CompleteAsync(bool suppressProducerException)
    {
        if (_isCompleted)
        {
            return;
        }

        _isCompleted = true;

        try
        {
            if (suppressProducerException && !_producerTask.IsCompleted)
            {
                try
                {
                    await _producerCancellationSource.CancelAsync();
                }
                catch
                {
                    // Ошибка callback отмены не отменяет обязанность дождаться producer и сохранить первичный сбой.
                }
            }

            await _producerTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (suppressProducerException && _producerCancellationSource.IsCancellationRequested)
        {
            // Consumer уже завершил работу и явно остановил producer.
        }
        catch when (suppressProducerException)
        {
            // Первичное исключение уже определено consumer-ом или внешней отменой.
        }
        finally
        {
            _producerCancellationSource.Dispose();
            _inFlightSlots.Dispose();
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => CompleteAsync(suppressProducerException: true);

    private static async Task ProduceAsync(
        Func<CancellationToken, IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>>> readBatches,
        ChannelWriter<BufferedSourceBatch<TItem, TCheckpoint>> writer,
        SemaphoreSlim inFlightSlots,
        CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            var batches = readBatches(cancellationToken)
                ?? throw new InvalidOperationException("Источник данных вернул пустую последовательность пакетов.");

            await using var enumerator = batches.GetAsyncEnumerator(cancellationToken);
            var sequence = 0L;

            while (await writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
            {
                await inFlightSlots.WaitAsync(cancellationToken).ConfigureAwait(false);
                var transferred = false;

                try
                {
                    if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
                    {
                        break;
                    }

                    var bufferedBatch = new BufferedSourceBatch<TItem, TCheckpoint>
                    {
                        Sequence = ++sequence,
                        Batch = enumerator.Current,
                    };

                    await writer.WriteAsync(bufferedBatch, cancellationToken).ConfigureAwait(false);
                    transferred = true;
                }
                finally
                {
                    if (!transferred)
                    {
                        inFlightSlots.Release();
                    }
                }
            }

            writer.TryComplete();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            writer.TryComplete();
            throw;
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
            throw;
        }
    }
}
