using System.Threading.Channels;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Core.Jobs;

namespace BatchFlow.Core.Execution.Buffering;

/// <summary>
/// Представляет канал между чтением batch-ей из источника и их обработкой.
/// </summary>
/// <typeparam name="TItem">Тип элементов внутри пакета.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки.</typeparam>
internal sealed class BufferedBatchChannel<TItem, TCheckpoint> : IAsyncDisposable
{
    private readonly CancellationTokenSource _producerCancellationSource;
    private readonly Task _producerTask;
    private bool _isCompleted;

    private BufferedBatchChannel(
        ChannelReader<SourceBatch<TItem, TCheckpoint>> reader,
        Task producerTask,
        CancellationTokenSource producerCancellationSource)
    {
        Reader = reader;
        _producerTask = producerTask;
        _producerCancellationSource = producerCancellationSource;
    }

    /// <summary>
    /// Reader bounded channel-а, из которого consumer последовательно получает batch-и.
    /// </summary>
    public ChannelReader<SourceBatch<TItem, TCheckpoint>> Reader { get; }

    /// <summary>
    /// Запускает producer, который читает batch-и из источника и публикует их в bounded channel.
    /// </summary>
    public static BufferedBatchChannel<TItem, TCheckpoint> Start(
        BatchJobDefinition<TItem, TCheckpoint> job,
        IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> batches,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(batches);

        var producerCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var channel = Channel.CreateBounded<SourceBatch<TItem, TCheckpoint>>(
            new BoundedChannelOptions(job.PrefetchCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true,
                AllowSynchronousContinuations = false,
            });

        var producerTask = ProduceAsync(batches, channel.Writer, producerCancellationSource.Token);

        return new BufferedBatchChannel<TItem, TCheckpoint>(
            channel.Reader,
            producerTask,
            producerCancellationSource);
    }

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

        if (suppressProducerException && !_producerTask.IsCompleted)
        {
            _producerCancellationSource.Cancel();
        }

        try
        {
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
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => CompleteAsync(suppressProducerException: true);

    private static async Task ProduceAsync(
        IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> batches,
        ChannelWriter<SourceBatch<TItem, TCheckpoint>> writer,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var enumerator = batches.GetAsyncEnumerator(cancellationToken);

            while (await writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    break;
                }

                var batch = enumerator.Current;

                if (!writer.TryWrite(batch))
                {
                    await writer.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
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
