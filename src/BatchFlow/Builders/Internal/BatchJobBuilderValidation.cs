using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Sources;

namespace BatchFlow.Builders.Internal;

internal static class BatchJobBuilderValidation
{
    public static string ValidateJobName(string jobName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobName);
        return jobName;
    }

    public static void ValidateBatchSize(int batchSize)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize,
                "Размер пакета должен быть положительным числом.");
        }
    }

    public static void ValidateDegreeOfParallelism(int degreeOfParallelism)
    {
        if (degreeOfParallelism <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(degreeOfParallelism), degreeOfParallelism,
                "Степень параллелизма должна быть положительным числом.");
        }
    }

    public static void ValidateRetryOptions(BatchRetryOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.MaxRetryAttempts < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.MaxRetryAttempts,
                "Количество повторных попыток не может быть отрицательным.");
        }

        if (options.Delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.Delay,
                "Задержка между повторными попытками не может быть отрицательной.");
        }
    }

    public static void ValidateTerminalFailureAction(BatchFailureTerminalAction terminalAction)
    {
        if (!Enum.IsDefined(terminalAction))
        {
            throw new ArgumentOutOfRangeException(nameof(terminalAction), terminalAction,
                "Указано неизвестное terminal-действие встроенной политики ошибок.");
        }
    }

    public static string ValidateCheckpointId(string checkpointId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointId);
        return checkpointId;
    }

    public static void ValidateCheckpointingSupport<TItem, TCheckpoint>(IBatchSource<TItem, TCheckpoint> source)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (!source.SupportsResume)
        {
            throw new InvalidOperationException("Нельзя подключить checkpoint store к источнику, который не поддерживает возобновление чтения.");
        }
    }

    public static void ValidateBuildState<TItem, TCheckpoint>(BatchJobBuilderState<TItem, TCheckpoint> state)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(state.Source);

        ValidateJobName(state.JobName);
        ValidateBatchSize(state.BatchSize);
        ValidateDegreeOfParallelism(state.DegreeOfParallelism);

        if (state.ProcessingMode is null)
        {
            throw new InvalidOperationException(
                "Невозможно собрать batch job без обработчика. Сначала настройте ProcessEach(...) или ProcessBatch(...).");
        }

        if (state.CheckpointStore is null && state.CheckpointId is not null)
        {
            throw new InvalidOperationException("Нельзя указать checkpoint id без подключения checkpoint store.");
        }

        if (state.CheckpointStore is not null)
        {
            ValidateCheckpointingSupport(state.Source);
        }
    }
}
