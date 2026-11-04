using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Runtime.CompilerServices;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Postgres.Internal;
using BatchFlow.Postgres.Options;
using Npgsql;

namespace BatchFlow.Postgres.Sources;

/// <summary>
/// Представляет расширенный PostgreSQL-источник, который читает данные пакетами по SQL-запросу.
/// Источник повторно выполняет keyset-based запрос, каждый раз передавая актуальный чекпоинт и размер батча.
/// </summary>
/// <typeparam name="TItem">Тип элементов, которые возвращает источник.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, используемой для возобновления чтения.</typeparam>
public class PostgresBatchSource<TItem, TCheckpoint> : PostgresBatchSourceBase<TItem, TCheckpoint>
{
    private const string RowCountMetadataKey = "postgres.rowCount";
    private const string ReadIterationMetadataKey = "postgres.readIteration";

    private readonly PostgresBatchSourceOptions<TItem, TCheckpoint> _options;

    /// <summary>
    /// Инициализирует PostgreSQL-источник пакетного чтения.
    /// </summary>
    /// <param name="options">Параметры SQL-запроса, материализации и возобновления чтения.</param>
    /// <exception cref="ArgumentException">
    /// Параметры запроса некорректны или <c>ReaderBehavior</c> содержит <see cref="CommandBehavior.CloseConnection" />.
    /// Соединение должно оставаться открытым между порциями; его жизненным циклом управляет выбранная модель владения.
    /// </exception>
    public PostgresBatchSource(PostgresBatchSourceOptions<TItem, TCheckpoint> options)
        : base(options)
    {
        _options = ValidateOptions(options);
    }

    internal PostgresBatchSource(
        PostgresBatchSourceOptions<TItem, TCheckpoint> options,
        IPostgresConnectionFactory connectionFactory)
        : base(options, connectionFactory)
    {
        _options = ValidateOptions(options);
    }

    /// <inheritdoc />
    protected override async IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> ReadCoreAsync(
        NpgsqlConnection connection,
        BatchSourceContext<TCheckpoint> context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var currentResumeFrom = context.ResumeFrom;
        var readIteration = 0L;
        var hasMoreData = true;

        while (hasMoreData)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var batchReadResult = await ReadNextBatchAsync(
                connection,
                context,
                currentResumeFrom,
                readIteration,
                cancellationToken);

            if (batchReadResult is null)
            {
                hasMoreData = false;
                continue;
            }

            yield return new SourceBatch<TItem, TCheckpoint>
            {
                Items = batchReadResult.Value.Items,
                Checkpoint = batchReadResult.Value.LastRow.Checkpoint,
                Metadata = CreateMetadata(batchReadResult.Value.Items.Count, readIteration),
            };

            currentResumeFrom = CreateSyntheticResumeCheckpoint(batchReadResult.Value.LastRow.Checkpoint);
            readIteration++;
        }
    }

    /// <summary>
    /// Выполняет SQL-команду и возвращает reader результата.
    /// Метод сделан виртуальным, чтобы источник можно было качественно unit-тестировать без живой PostgreSQL-базы.
    /// </summary>
    /// <param name="command">Подготовленная SQL-команда.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Reader результата SQL-команды.</returns>
    protected virtual async ValueTask<DbDataReader> ExecuteReaderAsync(NpgsqlCommand command, CancellationToken cancellationToken)
        => await command.ExecuteReaderAsync(_options.ReaderBehavior, cancellationToken);

    private async ValueTask<BatchReadResult?> ReadNextBatchAsync(
        NpgsqlConnection connection,
        BatchSourceContext<TCheckpoint> context,
        StoredCheckpoint<TCheckpoint>? resumeFrom,
        long readIteration,
        CancellationToken cancellationToken)
    {
        await using var command = CreateCommand(connection, _options.CommandText);
        BindParameters(command.Parameters, context, resumeFrom, readIteration);

        await using var reader = await ExecuteReaderAsync(command, cancellationToken);

        var items = new List<TItem>(context.RequestedBatchSize);
        PostgresBatchSourceRow<TItem, TCheckpoint> lastRow = default;

        while (await reader.ReadAsync(cancellationToken))
        {
            var row = await _options.RowMaterializer(reader, cancellationToken);
            if (items.Count > 0)
            {
                EnsureCheckpointProgression(lastRow.Checkpoint, row.Checkpoint);
            }
            else if (resumeFrom is not null)
            {
                EnsureCheckpointProgression(resumeFrom.Value, row.Checkpoint);
            }

            items.Add(row.Item);
            lastRow = row;

            if (items.Count > context.RequestedBatchSize)
            {
                throw new InvalidOperationException(
                    "PostgreSQL-источник получил из SQL-запроса больше строк, чем разрешает runtime batch size. " +
                    "Запрос должен уважать параметр batch size и возвращать не более одного batch-а за вызов.");
            }
        }

        return items.Count == 0
            ? null
            : new BatchReadResult(items, lastRow);
    }

    private static PostgresBatchSourceOptions<TItem, TCheckpoint> ValidateOptions(
        PostgresBatchSourceOptions<TItem, TCheckpoint> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.CommandText);
        ArgumentNullException.ThrowIfNull(options.RowMaterializer);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.BatchSizeParameterName);
        ArgumentNullException.ThrowIfNull(options.ResumeCheckpointValueFactory);
        ArgumentNullException.ThrowIfNull(options.CheckpointComparer);

        if (options.ResumeCheckpointParameterName is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(options.ResumeCheckpointParameterName);

            if (string.Equals(
                    options.BatchSizeParameterName.TrimStart('@', ':'),
                    options.ResumeCheckpointParameterName.TrimStart('@', ':'),
                    StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("Имена параметров batch size и checkpoint должны различаться.", nameof(options));
            }
        }

        if (string.IsNullOrWhiteSpace(options.BatchSizeParameterName.TrimStart('@', ':')) ||
            options.ResumeCheckpointParameterName is not null &&
            string.IsNullOrWhiteSpace(options.ResumeCheckpointParameterName.TrimStart('@', ':')))
        {
            throw new ArgumentException("Имя SQL-параметра должно содержать имя, а не только префикс.", nameof(options));
        }

        const CommandBehavior allowedReaderBehavior = CommandBehavior.SequentialAccess | CommandBehavior.SingleResult;

        if ((options.ReaderBehavior & ~allowedReaderBehavior) != 0)
        {
            throw new ArgumentException(
                "ReaderBehavior поддерживает только Default, SequentialAccess и SingleResult. " +
                "CloseConnection и другие флаги несовместимы с контрактом пакетного чтения.",
                nameof(options));
        }

        return options;
    }

    private void BindParameters(NpgsqlParameterCollection parameters, BatchSourceContext<TCheckpoint> sourceContext,
        StoredCheckpoint<TCheckpoint>? resumeFrom, long readIteration)
    {
        parameters.Clear();
        parameters.AddWithValue(_options.BatchSizeParameterName, _options.BatchSizeParameterType, sourceContext.RequestedBatchSize);

        if (_options.ResumeCheckpointParameterName is not null)
        {
            BindResumeCheckpointParameter(parameters, resumeFrom);
        }

        _options.ParameterBinder?.Invoke(
            parameters,
            new PostgresBatchSourceParameterContext<TCheckpoint>
            {
                SourceContext = sourceContext,
                ResumeFrom = resumeFrom,
                BatchSize = sourceContext.RequestedBatchSize,
                ReadIteration = readIteration,
            });
    }

    private void BindResumeCheckpointParameter(NpgsqlParameterCollection parameters, StoredCheckpoint<TCheckpoint>? resumeFrom)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        var parameterValue = _options.ResumeCheckpointValueFactory(resumeFrom);

        if (parameterValue is null && !_options.BindResumeCheckpointWhenMissing)
        {
            return;
        }

        if (parameterValue is null || parameterValue is DBNull)
        {
            if (_options.ResumeCheckpointParameterType is null)
            {
                throw new InvalidOperationException(
                    "Невозможно автоматически передать NULL-checkpoint в SQL-параметр без явного ResumeCheckpointParameterType. " +
                    "Либо укажите тип параметра, либо отключите стандартную подстановку checkpoint и используйте ParameterBinder.");
            }

            parameters.Add(
                new NpgsqlParameter(_options.ResumeCheckpointParameterName!, _options.ResumeCheckpointParameterType.Value)
                {
                    Value = DBNull.Value,
                });

            return;
        }

        if (_options.ResumeCheckpointParameterType is { } checkpointParameterType)
        {
            parameters.AddWithValue(_options.ResumeCheckpointParameterName!, checkpointParameterType, parameterValue);
            return;
        }

        parameters.AddWithValue(_options.ResumeCheckpointParameterName!, parameterValue);
    }

    private void EnsureCheckpointProgression(TCheckpoint previousCheckpoint, TCheckpoint nextCheckpoint)
    {
        if (!_options.EnforceCheckpointProgression)
        {
            return;
        }

        var isValid = _options.CheckpointProgressionValidator is { } validator
            ? validator(previousCheckpoint, nextCheckpoint)
            : !_options.CheckpointComparer.Equals(previousCheckpoint, nextCheckpoint);

        if (!isValid)
        {
            throw new InvalidOperationException(
                "SQL-запрос PostgreSQL-источника не продвинул checkpoint в допустимом порядке. " +
                "Проверьте keyset-условие, сортировку и пользовательскую проверку порядка.");
        }
    }

    private static IReadOnlyDictionary<string, string> CreateMetadata(int rowCount, long readIteration) =>
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [RowCountMetadataKey] = rowCount.ToString(CultureInfo.InvariantCulture),
            [ReadIterationMetadataKey] = readIteration.ToString(CultureInfo.InvariantCulture),
        };

    private static StoredCheckpoint<TCheckpoint> CreateSyntheticResumeCheckpoint(TCheckpoint checkpoint) =>
        new()
        {
            Value = checkpoint,
            SavedAt = DateTimeOffset.UtcNow,
        };

    private readonly record struct BatchReadResult(
        IReadOnlyList<TItem> Items,
        PostgresBatchSourceRow<TItem, TCheckpoint> LastRow);
}
