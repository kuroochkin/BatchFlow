using System.Data;
using System.Data.Common;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Postgres.Sources;
using Npgsql;
using NpgsqlTypes;

namespace BatchFlow.Postgres.Options;

/// <summary>
/// Содержит общие параметры PostgreSQL-источника данных для пакетного чтения.
/// </summary>
public record PostgresBatchSourceOptions
{
    /// <summary>
    /// Определяет, как источник будет получать подключения к PostgreSQL.
    /// </summary>
    public required PostgresConnectionOptions Connection { get; init; }

    /// <summary>
    /// Ограничение времени выполнения SQL-команды в секундах.
    /// Если значение не задано, используется настройка драйвера по умолчанию.
    /// </summary>
    public int? CommandTimeoutSeconds { get; init; }
}

/// <summary>
/// Содержит параметры PostgreSQL-источника, который читает данные пакетами по SQL-запросу.
/// </summary>
/// <typeparam name="TItem">Тип элементов, которые должны быть материализованы из результата запроса.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, продвигающей чтение.</typeparam>
public sealed record PostgresBatchSourceOptions<TItem, TCheckpoint> : PostgresBatchSourceOptions
{
    /// <summary>
    /// SQL-запрос, который должен вернуть не более одного батча данных.
    /// Обычно запрос использует keyset-фильтр по чекпоинту и ограничение по размеру батча.
    /// </summary>
    public required string CommandText { get; init; }

    /// <summary>
    /// Материализует одну строку результата в пользовательскую модель и соответствующую ей контрольную точку.
    /// Последняя контрольная точка из батча будет сохранена в качестве чекпоинта для следующего чтения.
    /// </summary>
    public required Func<DbDataReader, CancellationToken, ValueTask<PostgresBatchSourceRow<TItem, TCheckpoint>>> RowMaterializer { get; init; }

    /// <summary>
    /// Имя SQL-параметра, в который будет подставлен runtime batch size.
    /// Значение по умолчанию - <c>batch_size</c>.
    /// </summary>
    public string BatchSizeParameterName { get; init; } = "batch_size";

    /// <summary>
    /// Тип SQL-параметра, через который передается batch size.
    /// Значение по умолчанию - <see cref="NpgsqlDbType.Integer" />.
    /// </summary>
    public NpgsqlDbType BatchSizeParameterType { get; init; } = NpgsqlDbType.Integer;

    /// <summary>
    /// Имя SQL-параметра, в который будет подставлен checkpoint для возобновления чтения.
    /// Если значение равно <see langword="null" />, стандартная подстановка checkpoint отключается и пользователь должен добавить параметры сам через <see cref="ParameterBinder" />.
    /// </summary>
    public string? ResumeCheckpointParameterName { get; init; } = "resume_checkpoint";

    /// <summary>
    /// Явный тип SQL-параметра для чекпоинта.
    /// Особенно важен для первого запуска, когда checkpoint может отсутствовать и параметр должен быть отправлен как <c>NULL</c>.
    /// </summary>
    public NpgsqlDbType? ResumeCheckpointParameterType { get; init; }

    /// <summary>
    /// Определяет, нужно ли добавлять checkpoint-параметр даже при первом запуске, когда checkpoint еще отсутствует.
    /// Это удобно для SQL-шаблонов вида <c>(@resume_checkpoint is null or id &gt; @resume_checkpoint)</c>.
    /// </summary>
    public bool BindResumeCheckpointWhenMissing { get; init; } = true;

    /// <summary>
    /// Преобразует текущий чекпоинт в значение SQL-параметра.
    /// По умолчанию используется само значение checkpoint без дополнительной трансформации.
    /// </summary>
    public Func<StoredCheckpoint<TCheckpoint>?, object?> ResumeCheckpointValueFactory { get; init; } =
        static checkpoint => checkpoint is null ? null : checkpoint.Value;

    /// <summary>
    /// Позволяет добавить дополнительные SQL-параметры или переопределить стандартную схему параметризации.
    /// Этот механизм особенно полезен для составных checkpoint-ов и пользовательских SQL-шаблонов.
    /// </summary>
    public Action<NpgsqlParameterCollection, PostgresBatchSourceParameterContext<TCheckpoint>>? ParameterBinder { get; init; }

    /// <summary>
    /// Поведение чтения результата из <see cref="DbDataReader" />.
    /// По умолчанию используется <see cref="CommandBehavior.SequentialAccess" /> и <see cref="CommandBehavior.SingleResult" />.
    /// </summary>
    /// <remarks>
    /// Флаг <see cref="CommandBehavior.CloseConnection" /> запрещен, в том числе в комбинации с другими флагами.
    /// Reader освобождается после каждой порции, а соединение используется до окончания перечисления.
    /// Несовместимая настройка отклоняется при создании источника независимо от модели владения соединением.
    /// Поддерживаются только Default, SequentialAccess, SingleResult и комбинация двух последних флагов.
    /// </remarks>
    public CommandBehavior ReaderBehavior { get; init; } =
        CommandBehavior.SequentialAccess | CommandBehavior.SingleResult;

    /// <summary>
    /// Сравниватель checkpoint-ов, который используется для контроля продвижения keyset-чтения.
    /// </summary>
    public IEqualityComparer<TCheckpoint> CheckpointComparer { get; init; } = EqualityComparer<TCheckpoint>.Default;

    /// <summary>
    /// Проверяет порядок соседних checkpoint: первый аргумент обозначает предыдущую позицию, второй - новую.
    /// Возвращает <see langword="true" />, если переход допустим, например <c>next &gt; previous</c>.
    /// </summary>
    /// <remarks>
    /// Проверяется каждая строка, включая переход от ResumeFrom к первой строке.
    /// Можно задать убывающий или составной порядок. Если делегат не задан, проверяется только неравенство позиций.
    /// При <see cref="EnforceCheckpointProgression" /> = <see langword="false" /> проверки отключены.
    /// Проверка порядка не обеспечивает консистентность изменяющейся таблицы и не устраняет поздние коммиты.
    /// </remarks>
    public Func<TCheckpoint, TCheckpoint, bool>? CheckpointProgressionValidator { get; init; }

    /// <summary>
    /// Включает проверку перехода к каждой следующей строке, включая границу возобновления.
    /// По умолчанию соседние checkpoint должны различаться; для строгого порядка задайте CheckpointProgressionValidator.
    /// </summary>
    public bool EnforceCheckpointProgression { get; init; } = true;
}
