using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;

namespace BatchFlow.Postgres.Options;

/// <summary>
/// Передает в пользовательский binder информацию о текущем цикле чтения PostgreSQL-источника.
/// </summary>
/// <typeparam name="TCheckpoint">Тип контрольной точки, используемой источником.</typeparam>
public sealed record PostgresBatchSourceParameterContext<TCheckpoint>
{
    /// <summary>
    /// Исходный контекст чтения.
    /// </summary>
    public required BatchSourceContext<TCheckpoint> SourceContext { get; init; }

    /// <summary>
    /// Текущая контрольная точка, от которой должен быть прочитан следующий батч.
    /// Для первого чтения значение может отсутствовать.
    /// </summary>
    public StoredCheckpoint<TCheckpoint>? ResumeFrom { get; init; }

    /// <summary>
    /// Размер батча, который должен использовать SQL-запрос.
    /// </summary>
    public required int BatchSize { get; init; }

    /// <summary>
    /// Порядковый номер цикла чтения внутри одного запуска.
    /// </summary>
    public required long ReadIteration { get; init; }
}
