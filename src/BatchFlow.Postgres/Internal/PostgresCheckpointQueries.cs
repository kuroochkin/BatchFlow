namespace BatchFlow.Postgres.Internal;

/// <summary>
/// Содержит SQL-шаблоны хранилища контрольных точек.
/// Подстановка экранированных имен выполняется один раз при создании хранилища;
/// значения данных передаются отдельными SQL-параметрами.
/// </summary>
internal static class PostgresCheckpointQueries
{
    /// <summary>
    /// Создает схему и таблицу. Аргумент 0 - экранированное имя схемы,
    /// аргумент 1 - полное экранированное имя таблицы.
    /// </summary>
    internal const string EnsureStorage = """
        create schema if not exists {0};
        create table if not exists {1}
        (
            checkpoint_id text primary key,
            checkpoint_value jsonb not null,
            sequence_number bigint null,
            concurrency_token text not null,
            saved_at timestamptz not null,
            metadata jsonb not null
        );
        """;

    /// <summary>
    /// Загружает контрольную точку. Аргумент 0 - полное экранированное имя таблицы.
    /// </summary>
    internal const string Load = """
        select checkpoint_value, sequence_number, concurrency_token, saved_at, metadata
        from {0}
        where checkpoint_id = @checkpoint_id;
        """;

    /// <summary>
    /// Создает контрольную точку, не перезаписывая существующую запись.
    /// Аргумент 0 - полное экранированное имя таблицы.
    /// </summary>
    internal const string Insert = """
        insert into {0}
        (
            checkpoint_id,
            checkpoint_value,
            sequence_number,
            concurrency_token,
            saved_at,
            metadata
        )
        values
        (
            @checkpoint_id,
            @checkpoint_value,
            @sequence_number,
            @concurrency_token,
            @saved_at,
            @metadata
        )
        on conflict (checkpoint_id) do nothing
        returning checkpoint_id;
        """;

    /// <summary>
    /// Обновляет контрольную точку при совпадении токена конкуренции.
    /// Аргумент 0 - полное экранированное имя таблицы.
    /// </summary>
    internal const string Update = """
        update {0}
        set
            checkpoint_value = @checkpoint_value,
            sequence_number = @sequence_number,
            concurrency_token = @concurrency_token,
            saved_at = @saved_at,
            metadata = @metadata
        where checkpoint_id = @checkpoint_id
          and concurrency_token = @expected_concurrency_token
        returning checkpoint_id;
        """;
}
