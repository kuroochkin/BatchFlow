namespace BatchFlow.Postgres.Options;

/// <summary>
/// Определяет способ, которым PostgreSQL-адаптер получает подключение к базе данных.
/// </summary>
public enum PostgresConnectionMode
{
    /// <summary>
    /// Подключение будет создаваться по строке подключения.
    /// </summary>
    ConnectionString = 0,

    /// <summary>
    /// Подключение будет открываться через уже подготовленный <c>NpgsqlDataSource</c>.
    /// </summary>
    DataSource = 1,

    /// <summary>
    /// Подключение будет предоставляться пользовательской фабрикой <c>NpgsqlConnection</c>.
    /// </summary>
    ConnectionFactory = 2,
}
