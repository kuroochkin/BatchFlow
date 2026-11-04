using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;

namespace BatchFlow.EntityFrameworkCore.Options;

/// <summary>
/// Задает keyset-чтение через EF Core: базовый запрос, стабильный порядок и переход после контрольной точки.
/// Параметры выполнения пакетов поступают от runtime, а контекст создается отдельно для каждой порции.
/// </summary>
/// <typeparam name="TContext">Тип настроенного контекста приложения.</typeparam>
/// <typeparam name="TItem">Ссылочный тип сущности или полностью материализуемой модели результата.</typeparam>
/// <typeparam name="TCheckpoint">Тип неизменяемой позиции, не допускающий <see langword="null" />.</typeparam>
public sealed record EfCoreBatchSourceOptions<TContext, TItem, TCheckpoint>
    where TContext : DbContext
    where TItem : class
    where TCheckpoint : notnull
{
    /// <summary>
    /// Получение нового контекста на каждую порцию. Допускаются фабрика EF Core и пользовательские фабрики.
    /// Готовый внешний контекст не допускается: источник обязан освободить контекст до выдачи пакета.
    /// </summary>
    public required EntityFrameworkCoreContextOptions<TContext> Context { get; init; }

    /// <summary>
    /// Строит отложенный запрос через переданный контекст. Можно задавать фильтры и проекцию в модель результата.
    /// Нельзя возвращать запрос из другого контекста, выполнять его заранее или применять Skip/Take
    /// в основной цепочке: границы порции и переход по checkpoint принадлежат источнику.
    /// </summary>
    public required Func<TContext, IQueryable<TItem>> QueryFactory { get; init; }

    /// <summary>
    /// Добавляет к предоставленному запросу один OrderBy/OrderByDescending и при необходимости ThenBy/ThenByDescending.
    /// Полный набор ключей должен давать уникальный, стабильный порядок и входить в checkpoint.
    /// Делегат не должен заменять исходный запрос, добавлять другие операции или менять состав выборки.
    /// </summary>
    public required Func<IQueryable<TItem>, IOrderedQueryable<TItem>> ApplyStableOrdering { get; init; }

    /// <summary>
    /// Создает SQL-транслируемый предикат чтения строго после позиции. Источник сам применяет его через Where.
    /// На первом чтении без сохраненной позиции предикат не вызывается.
    /// Для составного порядка задается лексикографическое продолжение по всем ключам сортировки.
    /// </summary>
    public required Func<TCheckpoint, Expression<Func<TItem, bool>>> ResumePredicateFactory { get; init; }

    /// <summary>
    /// Получает позицию из материализованного элемента. Функция должна быть чистой, возвращать неизменяемое
    /// значение, отличное от null, и не обращаться к БД или ленивым навигационным свойствам.
    /// </summary>
    public required Func<TItem, TCheckpoint> CheckpointSelector { get; init; }

    /// <summary>
    /// Сравнивает позиции в порядке обхода: для допустимого перехода Compare(previous, next) должен быть меньше нуля.
    /// Для убывающего чтения нужен обратный сравниватель; для составного - лексикографический.
    /// Правила сравнения строк, GUID и других ключей должны совпадать с фактическим порядком провайдера БД.
    /// Проверка обязательна, в том числе для одного дополнительного элемента за границей пакета.
    /// </summary>
    public required IComparer<TCheckpoint> CheckpointComparer { get; init; }
}
