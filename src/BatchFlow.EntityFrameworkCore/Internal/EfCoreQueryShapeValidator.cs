using System.Linq.Expressions;

namespace BatchFlow.EntityFrameworkCore.Internal;

internal static class EfCoreQueryShapeValidator
{
    public static void ValidateBaseQuery(Expression expression)
    {
        var current = expression;

        while (current is MethodCallExpression call && call.Arguments.Count > 0)
        {
            if (call.Method.DeclaringType == typeof(Queryable)
                && call.Method.Name is nameof(Queryable.Skip) or nameof(Queryable.Take)
                    or nameof(Queryable.SkipLast) or nameof(Queryable.TakeLast) or nameof(Queryable.Reverse))
            {
                throw new InvalidOperationException(
                    "Базовый EF-запрос не должен содержать Skip, Take или Reverse в основной цепочке. Используйте keyset-предикат и стабильную сортировку.");
            }

            current = call.Arguments[0];
        }
    }

    public static void ValidateOrdering(Expression orderedExpression, Expression sourceExpression)
    {
        var current = orderedExpression;

        while (current is MethodCallExpression thenBy && IsOrdering(thenBy, secondary: true))
        {
            current = thenBy.Arguments[0];
        }

        if (current is not MethodCallExpression orderBy
            || !IsOrdering(orderBy, secondary: false)
            || !ReferenceEquals(orderBy.Arguments[0], sourceExpression))
        {
            throw new InvalidOperationException(
                "Стабильная сортировка должна добавлять OrderBy и ThenBy к предоставленному запросу, не заменяя его и не добавляя другие операции.");
        }
    }

    private static bool IsOrdering(MethodCallExpression call, bool secondary) =>
        call.Method.DeclaringType == typeof(Queryable)
        && call.Arguments.Count == 2
        && (secondary
            ? call.Method.Name is nameof(Queryable.ThenBy) or nameof(Queryable.ThenByDescending)
            : call.Method.Name is nameof(Queryable.OrderBy) or nameof(Queryable.OrderByDescending));
}
