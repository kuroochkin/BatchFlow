namespace BatchFlow.Abstractions.Failures;

/// <summary>
/// Определяет стратегию обработки ошибок и подсказывает движку, как продолжить выполнение.
/// </summary>
/// <typeparam name="TItem">Тип элемента, связанного с ошибкой обработки.</typeparam>
/// <typeparam name="TCheckpoint">Тип контрольной точки, связанной с ошибкой обработки.</typeparam>
public interface IFailurePolicy<TItem, TCheckpoint>
{
    /// <summary>
    /// Принимает решение о дальнейших действиях на основании сведений об ошибке.
    /// </summary>
    ValueTask<FailureDecision> DecideAsync(
        FailureContext<TItem, TCheckpoint> context,
        CancellationToken cancellationToken);
}
