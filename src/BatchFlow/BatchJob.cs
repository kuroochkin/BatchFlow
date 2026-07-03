using BatchFlow.Builders;
using BatchFlow.Builders.Internal;

namespace BatchFlow;

/// <summary>
/// Предоставляет точку входа для fluent-сборки пакетной задачи.
/// </summary>
public static class BatchJob
{
    /// <summary>
    /// Начинает сборку новой пакетной задачи с указанным именем.
    /// </summary>
    /// <param name="jobName">Понятное и стабильное имя задачи, используемое в диагностике и по умолчанию в checkpoint-идентификаторе.</param>
    /// <returns>Стадию builder, на которой нужно выбрать источник данных.</returns>
    public static IBatchJobSourceSelectionStage Named(string jobName) =>
        new BatchJobSourceSelectionStage(jobName);
}