using BatchFlow;
using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Results;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Extensions.Logging;
using BatchFlow.Testing.Checkpoints;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Xunit;

namespace BatchFlow.Core.Tests.Extensions.Logging;

public sealed class BatchFlowProgressLoggingTests
{
    [Fact]
    public async Task WithProgressLogging_should_log_full_runtime_lifecycle_for_successful_job()
    {
        var logger = new RecordingLogger(minLevel: LogLevel.Trace);
        var checkpointStore = new InMemoryCheckpointStore<int>();
        checkpointStore.Seed(
            "orders-job",
            new StoredCheckpoint<int>
            {
                Value = 100,
                SequenceNumber = 7,
                ConcurrencyToken = "seed-token",
                SavedAt = new DateTimeOffset(2026, 7, 5, 10, 0, 0, TimeSpan.Zero),
            });

        var summary = await BatchJob
            .Named("orders-job")
            .From(new LoggingResumableSource([101, 102]))
            .WithBatchSize(2)
            .WithCheckpoint("orders-job", checkpointStore)
            .WithProgressLogging(
                logger,
                options =>
                {
                    options.IncludeCheckpointValue = true;
                    options.IncludeSourceMetadata = true;
                })
            .RunEachAsync((_, _, _) => new ValueTask<ProcessingResult>(ProcessingResult.Success()));

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);

        logger.Entries.Should().OnlyContain(entry => entry.Message.Contains($"RunId={summary.Job.RunId}"));

        logger.Entries.Select(entry => entry.EventId.Id).Should().Equal(
            BatchFlowLoggingEventIds.JobStarted.Id,
            BatchFlowLoggingEventIds.BatchStarted.Id,
            BatchFlowLoggingEventIds.BatchCompleted.Id,
            BatchFlowLoggingEventIds.CheckpointSaved.Id,
            BatchFlowLoggingEventIds.JobCompleted.Id);

        logger.Entries[0].Level.Should().Be(LogLevel.Information);
        logger.Entries[1].Level.Should().Be(LogLevel.Debug);
        logger.Entries[2].Level.Should().Be(LogLevel.Information);
        logger.Entries[3].Level.Should().Be(LogLevel.Debug);
        logger.Entries[4].Level.Should().Be(LogLevel.Information);

        logger.Entries[0].Message.Should().Contain("Запуск batch job orders-job");
        logger.Entries[0].Message.Should().Contain("ResumeSequence=7");
        logger.Entries[0].Message.Should().Contain("ResumeCheckpoint=100");

        logger.Entries[1].Message.Should().Contain("BatchSequence=1");
        logger.Entries[1].Message.Should().Contain("SourceMetadata=page=page-102");

        logger.Entries[2].Message.Should().Contain("SucceededCount=2");
        logger.Entries[2].Message.Should().Contain("SkippedCount=0");

        logger.Entries[3].Message.Should().Contain("SequenceNumber=1");
        logger.Entries[3].Message.Should().Contain("CheckpointValue=102");

        logger.Entries[4].Message.Should().Contain("Status=Succeeded");
        logger.Entries[4].Message.Should().Contain("ProcessedBatchCount=1");
        logger.Entries[4].Message.Should().Contain("SucceededItemCount=2");
        logger.Entries[4].Message.Should().Contain("SkippedItemCount=0");
        logger.Entries[4].Message.Should().Contain("LastCheckpointSequence=1");
        logger.Entries[4].Message.Should().Contain("LastCheckpointValue=102");
    }

    [Fact]
    public async Task WithProgressLogging_should_log_batch_failure_and_final_job_failure()
    {
        var logger = new RecordingLogger(minLevel: LogLevel.Trace);

        var summary = await BatchJob
            .Named("failing-job")
            .From(new LoggingNonResumableSource([1, 2]))
            .WithBatchSize(2)
            .WithProgressLogging(logger)
            .RunBatchAsync((_, _, _) =>
                ValueTask.FromException<BatchProcessingResult>(
                    new InvalidOperationException("batch processing failed")));

        summary.Status.Should().Be(JobCompletionStatus.Failed);

        logger.Entries.Select(entry => entry.EventId.Id).Should().Equal(
            BatchFlowLoggingEventIds.JobStarted.Id,
            BatchFlowLoggingEventIds.BatchStarted.Id,
            BatchFlowLoggingEventIds.BatchFailed.Id,
            BatchFlowLoggingEventIds.JobCompleted.Id);

        logger.Entries[2].Level.Should().Be(LogLevel.Error);
        logger.Entries[2].Exception.Should().BeOfType<InvalidOperationException>();
        logger.Entries[2].Exception!.Message.Should().Be("batch processing failed");
        logger.Entries[2].Message.Should().Contain("BatchSequence=1");
        logger.Entries[2].Message.Should().Contain("ItemCount=2");

        logger.Entries[3].Level.Should().Be(LogLevel.Error);
        logger.Entries[3].Exception.Should().BeOfType<InvalidOperationException>();
        logger.Entries[3].Message.Should().Contain("Status=Failed");
        logger.Entries[3].Message.Should().Contain("ProcessedBatchCount=0");
    }

    [Fact]
    public async Task WithProgressLogging_on_built_definition_should_append_logging_observer()
    {
        var logger = new RecordingLogger(minLevel: LogLevel.Trace);

        var builtJob = BatchJob
            .Named("definition-logging")
            .From(new LoggingNonResumableSource([1]))
            .ProcessEach((_, _, _) => new ValueTask<ProcessingResult>(ProcessingResult.Success()))
            .Build()
            .WithProgressLogging(logger);

        var summary = await builtJob.RunAsync();

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        logger.Entries.Should().Contain(entry => entry.EventId.Id == BatchFlowLoggingEventIds.JobStarted.Id);
        logger.Entries.Should().Contain(entry => entry.EventId.Id == BatchFlowLoggingEventIds.JobCompleted.Id);
    }

    [Fact]
    public async Task Logging_should_include_retry_skip_reasons_exceptions_and_run_id()
    {
        var logger = new RecordingLogger(LogLevel.Trace);
        var expected = new InvalidOperationException("transient failure");
        var summary = await BatchJob.Named("retry-skip-logging").From(new LoggingResumableSource([1]))
            .WithRetry(1).WithSkipOnFailure().WithProgressLogging(logger)
            .RunEachAsync((_, _, _) => ValueTask.FromException<ProcessingResult>(expected));

        summary.Status.Should().Be(JobCompletionStatus.CompletedWithSkips);
        var retry = logger.Entries.Single(entry => entry.EventId == BatchFlowLoggingEventIds.ProcessingRetry);
        var skip = logger.Entries.Single(entry => entry.EventId == BatchFlowLoggingEventIds.ProcessingSkipped);
        retry.Exception.Should().BeSameAs(expected);
        skip.Exception.Should().BeSameAs(expected);
        skip.Message.Should().Contain("Reason=").And.Contain("SkippedCount=1");
        logger.Entries.Should().OnlyContain(entry => entry.Message.Contains($"RunId={summary.Job.RunId}"));
    }

    private sealed class LoggingResumableSource(IReadOnlyList<int> items) : IBatchSource<int, int>
    {
        private readonly IReadOnlyList<int> _items = items;

        public async IAsyncEnumerable<SourceBatch<int, int>> ReadAsync(
            BatchSourceContext<int> context,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await Task.Yield();

            yield return new SourceBatch<int, int>
            {
                Items = _items,
                Checkpoint = _items[^1],
                Metadata = new Dictionary<string, string>
                {
                    ["page"] = $"page-{_items[^1]}",
                },
            };
        }
    }

    private sealed class LoggingNonResumableSource(IReadOnlyList<int> items) : IBatchSource<int>
    {
        private readonly IReadOnlyList<int> _items = items;

        public async IAsyncEnumerable<SourceBatch<int, NoCheckpoint>> ReadAsync(
            BatchSourceContext<NoCheckpoint> context,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await Task.Yield();

            yield return new SourceBatch<int, NoCheckpoint>
            {
                Items = _items,
                Checkpoint = NoCheckpoint.Value,
                Metadata = new Dictionary<string, string>
                {
                    ["page"] = "single-page",
                },
            };
        }
    }

    private sealed class RecordingLogger(LogLevel minLevel) : ILogger
    {
        private readonly LogLevel _minLevel = minLevel;

        public IReadOnlyList<LogEntry> Entries => _entries;

        private readonly List<LogEntry> _entries = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None && logLevel >= _minLevel;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            _entries.Add(new LogEntry(
                logLevel,
                eventId,
                formatter(state, exception),
                exception));
        }
    }

    private sealed record LogEntry(LogLevel Level, EventId EventId, string Message, Exception? Exception);

    private sealed class NullScope : IDisposable
    {
        public static NullScope Instance { get; } = new();

        public void Dispose()
        {
        }
    }
}
