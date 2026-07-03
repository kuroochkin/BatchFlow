using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Results;
using BatchFlow.Abstractions.Sources;
using BatchFlow.Core.Execution;
using BatchFlow.Core.Execution.Models;
using BatchFlow.Core.Jobs;
using BatchFlow.Core.Processing;
using BatchFlow.Testing.Checkpoints;
using BatchFlow.Testing.Processing;
using BatchFlow.Testing.Sources;
using FluentAssertions;
using Xunit;

namespace BatchFlow.Core.Tests.Builders;

public sealed class BatchJobBuilderTests
{
    [Fact]
    public async Task Build_should_create_definition_for_resumable_source_with_item_delegate_and_checkpoint()
    {
        var source = new DelegateBatchSource<int, long>((_, _) => EmptyBatches<int, long>());
        var checkpointStore = new InMemoryCheckpointStore<long>();

        var job = BatchJob
            .Named("user-migration")
            .From(source)
            .WithBatchSize(500)
            .WithDegreeOfParallelism(4)
            .WithCheckpoint("user-migration-v1", checkpointStore)
            .ProcessEach((item, _, _) => new ValueTask<ProcessingResult>(ProcessingResult.Success($"item:{item}")))
            .Build();

        job.Should().NotBeNull();
        job.JobName.Should().Be("user-migration");
        job.BatchSize.Should().Be(500);
        job.DegreeOfParallelism.Should().Be(4);
        job.CheckpointId.Should().Be("user-migration-v1");
        job.CheckpointStore.Should().BeSameAs(checkpointStore);
        job.Source.Should().BeSameAs(source);

        var processingMode = job.ProcessingMode.Should().BeOfType<ItemProcessorRegistration<int, long>>().Subject;
        var result = await processingMode.Processor.ProcessAsync(42, CreateItemContext(15L), CancellationToken.None);

        result.Disposition.Should().Be(ProcessingDisposition.Succeeded);
        result.Reason.Should().Be("item:42");
    }

    [Fact]
    public async Task Build_should_wrap_successful_batch_delegate_into_all_succeeded_result()
    {
        var source = new DelegateBatchSource<string, long>((_, _) => EmptyBatches<string, long>());

        var job = BatchJob
            .Named("catalog-sync")
            .From(source)
            .ProcessBatch((_, _, _) => ValueTask.CompletedTask)
            .Build();

        job.JobName.Should().Be("catalog-sync");
        job.BatchSize.Should().Be(100);
        job.DegreeOfParallelism.Should().Be(1);
        job.CheckpointStore.Should().BeNull();
        job.CheckpointId.Should().BeNull();

        var processingMode = job.ProcessingMode.Should().BeOfType<BatchProcessorRegistration<string, long>>().Subject;
        var result = await processingMode.Processor.ProcessAsync(["a", "b", "c"],
            CreateBatchContext(25L, itemCount: 3),
            CancellationToken.None);

        result.ItemCount.Should().Be(3);
        result.SucceededCount.Should().Be(3);
        result.SkippedCount.Should().Be(0);
        result.IsFullyAccountedFor.Should().BeTrue();
    }

    [Fact]
    public async Task Build_should_create_definition_for_non_resumable_source_without_checkpoint_configuration()
    {
        var source = new NonResumableTestSource<int>();
        var processor = new DelegateItemProcessor<int, NoCheckpoint>(
            (item, _, _) => new ValueTask<ProcessingResult>(ProcessingResult.Success($"value:{item}")));

        BatchJobDefinition<int, NoCheckpoint> job = BatchJob
            .Named("file-import")
            .From(source)
            .WithBatchSize(25)
            .WithDegreeOfParallelism(2)
            .ProcessEach(processor)
            .Build();

        job.JobName.Should().Be("file-import");
        job.BatchSize.Should().Be(25);
        job.DegreeOfParallelism.Should().Be(2);
        job.CheckpointStore.Should().BeNull();
        job.CheckpointId.Should().BeNull();
        job.Source.SupportsResume.Should().BeFalse();

        var processingMode = job.ProcessingMode.Should().BeOfType<ItemProcessorRegistration<int, NoCheckpoint>>().Subject;
        var result = await processingMode.Processor.ProcessAsync(
            7,
            CreateItemContext(NoCheckpoint.Value),
            CancellationToken.None);

        result.Reason.Should().Be("value:7");
    }

    [Fact]
    public void Named_should_throw_when_job_name_is_empty()
    {
        Action action = () => BatchJob.Named(" ");

        action.Should()
            .Throw<ArgumentException>()
            .WithMessage("*jobName*");
    }

    [Fact]
    public void WithBatchSize_should_throw_when_value_is_not_positive()
    {
        var source = new DelegateBatchSource<int, long>((_, _) => EmptyBatches<int, long>());
        var builder = BatchJob.Named("job").From(source);

        Action action = () => builder.WithBatchSize(0);

        action.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void WithDegreeOfParallelism_should_throw_when_value_is_not_positive()
    {
        var source = new DelegateBatchSource<int, long>((_, _) => EmptyBatches<int, long>());
        var builder = BatchJob.Named("job").From(source);

        Action action = () => builder.WithDegreeOfParallelism(0);

        action.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void WithCheckpoint_should_throw_when_source_reports_no_resume_support()
    {
        var source = new DelegateBatchSource<int, long>((_, _) => EmptyBatches<int, long>(), supportsResume: false);
        var checkpointStore = new InMemoryCheckpointStore<long>();
        var builder = BatchJob.Named("job").From(source);

        Action action = () => builder.WithCheckpoint(checkpointStore);

        action.Should()
            .Throw<InvalidOperationException>()
            .WithMessage("*не поддерживает возобновление чтения*");
    }

    [Fact]
    public async Task RunAsync_should_execute_ready_builder_without_explicit_engine()
    {
        var processedItems = new List<int>();

        var summary = await BatchJob
            .Named("inline-run")
            .From(new NonResumableDataSource<int>([1, 2, 3]))
            .WithBatchSize(2)
            .ProcessEach((item, _, _) =>
            {
                processedItems.Add(item);
                return new ValueTask<ProcessingResult>(ProcessingResult.Success());
            })
            .RunAsync();

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        summary.ProcessedBatchCount.Should().Be(2);
        summary.SucceededItemCount.Should().Be(3);
        summary.SkippedItemCount.Should().Be(0);
        processedItems.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task RunAsync_should_use_explicit_run_options_for_built_definition()
    {
        var executionEngine = new RecordingExecutionEngine();

        var job = BatchJob
            .Named("definition-run")
            .From(new DelegateBatchSource<int, long>((_, _) => EmptyBatches<int, long>()))
            .ProcessEach((_, _, _) => new ValueTask<ProcessingResult>(ProcessingResult.Success()))
            .Build();

        var summary = await job.RunAsync(
            new BatchJobRunOptions
            {
                RunId = "custom-run-001",
                ExecutionEngine = executionEngine,
            });

        executionEngine.CapturedRequest.Should().NotBeNull();
        executionEngine.CapturedRequest!.RunId.Should().Be("custom-run-001");
        executionEngine.CapturedRequest.Job.Should().BeSameAs(job);
        summary.Job.RunId.Should().Be("custom-run-001");
        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
    }

    private static JobExecutionContext CreateJobContext() =>
        new()
        {
            JobName = "test-job",
            RunId = "run-1",
            BatchSize = 100,
            DegreeOfParallelism = 1,
        };

    private static BatchProcessingContext<TCheckpoint> CreateBatchContext<TCheckpoint>(TCheckpoint checkpoint, int itemCount) =>
        new()
        {
            Job = CreateJobContext(),
            BatchSequence = 1,
            BatchCheckpoint = checkpoint,
            ItemCount = itemCount,
        };

    private static ItemProcessingContext<TCheckpoint> CreateItemContext<TCheckpoint>(TCheckpoint checkpoint) =>
        new()
        {
            Job = CreateJobContext(),
            BatchSequence = 1,
            BatchCheckpoint = checkpoint,
            ItemIndex = 0,
            BatchItemCount = 1,
        };

    private static async IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> EmptyBatches<TItem, TCheckpoint>()
    {
        await Task.CompletedTask;
        yield break;
    }

    private sealed class NonResumableTestSource<TItem> : IBatchSource<TItem>
    {
        public IAsyncEnumerable<SourceBatch<TItem, NoCheckpoint>> ReadAsync(
            BatchSourceContext<NoCheckpoint> context,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(context);
            return EmptyBatches<TItem, NoCheckpoint>();
        }
    }

    private sealed class NonResumableDataSource<TItem>(IReadOnlyList<TItem> items) : IBatchSource<TItem>
    {
        private readonly IReadOnlyList<TItem> _items = items ?? throw new ArgumentNullException(nameof(items));

        public async IAsyncEnumerable<SourceBatch<TItem, NoCheckpoint>> ReadAsync(
            BatchSourceContext<NoCheckpoint> context,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(context);

            for (var index = 0; index < _items.Count; index += context.RequestedBatchSize)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var batchItems = _items
                    .Skip(index)
                    .Take(context.RequestedBatchSize)
                    .ToArray();

                if (batchItems.Length == 0)
                {
                    yield break;
                }

                await Task.CompletedTask;

                yield return new SourceBatch<TItem, NoCheckpoint>
                {
                    Items = batchItems,
                    Checkpoint = NoCheckpoint.Value,
                };
            }
        }
    }

    private sealed class RecordingExecutionEngine : IBatchJobExecutionEngine
    {
        public BatchJobExecutionRequest<int, long>? CapturedRequest { get; private set; }

        public ValueTask<BatchJobExecutionSummary<TCheckpoint>> ExecuteAsync<TItem, TCheckpoint>(
            BatchJobExecutionRequest<TItem, TCheckpoint> request,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(request);

            if (request is BatchJobExecutionRequest<int, long> typedRequest)
            {
                CapturedRequest = typedRequest;
            }

            return ValueTask.FromResult(
                new BatchJobExecutionSummary<TCheckpoint>
                {
                    Job = new JobExecutionContext
                    {
                        JobName = request.Job.JobName,
                        RunId = request.RunId ?? "generated-run-id",
                        BatchSize = request.Job.BatchSize,
                        DegreeOfParallelism = request.Job.DegreeOfParallelism,
                    },
                    Status = JobCompletionStatus.Succeeded,
                });
        }
    }
}
