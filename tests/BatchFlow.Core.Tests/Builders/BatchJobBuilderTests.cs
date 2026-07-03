using BatchFlow.Abstractions.Observability;
using BatchFlow.Abstractions.Execution;
using BatchFlow.Abstractions.Failures;
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
    public void WithRetry_should_throw_when_max_retry_attempts_is_negative()
    {
        var source = new DelegateBatchSource<int, long>((_, _) => EmptyBatches<int, long>());
        var builder = BatchJob.Named("job").From(source);

        Action action = () => builder.WithRetry(-1);

        action.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void WithRetry_should_throw_when_delay_is_negative()
    {
        var source = new DelegateBatchSource<int, long>((_, _) => EmptyBatches<int, long>());
        var builder = BatchJob.Named("job").From(source);

        Action action = () => builder.WithRetry(3, TimeSpan.FromMilliseconds(-1));

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
    public async Task Build_should_attach_fixed_retry_policy_when_with_retry_is_used()
    {
        var source = new DelegateBatchSource<int, long>((_, _) => EmptyBatches<int, long>());

        var job = BatchJob
            .Named("retry-enabled-job")
            .From(source)
            .WithRetry(new BatchRetryOptions
            {
                MaxRetryAttempts = 2,
                Delay = TimeSpan.FromMilliseconds(25),
            })
            .ProcessEach((_, _, _) => new ValueTask<ProcessingResult>(ProcessingResult.Success()))
            .Build();

        job.FailurePolicy.Should().BeOfType<FixedRetryFailurePolicy<int, long>>();

        var decision = await job.FailurePolicy!.DecideAsync(
            CreateFailureContext<long>(attempt: 1, new InvalidOperationException("temporary failure")),
            CancellationToken.None);

        decision.Action.Should().Be(FailureAction.Retry);
        decision.Delay.Should().Be(TimeSpan.FromMilliseconds(25));
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
    public async Task RunEachAsync_should_execute_builder_and_pass_context_and_cancellation_token()
    {
        var source = new DelegateBatchSource<int, long>(
            (_, _) => ToAsyncEnumerable(
                CreateBatch([10, 11], checkpoint: 100L, metadata: "page-100"),
                CreateBatch([12], checkpoint: 101L, metadata: "page-101")));

        var checkpointStore = new InMemoryCheckpointStore<long>();
        var capturedContexts = new List<ItemProcessingContext<long>>();
        var tokenMatches = new List<bool>();
        using var cancellationTokenSource = new CancellationTokenSource();

        var summary = await BatchJob
            .Named("run-each-resumable")
            .From(source)
            .WithBatchSize(2)
            .WithCheckpoint("run-each-checkpoint", checkpointStore)
            .RunEachAsync(
                (item, context, token) =>
                {
                    capturedContexts.Add(context);
                    tokenMatches.Add(token == cancellationTokenSource.Token);

                    return new ValueTask<ProcessingResult>(ProcessingResult.Success($"processed:{item}"));
                },
                cancellationTokenSource.Token);

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        summary.ProcessedBatchCount.Should().Be(2);
        summary.SucceededItemCount.Should().Be(3);
        summary.SkippedItemCount.Should().Be(0);
        summary.LastPersistedCheckpoint.Should().NotBeNull();
        summary.LastPersistedCheckpoint!.Value.Should().Be(101L);

        tokenMatches.Should().OnlyContain(match => match);

        capturedContexts.Should().HaveCount(3);

        capturedContexts[0].BatchSequence.Should().Be(1);
        capturedContexts[0].BatchCheckpoint.Should().Be(100L);
        capturedContexts[0].ItemIndex.Should().Be(0);
        capturedContexts[0].BatchItemCount.Should().Be(2);
        capturedContexts[0].Attempt.Should().Be(1);
        capturedContexts[0].SourceMetadata["page"].Should().Be("page-100");

        capturedContexts[1].BatchSequence.Should().Be(1);
        capturedContexts[1].BatchCheckpoint.Should().Be(100L);
        capturedContexts[1].ItemIndex.Should().Be(1);
        capturedContexts[1].BatchItemCount.Should().Be(2);

        capturedContexts[2].BatchSequence.Should().Be(2);
        capturedContexts[2].BatchCheckpoint.Should().Be(101L);
        capturedContexts[2].ItemIndex.Should().Be(0);
        capturedContexts[2].BatchItemCount.Should().Be(1);
        capturedContexts[2].SourceMetadata["page"].Should().Be("page-101");
    }

    [Fact]
    public async Task RunEachAsync_should_retry_failed_item_until_success_when_with_retry_is_configured()
    {
        var attempts = new List<int>();

        var summary = await BatchJob
            .Named("run-each-retry")
            .From(new NonResumableDataSource<int>([1]))
            .WithBatchSize(1)
            .WithRetry(2, TimeSpan.Zero)
            .RunEachAsync((item, context, _) =>
            {
                attempts.Add(context.Attempt);

                if (context.Attempt < 3)
                {
                    throw new InvalidOperationException($"temporary failure for item {item}");
                }

                return new ValueTask<ProcessingResult>(ProcessingResult.Success());
            });

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        summary.ProcessedBatchCount.Should().Be(1);
        summary.SucceededItemCount.Should().Be(1);
        summary.SkippedItemCount.Should().Be(0);
        attempts.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task RunEachAsync_should_return_completed_with_skips_when_handler_skips_item()
    {
        var summary = await BatchJob
            .Named("run-each-skips")
            .From(new NonResumableDataSource<int>([1, 2, 3]))
            .WithBatchSize(2)
            .RunEachAsync((item, _, _) =>
                new ValueTask<ProcessingResult>(
                    item == 2
                        ? ProcessingResult.Skipped("skip-second-item", code: "skip-2")
                        : ProcessingResult.Success()));

        summary.Status.Should().Be(JobCompletionStatus.CompletedWithSkips);
        summary.ProcessedBatchCount.Should().Be(2);
        summary.SucceededItemCount.Should().Be(2);
        summary.SkippedItemCount.Should().Be(1);
        summary.Exception.Should().BeNull();
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

    [Fact]
    public async Task RunEachAsync_should_use_explicit_run_options()
    {
        var executionEngine = new RecordingExecutionEngine();

        var summary = await BatchJob
            .Named("run-each-options")
            .From(new DelegateBatchSource<int, long>((_, _) => EmptyBatches<int, long>()))
            .RunEachAsync(
                (_, _, _) => new ValueTask<ProcessingResult>(ProcessingResult.Success()),
                new BatchJobRunOptions
                {
                    RunId = "run-each-custom-001",
                    ExecutionEngine = executionEngine,
                });

        executionEngine.CapturedRequest.Should().NotBeNull();
        executionEngine.CapturedRequest!.RunId.Should().Be("run-each-custom-001");
        summary.Job.RunId.Should().Be("run-each-custom-001");
        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
    }

    [Fact]
    public async Task RunBatchAsync_should_execute_builder_and_pass_context_and_cancellation_token()
    {
        var source = new DelegateBatchSource<int, long>(
            (_, _) => ToAsyncEnumerable(
                CreateBatch([21, 22], checkpoint: 200L, metadata: "page-200"),
                CreateBatch([23], checkpoint: 201L, metadata: "page-201")));

        var checkpointStore = new InMemoryCheckpointStore<long>();
        var capturedContexts = new List<BatchProcessingContext<long>>();
        var tokenMatches = new List<bool>();
        using var cancellationTokenSource = new CancellationTokenSource();

        var summary = await BatchJob
            .Named("run-batch-resumable")
            .From(source)
            .WithBatchSize(2)
            .WithCheckpoint("run-batch-checkpoint", checkpointStore)
            .RunBatchAsync(
                (items, context, token) =>
                {
                    capturedContexts.Add(context);
                    tokenMatches.Add(token == cancellationTokenSource.Token);

                    return new ValueTask<BatchProcessingResult>(BatchProcessingResult.AllSucceeded(items.Count));
                },
                cancellationTokenSource.Token);

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        summary.ProcessedBatchCount.Should().Be(2);
        summary.SucceededItemCount.Should().Be(3);
        summary.SkippedItemCount.Should().Be(0);
        summary.LastPersistedCheckpoint.Should().NotBeNull();
        summary.LastPersistedCheckpoint!.Value.Should().Be(201L);

        tokenMatches.Should().OnlyContain(match => match);
        capturedContexts.Should().HaveCount(2);

        capturedContexts[0].BatchSequence.Should().Be(1);
        capturedContexts[0].BatchCheckpoint.Should().Be(200L);
        capturedContexts[0].ItemCount.Should().Be(2);
        capturedContexts[0].Attempt.Should().Be(1);
        capturedContexts[0].SourceMetadata["page"].Should().Be("page-200");

        capturedContexts[1].BatchSequence.Should().Be(2);
        capturedContexts[1].BatchCheckpoint.Should().Be(201L);
        capturedContexts[1].ItemCount.Should().Be(1);
        capturedContexts[1].SourceMetadata["page"].Should().Be("page-201");
    }

    [Fact]
    public async Task RunBatchAsync_should_retry_failed_batch_until_success_when_with_retry_is_configured()
    {
        var attempts = new List<int>();

        var summary = await BatchJob
            .Named("run-batch-retry")
            .From(new NonResumableDataSource<int>([1, 2]))
            .WithBatchSize(2)
            .ProcessBatch((items, context, _) =>
            {
                attempts.Add(context.Attempt);

                if (context.Attempt < 3)
                {
                    throw new InvalidOperationException($"temporary batch failure for {items.Count} items");
                }

                return new ValueTask<BatchProcessingResult>(BatchProcessingResult.AllSucceeded(items.Count));
            })
            .WithRetry(new BatchRetryOptions
            {
                MaxRetryAttempts = 2,
                Delay = TimeSpan.Zero,
            })
            .RunAsync();

        summary.Status.Should().Be(JobCompletionStatus.Succeeded);
        summary.ProcessedBatchCount.Should().Be(1);
        summary.SucceededItemCount.Should().Be(2);
        summary.SkippedItemCount.Should().Be(0);
        attempts.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task RunBatchAsync_should_return_completed_with_skips_when_handler_returns_mixed_result()
    {
        var summary = await BatchJob
            .Named("run-batch-skips")
            .From(new NonResumableDataSource<int>([1, 2, 3]))
            .WithBatchSize(2)
            .RunBatchAsync((items, _, _) =>
                new ValueTask<BatchProcessingResult>(
                    BatchProcessingResult.Completed(
                        itemCount: items.Count,
                        succeededCount: items.Count - 1,
                        skippedCount: 1,
                        issues:
                        [
                            new BatchProcessingIssue
                            {
                                ItemIndex = items.Count - 1,
                                Code = "skip-last",
                                Message = "Последний элемент пакета пропущен.",
                            },
                        ],
                        reason: "mixed-batch-result")));

        summary.Status.Should().Be(JobCompletionStatus.CompletedWithSkips);
        summary.ProcessedBatchCount.Should().Be(2);
        summary.SucceededItemCount.Should().Be(1);
        summary.SkippedItemCount.Should().Be(2);
        summary.Exception.Should().BeNull();
    }

    [Fact]
    public async Task RunBatchAsync_should_use_explicit_run_options()
    {
        var executionEngine = new RecordingExecutionEngine();

        var summary = await BatchJob
            .Named("run-batch-options")
            .From(new DelegateBatchSource<int, long>((_, _) => EmptyBatches<int, long>()))
            .RunBatchAsync(
                (_, _, _) => new ValueTask<BatchProcessingResult>(BatchProcessingResult.AllSucceeded(0)),
                new BatchJobRunOptions
                {
                    RunId = "run-batch-custom-001",
                    ExecutionEngine = executionEngine,
                });

        executionEngine.CapturedRequest.Should().NotBeNull();
        executionEngine.CapturedRequest!.RunId.Should().Be("run-batch-custom-001");
        summary.Job.RunId.Should().Be("run-batch-custom-001");
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

    private static FailureContext<int, TCheckpoint> CreateFailureContext<TCheckpoint>(
        int attempt,
        Exception exception) =>
        new()
        {
            Job = CreateJobContext(),
            BatchSequence = 1,
            BatchCheckpoint = default!,
            BatchItems = [1],
            Attempt = attempt,
            Exception = exception,
            Scope = FailureScope.Item,
            ItemIndex = 0,
            FailedItem = 1,
        };

    private static async IAsyncEnumerable<SourceBatch<TItem, TCheckpoint>> EmptyBatches<TItem, TCheckpoint>()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static async IAsyncEnumerable<SourceBatch<int, long>> ToAsyncEnumerable(
        params SourceBatch<int, long>[] batches)
    {
        foreach (var batch in batches)
        {
            yield return batch;
            await Task.Yield();
        }
    }

    private static SourceBatch<int, long> CreateBatch(
        IReadOnlyList<int> items,
        long checkpoint,
        string metadata) =>
        new()
        {
            Items = items,
            Checkpoint = checkpoint,
            Metadata = new Dictionary<string, string>
            {
                ["page"] = metadata,
            },
        };

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
