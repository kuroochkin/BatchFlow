using BatchFlow.Abstractions.Failures;
using BatchFlow.Abstractions.Execution;
using FluentAssertions;
using Xunit;

namespace BatchFlow.Core.Tests.Failures;

public sealed class FixedRetryFailurePolicyTests
{
    [Fact]
    public async Task DecideAsync_should_return_retry_while_attempts_are_available()
    {
        var policy = new FixedRetryFailurePolicy<int, long>(
            new BatchRetryOptions
            {
                MaxRetryAttempts = 3,
                Delay = TimeSpan.FromMilliseconds(50),
            });

        var decision = await policy.DecideAsync(
            CreateFailureContext(attempt: 1, new InvalidOperationException("temporary failure")),
            CancellationToken.None);

        decision.Action.Should().Be(FailureAction.Retry);
        decision.Delay.Should().Be(TimeSpan.FromMilliseconds(50));
    }

    [Fact]
    public async Task DecideAsync_should_return_stop_when_retry_attempts_are_exhausted()
    {
        var policy = new FixedRetryFailurePolicy<int, long>(
            new BatchRetryOptions
            {
                MaxRetryAttempts = 2,
            });

        var decision = await policy.DecideAsync(
            CreateFailureContext(attempt: 3, new InvalidOperationException("permanent failure")),
            CancellationToken.None);

        decision.Action.Should().Be(FailureAction.Stop);
        decision.Reason.Should().Be("Повторные попытки обработки исчерпаны.");
    }

    [Fact]
    public async Task DecideAsync_should_return_stop_when_exception_is_filtered_out()
    {
        var policy = new FixedRetryFailurePolicy<int, long>(
            new BatchRetryOptions
            {
                MaxRetryAttempts = 5,
                ShouldHandle = exception => exception is TimeoutException,
            });

        var decision = await policy.DecideAsync(
            CreateFailureContext(attempt: 1, new InvalidOperationException("non-transient failure")),
            CancellationToken.None);

        decision.Action.Should().Be(FailureAction.Stop);
        decision.Reason.Should().Be("Исключение не подпадает под встроенную стратегию повторных попыток.");
    }

    [Fact]
    public async Task DecideAsync_should_return_skip_when_retry_attempts_are_exhausted_and_terminal_action_is_skip()
    {
        var policy = new FixedRetryFailurePolicy<int, long>(
            new BatchRetryOptions
            {
                MaxRetryAttempts = 1,
            },
            BatchFailureTerminalAction.Skip);

        var decision = await policy.DecideAsync(
            CreateFailureContext(attempt: 2, new InvalidOperationException("permanent failure")),
            CancellationToken.None);

        decision.Action.Should().Be(FailureAction.Skip);
        decision.Reason.Should().Be("Повторные попытки обработки исчерпаны.");
    }

    [Fact]
    public async Task DecideAsync_should_return_skip_when_exception_is_filtered_out_and_terminal_action_is_skip()
    {
        var policy = new FixedRetryFailurePolicy<int, long>(
            new BatchRetryOptions
            {
                MaxRetryAttempts = 5,
                ShouldHandle = exception => exception is TimeoutException,
            },
            BatchFailureTerminalAction.Skip);

        var decision = await policy.DecideAsync(
            CreateFailureContext(attempt: 1, new InvalidOperationException("non-transient failure")),
            CancellationToken.None);

        decision.Action.Should().Be(FailureAction.Skip);
        decision.Reason.Should().Be("Исключение не подпадает под встроенную стратегию повторных попыток.");
    }

    private static FailureContext<int, long> CreateFailureContext(int attempt, Exception exception) =>
        new()
        {
            Job = new JobExecutionContext
            {
                JobName = "retry-tests",
                RunId = "run-1",
                BatchSize = 100,
                DegreeOfParallelism = 1,
            },
            BatchSequence = 1,
            BatchCheckpoint = 10L,
            BatchItems = [1],
            Attempt = attempt,
            Exception = exception,
            Scope = FailureScope.Item,
            ItemIndex = 0,
            FailedItem = 1,
        };
}
