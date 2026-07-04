using BatchFlow.Abstractions.Checkpoints;
using BatchFlow.Checkpoints;
using FluentAssertions;
using Xunit;

namespace BatchFlow.Core.Tests.Checkpoints;

public sealed class InMemoryCheckpointStoreTests
{
    [Fact]
    public async Task LoadAsync_should_return_null_when_checkpoint_is_missing()
    {
        var store = new InMemoryCheckpointStore<long>();

        var checkpoint = await store.LoadAsync("users-import", CancellationToken.None);

        checkpoint.Should().BeNull();
    }

    [Fact]
    public async Task SaveAsync_should_persist_checkpoint_and_assign_new_concurrency_token()
    {
        var store = new InMemoryCheckpointStore<long>();
        var savedAt = new DateTimeOffset(2026, 7, 3, 9, 15, 0, TimeSpan.Zero);

        var saved = await store.SaveAsync(
            "users-import",
            new StoredCheckpoint<long>
            {
                Value = 150,
                SequenceNumber = 3,
                SavedAt = savedAt,
                Metadata = new Dictionary<string, string>
                {
                    ["page"] = "customers-003",
                },
            },
            expectedConcurrencyToken: null,
            CancellationToken.None);

        saved.Value.Should().Be(150);
        saved.SequenceNumber.Should().Be(3);
        saved.SavedAt.Should().Be(savedAt);
        saved.Metadata.Should().ContainKey("page").WhoseValue.Should().Be("customers-003");
        saved.ConcurrencyToken.Should().NotBeNullOrWhiteSpace();

        var loaded = await store.LoadAsync("users-import", CancellationToken.None);

        loaded.Should().NotBeNull();
        loaded.Should().BeEquivalentTo(saved);
        loaded.Should().NotBeSameAs(saved);
    }

    [Fact]
    public async Task SaveAsync_should_throw_when_expected_concurrency_token_is_stale()
    {
        var store = new InMemoryCheckpointStore<long>();

        var firstSave = await store.SaveAsync(
            "users-import",
            new StoredCheckpoint<long>
            {
                Value = 100,
                SequenceNumber = 1,
            },
            expectedConcurrencyToken: null,
            CancellationToken.None);

        var action = () => store.SaveAsync(
            "users-import",
            new StoredCheckpoint<long>
            {
                Value = 200,
                SequenceNumber = 2,
            },
            expectedConcurrencyToken: "stale-token",
            CancellationToken.None).AsTask();

        await action.Should()
            .ThrowAsync<CheckpointConflictException>()
            .WithMessage("*users-import*");

        var loaded = await store.LoadAsync("users-import", CancellationToken.None);
        loaded.Should().NotBeNull();
        loaded!.Value.Should().Be(firstSave.Value);
        loaded.SequenceNumber.Should().Be(firstSave.SequenceNumber);
    }

    [Fact]
    public async Task SaveAsync_should_support_restart_scenario_with_same_store_instance()
    {
        var store = new InMemoryCheckpointStore<long>();

        var firstSave = await store.SaveAsync(
            "orders-sync",
            new StoredCheckpoint<long>
            {
                Value = 42,
                SequenceNumber = 5,
                Metadata = new Dictionary<string, string>
                {
                    ["page"] = "orders-005",
                },
            },
            expectedConcurrencyToken: null,
            CancellationToken.None);

        var resumeCheckpoint = await store.LoadAsync("orders-sync", CancellationToken.None);

        var secondSave = await store.SaveAsync(
            "orders-sync",
            new StoredCheckpoint<long>
            {
                Value = 56,
                SequenceNumber = 6,
                Metadata = new Dictionary<string, string>
                {
                    ["page"] = "orders-006",
                },
            },
            expectedConcurrencyToken: resumeCheckpoint!.ConcurrencyToken,
            CancellationToken.None);

        secondSave.Value.Should().Be(56);
        secondSave.SequenceNumber.Should().Be(6);
        secondSave.ConcurrencyToken.Should().NotBe(firstSave.ConcurrencyToken);

        var loaded = await store.LoadAsync("orders-sync", CancellationToken.None);
        loaded.Should().NotBeNull();
        loaded!.Value.Should().Be(56);
        loaded.SequenceNumber.Should().Be(6);
        loaded.Metadata["page"].Should().Be("orders-006");
    }

    [Fact]
    public async Task Store_should_clone_checkpoint_metadata_to_protect_internal_state()
    {
        var store = new InMemoryCheckpointStore<long>();
        var metadata = new Dictionary<string, string>
        {
            ["page"] = "users-001",
        };

        store.Seed(
            "users-import",
            new StoredCheckpoint<long>
            {
                Value = 10,
                SequenceNumber = 1,
                Metadata = metadata,
            });

        metadata["page"] = "tampered-after-seed";

        var loaded = await store.LoadAsync("users-import", CancellationToken.None);
        loaded.Should().NotBeNull();
        loaded!.Metadata["page"].Should().Be("users-001");

        var loadedDictionary = loaded.Metadata.Should().BeAssignableTo<Dictionary<string, string>>().Subject;
        loadedDictionary["page"] = "tampered-after-load";

        store.TryGet("users-import", out var currentCheckpoint).Should().BeTrue();
        currentCheckpoint.Should().NotBeNull();
        currentCheckpoint!.Metadata["page"].Should().Be("users-001");
    }
}
