using BatchFlow.Postgres.Checkpoints;
using FluentAssertions;
using Xunit;

namespace BatchFlow.Postgres.Tests.Checkpoints;

public sealed class JsonPostgresCheckpointSerializerTests
{
    [Fact]
    public void Serialize_and_deserialize_should_roundtrip_checkpoint_value()
    {
        var serializer = new JsonPostgresCheckpointSerializer<DemoCheckpoint>();
        var checkpoint = new DemoCheckpoint(42, "orders");

        var payload = serializer.Serialize(checkpoint);
        var restored = serializer.Deserialize(payload);

        restored.Should().BeEquivalentTo(checkpoint);
    }

    private sealed record DemoCheckpoint(int LastProcessedId, string StreamName);
}
