using Streamline.Client;
using Xunit;

namespace Streamline.Client.Tests;

public class ClientMetricsTests
{
    [Fact]
    public void NewMetrics_AllCountersAreZero()
    {
        var metrics = new ClientMetrics();
        var snap = metrics.Snapshot();

        Assert.Equal(0, snap.MessagesProduced);
        Assert.Equal(0, snap.MessagesConsumed);
        Assert.Equal(0, snap.BytesSent);
        Assert.Equal(0, snap.BytesReceived);
        Assert.Equal(0, snap.ErrorsTotal);
        Assert.Equal(0.0, snap.ProduceLatencyAvgMs);
        Assert.Equal(0.0, snap.ConsumeLatencyAvgMs);
    }

    [Fact]
    public void UptimeMs_IsNonNegative()
    {
        var metrics = new ClientMetrics();
        var snap = metrics.Snapshot();
        Assert.True(snap.UptimeMs >= 0);
    }

    [Fact]
    public void RecordProduce_IncrementsCounters()
    {
        var metrics = new ClientMetrics();
        metrics.RecordProduce(5, 1024, 10.0);

        var snap = metrics.Snapshot();
        Assert.Equal(5, snap.MessagesProduced);
        Assert.Equal(1024, snap.BytesSent);
    }

    [Fact]
    public void RecordProduce_Accumulates()
    {
        var metrics = new ClientMetrics();
        metrics.RecordProduce(3, 100, 5.0);
        metrics.RecordProduce(7, 200, 15.0);

        var snap = metrics.Snapshot();
        Assert.Equal(10, snap.MessagesProduced);
        Assert.Equal(300, snap.BytesSent);
    }

    [Fact]
    public void RecordConsume_IncrementsCounters()
    {
        var metrics = new ClientMetrics();
        metrics.RecordConsume(10, 4096, 15.0);

        var snap = metrics.Snapshot();
        Assert.Equal(10, snap.MessagesConsumed);
        Assert.Equal(4096, snap.BytesReceived);
    }

    [Fact]
    public void RecordConsume_Accumulates()
    {
        var metrics = new ClientMetrics();
        metrics.RecordConsume(5, 512, 8.0);
        metrics.RecordConsume(15, 1024, 12.0);

        var snap = metrics.Snapshot();
        Assert.Equal(20, snap.MessagesConsumed);
        Assert.Equal(1536, snap.BytesReceived);
    }

    [Fact]
    public void RecordError_IncrementsErrorCount()
    {
        var metrics = new ClientMetrics();
        metrics.RecordError();
        metrics.RecordError();
        metrics.RecordError();

        Assert.Equal(3, metrics.Snapshot().ErrorsTotal);
    }

    [Fact]
    public void ProduceLatencyAvg_CalculatedCorrectly()
    {
        var metrics = new ClientMetrics();
        metrics.RecordProduce(1, 100, 10.0);
        metrics.RecordProduce(1, 100, 20.0);
        metrics.RecordProduce(1, 100, 30.0);

        Assert.Equal(20.0, metrics.Snapshot().ProduceLatencyAvgMs);
    }

    [Fact]
    public void ConsumeLatencyAvg_CalculatedCorrectly()
    {
        var metrics = new ClientMetrics();
        metrics.RecordConsume(1, 100, 5.0);
        metrics.RecordConsume(1, 100, 15.0);

        Assert.Equal(10.0, metrics.Snapshot().ConsumeLatencyAvgMs);
    }

    [Fact]
    public void Snapshot_ReturnsIndependentCopy()
    {
        var metrics = new ClientMetrics();
        var snap1 = metrics.Snapshot();
        metrics.RecordProduce(5, 100, 10.0);
        var snap2 = metrics.Snapshot();

        Assert.Equal(0, snap1.MessagesProduced);
        Assert.Equal(5, snap2.MessagesProduced);
    }

    [Fact]
    public void MetricsSnapshot_IsRecord()
    {
        var snap = new MetricsSnapshot(1, 2, 3, 4, 5, 6.0, 7.0, 8);
        Assert.Equal(1, snap.MessagesProduced);
        Assert.Equal(2, snap.MessagesConsumed);
        Assert.Equal(3, snap.BytesSent);
        Assert.Equal(4, snap.BytesReceived);
        Assert.Equal(5, snap.ErrorsTotal);
        Assert.Equal(6.0, snap.ProduceLatencyAvgMs);
        Assert.Equal(7.0, snap.ConsumeLatencyAvgMs);
        Assert.Equal(8, snap.UptimeMs);
    }

    [Fact]
    public void MetricsSnapshot_RecordEquality()
    {
        var a = new MetricsSnapshot(1, 2, 3, 4, 5, 6.0, 7.0, 100);
        var b = new MetricsSnapshot(1, 2, 3, 4, 5, 6.0, 7.0, 100);
        Assert.Equal(a, b);
    }

    [Fact]
    public async Task ConcurrentAccess_IsThreadSafe()
    {
        var metrics = new ClientMetrics();
        var tasks = new List<Task>();

        for (int i = 0; i < 100; i++)
        {
            tasks.Add(Task.Run(() => metrics.RecordProduce(1, 10, 1.0)));
            tasks.Add(Task.Run(() => metrics.RecordConsume(1, 10, 1.0)));
            tasks.Add(Task.Run(() => metrics.RecordError()));
        }

        await Task.WhenAll(tasks);

        var snap = metrics.Snapshot();
        Assert.Equal(100, snap.MessagesProduced);
        Assert.Equal(100, snap.MessagesConsumed);
        Assert.Equal(100, snap.ErrorsTotal);
    }

    [Fact]
    public void MixedOperations_IndependentTracking()
    {
        var metrics = new ClientMetrics();
        metrics.RecordProduce(1, 100, 10.0);
        metrics.RecordConsume(1, 200, 20.0);
        metrics.RecordError();

        var snap = metrics.Snapshot();
        Assert.Equal(1, snap.MessagesProduced);
        Assert.Equal(1, snap.MessagesConsumed);
        Assert.Equal(100, snap.BytesSent);
        Assert.Equal(200, snap.BytesReceived);
        Assert.Equal(1, snap.ErrorsTotal);
        Assert.Equal(10.0, snap.ProduceLatencyAvgMs);
        Assert.Equal(20.0, snap.ConsumeLatencyAvgMs);
    }
}
