using System.Diagnostics;

namespace Streamline.Client;

/// <summary>Point-in-time metrics snapshot.</summary>
public sealed record MetricsSnapshot(
    long MessagesProduced,
    long MessagesConsumed,
    long BytesSent,
    long BytesReceived,
    long ErrorsTotal,
    double ProduceLatencyAvgMs,
    double ConsumeLatencyAvgMs,
    long UptimeMs);

/// <summary>Thread-safe client metrics collector.</summary>
public sealed class ClientMetrics
{
    private long _messagesProduced;
    private long _messagesConsumed;
    private long _bytesSent;
    private long _bytesReceived;
    private long _errorsTotal;
    private double _produceLatencySum;
    private long _produceLatencyCount;
    private double _consumeLatencySum;
    private long _consumeLatencyCount;
    private readonly Stopwatch _uptime = Stopwatch.StartNew();
    private readonly object _lock = new();

    /// <summary>Record a successful produce operation.</summary>
    public void RecordProduce(long messageCount, long bytes, double latencyMs)
    {
        Interlocked.Add(ref _messagesProduced, messageCount);
        Interlocked.Add(ref _bytesSent, bytes);
        lock (_lock) { _produceLatencySum += latencyMs; _produceLatencyCount++; }
    }

    /// <summary>Record a successful consume operation.</summary>
    public void RecordConsume(long messageCount, long bytes, double latencyMs)
    {
        Interlocked.Add(ref _messagesConsumed, messageCount);
        Interlocked.Add(ref _bytesReceived, bytes);
        lock (_lock) { _consumeLatencySum += latencyMs; _consumeLatencyCount++; }
    }

    /// <summary>Record an error.</summary>
    public void RecordError() => Interlocked.Increment(ref _errorsTotal);

    /// <summary>Get a point-in-time snapshot of all metrics.</summary>
    public MetricsSnapshot Snapshot()
    {
        lock (_lock)
        {
            var produceAvg = _produceLatencyCount > 0 ? _produceLatencySum / _produceLatencyCount : 0;
            var consumeAvg = _consumeLatencyCount > 0 ? _consumeLatencySum / _consumeLatencyCount : 0;
            return new MetricsSnapshot(
                Interlocked.Read(ref _messagesProduced),
                Interlocked.Read(ref _messagesConsumed),
                Interlocked.Read(ref _bytesSent),
                Interlocked.Read(ref _bytesReceived),
                Interlocked.Read(ref _errorsTotal),
                produceAvg, consumeAvg,
                _uptime.ElapsedMilliseconds);
        }
    }
}
