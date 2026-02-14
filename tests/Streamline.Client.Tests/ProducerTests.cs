using Streamline.Client;
using Xunit;

namespace Streamline.Client.Tests;

public class ProducerTests
{
    private IProducer<string, string> CreateProducer(ProducerOptions? options = null)
    {
        var client = new StreamlineClient("localhost:9092");
        return options != null
            ? client.CreateProducer<string, string>(options)
            : client.CreateProducer<string, string>();
    }

    // --- Creation and Configuration ---

    [Fact]
    public void CreateProducer_WithDefaultOptions_ReturnsProducer()
    {
        var producer = CreateProducer();
        Assert.NotNull(producer);
    }

    [Fact]
    public void CreateProducer_WithCustomOptions_ReturnsProducer()
    {
        var options = new ProducerOptions
        {
            BatchSize = 32768,
            LingerMs = 5,
            MaxRequestSize = 2097152,
            CompressionType = CompressionType.Zstd,
            Retries = 10,
            RetryBackoffMs = 200,
            Idempotent = true,
        };
        var producer = CreateProducer(options);
        Assert.NotNull(producer);
    }

    [Fact]
    public void CreateProducer_WithDifferentTypeParameters_ReturnsProducer()
    {
        var client = new StreamlineClient("localhost:9092");
        var producer = client.CreateProducer<int, byte[]>();
        Assert.NotNull(producer);
    }

    [Fact]
    public async Task CreateProducer_AfterClientDisposed_ThrowsObjectDisposedException()
    {
        var client = new StreamlineClient("localhost:9092");
        await client.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => client.CreateProducer<string, string>());
    }

    // --- SendAsync ---

    [Fact]
    public async Task SendAsync_ReturnsRecordMetadata()
    {
        var producer = CreateProducer();
        var result = await producer.SendAsync("test-topic", "key1", "value1");

        Assert.Equal("test-topic", result.Topic);
        Assert.Equal(0, result.Partition);
        Assert.True(result.Offset > 0);
        Assert.True(result.Timestamp <= DateTimeOffset.UtcNow);
    }

    [Fact]
    public async Task SendAsync_WithNullKey_Succeeds()
    {
        var producer = CreateProducer();
        var result = await producer.SendAsync("test-topic", null, "value1");

        Assert.Equal("test-topic", result.Topic);
    }

    [Fact]
    public async Task SendAsync_WithHeaders_ReturnsRecordMetadata()
    {
        var producer = CreateProducer();
        var headers = new Headers()
            .Add("trace-id", "abc-123")
            .Add("source", "unit-test");

        var result = await producer.SendAsync("test-topic", "key1", "value1", headers);

        Assert.Equal("test-topic", result.Topic);
        Assert.Equal(0, result.Partition);
        Assert.True(result.Offset > 0);
    }

    [Fact]
    public async Task SendAsync_WithNullHeaders_Succeeds()
    {
        var producer = CreateProducer();
        var result = await producer.SendAsync("test-topic", "key1", "value1", null);

        Assert.Equal("test-topic", result.Topic);
    }

    [Fact]
    public async Task SendAsync_MultipleTimes_EachReturnsMetadata()
    {
        var producer = CreateProducer();

        var result1 = await producer.SendAsync("topic-a", "k1", "v1");
        var result2 = await producer.SendAsync("topic-b", "k2", "v2");

        Assert.Equal("topic-a", result1.Topic);
        Assert.Equal("topic-b", result2.Topic);
    }

    [Fact]
    public async Task SendAsync_WithCancellationToken_CanBeCancelled()
    {
        var producer = CreateProducer();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => producer.SendAsync("test-topic", "key", "value", cts.Token));
    }

    [Fact]
    public async Task SendAsync_WithHeadersAndCancellationToken_CanBeCancelled()
    {
        var producer = CreateProducer();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => producer.SendAsync("test-topic", "key", "value", new Headers(), cts.Token));
    }

    // --- FlushAsync ---

    [Fact]
    public async Task FlushAsync_CompletesSuccessfully()
    {
        var producer = CreateProducer();
        await producer.FlushAsync();
    }

    [Fact]
    public async Task FlushAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var producer = CreateProducer();
        await producer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.FlushAsync());
    }

    // --- DisposeAsync ---

    [Fact]
    public async Task DisposeAsync_CompletesSuccessfully()
    {
        var producer = CreateProducer();
        await producer.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        var producer = CreateProducer();
        await producer.DisposeAsync();
        await producer.DisposeAsync();
    }

    [Fact]
    public async Task SendAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var producer = CreateProducer();
        await producer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => producer.SendAsync("test-topic", "key", "value"));
    }

    [Fact]
    public async Task SendAsync_WithHeaders_AfterDispose_ThrowsObjectDisposedException()
    {
        var producer = CreateProducer();
        await producer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => producer.SendAsync("test-topic", "key", "value", new Headers()));
    }

    // --- RecordMetadata ---

    [Fact]
    public void RecordMetadata_RecordEquality()
    {
        var ts = DateTimeOffset.UtcNow;
        var a = new RecordMetadata("topic", 0, 100, ts);
        var b = new RecordMetadata("topic", 0, 100, ts);

        Assert.Equal(a, b);
    }

    [Fact]
    public void RecordMetadata_RecordInequality()
    {
        var ts = DateTimeOffset.UtcNow;
        var a = new RecordMetadata("topic-a", 0, 100, ts);
        var b = new RecordMetadata("topic-b", 0, 100, ts);

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void RecordMetadata_Properties()
    {
        var ts = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var metadata = new RecordMetadata("my-topic", 3, 42, ts);

        Assert.Equal("my-topic", metadata.Topic);
        Assert.Equal(3, metadata.Partition);
        Assert.Equal(42, metadata.Offset);
        Assert.Equal(ts, metadata.Timestamp);
    }
}
