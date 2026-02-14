using Streamline.Client;
using Xunit;

namespace Streamline.Client.Tests;

public class ConsumerTests
{
    private IConsumer<string, string> CreateConsumer(
        string topic = "test-topic",
        ConsumerOptions? options = null)
    {
        var client = new StreamlineClient("localhost:9092");
        return options != null
            ? client.CreateConsumer<string, string>(topic, options)
            : client.CreateConsumer<string, string>(topic, "test-group");
    }

    // --- Creation ---

    [Fact]
    public void CreateConsumer_WithGroupId_ReturnsConsumer()
    {
        var consumer = CreateConsumer();
        Assert.NotNull(consumer);
    }

    [Fact]
    public void CreateConsumer_WithCustomOptions_ReturnsConsumer()
    {
        var options = new ConsumerOptions
        {
            GroupId = "custom-group",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = false,
            AutoCommitInterval = TimeSpan.FromSeconds(10),
            SessionTimeout = TimeSpan.FromSeconds(60),
            HeartbeatInterval = TimeSpan.FromSeconds(5),
            MaxPollRecords = 1000,
        };
        var consumer = CreateConsumer(options: options);
        Assert.NotNull(consumer);
    }

    [Fact]
    public void CreateConsumer_WithDifferentTypeParameters_ReturnsConsumer()
    {
        var client = new StreamlineClient("localhost:9092");
        var consumer = client.CreateConsumer<int, byte[]>("test-topic", "test-group");
        Assert.NotNull(consumer);
    }

    [Fact]
    public async Task CreateConsumer_AfterClientDisposed_ThrowsObjectDisposedException()
    {
        var client = new StreamlineClient("localhost:9092");
        await client.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(
            () => client.CreateConsumer<string, string>("test-topic", "test-group"));
    }

    // --- SubscribeAsync ---

    [Fact]
    public async Task SubscribeAsync_CompletesSuccessfully()
    {
        var consumer = CreateConsumer();
        await consumer.SubscribeAsync();
    }

    [Fact]
    public async Task SubscribeAsync_CalledTwice_IsIdempotent()
    {
        var consumer = CreateConsumer();
        await consumer.SubscribeAsync();
        await consumer.SubscribeAsync();
    }

    [Fact]
    public async Task SubscribeAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var consumer = CreateConsumer();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.SubscribeAsync());
    }

    // --- PollAsync ---

    [Fact]
    public async Task PollAsync_WhenSubscribed_ReturnsEmptyList()
    {
        var consumer = CreateConsumer();
        await consumer.SubscribeAsync();

        var records = await consumer.PollAsync(TimeSpan.FromMilliseconds(10));

        Assert.NotNull(records);
        Assert.Empty(records);
    }

    [Fact]
    public async Task PollAsync_WhenNotSubscribed_ThrowsInvalidOperationException()
    {
        var consumer = CreateConsumer();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => consumer.PollAsync(TimeSpan.FromMilliseconds(10)));
    }

    [Fact]
    public async Task PollAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var consumer = CreateConsumer();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.PollAsync(TimeSpan.FromMilliseconds(10)));
    }

    [Fact]
    public async Task PollAsync_WithCancellationToken_CanBeCancelled()
    {
        var consumer = CreateConsumer();
        await consumer.SubscribeAsync();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => consumer.PollAsync(TimeSpan.FromSeconds(10), cts.Token));
    }

    // --- ConsumeAsync ---

    [Fact]
    public async Task ConsumeAsync_WhenNotSubscribed_ThrowsInvalidOperationException()
    {
        var consumer = CreateConsumer();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in consumer.ConsumeAsync())
            {
                break;
            }
        });
    }

    [Fact]
    public async Task ConsumeAsync_WhenCancelled_StopsEnumeration()
    {
        var consumer = CreateConsumer();
        await consumer.SubscribeAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var count = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in consumer.ConsumeAsync(cts.Token))
            {
                count++;
            }
        });

        Assert.Equal(0, count);
    }

    [Fact]
    public async Task ConsumeAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var consumer = CreateConsumer();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await foreach (var _ in consumer.ConsumeAsync())
            {
                break;
            }
        });
    }

    // --- CommitAsync ---

    [Fact]
    public async Task CommitAsync_CompletesSuccessfully()
    {
        var consumer = CreateConsumer();
        await consumer.CommitAsync();
    }

    [Fact]
    public async Task CommitAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var consumer = CreateConsumer();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.CommitAsync());
    }

    // --- SeekToBeginningAsync ---

    [Fact]
    public async Task SeekToBeginningAsync_CompletesSuccessfully()
    {
        var consumer = CreateConsumer();
        await consumer.SeekToBeginningAsync();
    }

    [Fact]
    public async Task SeekToBeginningAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var consumer = CreateConsumer();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.SeekToBeginningAsync());
    }

    // --- SeekToEndAsync ---

    [Fact]
    public async Task SeekToEndAsync_CompletesSuccessfully()
    {
        var consumer = CreateConsumer();
        await consumer.SeekToEndAsync();
    }

    [Fact]
    public async Task SeekToEndAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var consumer = CreateConsumer();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.SeekToEndAsync());
    }

    // --- SeekAsync ---

    [Fact]
    public async Task SeekAsync_CompletesSuccessfully()
    {
        var consumer = CreateConsumer();
        await consumer.SeekAsync(partition: 0, offset: 42);
    }

    [Fact]
    public async Task SeekAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var consumer = CreateConsumer();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.SeekAsync(partition: 0, offset: 42));
    }

    // --- DisposeAsync ---

    [Fact]
    public async Task DisposeAsync_CompletesSuccessfully()
    {
        var consumer = CreateConsumer();
        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        var consumer = CreateConsumer();
        await consumer.DisposeAsync();
        await consumer.DisposeAsync();
    }

    // --- ConsumerRecord ---

    [Fact]
    public void ConsumerRecord_Properties()
    {
        var ts = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var headers = new Headers().Add("key", "value");
        var record = new ConsumerRecord<string, string>(
            "my-topic", 2, 99, ts, "my-key", "my-value", headers);

        Assert.Equal("my-topic", record.Topic);
        Assert.Equal(2, record.Partition);
        Assert.Equal(99, record.Offset);
        Assert.Equal(ts, record.Timestamp);
        Assert.Equal("my-key", record.Key);
        Assert.Equal("my-value", record.Value);
        Assert.False(record.Headers.IsEmpty);
    }

    [Fact]
    public void ConsumerRecord_RecordEquality()
    {
        var ts = DateTimeOffset.UtcNow;
        var headers = new Headers();
        var a = new ConsumerRecord<string, string>("topic", 0, 1, ts, "k", "v", headers);
        var b = new ConsumerRecord<string, string>("topic", 0, 1, ts, "k", "v", headers);

        Assert.Equal(a, b);
    }

    [Fact]
    public void ConsumerRecord_WithNullKey()
    {
        var ts = DateTimeOffset.UtcNow;
        var record = new ConsumerRecord<string, string>(
            "topic", 0, 1, ts, null, "value", new Headers());

        Assert.Null(record.Key);
        Assert.Equal("value", record.Value);
    }
}
