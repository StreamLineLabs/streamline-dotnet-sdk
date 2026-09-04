using Streamline.TestSupport;
using Xunit;

namespace Streamline.Client.Tests;

/// <summary>
/// Hermetic producer tests. The librdkafka handle is created lazily, so creating,
/// validating and disposing a producer never contacts a broker.
/// </summary>
public class ProducerTests
{
    private static StreamlineOptions UnitOptions() => new()
    {
        BootstrapServers = StreamlineTestEnvironment.UnitBootstrapServers,
        ConnectTimeout = TimeSpan.FromMilliseconds(100),
        RequestTimeout = TimeSpan.FromMilliseconds(100),
    };

    private static IProducer<string, string> CreateProducer(ProducerOptions? options = null)
    {
        var client = new StreamlineClient(UnitOptions());
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
        var client = new StreamlineClient(UnitOptions());
        var producer = client.CreateProducer<int, byte[]>();
        Assert.NotNull(producer);
    }

    [Fact]
    public async Task CreateTransactionalProducer_ExposesDocumentedCapability()
    {
        await using IStreamlineClient client = new StreamlineClient(UnitOptions());
        await using var producer = client.CreateTransactionalProducer<string, string>();

        Assert.IsAssignableFrom<ITransactionalProducer<string, string>>(producer);
    }

    [Fact]
    public async Task Transaction_AbortCancelsBufferedDeliveryTasks()
    {
        await using IStreamlineClient client = new StreamlineClient(UnitOptions());
        await using var producer = client.CreateTransactionalProducer<string, string>();
        producer.BeginTransaction();

        var pending = producer.SendTransactionalAsync("test-topic", "key", "value");
        producer.AbortTransaction();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
    }

    [Fact]
    public async Task Transaction_EmptyCommitCompletesWithoutNetworkAccess()
    {
        await using IStreamlineClient client = new StreamlineClient(UnitOptions());
        await using var producer = client.CreateTransactionalProducer<string, string>();
        producer.BeginTransaction();

        var metadata = await producer.CommitTransactionAsync();

        Assert.Empty(metadata);
    }

    [Fact]
    public async Task Transaction_CancelledCommitSettlesTasksAfterClosingCommitState()
    {
        await using IStreamlineClient client = new StreamlineClient(UnitOptions());
        await using var producer = client.CreateTransactionalProducer<string, string>();
        using var cancellation = new CancellationTokenSource();
        producer.BeginTransaction();
        var pending = producer.SendTransactionalAsync("test-topic", "key", "value");
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => producer.CommitTransactionAsync(cancellation.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);

        producer.BeginTransaction();
        producer.AbortTransaction();
    }

    [Fact]
    public async Task Transaction_RejectsInvalidStateTransitions()
    {
        await using IStreamlineClient client = new StreamlineClient(UnitOptions());
        await using var producer = client.CreateTransactionalProducer<string, string>();

        Assert.Throws<InvalidOperationException>(() =>
        {
            _ = producer.SendTransactionalAsync("test-topic", "key", "value");
        });

        producer.BeginTransaction();
        Assert.Throws<InvalidOperationException>(producer.BeginTransaction);
        producer.AbortTransaction();
        Assert.Throws<InvalidOperationException>(producer.AbortTransaction);
    }

    [Fact]
    public async Task Transaction_DisposeCancelsBufferedDeliveryTasks()
    {
        await using IStreamlineClient client = new StreamlineClient(UnitOptions());
        var producer = client.CreateTransactionalProducer<string, string>();
        producer.BeginTransaction();
        var pending = producer.SendTransactionalAsync("test-topic", "key", "value");

        await producer.DisposeAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
    }

    [Fact]
    public async Task CreateProducer_AfterClientDisposed_ThrowsObjectDisposedException()
    {
        var client = new StreamlineClient(UnitOptions());
        await client.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => client.CreateProducer<string, string>());
    }

    // --- Argument validation (fails before any network access) ---

    [Fact]
    public async Task SendAsync_WithCancelledToken_DoesNotSend()
    {
        var producer = CreateProducer();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => producer.SendAsync("test-topic", "key", "value", cts.Token));
    }

    [Fact]
    public async Task SendAsync_WithHeadersAndCancelledToken_DoesNotSend()
    {
        var producer = CreateProducer();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => producer.SendAsync("test-topic", "key", "value", new Headers(), cts.Token));
    }

    // --- FlushAsync ---

    [Fact]
    public async Task FlushAsync_WithNothingProduced_CompletesSuccessfully()
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

    [Fact]
    public async Task SendBatchAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var producer = CreateProducer();
        await producer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => producer.SendBatchAsync("test-topic", [((string?)"k", "v")]));
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

/// <summary>
/// Producer tests that require a live broker to accept and acknowledge records.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ProducerIntegrationTests
{
    private readonly IntegrationServerFixture _server;

    /// <summary>Creates the test class with the shared integration fixture.</summary>
    /// <param name="server">Fixture describing the configured Streamline endpoints.</param>
    public ProducerIntegrationTests(IntegrationServerFixture server)
    {
        _server = server;
    }

    private IProducer<string, string> CreateProducer()
    {
        var client = new StreamlineClient(new StreamlineOptions
        {
            BootstrapServers = _server.BootstrapServers,
            Admin = new AdminOptions { HttpBaseUrl = _server.HttpBaseUrl },
        });
        return client.CreateProducer<string, string>();
    }

    [IntegrationFact]
    public async Task SendAsync_ReturnsRecordMetadata()
    {
        await using var producer = CreateProducer();
        var result = await producer.SendAsync("test-topic", "key1", "value1");

        Assert.Equal("test-topic", result.Topic);
        Assert.True(result.Offset >= 0);
        Assert.True(result.Timestamp <= DateTimeOffset.UtcNow);
    }

    [IntegrationFact]
    public async Task SendAsync_WithNullKey_Succeeds()
    {
        await using var producer = CreateProducer();
        var result = await producer.SendAsync("test-topic", null, "value1");

        Assert.Equal("test-topic", result.Topic);
    }

    [IntegrationFact]
    public async Task SendAsync_WithHeaders_ReturnsRecordMetadata()
    {
        await using var producer = CreateProducer();
        var headers = new Headers()
            .Add("trace-id", "abc-123")
            .Add("source", "integration-test");

        var result = await producer.SendAsync("test-topic", "key1", "value1", headers);

        Assert.Equal("test-topic", result.Topic);
        Assert.True(result.Offset >= 0);
    }

    [IntegrationFact]
    public async Task SendAsync_MultipleTimes_EachReturnsMetadata()
    {
        await using var producer = CreateProducer();

        var result1 = await producer.SendAsync("topic-a", "k1", "v1");
        var result2 = await producer.SendAsync("topic-b", "k2", "v2");

        Assert.Equal("topic-a", result1.Topic);
        Assert.Equal("topic-b", result2.Topic);
    }

    [IntegrationFact]
    public async Task FlushAsync_AfterSend_CompletesSuccessfully()
    {
        await using var producer = CreateProducer();
        await producer.SendAsync("test-topic", "k", "v");
        await producer.FlushAsync();
    }
}
