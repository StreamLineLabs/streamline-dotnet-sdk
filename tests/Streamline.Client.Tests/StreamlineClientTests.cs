using Streamline.TestSupport;
using Xunit;

namespace Streamline.Client.Tests;

/// <summary>
/// Hermetic tests for <see cref="StreamlineClient"/> construction and lifecycle.
/// Nothing here opens a socket: the librdkafka handle is created lazily, so a client
/// that is only constructed and disposed never contacts a broker.
/// </summary>
public class StreamlineClientTests
{
    private static StreamlineOptions UnitOptions() => new()
    {
        BootstrapServers = StreamlineTestEnvironment.UnitBootstrapServers,
        ConnectTimeout = TimeSpan.FromMilliseconds(100),
        RequestTimeout = TimeSpan.FromMilliseconds(100),
        Admin = new AdminOptions
        {
            HttpBaseUrl = StreamlineTestEnvironment.UnitHttpBaseUrl,
            Timeout = TimeSpan.FromMilliseconds(100),
        },
    };

    private static StreamlineClient CreateClient() => new(UnitOptions());

    [Fact]
    public void Constructor_WithBootstrapServers_SetsDefaults()
    {
        var client = new StreamlineClient("broker1:9092");
        Assert.NotNull(client);
    }

    [Fact]
    public void Constructor_WithOptions_AcceptsCustomConfig()
    {
        var options = new StreamlineOptions
        {
            BootstrapServers = "broker1:9092,broker2:9092",
            ConnectionPoolSize = 8,
        };
        var client = new StreamlineClient(options);
        Assert.NotNull(client);
    }

    [Fact]
    public void Constructor_WithNullOptions_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new StreamlineClient(null!, null));
    }

    [Fact]
    public async Task DisposeAsync_MarksClientAsDisposed()
    {
        var client = CreateClient();
        await client.DisposeAsync();

        Assert.False(await client.IsHealthyAsync());
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_DoesNotThrow()
    {
        var client = CreateClient();
        await client.DisposeAsync();
        await client.DisposeAsync();
    }

    [Fact]
    public void CreateProducer_ReturnsProducer()
    {
        var client = CreateClient();
        var producer = client.CreateProducer<string, string>();
        Assert.NotNull(producer);
    }

    [Fact]
    public void CreateProducer_WithOptions_ReturnsProducer()
    {
        var client = CreateClient();
        var producer = client.CreateProducer<string, string>(new ProducerOptions { Retries = 5 });
        Assert.NotNull(producer);
    }

    [Fact]
    public void CreateConsumer_ReturnsConsumer()
    {
        var client = CreateClient();
        var consumer = client.CreateConsumer<string, string>("test-topic", "test-group");
        Assert.NotNull(consumer);
    }

    [Fact]
    public void CreateConsumer_WithOptions_ReturnsConsumer()
    {
        var client = CreateClient();
        var consumer = client.CreateConsumer<string, string>("test-topic", new ConsumerOptions
        {
            GroupId = "my-group",
            AutoOffsetReset = AutoOffsetReset.Latest,
        });
        Assert.NotNull(consumer);
    }

    [Fact]
    public void Client_ExposesRetryPolicy()
    {
        var client = CreateClient();
        Assert.NotNull(client.RetryPolicy);
    }

    [Fact]
    public void Client_ExposesConnectionManager()
    {
        var client = CreateClient();
        Assert.NotNull(client.ConnectionManager);
    }

    [Fact]
    public async Task ProduceAsync_AfterDispose_ThrowsObjectDisposedException()
    {
        var client = CreateClient();
        await client.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => client.ProduceAsync("test-topic", "key", "value"));
    }

    [Fact]
    public async Task CreateAdmin_ReturnsAdminClient()
    {
        var options = UnitOptions();
        options.Admin.Timeout = TimeSpan.FromMilliseconds(250);

        var client = new StreamlineClient(options);
        await using (var admin = client.CreateAdmin())
        {
            Assert.NotNull(admin);
        }

        await client.DisposeAsync();
    }
}

/// <summary>
/// Tests for <see cref="StreamlineClient"/> operations that require a live broker.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class StreamlineClientIntegrationTests
{
    private readonly IntegrationServerFixture _server;

    /// <summary>Creates the test class with the shared integration fixture.</summary>
    /// <param name="server">Fixture describing the configured Streamline endpoints.</param>
    public StreamlineClientIntegrationTests(IntegrationServerFixture server)
    {
        _server = server;
    }

    private StreamlineClient CreateClient() => new(new StreamlineOptions
    {
        BootstrapServers = _server.BootstrapServers,
        Admin = new AdminOptions { HttpBaseUrl = _server.HttpBaseUrl },
    });

    [IntegrationFact]
    public async Task ProduceAsync_ReturnsMetadata()
    {
        await using var client = CreateClient();
        var result = await client.ProduceAsync("test-topic", "key", "value");

        Assert.Equal("test-topic", result.Topic);
        Assert.True(result.Offset >= 0);
    }

    [IntegrationFact]
    public async Task ProduceAsync_WithHeaders_ReturnsMetadata()
    {
        await using var client = CreateClient();
        var headers = new Headers().Add("trace-id", "abc-123");
        var result = await client.ProduceAsync("test-topic", "key", "value", headers);

        Assert.Equal("test-topic", result.Topic);
    }

    [IntegrationFact]
    public async Task IsHealthyAsync_ReturnsTrueAgainstRunningServer()
    {
        await using var client = CreateClient();
        Assert.True(await client.IsHealthyAsync());
    }
}
