using Streamline.Client;
using Xunit;

namespace Streamline.Client.Tests;

public class StreamlineClientTests
{
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
    public async Task ProduceAsync_ReturnsMetadata()
    {
        var client = new StreamlineClient("localhost:9092");
        var result = await client.ProduceAsync("test-topic", "key", "value");

        Assert.Equal("test-topic", result.Topic);
        Assert.Equal(0, result.Partition);
        Assert.True(result.Offset > 0);
    }

    [Fact]
    public async Task ProduceAsync_WithHeaders_ReturnsMetadata()
    {
        var client = new StreamlineClient("localhost:9092");
        var headers = new Headers().Add("trace-id", "abc-123");
        var result = await client.ProduceAsync("test-topic", "key", "value", headers);

        Assert.Equal("test-topic", result.Topic);
    }

    [Fact]
    public async Task IsHealthyAsync_ReturnsFalseWhenNoServer()
    {
        var options = new StreamlineOptions
        {
            BootstrapServers = "localhost:9092",
            RequestTimeout = TimeSpan.FromMilliseconds(100),
            ConnectTimeout = TimeSpan.FromMilliseconds(100),
        };
        var client = new StreamlineClient(options);
        // Without a running server, health check returns false
        Assert.False(await client.IsHealthyAsync());
    }

    [Fact]
    public async Task DisposeAsync_MarksClientAsDisposed()
    {
        var options = new StreamlineOptions
        {
            BootstrapServers = "localhost:9092",
            RequestTimeout = TimeSpan.FromMilliseconds(100),
            ConnectTimeout = TimeSpan.FromMilliseconds(100),
        };
        var client = new StreamlineClient(options);
        await client.DisposeAsync();
        Assert.False(await client.IsHealthyAsync());
    }

    [Fact]
    public void CreateProducer_ReturnsProducer()
    {
        var client = new StreamlineClient("localhost:9092");
        var producer = client.CreateProducer<string, string>();
        Assert.NotNull(producer);
    }

    [Fact]
    public void CreateProducer_WithOptions_ReturnsProducer()
    {
        var client = new StreamlineClient("localhost:9092");
        var producer = client.CreateProducer<string, string>(new ProducerOptions { Retries = 5 });
        Assert.NotNull(producer);
    }

    [Fact]
    public void CreateConsumer_ReturnsConsumer()
    {
        var client = new StreamlineClient("localhost:9092");
        var consumer = client.CreateConsumer<string, string>("test-topic", "test-group");
        Assert.NotNull(consumer);
    }

    [Fact]
    public void CreateConsumer_WithOptions_ReturnsConsumer()
    {
        var client = new StreamlineClient("localhost:9092");
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
        var client = new StreamlineClient("localhost:9092");
        Assert.NotNull(client.RetryPolicy);
    }

    [Fact]
    public void Client_ExposesConnectionManager()
    {
        var client = new StreamlineClient("localhost:9092");
        Assert.NotNull(client.ConnectionManager);
    }
}
