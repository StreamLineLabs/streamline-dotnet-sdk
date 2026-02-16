# Testcontainers Streamline (.NET)

Testcontainers module for [Streamline](https://github.com/streamlinelabs/streamline) - The Redis of Streaming.

## Features

- Kafka-compatible container for integration testing
- Fast startup (~100ms vs seconds for Kafka)
- Low memory footprint (<50MB)
- No ZooKeeper or KRaft required
- Built-in health check wait strategy
- Full async/await and `IAsyncDisposable` support
- Pre-configured `StreamlineClient` factory method

## Installation

```bash
dotnet add package Streamline.TestContainers
```

## Usage

### Basic Usage

```csharp
using Streamline.TestContainers;

await using var container = new StreamlineBuilder().Build();
await container.StartAsync();

var bootstrapServers = container.GetBootstrapServers();
// Use with any Kafka client
```

### With xUnit and IAsyncLifetime

```csharp
using Streamline.TestContainers;
using Xunit;

public class StreamlineFixture : IAsyncLifetime
{
    public StreamlineContainer Container { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        Container = new StreamlineBuilder()
            .WithDebugLogging()
            .Build();
        await Container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await Container.DisposeAsync();
    }
}

[CollectionDefinition("Streamline")]
public class StreamlineCollection : ICollectionFixture<StreamlineFixture>;

[Collection("Streamline")]
public class MyStreamlineTests
{
    private readonly StreamlineFixture _fixture;

    public MyStreamlineTests(StreamlineFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void Should_provide_bootstrap_servers()
    {
        var servers = _fixture.Container.GetBootstrapServers();
        Assert.Contains(":", servers);
    }

    [Fact]
    public async Task Should_be_healthy()
    {
        await _fixture.Container.AssertHealthyAsync();
    }
}
```

### With NUnit

```csharp
using NUnit.Framework;
using Streamline.TestContainers;

[TestFixture]
public class StreamlineIntegrationTests
{
    private StreamlineContainer _container = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _container = new StreamlineBuilder()
            .WithDebugLogging()
            .Build();
        await _container.StartAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        await _container.DisposeAsync();
    }

    [Test]
    public void Should_have_bootstrap_servers()
    {
        var servers = _container.GetBootstrapServers();
        Assert.That(servers, Does.Contain(":"));
    }

    [Test]
    public async Task Should_respond_to_health_check()
    {
        await _container.AssertHealthyAsync();
    }
}
```

### Configuration Options

```csharp
// Debug logging
await using var container = new StreamlineBuilder()
    .WithDebugLogging()
    .Build();

// Specific version
await using var container = new StreamlineBuilder()
    .WithTag("0.2.0")
    .Build();

// In-memory mode (no persistence)
await using var container = new StreamlineBuilder()
    .WithInMemory()
    .Build();

// Playground mode (demo topics pre-loaded)
await using var container = new StreamlineBuilder()
    .WithPlayground()
    .Build();

// Custom environment variables
await using var container = new StreamlineBuilder()
    .WithStreamlineEnv("MAX_MESSAGE_SIZE", "10485760")
    .Build();

// Custom startup timeout
await using var container = new StreamlineBuilder()
    .WithStartupTimeout(TimeSpan.FromSeconds(60))
    .Build();
```

### Creating Topics

```csharp
await using var container = new StreamlineBuilder().Build();
await container.StartAsync();

// Create a single topic
await container.CreateTopicAsync("my-topic", partitions: 3);

// Create multiple topics
await container.CreateTopicsAsync(new Dictionary<string, int>
{
    ["events"] = 6,
    ["commands"] = 3,
    ["dead-letter"] = 1,
});

// Wait for topics to become available
await container.WaitForTopicsAsync(
    new[] { "events", "commands" },
    timeout: TimeSpan.FromSeconds(10));
```

### Access HTTP API

```csharp
await using var container = new StreamlineBuilder().Build();
await container.StartAsync();

var healthUrl = container.GetHealthUrl();   // http://localhost:PORT/health
var metricsUrl = container.GetMetricsUrl(); // http://localhost:PORT/metrics
var infoUrl = container.GetInfoUrl();       // http://localhost:PORT/info

using var httpClient = new HttpClient();
var response = await httpClient.GetAsync(healthUrl);
Console.WriteLine($"Status: {response.StatusCode}");
```

### Using the Built-in Client Factory

```csharp
await using var container = new StreamlineBuilder().Build();
await container.StartAsync();

// Create a StreamlineClient pre-configured for this container
await using var client = container.CreateClient();

// Or with custom options
await using var client = container.CreateClient(options =>
{
    options.RequestTimeout = TimeSpan.FromSeconds(10);
    options.Producer.BatchSize = 32768;
});

await client.ProduceAsync("my-topic", "key", "Hello from testcontainers!");
```

## API Reference

### StreamlineBuilder

| Method | Description |
|--------|-------------|
| `WithTag(string)` | Sets the Docker image tag |
| `WithLogLevel(string)` | Sets log level (trace, debug, info, warn, error) |
| `WithDebugLogging()` | Enables debug logging |
| `WithTraceLogging()` | Enables trace logging |
| `WithInMemory()` | Enables in-memory storage mode |
| `WithPlayground()` | Enables playground mode |
| `WithStreamlineEnv(name, value)` | Sets a Streamline environment variable |
| `WithStartupTimeout(TimeSpan)` | Sets the startup timeout |
| `Build()` | Creates the container instance |

### StreamlineContainer

| Method | Description |
|--------|-------------|
| `GetBootstrapServers()` | Returns Kafka bootstrap servers string |
| `GetHttpUrl()` | Returns HTTP API base URL |
| `GetHealthUrl()` | Returns health check endpoint URL |
| `GetMetricsUrl()` | Returns Prometheus metrics URL |
| `GetInfoUrl()` | Returns server info endpoint URL |
| `CreateTopicAsync(name, partitions)` | Creates a topic |
| `CreateTopicsAsync(topics)` | Creates multiple topics |
| `ProduceMessageAsync(topic, value, key)` | Produces a message via CLI |
| `AssertHealthyAsync()` | Asserts the container is healthy |
| `AssertTopicExistsAsync(name)` | Asserts a topic exists |
| `WaitForTopicsAsync(topics, timeout)` | Waits for topics to exist |
| `CreateClient()` | Creates a pre-configured StreamlineClient |
| `CreateClient(configureOptions)` | Creates a StreamlineClient with custom options |

## Requirements

- .NET 8.0 or later
- Docker runtime (Docker Desktop, Colima, Podman, etc.)

## License

Apache-2.0
