# Streamline .NET SDK

[![CI](https://github.com/streamlinelabs/streamline-dotnet-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/streamlinelabs/streamline-dotnet-sdk/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-8%2B-purple.svg)](https://dotnet.microsoft.com/)
[![Docs](https://img.shields.io/badge/docs-streamlinelabs.dev-blue.svg)](https://streamlinelabs.dev/docs/sdks/dotnet)

Native .NET client library for Streamline streaming platform.

## Installation

```bash
dotnet add package Streamline.Client
```

## Quick Start

### Producer

```csharp
using Streamline.Client;

// Create client
var client = new StreamlineClient("localhost:9092");

// Produce a message
var metadata = await client.ProduceAsync("my-topic", "key", "Hello, Streamline!");
Console.WriteLine($"Produced to partition {metadata.Partition} at offset {metadata.Offset}");
```

### Consumer

```csharp
using Streamline.Client;

var client = new StreamlineClient("localhost:9092");

await using var consumer = client.CreateConsumer<string, string>("my-topic", "my-group");
await consumer.SubscribeAsync();

await foreach (var record in consumer.ConsumeAsync())
{
    Console.WriteLine($"Received: {record.Value} at offset {record.Offset}");
}
```

### With Dependency Injection

```csharp
// In Program.cs or Startup.cs
services.AddStreamline(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.Producer.BatchSize = 16384;
    options.Consumer.GroupId = "my-app";
});

// In your service
public class EventService
{
    private readonly IStreamlineClient _client;

    public EventService(IStreamlineClient client)
    {
        _client = client;
    }

    public async Task PublishEventAsync(string topic, Event evt)
    {
        await _client.ProduceAsync(topic, evt.Id, evt);
    }
}
```

## Features

- Async/await support throughout
- `IAsyncEnumerable` for consuming messages
- Dependency injection integration
- Connection pooling
- Automatic reconnection
- Compression support (LZ4, Zstd, Snappy, Gzip)
- [Testcontainers integration](./testcontainers/README.md) for seamless integration testing
- OpenTelemetry-compatible distributed tracing via `System.Diagnostics.ActivitySource`

## OpenTelemetry Tracing

The SDK provides distributed tracing using the standard .NET `ActivitySource` API,
which is built into the runtime and compatible with OpenTelemetry .NET SDK. No
additional NuGet packages are required for basic tracing.

### Setup with OpenTelemetry

```csharp
// In Program.cs
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddSource(StreamlineActivitySource.SourceName) // "Streamline.Client"
        .AddOtlpExporter());
```

### Usage

```csharp
using Streamline.Client.Telemetry;

// Produce with tracing
using var produceActivity = StreamlineActivitySource.StartProduce("orders");
var headers = new Headers();
StreamlineActivitySource.InjectContext(headers); // propagate trace context
await producer.SendAsync("orders", key, value, headers);

// Consume with tracing
using var consumeActivity = StreamlineActivitySource.StartConsume("events");
var records = await consumer.PollAsync(TimeSpan.FromMilliseconds(100));
consumeActivity?.SetTag("messaging.batch.message_count", records.Count.ToString());

// Process individual records with context extraction
foreach (var record in records)
{
    var parentCtx = StreamlineActivitySource.ExtractContext(record.Headers);
    using var processActivity = StreamlineActivitySource.StartProcess(
        record.Topic, record.Partition, record.Offset, parentCtx);
    ProcessRecord(record);
}
```

### Span Conventions

| Attribute | Value |
|-----------|-------|
| Activity name | `{topic} {operation}` (e.g., "orders produce") |
| `messaging.system` | `streamline` |
| `messaging.destination.name` | Topic name |
| `messaging.operation` | `produce`, `consume`, or `process` |
| Activity kind | `Producer` for produce, `Consumer` for consume |

When no OpenTelemetry listener is registered, `StartActivity` returns null and
there is zero overhead. Trace context is propagated via W3C TraceContext headers.

## Configuration

### Client Options

```csharp
var options = new StreamlineOptions
{
    BootstrapServers = "localhost:9092",
    ConnectionPoolSize = 4,
    ConnectTimeout = TimeSpan.FromSeconds(30),
    RequestTimeout = TimeSpan.FromSeconds(30)
};

var client = new StreamlineClient(options);
```

### Producer Options

```csharp
var producerOptions = new ProducerOptions
{
    BatchSize = 16384,
    LingerMs = 1,
    CompressionType = CompressionType.Lz4,
    Retries = 3
};

var producer = client.CreateProducer<string, string>(producerOptions);
```

### Consumer Options

```csharp
var consumerOptions = new ConsumerOptions
{
    GroupId = "my-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true,
    MaxPollRecords = 500
};

var consumer = client.CreateConsumer<string, string>("my-topic", consumerOptions);
```

## Requirements

- .NET 8.0 or later
- Streamline server 0.2.0 or later

## Testing

### Testcontainers (Recommended)

Use the `Streamline.TestContainers` package for fully automated integration testing with Docker:

```bash
dotnet add package Streamline.TestContainers
```

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

[Collection("Streamline")]
public class MyIntegrationTests
{
    private readonly StreamlineFixture _fixture;

    public MyIntegrationTests(StreamlineFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Should_produce_and_consume()
    {
        await using var client = _fixture.Container.CreateClient();
        await client.ProduceAsync("my-topic", "key", "Hello!");
    }
}
```

See the [testcontainers README](./testcontainers/README.md) for full documentation.

### Manual Integration Tests

Start a local Streamline server:

```bash
docker compose -f docker-compose.test.yml up -d
```

Run tests:

```bash
dotnet test
```

## API Reference

### Client

| Method | Description |
|--------|-------------|
| `new StreamlineClient(bootstrapServers)` | Create a new client |
| `new StreamlineClient(options, logger)` | Create with configuration |
| `await client.ProduceAsync(topic, key, value)` | Send a message |
| `await client.ProduceAsync(topic, key, value, headers)` | Send with headers |
| `client.CreateProducer<TKey, TValue>()` | Create a producer |
| `client.CreateConsumer<TKey, TValue>(topic, groupId)` | Create a consumer |
| `await client.IsHealthyAsync()` | Check health status |

### Producer

| Method | Description |
|--------|-------------|
| `await producer.SendAsync(topic, key, value)` | Send a message |
| `await producer.SendAsync(topic, key, value, headers)` | Send with headers |
| `await producer.FlushAsync()` | Flush buffered messages |

### Consumer

| Method | Description |
|--------|-------------|
| `await consumer.SubscribeAsync()` | Subscribe to topic |
| `consumer.ConsumeAsync()` | Get `IAsyncEnumerable` of messages |
| `await consumer.PollAsync(timeout)` | Poll for records |
| `await consumer.CommitAsync()` | Commit offsets |
| `await consumer.SeekToBeginningAsync()` | Seek to start |
| `await consumer.SeekToEndAsync()` | Seek to end |
| `await consumer.SeekAsync(partition, offset)` | Seek to specific offset |

## License

Apache 2.0
