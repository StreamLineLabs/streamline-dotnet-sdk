# Streamline .NET SDK

[![CI](https://github.com/streamlinelabs/streamline-dotnet-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/streamlinelabs/streamline-dotnet-sdk/actions/workflows/ci.yml)
[![codecov](https://img.shields.io/codecov/c/github/streamlinelabs/streamline-dotnet-sdk?style=flat-square)](https://codecov.io/gh/streamlinelabs/streamline-dotnet-sdk)
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

### Transactions

```csharp
using var producer = client.CreateProducer<string, string>();
await producer.BeginTransactionAsync();
try
{
    await producer.SendAsync("orders", "k1", "v1");
    await producer.SendAsync("orders", "k2", "v2");
    await producer.CommitTransactionAsync();
}
catch
{
    await producer.AbortTransactionAsync();
    throw;
}
```

> **Note:** Transactions use client-side buffering. Messages are collected and sent as a batch
> on commit, providing all-or-nothing delivery at the client level.

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
- Admin client (topic management, consumer groups, SQL queries via HTTP REST API)
- SQL query support via `QueryClient`
- Dependency injection integration (`AddStreamline()`, `AddStreamlineAdmin()`)
- Connection pooling
- Automatic reconnection
- Compression support (LZ4, Zstd, Snappy, Gzip)
- TLS/mTLS support with configurable certificates
- SASL authentication (PLAIN, SCRAM-SHA-256/512)
- Schema Registry integration
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

### Admin Client

```csharp
var admin = client.CreateAdmin("http://localhost:9094");

// Cluster overview
var cluster = await admin.GetClusterInfoAsync();
Console.WriteLine($"Cluster: {cluster.ClusterId}, Brokers: {cluster.Brokers.Count}");

// Consumer group lag monitoring
var lag = await admin.GetConsumerGroupLagAsync("my-group");
Console.WriteLine($"Total lag: {lag.TotalLag}");
foreach (var p in lag.Partitions)
    Console.WriteLine($"  {p.Topic}:{p.Partition} lag={p.Lag}");

// Message inspection
var messages = await admin.InspectMessagesAsync("events", partition: 0, limit: 10);
foreach (var m in messages)
    Console.WriteLine($"offset={m.Offset} value={m.Value}");

// Latest messages
var latest = await admin.LatestMessagesAsync("events", count: 5);

// Server metrics
var metrics = await admin.MetricsHistoryAsync();

// Basic operations
await admin.CreateTopicAsync("events", partitions: 3);
var topics = await admin.ListTopicsAsync();
var info = await admin.GetServerInfoAsync();
var healthy = await admin.IsHealthyAsync();
```

## Error Handling

```csharp
using Streamline;

try
{
    await client.ProduceAsync("my-topic", "key", "value");
}
catch (TopicNotFoundException ex)
{
    Console.WriteLine($"Topic not found: {ex.Message}");
    Console.WriteLine($"Hint: {ex.Hint}");
}
catch (StreamlineException ex) when (ex.Retryable)
{
    Console.WriteLine($"Retryable error: {ex.Message}");
}
catch (StreamlineException ex)
{
    Console.WriteLine($"Fatal error: {ex.Message}");
}
```

## Configuration Reference

### Client (`StreamlineOptions`)

| Property | Default | Description |
|---|---|---|
| `BootstrapServers` | `"localhost:9092"` | Comma-separated broker addresses |
| `ConnectionPoolSize` | `4` | Number of connections per broker |
| `ConnectTimeout` | `10s` | Connection timeout (`TimeSpan`) |
| `RequestTimeout` | `30s` | Request timeout (`TimeSpan`) |

### Producer (`StreamlineProducerOptions`)

| Property | Default | Description |
|---|---|---|
| `BatchSize` | `16384` | Maximum batch size in bytes |
| `LingerMs` | `0` | Batch linger time in milliseconds |
| `CompressionType` | `None` | Compression: `None`, `Gzip`, `Snappy`, `Lz4`, `Zstd` |
| `Retries` | `3` | Retries on transient failures |
| `Acks` | `Leader` | Acknowledgment: `None`, `Leader`, `All` |
| `EnableIdempotence` | `false` | Enable exactly-once semantics |

### Consumer (`StreamlineConsumerOptions`)

| Property | Default | Description |
|---|---|---|
| `GroupId` | *(required)* | Consumer group identifier |
| `AutoOffsetReset` | `Latest` | Start position: `Earliest`, `Latest` |
| `EnableAutoCommit` | `true` | Automatically commit offsets |
| `MaxPollRecords` | `500` | Maximum records per poll |
| `SessionTimeoutMs` | `30000` | Session timeout in milliseconds |

### Security

| Property | Default | Description |
|---|---|---|
| `SecurityProtocol` | `Plaintext` | Protocol: `Plaintext`, `Ssl`, `SaslPlaintext`, `SaslSsl` |
| `SaslMechanism` | — | SASL mechanism: `Plain`, `ScramSha256`, `ScramSha512` |
| `SslCaLocation` | — | Path to CA certificate |

## Circuit Breaker

Protect your application from cascading failures when the Streamline server is unresponsive:

```csharp
using Streamline.Client;

var breaker = new CircuitBreaker(new CircuitBreakerOptions
{
    FailureThreshold = 5,      // Open after 5 consecutive failures
    SuccessThreshold = 2,      // Close after 2 half-open successes
    OpenTimeout = TimeSpan.FromSeconds(30),
});

breaker.OnStateChange += (from, to) =>
    logger.LogInformation("Circuit: {From} → {To}", from, to);

// Wrap async producer calls
var metadata = await breaker.ExecuteAsync(async () =>
    await producer.SendAsync("events", "user-1", eventPayload)
);
```

When the circuit is open, `ExecuteAsync` throws a retryable `StreamlineException`. See the [Circuit Breaker guide](https://streamlinelabs.dev/docs/features/circuit-breaker) for details.

## Examples

The [`examples/`](examples/) directory contains runnable examples:

| Example | Description |
|---------|-------------|
| [BasicUsage.cs](examples/BasicUsage.cs) | Produce, consume, and admin operations |
| [QueryUsage](examples/QueryUsage/Program.cs) | SQL analytics with the embedded query engine |
| [SchemaRegistryUsage](examples/SchemaRegistryUsage/Program.cs) | Schema registration and validation |
| [CircuitBreakerUsage](examples/CircuitBreakerUsage/Program.cs) | Resilient production with circuit breaker |
| [SecurityUsage](examples/SecurityUsage/Program.cs) | TLS and SASL authentication |

Run any example:

```bash
dotnet run --project examples/QueryUsage
```

## Contributing

Contributions are welcome! Please see the [organization contributing guide](https://github.com/streamlinelabs/.github/blob/main/CONTRIBUTING.md) for guidelines.

## License

Apache 2.0

## Security

To report a security vulnerability, please email **security@streamline.dev**.
Do **not** open a public issue.

See the [Security Policy](https://github.com/streamlinelabs/streamline/blob/main/SECURITY.md) for details.









