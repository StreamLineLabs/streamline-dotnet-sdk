# CLAUDE.md — Streamline .NET SDK

## Overview
.NET 8 SDK for [Streamline](https://github.com/streamlinelabs/streamline) with `IAsyncEnumerable` consumption pattern and DI support. Communicates via the Kafka wire protocol on port 9092.

## Build & Test
```bash
dotnet restore                              # Restore packages
dotnet build --no-restore                   # Build
dotnet test --no-build --verbosity normal   # Run the hermetic test suite (no server needed)
```

The default test run is **hermetic and bounded**: it contacts no broker, no HTTP
endpoint, and no `localhost`. Broker-dependent tests are opt-in:

```bash
STREAMLINE_IMAGE=<image> docker compose -f docker-compose.test.yml up -d --wait
STREAMLINE_INTEGRATION=1 dotnet test --filter "Category=Integration"
```

When `STREAMLINE_INTEGRATION` is set but the endpoints are unreachable, the run
**fails fast** with an actionable message rather than skipping or retrying.

## Architecture
```
src/Streamline.Client/
├── StreamlineClient.cs          # Main client implementing IStreamlineClient
├── StreamlineProducer.cs        # Producer with batching
├── StreamlineConsumer.cs        # Consumer with IAsyncEnumerable<T>
├── StreamlineAdmin.cs           # Topic/group admin
├── Configuration/
│   ├── StreamlineOptions.cs     # Options pattern configuration
│   └── ServiceCollectionExtensions.cs  # DI registration
├── Exceptions/
│   ├── StreamlineException.cs   # Base exception with ErrorCode, Retryable, Hint
│   ├── StreamlineConnectionException.cs
│   ├── StreamlineAuthenticationException.cs
│   └── StreamlineTimeoutException.cs
├── Models/
│   ├── ConsumerRecord.cs
│   ├── ProducerRecord.cs
│   └── Headers.cs
└── Internal/
    ├── ConnectionManager.cs
    ├── KafkaTimeouts.cs         # Bounded librdkafka timeouts
    └── RetryPolicy.cs

tests/Streamline.Client.Tests/  # xUnit unit tests (hermetic by default)
tests/Streamline.Conformance/   # Cross-SDK conformance suite (opt-in)
tests/Streamline.TestSupport/   # Shared test gating: IntegrationFact, fixtures, probes
examples/                       # Runnable example projects (compiled by the solution)
benchmarks/                     # BenchmarkDotNet project
testcontainers/                 # Testcontainers integration
```

## Coding Conventions
- **Nullable reference types**: Enabled (`<Nullable>enable</Nullable>`)
- **IAsyncEnumerable**: Use for streaming consumption (`ConsumeAsync()`)
- **Options pattern**: Use `IOptions<StreamlineOptions>` for configuration
- **DI registration**: `services.AddStreamline(options => { ... })`
- **XML docs**: Required on all public APIs (`<summary>`, `<param>`, `<returns>`)
- **Exception hierarchy**: All exceptions derive from `StreamlineException`
- **Naming**: PascalCase for public members, _camelCase for private fields

## Consumer Pattern
```csharp
await foreach (var record in consumer.ConsumeAsync<string, string>(cancellationToken))
{
    Console.WriteLine($"{record.Key}: {record.Value}");
}
```

## Testing
- xUnit 2.6 for unit tests
- BenchmarkDotNet for performance benchmarks
- Testcontainers 3.10 for integration tests
- **Hermetic by default**: a unit test must never open a connection. Client handles are
  created lazily, so constructing a producer/consumer does not connect.
- Never point a unit test at `localhost`. Use
  `StreamlineTestEnvironment.UnitBootstrapServers` / `UnitHttpBaseUrl` (RFC 6761
  `.invalid` hosts) or an `HttpMessageHandler` stub.
- Tests that genuinely need a server use `[IntegrationFact]` / `[IntegrationTheory]`
  from `Streamline.TestSupport` and join `[Collection(IntegrationCollection.Name)]`.
  Each test assembly declares its own `[CollectionDefinition]` — xUnit only discovers
  them in the assembly under test.
- Every network client must have a bounded timeout. `KafkaTimeouts` maps
  `StreamlineOptions.RequestTimeout` / `ConnectTimeout` onto librdkafka settings;
  `HttpClient` instances must set `Timeout`.
