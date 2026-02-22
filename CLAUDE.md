# CLAUDE.md — Streamline .NET SDK

## Overview
.NET 8 SDK for [Streamline](https://github.com/streamlinelabs/streamline) with `IAsyncEnumerable` consumption pattern and DI support. Communicates via the Kafka wire protocol on port 9092.

## Build & Test
```bash
dotnet restore                              # Restore packages
dotnet build --no-restore                   # Build
dotnet test --no-build --verbosity normal   # Run tests
```

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
    └── RetryPolicy.cs

tests/Streamline.Client.Tests/  # xUnit test project
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
