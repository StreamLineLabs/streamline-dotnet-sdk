# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added
- Circuit breaker pattern (`CircuitBreaker.cs`) with async `ExecuteAsync<T>`, ILogger integration, and state change events
- Circuit breaker integration in `Producer` — automatically checks CB before SendAsync/SendBatchAsync
- Circuit breaker test suite (14 tests covering state transitions, ExecuteAsync, exception classification)
- `SendBatchAsync` on `IProducer<TKey, TValue>` for bulk message publishing with Task.WhenAll
- Circuit breaker usage example (`CircuitBreakerUsage/Program.cs`)
- TLS/SASL authentication example (`SecurityUsage/Program.cs`)
- AdminClient: `GetClusterInfoAsync()`, `GetConsumerGroupLagAsync()`, `GetConsumerGroupTopicLagAsync()`
- AdminClient: `InspectMessagesAsync()`, `LatestMessagesAsync()`, `MetricsHistoryAsync()`
- Model types: `ClusterInfo`, `BrokerInfo`, `ConsumerLag`, `ConsumerGroupLag`, `InspectedMessage`, `MetricPoint`
- Conformance tests: all 38 `Skip=TODO` stubs replaced with real implementations (P05–P08, C03–C08, G01–G06, A01–A06, S01–S06, E01–E04, F01–F04)

### Fixed
- `StreamlineClient.ProduceAsync` now delegates to real Confluent.Kafka producer (was returning fake metadata)
- Producer and Consumer now wire TLS/SASL options from `StreamlineOptions` to Confluent.Kafka config
- Producer now wires `CompressionType` to Confluent.Kafka config (was ignored)
- Producer now wires `Retries`, `RetryBackoffMs`, and `EnableIdempotence` from `ProducerOptions`
- Consumer now wires `AutoCommitIntervalMs`, `SessionTimeoutMs`, `HeartbeatIntervalMs`
- `AdminClient.DescribeConsumerGroupAsync` uses correct error code (was `TopicNotFound`)

### Changed
- refactor: simplify connection retry logic (2026-03-06)
- **Changed**: update Confluent.Kafka dependency
- **Added**: add IAsyncEnumerable consumer extensions

### Fixed
- Handle null partition assignment gracefully

### Changed
- Simplify consumer group rebalance logic


## [0.2.0] - 2026-02-18

### Added
- `StreamlineClient` targeting .NET 8.0 with nullable reference types
- `Producer` and `Consumer` with `IAsyncDisposable` support
- `IAsyncEnumerable<T>` streaming consumption
- `Admin` client for topic management
- 9-type exception hierarchy with error codes
- `CancellationToken` support throughout all APIs
- MS.Extensions.DependencyInjection integration (`AddStreamline()`)
- 29 unit tests across 4 test files

### Infrastructure
- CI pipeline with dotnet test, coverage reporting, and .NET 8.0 matrix
- CodeQL security scanning
- Release workflow with NuGet publishing
- Release drafter for automated release notes
- Dependabot for dependency updates
- CONTRIBUTING.md with development setup guide
- Security policy (SECURITY.md)
- EditorConfig for consistent formatting
- Issue templates for bug reports and feature requests

## [0.1.0] - 2026-02-18

### Added
- Initial release of Streamline .NET SDK
- Targeting .NET 8.0 with modern C# features
- `IAsyncEnumerable` streaming support
- Dependency injection integration
- Apache 2.0 license
- test: add ClientMetrics integration test suite
- test: add SchemaModel validation boundary tests
