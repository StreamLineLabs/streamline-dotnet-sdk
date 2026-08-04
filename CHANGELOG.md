# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

### Added
- `Streamline.TestSupport` shared test project: `StreamlineTestEnvironment`,
  `IntegrationFactAttribute` / `IntegrationTheoryAttribute` (tagged `Category=Integration`),
  `IntegrationServerFixture` and `IntegrationEndpointProbe`.
- Opt-in integration testing via `STREAMLINE_INTEGRATION=1`. When enabled but the
  configured endpoints are unreachable, the run fails fast with an actionable message
  instead of retrying `localhost`.
- Configurable test endpoints and image: `STREAMLINE_BOOTSTRAP_SERVERS`
  (alias `STREAMLINE_BOOTSTRAP`), `STREAMLINE_HTTP_URL` (alias `STREAMLINE_HTTP`),
  `STREAMLINE_IMAGE`, `STREAMLINE_KAFKA_PORT`, `STREAMLINE_HTTP_PORT` and
  `STREAMLINE_READY_TIMEOUT_SECONDS`.
- `AdminClient(httpBaseUrl, authToken, timeout)` and `QueryClient(baseUrl, timeout)`
  overloads, plus `AdminClient.DefaultTimeout` / `QueryClient.DefaultTimeout`. The
  pre-existing constructor signatures are retained, so the change is binary compatible.
- `StreamlineClient.CreateAdmin(httpBaseUrl, authToken)` for callers that need to send a
  bearer token to an endpoint other than the configured one.
- Regression tests covering test selection, environment parsing and probe bounding.
- Make targets `integration-up`, `integration-down`, `conformance-test` and `test-all`.

### Changed
- **The default `dotnet test` run is now hermetic and bounded.** It contacts no broker,
  no HTTP endpoint and no `localhost`, and completes in seconds instead of hanging.
  Broker-dependent tests are skipped unless explicitly enabled.
- Producers and consumers create their librdkafka handle lazily, so constructing or
  disposing a client that is never used performs no network I/O.
- Producers and consumers now receive explicit bounded `message.timeout.ms`,
  `socket.timeout.ms` and `socket.connection.setup.timeout.ms` derived from
  `StreamlineOptions.RequestTimeout` / `ConnectTimeout`, replacing librdkafka's
  five-minute delivery default.
- `docker-compose.test.yml` takes the server image and host ports from environment
  variables instead of a hard-coded (and unavailable) `0.3.0` tag.
- `tests/Streamline.Conformance` and `tests/Streamline.TestSupport` are part of
  `Streamline.sln`, so `dotnet restore` / `build` / `test` cover them.
- Every `examples/` sample is a project in the solution and is compiled by
  `dotnet build`, so API drift now breaks the build.

### Fixed
- `dotnet test` no longer hangs: producer, consumer and admin operations against an
  unreachable server now time out instead of blocking indefinitely.
- `tests/Streamline.Conformance` no longer fails to compile (17 errors from API drift:
  `SchemaType` → `SchemaFormat`, `TlsOptions.Enabled`/`CertificatePath`/`KeyPath`,
  `StreamlineException.Retryable` → `IsRetryable`, `ConsumerGroupMetadata.GroupId` → `Id`).
- `examples/SchemaRegistryUsage` and `examples/SecurityUsage` used APIs that do not
  exist (`RegisterAsync`, `SchemaType`, a `StreamlineClient(string, StreamlineOptions)`
  constructor, `ConsumerOptions.SchemaRegistryUrl`).
- `CircuitBreaker.ExecuteAsync` no longer trips the circuit on non-retryable failures
  such as authentication errors.
- `Consumer.PollAsync` and `Consumer.ConsumeAsync` now honour cancellation by throwing
  `OperationCanceledException` instead of silently returning.
- `Consumer.SeekAsync` validates the offset before contacting the broker.
- `StreamlineClient.CreateAdmin()` now applies the configured `AdminOptions.Timeout`,
  which was previously ignored; `Consumer.SearchAsync` bounds its HTTP client too.
- `Producer.FlushAsync` is bounded and now throws `StreamlineTimeoutException` when the
  budget expires with messages still in flight, instead of blocking forever (or, as an
  intermediate fix, reporting success); disposal drains with the same bound.
- `Producer`/`Consumer` serialise lazy handle creation with disposal, so a concurrent
  first use cannot leak a native librdkafka handle past `DisposeAsync`.


## [0.3.0] - 2026-04-20

### Added
- `Streamline.Client.Moonshot` namespace — async HTTP clients for the
  Streamline Moonshot control plane (port `9094`): `BranchesClient`,
  `ContractsClient`, `AttestationClient`, `SearchClient`, `MemoryClient`.
- Clients implement `IAsyncDisposable`; share `MoonshotClientOptions` and
  `MoonshotException`.

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
