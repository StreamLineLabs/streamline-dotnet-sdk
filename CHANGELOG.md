# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- **Changed**: simplify producer configuration builder
- **Testing**: add benchmark suite for throughput measurement
- **Fixed**: resolve connection timeout on reconnect
- **Added**: add IAsyncEnumerable consumer extensions
- **Fixed**: handle deserialization errors gracefully
- **Changed**: update Confluent.Kafka dependency
- **Changed**: simplify producer configuration builder
- **Testing**: add benchmark suite for throughput measurement
- **Fixed**: resolve connection timeout on reconnect
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
