# Contributing to Streamline .NET SDK

Thank you for your interest in contributing to the Streamline .NET SDK! This guide will help you get started.

## Getting Started

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests and linting
5. Commit your changes (`git commit -m "Add my feature"`)
6. Push to your fork (`git push origin feature/my-feature`)
7. Open a Pull Request

## Prerequisites

- .NET 8.0 SDK or later

## Development Setup

```bash
# Clone your fork
git clone https://github.com/<your-username>/streamline-dotnet-sdk.git
cd streamline-dotnet-sdk

# Restore dependencies
dotnet restore

# Build
dotnet build --no-restore

# Run tests
dotnet test --no-build --verbosity normal
```

## Running Tests

```bash
# Run all tests
dotnet test

# Run with verbose output
dotnet test --verbosity detailed

# Run a specific test project
dotnet test tests/Streamline.Client.Tests/

# Run with coverage
dotnet test --collect:"XPlat Code Coverage"
```

### Integration Tests

Integration tests require a running Streamline server:

```bash
# Start the server
docker compose -f docker-compose.test.yml up -d

# Run integration tests
dotnet test --filter "Category=Integration"

# Stop the server
docker compose -f docker-compose.test.yml down
```

## Code Style

- Follow standard C# conventions and existing code patterns
- Use XML doc comments (`///`) for public APIs
- Use `IAsyncEnumerable` for streaming consumption patterns
- Handle exceptions properly — use specific exception types
- Follow the [.NET naming guidelines](https://learn.microsoft.com/en-us/dotnet/standard/design-guidelines/naming-guidelines)

## Pull Request Guidelines

- Write clear commit messages
- Add tests for new functionality
- Update documentation if needed
- Ensure `dotnet build` and `dotnet test` pass before submitting

## Reporting Issues

- Use the **Bug Report** or **Feature Request** issue templates
- Search existing issues before creating a new one
- Include reproduction steps for bugs

## Code of Conduct

All contributors are expected to follow our [Code of Conduct](https://github.com/streamlinelabs/.github/blob/main/CODE_OF_CONDUCT.md).

## License

By contributing, you agree that your contributions will be licensed under the Apache-2.0 License.
