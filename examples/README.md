# Examples

Each example is a self-contained console project referenced by `Streamline.sln`, so
`dotnet build` at the repository root compiles them all and any API drift breaks the
build.

| Example | Description |
|---------|-------------|
| [BasicUsage](BasicUsage/Program.cs) | Produce, consume, and admin operations |
| [QueryUsage](QueryUsage/Program.cs) | SQL analytics with the embedded query engine |
| [SchemaRegistryUsage](SchemaRegistryUsage/Program.cs) | Schema registration and validation |
| [CircuitBreakerUsage](CircuitBreakerUsage/Program.cs) | Resilient production with a circuit breaker |
| [SecurityUsage](SecurityUsage/Program.cs) | TLS and SASL authentication |

## Prerequisites

- .NET 8+
- A running Streamline server (default: `localhost:9092` / `http://localhost:9094`)

## Running

Start Streamline. The image tag is configurable so you can pin whichever build is
available to you:

```bash
# Via the test compose stack
STREAMLINE_IMAGE=ghcr.io/streamlinelabs/streamline:0.3.0 \
  docker compose -f ../docker-compose.test.yml up -d --wait

# Or via Homebrew
streamline --playground
```

Run an example:

```bash
dotnet run --project examples/BasicUsage
dotnet run --project examples/QueryUsage
```

## Configuration

| Variable | Default | Used by |
|---|---|---|
| `STREAMLINE_BOOTSTRAP_SERVERS` | `localhost:9092` | BasicUsage, CircuitBreakerUsage, SchemaRegistryUsage, SecurityUsage |
| `STREAMLINE_BOOTSTRAP` | `localhost:9092` | QueryUsage |
| `STREAMLINE_HTTP` | `http://localhost:9094` | QueryUsage |
| `STREAMLINE_SCHEMA_REGISTRY_URL` | `http://localhost:9094` | SchemaRegistryUsage |

```bash
export STREAMLINE_BOOTSTRAP_SERVERS=my-server:9092
dotnet run --project examples/BasicUsage
```
