# Examples

## Prerequisites

- .NET 8+
- A running Streamline server (default: `localhost:9092`)

## Running

Start Streamline:

```bash
# Via Docker
docker run -p 9092:9092 -p 9094:9094 ghcr.io/streamlinelabs/streamline:0.2.0 --playground

# Or via Homebrew
streamline --playground
```

Run the example:

```bash
dotnet run --project examples
```

## Configuration

Set `STREAMLINE_BOOTSTRAP_SERVERS` to connect to a non-local server:

```bash
export STREAMLINE_BOOTSTRAP_SERVERS=my-server:9092
dotnet run --project examples
```
