// Demonstrates the circuit breaker pattern for resilient message production.
//
// Run with:
//   dotnet run --project examples/CircuitBreakerUsage/CircuitBreakerUsage.csproj

using Streamline.Client;

var bootstrapServers = Environment.GetEnvironmentVariable("STREAMLINE_BOOTSTRAP_SERVERS") ?? "localhost:9092";

var breaker = new CircuitBreaker(new CircuitBreakerOptions
{
    FailureThreshold = 5,
    SuccessThreshold = 2,
    OpenTimeout = TimeSpan.FromSeconds(30),
    HalfOpenMaxRequests = 3,
});

breaker.OnStateChange += (from, to) =>
    Console.WriteLine($"[Circuit Breaker] {from} → {to}");

await using var client = new StreamlineClient(bootstrapServers);
await using var producer = client.CreateProducer<string, string>();

for (var i = 0; i < 20; i++)
{
    try
    {
        var metadata = await breaker.ExecuteAsync(async () =>
            await producer.SendAsync("events", $"key-{i}", $"{{\"event\":\"click\",\"i\":{i}}}")
        );
        Console.WriteLine($"Sent message {i}: partition={metadata.Partition}, offset={metadata.Offset}");
    }
    catch (StreamlineException ex) when (ex.IsRetryable)
    {
        Console.WriteLine($"Retryable error (circuit: {breaker.State}): {ex.Message}");
        await Task.Delay(1000);
    }
    catch (StreamlineException ex)
    {
        Console.Error.WriteLine($"Non-retryable error: {ex.Message}");
        break;
    }
}

Console.WriteLine($"\nFinal circuit state: {breaker.State}");
