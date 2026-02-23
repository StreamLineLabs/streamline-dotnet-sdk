// Basic example demonstrating Streamline .NET SDK usage.
//
// Ensure a Streamline server is running at localhost:9092 before running:
//   streamline --playground
//
// Run with:
//   dotnet run --project examples/BasicUsage.csproj

using Streamline.Client;

// Create a client
await using var client = new StreamlineClient(Environment.GetEnvironmentVariable("STREAMLINE_BOOTSTRAP_SERVERS") ?? "localhost:9092");

// --- Produce Messages ---
Console.WriteLine("=== Producing Messages ===");

var metadata = await client.ProduceAsync("my-topic", "key-1", "Hello from .NET SDK!");
Console.WriteLine($"Produced: topic={metadata.Topic}, partition={metadata.Partition}, offset={metadata.Offset}");

var headers = new Headers()
    .Add("trace-id", "abc-123")
    .Add("source", "dotnet-example");
var metadata2 = await client.ProduceAsync("my-topic", "key-2", "{\"event\":\"user_signup\"}", headers);
Console.WriteLine($"Produced with headers at offset={metadata2.Offset}");

// --- Batch Produce with Producer ---
Console.WriteLine("\n=== Batch Producing ===");
await using var producer = client.CreateProducer<string, string>();
for (int i = 0; i < 5; i++)
{
    await producer.SendAsync("my-topic", $"key-{i}", $"Message {i}");
}
await producer.FlushAsync();
Console.WriteLine("Produced 5 messages");

// --- Consume Messages ---
Console.WriteLine("\n=== Consuming Messages ===");
await using var consumer = client.CreateConsumer<string, string>("my-topic", "dotnet-example-group");
await consumer.SubscribeAsync();

var records = await consumer.PollAsync(TimeSpan.FromSeconds(5));
foreach (var record in records)
{
    Console.WriteLine($"Received: partition={record.Partition}, offset={record.Offset}, key={record.Key}, value={record.Value}");
}

await consumer.CommitAsync();
Console.WriteLine("\nDone!");
