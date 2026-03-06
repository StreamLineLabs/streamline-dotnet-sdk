// Streamline SQL Query Example
//
// Demonstrates using Streamline's embedded analytics engine (DuckDB)
// to run SQL queries on streaming data.
//
// Prerequisites:
//   - Streamline server running
//   - dotnet add package Streamline.Client
//
// Run:
//   dotnet run

using Streamline.Client;
using Streamline.Client.Query;

var bootstrap = Environment.GetEnvironmentVariable("STREAMLINE_BOOTSTRAP") ?? "localhost:9092";
var httpUrl = Environment.GetEnvironmentVariable("STREAMLINE_HTTP") ?? "http://localhost:9094";

// Produce sample data
await using var client = new StreamlineClient(new StreamlineOptions { BootstrapServers = bootstrap });
var admin = client.CreateAdmin(httpUrl);

await admin.CreateTopicAsync("events", partitions: 1);
for (int i = 0; i < 10; i++)
{
    await client.ProduceAsync("events", $"key-{i}",
        $$"""{"user":"user-{{i}}","action":"click","value":{{i * 10}}}""");
}
Console.WriteLine("Produced 10 events");

// Query the data
using var queryClient = new QueryClient(httpUrl);

// Simple SELECT
Console.WriteLine("\n--- All events (limit 5) ---");
var result = await queryClient.QueryAsync("SELECT * FROM topic('events') LIMIT 5");
Console.WriteLine($"Columns: {result.Columns.Length}, Rows: {result.Rows.Length}");
foreach (var row in result.Rows)
{
    Console.WriteLine($"  [{string.Join(", ", row)}]");
}

// Aggregation
Console.WriteLine("\n--- Count by action ---");
result = await queryClient.QueryAsync(
    "SELECT action, COUNT(*) as cnt FROM topic('events') GROUP BY action");
foreach (var row in result.Rows)
{
    Console.WriteLine($"  [{string.Join(", ", row)}]");
}

// Query with options
Console.WriteLine("\n--- With custom timeout and limit ---");
result = await queryClient.QueryAsync(
    "SELECT * FROM topic('events') ORDER BY offset DESC",
    timeoutMs: 5000,
    maxRows: 3);
Console.WriteLine($"Returned {result.Rows.Length} rows");

// Explain query plan
Console.WriteLine("\n--- Query plan ---");
var plan = await queryClient.ExplainAsync(
    "SELECT * FROM topic('events') WHERE value > 50");
Console.WriteLine(plan);

await admin.DisposeAsync();
