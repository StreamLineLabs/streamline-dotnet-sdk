// Schema Registry example demonstrating Avro schema management and
// validated produce/consume with the Streamline .NET SDK.
//
// Ensure a Streamline server is running at localhost:9092 with the
// schema registry enabled on port 9094 before running:
//   streamline --playground
//
// Run with:
//   dotnet run --project examples/SchemaRegistryUsage

using System.Text.Json;
using Streamline.Client;
using Streamline.Client.Schema;

// Avro schema for a User record
const string userSchema = """
{
  "type": "record",
  "name": "User",
  "namespace": "com.streamline.examples",
  "fields": [
    {"name": "id",         "type": "int"},
    {"name": "name",       "type": "string"},
    {"name": "email",      "type": "string"},
    {"name": "created_at", "type": "string"}
  ]
}
""";

const string subject = "users-value";
const string topic = "users";

var bootstrapServers = Environment.GetEnvironmentVariable("STREAMLINE_BOOTSTRAP_SERVERS") ?? "localhost:9092";
var registryUrl = Environment.GetEnvironmentVariable("STREAMLINE_SCHEMA_REGISTRY_URL") ?? "http://localhost:9094";

// === 1. Create a Streamline client ===
await using var client = new StreamlineClient(bootstrapServers);

// === 2. Create a schema registry client ===
var registry = new SchemaRegistryClient(registryUrl);

// === 3. Register an Avro schema ===
Console.WriteLine("=== Registering Schema ===");
var schemaId = await registry.RegisterAsync(subject, userSchema, SchemaType.Avro);
Console.WriteLine($"Registered schema with id={schemaId} for subject={subject}");

// Retrieve the schema back by id
var retrieved = await registry.GetSchemaAsync(schemaId);
Console.WriteLine($"Retrieved schema: {retrieved}");

// === 4. Check schema compatibility ===
Console.WriteLine("\n=== Checking Compatibility ===");
var compatible = await registry.CheckCompatibilityAsync(subject, userSchema, SchemaType.Avro);
Console.WriteLine($"Schema compatible: {compatible}");

// === 5. Produce messages with schema validation ===
Console.WriteLine("\n=== Producing Messages with Schema ===");
await using var producer = client.CreateProducer<string, string>();

for (var i = 0; i < 5; i++)
{
    var user = new
    {
        id = i,
        name = $"user-{i}",
        email = $"user{i}@example.com",
        created_at = "2025-01-15T10:00:00Z",
    };

    var value = JsonSerializer.Serialize(user);
    var metadata = await producer.SendAsync(topic, $"user-{i}", value, schemaId);
    Console.WriteLine($"Produced user-{i} at partition={metadata.Partition}, offset={metadata.Offset}");
}

await producer.FlushAsync();

// === 6. Consume and deserialize with schema ===
Console.WriteLine("\n=== Consuming Messages with Schema ===");
await using var consumer = client.CreateConsumer<string, string>(topic, "dotnet-schema-group",
    new ConsumerOptions { SchemaRegistryUrl = registryUrl });
await consumer.SubscribeAsync();

var records = await consumer.PollAsync(TimeSpan.FromSeconds(5));
foreach (var record in records)
{
    var user = JsonSerializer.Deserialize<User>(record.Value);
    Console.WriteLine(
        $"Received: partition={record.Partition}, offset={record.Offset}, " +
        $"user={{id:{user?.Id}, name:{user?.Name}, email:{user?.Email}}}");
}

await consumer.CommitAsync();
Console.WriteLine("\nDone!");

// User record matching the registered Avro schema
internal record User(int Id, string Name, string Email, string CreatedAt);
