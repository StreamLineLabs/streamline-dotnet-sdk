// Demonstrates TLS and SASL authentication configuration.
//
// Shows how to connect to a Streamline server with:
// - TLS encryption (SSL/mTLS)
// - SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
//
// Run with:
//   dotnet run --project examples/SecurityUsage/SecurityUsage.csproj

using Streamline.Client;

var bootstrapServers = Environment.GetEnvironmentVariable("STREAMLINE_BOOTSTRAP_SERVERS") ?? "localhost:9092";

// =====================================================================
// Example 1: TLS only (server certificate validation)
// =====================================================================
Console.WriteLine("=== TLS Connection ===");
try
{
    await using var client = new StreamlineClient(bootstrapServers, new StreamlineOptions
    {
        SecurityProtocol = SecurityProtocol.Ssl,
        Tls = new TlsOptions
        {
            CaCertificatePath = "/path/to/ca-cert.pem",
            // For mutual TLS (mTLS), also set:
            // ClientCertificatePath = "/path/to/client-cert.pem",
            // ClientKeyPath = "/path/to/client-key.pem",
            SkipCertificateVerification = false,  // true only for self-signed in dev
        },
    });

    await using var producer = client.CreateProducer<string, string>();
    var metadata = await producer.SendAsync("secure-topic", "key", "Hello over TLS!");
    Console.WriteLine($"Message sent over TLS at offset={metadata.Offset}");
}
catch (StreamlineException ex)
{
    Console.WriteLine($"TLS example: {ex.Message}");
    Console.WriteLine("(Expected if server is not configured for TLS)");
}

// =====================================================================
// Example 2: SASL PLAIN authentication
// =====================================================================
Console.WriteLine("\n=== SASL PLAIN Authentication ===");
try
{
    await using var client = new StreamlineClient(bootstrapServers, new StreamlineOptions
    {
        SecurityProtocol = SecurityProtocol.SaslPlaintext,
        Sasl = new SaslOptions
        {
            Mechanism = SaslMechanism.Plain,
            Username = "my-user",
            Password = "my-password",
        },
    });

    await using var producer = client.CreateProducer<string, string>();
    var metadata = await producer.SendAsync("auth-topic", "key", "Hello with SASL PLAIN!");
    Console.WriteLine($"Message sent with SASL PLAIN at offset={metadata.Offset}");
}
catch (StreamlineException ex)
{
    Console.WriteLine($"SASL PLAIN example: {ex.Message}");
    Console.WriteLine("(Expected if server is not configured for SASL)");
}

// =====================================================================
// Example 3: SASL SCRAM-SHA-256 with TLS (most secure)
// =====================================================================
Console.WriteLine("\n=== SASL SCRAM-SHA-256 + TLS ===");
try
{
    await using var client = new StreamlineClient(bootstrapServers, new StreamlineOptions
    {
        SecurityProtocol = SecurityProtocol.SaslSsl,
        Tls = new TlsOptions
        {
            CaCertificatePath = "/path/to/ca-cert.pem",
        },
        Sasl = new SaslOptions
        {
            Mechanism = SaslMechanism.ScramSha256,
            Username = "my-user",
            Password = "my-password",
        },
    });

    await using var producer = client.CreateProducer<string, string>();
    var metadata = await producer.SendAsync("secure-auth-topic", "key", "Hello with SCRAM + TLS!");
    Console.WriteLine($"Message sent with SCRAM-SHA-256 + TLS at offset={metadata.Offset}");
}
catch (StreamlineException ex)
{
    Console.WriteLine($"SCRAM + TLS example: {ex.Message}");
    Console.WriteLine("(Expected if server is not configured for SASL+TLS)");
}

Console.WriteLine("\nDone! Adjust paths and credentials for your environment.");
