namespace Streamline.Client;

/// <summary>
/// Configuration options for the Streamline client.
/// </summary>
public class StreamlineOptions
{
    /// <summary>
    /// Bootstrap servers (comma-separated list of host:port).
    /// </summary>
    public string BootstrapServers { get; set; } =
        Environment.GetEnvironmentVariable("STREAMLINE_BOOTSTRAP_SERVERS") ?? "localhost:9092";

    /// <summary>
    /// Connection pool size.
    /// </summary>
    public int ConnectionPoolSize { get; set; } = 4;

    /// <summary>
    /// Connection timeout.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Request timeout.
    /// </summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Producer configuration.
    /// </summary>
    public ProducerOptions Producer { get; set; } = new();

    /// <summary>
    /// Consumer configuration.
    /// </summary>
    public ConsumerOptions Consumer { get; set; } = new();

    /// <summary>
    /// Security protocol for connections.
    /// </summary>
    public SecurityProtocol SecurityProtocol { get; set; } = SecurityProtocol.Plaintext;

    /// <summary>
    /// TLS/SSL configuration (used when SecurityProtocol is Ssl or SaslSsl).
    /// </summary>
    public TlsOptions? Tls { get; set; }

    /// <summary>
    /// SASL authentication configuration (used when SecurityProtocol is SaslPlaintext or SaslSsl).
    /// </summary>
    public SaslOptions? Sasl { get; set; }

    /// <summary>
    /// Admin client configuration for HTTP REST API operations.
    /// </summary>
    public AdminOptions Admin { get; set; } = new();
}

/// <summary>
/// Configuration options for the admin client HTTP API.
/// </summary>
public class AdminOptions
{
    /// <summary>
    /// Base URL for the Streamline HTTP API (default: http://localhost:9094).
    /// </summary>
    public string HttpBaseUrl { get; set; } =
        Environment.GetEnvironmentVariable("STREAMLINE_HTTP_URL") ?? "http://localhost:9094";

    /// <summary>
    /// Optional bearer token for authentication.
    /// </summary>
    public string? AuthToken { get; set; }

    /// <summary>
    /// HTTP request timeout.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);
}

/// <summary>
/// Producer configuration options.
/// </summary>
public class ProducerOptions
{
    /// <summary>
    /// Batch size in bytes.
    /// </summary>
    public int BatchSize { get; set; } = 16384;

    /// <summary>
    /// Linger time in milliseconds.
    /// </summary>
    public int LingerMs { get; set; } = 1;

    /// <summary>
    /// Maximum request size in bytes.
    /// </summary>
    public int MaxRequestSize { get; set; } = 1048576;

    /// <summary>
    /// Compression type.
    /// </summary>
    public CompressionType CompressionType { get; set; } = CompressionType.None;

    /// <summary>
    /// Number of retries.
    /// </summary>
    public int Retries { get; set; } = 3;

    /// <summary>
    /// Retry backoff in milliseconds.
    /// </summary>
    public int RetryBackoffMs { get; set; } = 100;

    /// <summary>
    /// Enable idempotent producer.
    /// </summary>
    public bool Idempotent { get; set; }
}

/// <summary>
/// Consumer configuration options.
/// </summary>
public class ConsumerOptions
{
    /// <summary>
    /// Consumer group ID.
    /// </summary>
    public string? GroupId { get; set; }

    /// <summary>
    /// Auto offset reset policy.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; set; } = AutoOffsetReset.Earliest;

    /// <summary>
    /// Enable auto-commit.
    /// </summary>
    public bool EnableAutoCommit { get; set; } = true;

    /// <summary>
    /// Auto-commit interval.
    /// </summary>
    public TimeSpan AutoCommitInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Session timeout.
    /// </summary>
    public TimeSpan SessionTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Heartbeat interval.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Maximum records per poll.
    /// </summary>
    public int MaxPollRecords { get; set; } = 500;
}

/// <summary>
/// Compression types.
/// </summary>
public enum CompressionType
{
    /// <summary>No compression.</summary>
    None,
    /// <summary>Gzip compression.</summary>
    Gzip,
    /// <summary>LZ4 compression.</summary>
    Lz4,
    /// <summary>Snappy compression.</summary>
    Snappy,
    /// <summary>Zstd compression.</summary>
    Zstd
}

/// <summary>
/// Auto offset reset policies.
/// </summary>
public enum AutoOffsetReset
{
    /// <summary>Start from the earliest offset.</summary>
    Earliest,
    /// <summary>Start from the latest offset.</summary>
    Latest,
    /// <summary>Throw an error if no offset is found.</summary>
    None
}

/// <summary>
/// Security protocol for connections.
/// </summary>
public enum SecurityProtocol
{
    /// <summary>No encryption or authentication.</summary>
    Plaintext,
    /// <summary>TLS encryption only.</summary>
    Ssl,
    /// <summary>SASL authentication without encryption.</summary>
    SaslPlaintext,
    /// <summary>SASL authentication with TLS encryption.</summary>
    SaslSsl
}

/// <summary>
/// SASL authentication mechanism.
/// </summary>
public enum SaslMechanism
{
    /// <summary>PLAIN mechanism (username/password in cleartext).</summary>
    Plain,
    /// <summary>SCRAM-SHA-256 mechanism.</summary>
    ScramSha256,
    /// <summary>SCRAM-SHA-512 mechanism.</summary>
    ScramSha512
}

/// <summary>
/// TLS/SSL configuration options.
/// </summary>
public class TlsOptions
{
    /// <summary>
    /// Path to the CA certificate file (PEM format).
    /// </summary>
    public string? CaCertificatePath { get; set; }

    /// <summary>
    /// Path to the client certificate file (PEM format) for mutual TLS.
    /// </summary>
    public string? ClientCertificatePath { get; set; }

    /// <summary>
    /// Path to the client private key file (PEM format) for mutual TLS.
    /// </summary>
    public string? ClientKeyPath { get; set; }

    /// <summary>
    /// Skip server certificate verification. NOT recommended for production.
    /// </summary>
    public bool SkipCertificateVerification { get; set; }
}

/// <summary>
/// SASL authentication options.
/// </summary>
public class SaslOptions
{
    /// <summary>
    /// SASL mechanism to use.
    /// </summary>
    public SaslMechanism Mechanism { get; set; } = SaslMechanism.Plain;

    /// <summary>
    /// Username for SASL authentication.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    /// Password for SASL authentication.
    /// </summary>
    public string? Password { get; set; }
}

