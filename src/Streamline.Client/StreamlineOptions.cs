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
    public bool Idempotent { get; set; } = false;
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
