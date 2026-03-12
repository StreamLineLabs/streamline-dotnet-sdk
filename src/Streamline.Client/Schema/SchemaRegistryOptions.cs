namespace Streamline.Client.Schema;

/// <summary>
/// Configuration options for the Streamline Schema Registry client.
/// </summary>
public class SchemaRegistryOptions
{
    /// <summary>
    /// Base URL of the Streamline Schema Registry HTTP API.
    /// Defaults to the <c>STREAMLINE_SCHEMA_REGISTRY_URL</c> environment variable
    /// or <c>http://localhost:9094</c>.
    /// </summary>
    public string BaseUrl { get; set; } =
        Environment.GetEnvironmentVariable("STREAMLINE_SCHEMA_REGISTRY_URL") ?? "http://localhost:9094";

    /// <summary>
    /// Maximum number of schemas to cache in memory. Set to 0 to disable caching.
    /// </summary>
    public int CacheCapacity { get; set; } = 1000;

    /// <summary>
    /// Timeout for HTTP requests to the schema registry.
    /// </summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Optional bearer token for authentication with the schema registry.
    /// </summary>
    public string? AuthToken { get; set; }
}
