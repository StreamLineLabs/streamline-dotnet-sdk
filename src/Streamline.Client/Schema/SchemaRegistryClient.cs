namespace Streamline.Client.Schema;

using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// Schema types supported by the registry.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum SchemaType
{
    /// <summary>Apache Avro schema format.</summary>
    Avro,
    /// <summary>Protocol Buffers schema format.</summary>
    Protobuf,
    /// <summary>JSON Schema format.</summary>
    Json
}

/// <summary>
/// Represents a registered schema.
/// </summary>
public record SchemaInfo(
    [property: JsonPropertyName("id")] int Id,
    [property: JsonPropertyName("subject")] string? Subject,
    [property: JsonPropertyName("version")] int? Version,
    [property: JsonPropertyName("schema_type")] SchemaType SchemaType,
    [property: JsonPropertyName("schema")] string Schema
);

/// <summary>
/// Client for the Streamline Schema Registry HTTP API.
/// </summary>
public class SchemaRegistryClient : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly string _baseUrl;

    /// <summary>
    /// Creates a new Schema Registry client.
    /// </summary>
    /// <param name="baseUrl">Base URL of the schema registry.</param>
    public SchemaRegistryClient(string baseUrl = "http://localhost:9094")
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _httpClient = new HttpClient { BaseAddress = new Uri(_baseUrl) };
    }

    /// <summary>
    /// Register a schema under a subject. Returns the schema ID.
    /// </summary>
    public async Task<int> RegisterAsync(string subject, string schema, SchemaType type, CancellationToken ct = default)
    {
        var request = new { schema, schema_type = type.ToString().ToUpperInvariant() };
        var response = await _httpClient.PostAsJsonAsync($"/api/schemas/subjects/{subject}/versions", request, ct);
        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct);
        return result.GetProperty("id").GetInt32();
    }

    /// <summary>
    /// Get a schema by its global ID.
    /// </summary>
    public async Task<SchemaInfo> GetSchemaAsync(int id, CancellationToken ct = default)
    {
        var response = await _httpClient.GetAsync($"/api/schemas/ids/{id}", ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<SchemaInfo>(cancellationToken: ct)
            ?? throw new InvalidOperationException("Empty response from schema registry");
    }

    /// <summary>
    /// Get all versions for a subject.
    /// </summary>
    public async Task<IReadOnlyList<int>> GetVersionsAsync(string subject, CancellationToken ct = default)
    {
        var response = await _httpClient.GetAsync($"/api/schemas/subjects/{subject}/versions", ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<int[]>(cancellationToken: ct)
            ?? Array.Empty<int>();
    }

    /// <summary>
    /// Check if a schema is compatible with the subject's existing schemas.
    /// </summary>
    public async Task<bool> CheckCompatibilityAsync(string subject, string schema, SchemaType type, CancellationToken ct = default)
    {
        var request = new { schema, schema_type = type.ToString().ToUpperInvariant() };
        var response = await _httpClient.PostAsJsonAsync($"/api/schemas/compatibility/subjects/{subject}/versions/latest", request, ct);
        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct);
        return result.GetProperty("is_compatible").GetBoolean();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _httpClient.Dispose();
        GC.SuppressFinalize(this);
    }
}
