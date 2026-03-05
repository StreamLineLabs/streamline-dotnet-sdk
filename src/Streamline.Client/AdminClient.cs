using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Streamline.Client;

/// <summary>
/// Interface for Streamline admin operations.
/// </summary>
public interface IAdminClient : IAsyncDisposable
{
    /// <summary>List all topics on the server.</summary>
    Task<IReadOnlyList<TopicMetadata>> ListTopicsAsync(CancellationToken cancellationToken = default);

    /// <summary>Get detailed information about a specific topic.</summary>
    Task<TopicMetadata> DescribeTopicAsync(string topic, CancellationToken cancellationToken = default);

    /// <summary>Create a new topic.</summary>
    Task CreateTopicAsync(string topic, int partitions = 1, int replicationFactor = 1, Dictionary<string, string>? config = null, CancellationToken cancellationToken = default);

    /// <summary>Delete a topic.</summary>
    Task DeleteTopicAsync(string topic, CancellationToken cancellationToken = default);

    /// <summary>List all consumer groups.</summary>
    Task<IReadOnlyList<ConsumerGroupMetadata>> ListConsumerGroupsAsync(CancellationToken cancellationToken = default);

    /// <summary>Get detailed information about a consumer group.</summary>
    Task<ConsumerGroupMetadata> DescribeConsumerGroupAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>Delete a consumer group.</summary>
    Task DeleteConsumerGroupAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>Execute a SQL query against the streaming data.</summary>
    Task<QueryResult> QueryAsync(string sql, CancellationToken cancellationToken = default);

    /// <summary>Get server information.</summary>
    Task<ServerInfo> GetServerInfoAsync(CancellationToken cancellationToken = default);

    /// <summary>Check if the server is healthy.</summary>
    Task<bool> IsHealthyAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Topic metadata returned by admin operations.
/// </summary>
public record TopicMetadata
{
    /// <summary>Topic name.</summary>
    [JsonPropertyName("name")]
    public string Name { get; init; } = "";

    /// <summary>Number of partitions.</summary>
    [JsonPropertyName("partitions")]
    public int Partitions { get; init; }

    /// <summary>Replication factor.</summary>
    [JsonPropertyName("replication_factor")]
    public int ReplicationFactor { get; init; }

    /// <summary>Total message count.</summary>
    [JsonPropertyName("message_count")]
    public long MessageCount { get; init; }

    /// <summary>Topic configuration.</summary>
    [JsonPropertyName("config")]
    public Dictionary<string, string>? Config { get; init; }
}

/// <summary>
/// Consumer group metadata returned by admin operations.
/// </summary>
public record ConsumerGroupMetadata
{
    /// <summary>Group ID.</summary>
    [JsonPropertyName("id")]
    public string Id { get; init; } = "";

    /// <summary>Group state (e.g., Stable, Empty, Dead).</summary>
    [JsonPropertyName("state")]
    public string State { get; init; } = "";

    /// <summary>Group members.</summary>
    [JsonPropertyName("members")]
    public List<ConsumerGroupMemberMetadata>? Members { get; init; }

    /// <summary>Protocol type.</summary>
    [JsonPropertyName("protocol")]
    public string? Protocol { get; init; }
}

/// <summary>
/// A member of a consumer group.
/// </summary>
public record ConsumerGroupMemberMetadata
{
    /// <summary>Member ID.</summary>
    [JsonPropertyName("id")]
    public string Id { get; init; } = "";

    /// <summary>Client ID.</summary>
    [JsonPropertyName("client_id")]
    public string ClientId { get; init; } = "";

    /// <summary>Host.</summary>
    [JsonPropertyName("host")]
    public string Host { get; init; } = "";

    /// <summary>Topic-partition assignments.</summary>
    [JsonPropertyName("assignments")]
    public List<string>? Assignments { get; init; }
}

/// <summary>
/// Result from a SQL query.
/// </summary>
public record QueryResult
{
    /// <summary>Column names.</summary>
    [JsonPropertyName("columns")]
    public List<string> Columns { get; init; } = new();

    /// <summary>Row data as string arrays.</summary>
    [JsonPropertyName("rows")]
    public List<List<string>> Rows { get; init; } = new();

    /// <summary>Total row count.</summary>
    [JsonPropertyName("row_count")]
    public int RowCount { get; init; }
}

/// <summary>
/// Server information.
/// </summary>
public record ServerInfo
{
    /// <summary>Server version.</summary>
    [JsonPropertyName("version")]
    public string Version { get; init; } = "";

    /// <summary>Uptime in seconds.</summary>
    [JsonPropertyName("uptime")]
    public long Uptime { get; init; }

    /// <summary>Number of topics.</summary>
    [JsonPropertyName("topic_count")]
    public int TopicCount { get; init; }

    /// <summary>Total message count.</summary>
    [JsonPropertyName("message_count")]
    public long MessageCount { get; init; }
}

/// <summary>
/// HTTP-based admin client for managing Streamline server resources.
/// Communicates with the HTTP REST API (default port 9094).
/// </summary>
public sealed class AdminClient : IAdminClient
{
    private readonly HttpClient _httpClient;
    private readonly bool _ownsHttpClient;
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>
    /// Creates an admin client for the specified HTTP API base URL.
    /// </summary>
    /// <param name="httpBaseUrl">Base URL of the Streamline HTTP API (e.g., "http://localhost:9094").</param>
    /// <param name="authToken">Optional bearer token for authentication.</param>
    public AdminClient(string httpBaseUrl, string? authToken = null)
    {
        _httpClient = new HttpClient { BaseAddress = new Uri(httpBaseUrl) };
        if (authToken is not null)
        {
            _httpClient.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", authToken);
        }
        _ownsHttpClient = true;
    }

    /// <summary>
    /// Creates an admin client with a custom HttpClient (for testing or DI).
    /// </summary>
    public AdminClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
        _ownsHttpClient = false;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<TopicMetadata>> ListTopicsAsync(CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(HttpMethod.Get, "/v1/topics", cancellationToken);
        return await DeserializeAsync<List<TopicMetadata>>(response, cancellationToken) ?? [];
    }

    /// <inheritdoc />
    public async Task<TopicMetadata> DescribeTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(HttpMethod.Get, $"/v1/topics/{Uri.EscapeDataString(topic)}", cancellationToken);
        return await DeserializeAsync<TopicMetadata>(response, cancellationToken)
            ?? throw new StreamlineTopicNotFoundException(topic);
    }

    /// <inheritdoc />
    public async Task CreateTopicAsync(string topic, int partitions = 1, int replicationFactor = 1, Dictionary<string, string>? config = null, CancellationToken cancellationToken = default)
    {
        var body = new { name = topic, partitions, replication_factor = replicationFactor, config };
        await SendAsync(HttpMethod.Post, "/v1/topics", cancellationToken, body);
    }

    /// <inheritdoc />
    public async Task DeleteTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        await SendAsync(HttpMethod.Delete, $"/v1/topics/{Uri.EscapeDataString(topic)}", cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ConsumerGroupMetadata>> ListConsumerGroupsAsync(CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(HttpMethod.Get, "/v1/consumer-groups", cancellationToken);
        return await DeserializeAsync<List<ConsumerGroupMetadata>>(response, cancellationToken) ?? [];
    }

    /// <inheritdoc />
    public async Task<ConsumerGroupMetadata> DescribeConsumerGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(HttpMethod.Get, $"/v1/consumer-groups/{Uri.EscapeDataString(groupId)}", cancellationToken);
        return await DeserializeAsync<ConsumerGroupMetadata>(response, cancellationToken)
            ?? throw new StreamlineException("Consumer group not found", StreamlineErrorCode.TopicNotFound);
    }

    /// <inheritdoc />
    public async Task DeleteConsumerGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        await SendAsync(HttpMethod.Delete, $"/v1/consumer-groups/{Uri.EscapeDataString(groupId)}", cancellationToken);
    }

    /// <inheritdoc />
    public async Task<QueryResult> QueryAsync(string sql, CancellationToken cancellationToken = default)
    {
        var body = new { query = sql };
        var response = await SendAsync(HttpMethod.Post, "/v1/query", cancellationToken, body);
        return await DeserializeAsync<QueryResult>(response, cancellationToken) ?? new QueryResult();
    }

    /// <inheritdoc />
    public async Task<ServerInfo> GetServerInfoAsync(CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(HttpMethod.Get, "/v1/info", cancellationToken);
        return await DeserializeAsync<ServerInfo>(response, cancellationToken) ?? new ServerInfo();
    }

    /// <inheritdoc />
    public async Task<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await SendAsync(HttpMethod.Get, "/health/live", cancellationToken);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_ownsHttpClient)
            _httpClient.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task<HttpResponseMessage> SendAsync(HttpMethod method, string path, CancellationToken cancellationToken, object? body = null)
    {
        using var request = new HttpRequestMessage(method, path);
        if (body is not null)
        {
            request.Content = JsonContent.Create(body, options: _jsonOptions);
        }

        HttpResponseMessage response;
        try
        {
            response = await _httpClient.SendAsync(request, cancellationToken);
        }
        catch (HttpRequestException ex)
        {
            throw new StreamlineConnectionException($"Admin request failed: {ex.Message}", ex);
        }

        if (response.IsSuccessStatusCode)
            return response;

        var errorBody = await response.Content.ReadAsStringAsync(cancellationToken);
        throw response.StatusCode switch
        {
            System.Net.HttpStatusCode.NotFound => new StreamlineTopicNotFoundException(path, $"Not found: {errorBody}"),
            System.Net.HttpStatusCode.Unauthorized => new StreamlineAuthenticationException($"Unauthorized: {errorBody}"),
            System.Net.HttpStatusCode.Forbidden => new StreamlineAuthorizationException($"Forbidden: {errorBody}"),
            _ => new StreamlineException($"HTTP {(int)response.StatusCode}: {errorBody}", StreamlineErrorCode.Connection),
        };
    }

    private async Task<T?> DeserializeAsync<T>(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        return await JsonSerializer.DeserializeAsync<T>(stream, _jsonOptions, cancellationToken);
    }
}
