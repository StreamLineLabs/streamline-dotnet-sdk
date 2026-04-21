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

    /// <summary>Get cluster overview including broker list.</summary>
    Task<ClusterInfo> GetClusterInfoAsync(CancellationToken cancellationToken = default);

    /// <summary>Get consumer group lag information.</summary>
    Task<ConsumerGroupLag> GetConsumerGroupLagAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>Get consumer lag for a specific topic within a group.</summary>
    Task<ConsumerGroupLag> GetConsumerGroupTopicLagAsync(string groupId, string topic, CancellationToken cancellationToken = default);

    /// <summary>Browse messages from a topic partition.</summary>
    Task<IReadOnlyList<InspectedMessage>> InspectMessagesAsync(string topic, int partition = 0, long? offset = null, int limit = 20, CancellationToken cancellationToken = default);

    /// <summary>Get the latest messages from a topic.</summary>
    Task<IReadOnlyList<InspectedMessage>> LatestMessagesAsync(string topic, int count = 10, CancellationToken cancellationToken = default);

    /// <summary>Get metrics history from the server.</summary>
    Task<IReadOnlyList<MetricPoint>> MetricsHistoryAsync(CancellationToken cancellationToken = default);
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

/// <summary>Cluster information including broker list.</summary>
public record ClusterInfo
{
    /// <summary>Unique cluster identifier.</summary>
    [JsonPropertyName("cluster_id")] public string ClusterId { get; init; } = "";
    /// <summary>Local broker identifier.</summary>
    [JsonPropertyName("broker_id")] public int BrokerId { get; init; }
    /// <summary>List of brokers in the cluster.</summary>
    [JsonPropertyName("brokers")] public IReadOnlyList<BrokerInfo> Brokers { get; init; } = [];
    /// <summary>Controller broker identifier.</summary>
    [JsonPropertyName("controller")] public int Controller { get; init; } = -1;
}

/// <summary>Information about a single broker.</summary>
public record BrokerInfo
{
    /// <summary>Broker identifier.</summary>
    [JsonPropertyName("id")] public int Id { get; init; }
    /// <summary>Broker hostname.</summary>
    [JsonPropertyName("host")] public string Host { get; init; } = "";
    /// <summary>Broker port number.</summary>
    [JsonPropertyName("port")] public int Port { get; init; } = 9092;
    /// <summary>Optional rack identifier for rack-aware replication.</summary>
    [JsonPropertyName("rack")] public string? Rack { get; init; }
}

/// <summary>Consumer lag for a single partition.</summary>
public record ConsumerLag
{
    /// <summary>Topic name.</summary>
    [JsonPropertyName("topic")] public string Topic { get; init; } = "";
    /// <summary>Partition number.</summary>
    [JsonPropertyName("partition")] public int Partition { get; init; }
    /// <summary>Current committed offset.</summary>
    [JsonPropertyName("current_offset")] public long CurrentOffset { get; init; }
    /// <summary>End (high-watermark) offset of the partition.</summary>
    [JsonPropertyName("end_offset")] public long EndOffset { get; init; }
    /// <summary>Consumer lag (end offset minus current offset).</summary>
    [JsonPropertyName("lag")] public long Lag { get; init; }
}

/// <summary>Aggregated consumer group lag.</summary>
public record ConsumerGroupLag
{
    /// <summary>Consumer group identifier.</summary>
    [JsonPropertyName("group_id")] public string GroupId { get; init; } = "";
    /// <summary>Per-partition lag details.</summary>
    [JsonPropertyName("partitions")] public IReadOnlyList<ConsumerLag> Partitions { get; init; } = [];
    /// <summary>Total lag across all partitions.</summary>
    [JsonPropertyName("total_lag")] public long TotalLag { get; init; }
}

/// <summary>A message returned by the inspection API.</summary>
public record InspectedMessage
{
    /// <summary>Message offset within the partition.</summary>
    [JsonPropertyName("offset")] public long Offset { get; init; }
    /// <summary>Message key, or null if unkeyed.</summary>
    [JsonPropertyName("key")] public string? Key { get; init; }
    /// <summary>Message value.</summary>
    [JsonPropertyName("value")] public string Value { get; init; } = "";
    /// <summary>Message timestamp in milliseconds since epoch.</summary>
    [JsonPropertyName("timestamp")] public long Timestamp { get; init; }
    /// <summary>Partition the message belongs to.</summary>
    [JsonPropertyName("partition")] public int Partition { get; init; }
    /// <summary>Message headers.</summary>
    [JsonPropertyName("headers")] public Dictionary<string, string> Headers { get; init; } = new();
}

/// <summary>A single metric data point.</summary>
public record MetricPoint
{
    /// <summary>Metric name.</summary>
    [JsonPropertyName("name")] public string Name { get; init; } = "";
    /// <summary>Metric value.</summary>
    [JsonPropertyName("value")] public double Value { get; init; }
    /// <summary>Metric labels (dimensions).</summary>
    [JsonPropertyName("labels")] public Dictionary<string, string> Labels { get; init; } = new();
    /// <summary>Timestamp when the metric was recorded.</summary>
    [JsonPropertyName("timestamp")] public long Timestamp { get; init; }
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
        TopicNameValidator.Validate(topic);
        var response = await SendAsync(HttpMethod.Get, $"/v1/topics/{Uri.EscapeDataString(topic)}", cancellationToken);
        return await DeserializeAsync<TopicMetadata>(response, cancellationToken)
            ?? throw new StreamlineTopicNotFoundException(topic);
    }

    /// <inheritdoc />
    public async Task CreateTopicAsync(string topic, int partitions = 1, int replicationFactor = 1, Dictionary<string, string>? config = null, CancellationToken cancellationToken = default)
    {
        TopicNameValidator.Validate(topic);
        var body = new { name = topic, partitions, replication_factor = replicationFactor, config };
        await SendAsync(HttpMethod.Post, "/v1/topics", cancellationToken, body);
    }

    /// <inheritdoc />
    public async Task DeleteTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        TopicNameValidator.Validate(topic);
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
            ?? throw new StreamlineException("Consumer group not found", StreamlineErrorCode.Unknown, hint: "Check that the consumer group ID is correct");
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
    public async Task<ClusterInfo> GetClusterInfoAsync(CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(HttpMethod.Get, "/v1/cluster", cancellationToken);
        return await response.Content.ReadFromJsonAsync<ClusterInfo>(_jsonOptions, cancellationToken)
            ?? new ClusterInfo();
    }

    /// <inheritdoc />
    public async Task<ConsumerGroupLag> GetConsumerGroupLagAsync(string groupId, CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(HttpMethod.Get, $"/v1/consumer-groups/{groupId}/lag", cancellationToken);
        return await response.Content.ReadFromJsonAsync<ConsumerGroupLag>(_jsonOptions, cancellationToken)
            ?? new ConsumerGroupLag { GroupId = groupId };
    }

    /// <inheritdoc />
    public async Task<ConsumerGroupLag> GetConsumerGroupTopicLagAsync(string groupId, string topic, CancellationToken cancellationToken = default)
    {
        TopicNameValidator.Validate(topic);
        var response = await SendAsync(HttpMethod.Get, $"/v1/consumer-groups/{groupId}/lag/{topic}", cancellationToken);
        return await response.Content.ReadFromJsonAsync<ConsumerGroupLag>(_jsonOptions, cancellationToken)
            ?? new ConsumerGroupLag { GroupId = groupId };
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<InspectedMessage>> InspectMessagesAsync(string topic, int partition = 0, long? offset = null, int limit = 20, CancellationToken cancellationToken = default)
    {
        TopicNameValidator.Validate(topic);
        var path = $"/v1/inspect/{topic}?partition={partition}&limit={limit}";
        if (offset.HasValue) path += $"&offset={offset.Value}";
        var response = await SendAsync(HttpMethod.Get, path, cancellationToken);
        return await response.Content.ReadFromJsonAsync<List<InspectedMessage>>(_jsonOptions, cancellationToken)
            ?? [];
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<InspectedMessage>> LatestMessagesAsync(string topic, int count = 10, CancellationToken cancellationToken = default)
    {
        TopicNameValidator.Validate(topic);
        var response = await SendAsync(HttpMethod.Get, $"/v1/inspect/{topic}/latest?count={count}", cancellationToken);
        return await response.Content.ReadFromJsonAsync<List<InspectedMessage>>(_jsonOptions, cancellationToken)
            ?? [];
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<MetricPoint>> MetricsHistoryAsync(CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(HttpMethod.Get, "/v1/metrics/history", cancellationToken);
        return await response.Content.ReadFromJsonAsync<List<MetricPoint>>(_jsonOptions, cancellationToken)
            ?? [];
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
