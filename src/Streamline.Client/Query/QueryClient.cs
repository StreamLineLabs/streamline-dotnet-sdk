namespace Streamline.Client.Query;

using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// Represents the result of a SQL query execution.
/// </summary>
/// <param name="Columns">Column definitions for the result set.</param>
/// <param name="Rows">Row data returned by the query.</param>
/// <param name="Metadata">Execution metadata for the query.</param>
public record QueryResult(
    [property: JsonPropertyName("columns")] ColumnInfo[] Columns,
    [property: JsonPropertyName("rows")] JsonElement[][] Rows,
    [property: JsonPropertyName("metadata")] QueryMetadata Metadata
);

/// <summary>
/// Describes a column in a query result set.
/// </summary>
/// <param name="Name">Column name.</param>
/// <param name="Type">Column data type.</param>
public record ColumnInfo(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("type")] string Type
);

/// <summary>
/// Metadata about a query execution.
/// </summary>
/// <param name="ExecutionTimeMs">Query execution time in milliseconds.</param>
/// <param name="RowsScanned">Total number of rows scanned.</param>
/// <param name="RowsReturned">Number of rows returned in the result.</param>
/// <param name="Truncated">Whether the result set was truncated.</param>
public record QueryMetadata(
    [property: JsonPropertyName("execution_time_ms")] long ExecutionTimeMs,
    [property: JsonPropertyName("rows_scanned")] long RowsScanned,
    [property: JsonPropertyName("rows_returned")] int RowsReturned,
    [property: JsonPropertyName("truncated")] bool Truncated
);

/// <summary>
/// Client for the Streamline unified query API.
/// </summary>
public class QueryClient : IDisposable
{
    private readonly HttpClient _httpClient;

    /// <summary>
    /// Initializes a new instance of the <see cref="QueryClient"/> class.
    /// </summary>
    /// <param name="baseUrl">Base URL of the Streamline HTTP API.</param>
    public QueryClient(string baseUrl = "http://localhost:9094")
    {
        _httpClient = new HttpClient { BaseAddress = new Uri(baseUrl.TrimEnd('/')) };
    }

    /// <summary>Execute a SQL query.</summary>
    public async Task<QueryResult> QueryAsync(string sql, CancellationToken ct = default)
    {
        return await QueryAsync(sql, 30000, 10000, ct);
    }

    /// <summary>Execute a SQL query with options.</summary>
    public async Task<QueryResult> QueryAsync(string sql, long timeoutMs, int maxRows, CancellationToken ct = default)
    {
        var request = new { sql, timeout_ms = timeoutMs, max_rows = maxRows, format = "json" };
        var response = await _httpClient.PostAsJsonAsync("/api/v1/query", request, ct);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<QueryResult>(cancellationToken: ct)
            ?? throw new InvalidOperationException("Empty query response");
    }

    /// <summary>Explain a query plan.</summary>
    public async Task<string> ExplainAsync(string sql, CancellationToken ct = default)
    {
        var request = new { sql };
        var response = await _httpClient.PostAsJsonAsync("/api/v1/query/explain", request, ct);
        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken: ct);
        return result.GetProperty("plan").GetString() ?? "";
    }

    /// <summary>
    /// Releases the resources used by the <see cref="QueryClient"/>.
    /// </summary>
    public void Dispose()
    {
        _httpClient.Dispose();
        GC.SuppressFinalize(this);
    }
}
