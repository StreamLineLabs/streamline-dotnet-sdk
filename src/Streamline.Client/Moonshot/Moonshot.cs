// SPDX-License-Identifier: Apache-2.0
//
// Moonshot HTTP client wrappers for the Streamline broker.
// Covers: M1 memory, M2 semantic search, M4 contracts + attestation, M5 branches.

using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace Streamline.Client.Moonshot;

/// <summary>
/// Exception thrown when a moonshot HTTP request fails with a non-success status.
/// </summary>
public sealed class MoonshotHttpException : StreamlineException
{
    /// <summary>HTTP status code returned by the broker.</summary>
    public int StatusCode { get; }

    /// <summary>Truncated response body (max 512 chars).</summary>
    public string ResponseBody { get; }

    /// <summary>Constructs a new <see cref="MoonshotHttpException"/>.</summary>
    public MoonshotHttpException(int statusCode, string body)
        : base($"moonshot http error: status={statusCode} body={Trim(body)}", StreamlineErrorCode.Unknown)
    {
        StatusCode = statusCode;
        ResponseBody = body;
    }

    private static string Trim(string body) => body.Length > 512 ? body[..512] : body;
}

/// <summary>
/// Shared base providing HTTP plumbing for moonshot clients.
/// </summary>
public abstract class MoonshotClientBase : IAsyncDisposable
{
    private readonly bool _ownsHttp;

    /// <summary>Underlying <see cref="HttpClient"/>.</summary>
    protected HttpClient Http { get; }

    /// <summary>JSON serializer options used by all clients.</summary>
    protected static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>Constructs a client given a configured <see cref="HttpClient"/>.</summary>
    protected MoonshotClientBase(HttpClient http)
    {
        Http = http ?? throw new ArgumentNullException(nameof(http));
        _ownsHttp = false;
    }

    /// <summary>Constructs a client from a base URL.</summary>
    protected MoonshotClientBase(string baseUrl, TimeSpan? timeout = null)
    {
        if (string.IsNullOrWhiteSpace(baseUrl))
        {
            throw new ArgumentException("baseUrl is required", nameof(baseUrl));
        }

        Http = new HttpClient { BaseAddress = new Uri(baseUrl.TrimEnd('/')), Timeout = timeout ?? TimeSpan.FromSeconds(10) };
        _ownsHttp = true;
    }

    /// <summary>Sends a request; throws <see cref="MoonshotHttpException"/> on non-success.</summary>
    protected async Task<JsonNode?> RequestAsync(
        HttpMethod method, string path, object? body, CancellationToken ct)
    {
        using var req = new HttpRequestMessage(method, path);
        if (body is not null)
        {
            req.Content = JsonContent.Create(body, options: JsonOpts);
        }
        using var resp = await Http.SendAsync(req, ct).ConfigureAwait(false);
        var raw = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        if (!resp.IsSuccessStatusCode)
        {
            throw new MoonshotHttpException((int)resp.StatusCode, raw);
        }
        return string.IsNullOrEmpty(raw) ? null : JsonNode.Parse(raw);
    }

    /// <summary>Validates that <paramref name="value"/> is non-null/empty.</summary>
    protected static void RequireNonEmpty(string name, string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"{name} must not be empty", name);
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_ownsHttp)
        {
            Http.Dispose();
        }
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}

// ---------------------------------------------------------------------------
// M5 — Branches
// ---------------------------------------------------------------------------

/// <summary>Branch view as returned by the broker.</summary>
public sealed record BranchView(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("created_at_ms")] long CreatedAtMs,
    [property: JsonPropertyName("message_count")] long MessageCount,
    [property: JsonPropertyName("metadata")] Dictionary<string, JsonElement>? Metadata,
    [property: JsonPropertyName("parent_id")] string? ParentId,
    [property: JsonPropertyName("fork_offset")] long? ForkOffset);

/// <summary>Single branch message.</summary>
public sealed record BranchMessage(
    [property: JsonPropertyName("offset")] long Offset,
    [property: JsonPropertyName("key")] string? Key,
    [property: JsonPropertyName("value")] string? Value,
    [property: JsonPropertyName("headers")] Dictionary<string, JsonElement>? Headers,
    [property: JsonPropertyName("timestamp_ms")] long? TimestampMs);

/// <summary>Optional fields when creating a branch.</summary>
public sealed record BranchCreateOptions(
    string? ParentId = null,
    long? ForkOffset = null,
    Dictionary<string, object?>? Metadata = null);

/// <summary>Admin client for M5 branches.</summary>
public sealed class BranchAdminClient : MoonshotClientBase
{
    /// <summary>Constructs from a configured HTTP client.</summary>
    public BranchAdminClient(HttpClient http) : base(http) { }

    /// <summary>Constructs from a base URL.</summary>
    public BranchAdminClient(string baseUrl, TimeSpan? timeout = null) : base(baseUrl, timeout) { }

    /// <summary>Creates a new branch off <paramref name="topic"/>.</summary>
    public async Task<BranchView> CreateAsync(
        string topic, string name, BranchCreateOptions? opts = null, CancellationToken ct = default)
    {
        RequireNonEmpty(nameof(topic), topic);
        RequireNonEmpty(nameof(name), name);
        var body = new Dictionary<string, object?>
        {
            ["topic"] = topic,
            ["name"] = name,
        };
        if (opts is not null)
        {
            if (opts.ParentId is not null) body["parent_id"] = opts.ParentId;
            if (opts.ForkOffset.HasValue) body["fork_offset"] = opts.ForkOffset.Value;
            if (opts.Metadata is not null) body["metadata"] = opts.Metadata;
        }
        var node = await RequestAsync(HttpMethod.Post, "/api/v1/branches", body, ct)
            .ConfigureAwait(false);
        return ParseView(node) ?? throw new MoonshotHttpException(200, "empty body");
    }

    /// <summary>Lists branches, optionally filtered by topic.</summary>
    public async Task<IReadOnlyList<BranchView>> ListAsync(
        string? topic = null, CancellationToken ct = default)
    {
        var path = topic is null
            ? "/api/v1/branches"
            : $"/api/v1/branches?topic={Uri.EscapeDataString(topic)}";
        var node = await RequestAsync(HttpMethod.Get, path, null, ct).ConfigureAwait(false);
        return ParseList(node);
    }

    /// <summary>Gets a single branch by id.</summary>
    public async Task<BranchView> GetAsync(string id, CancellationToken ct = default)
    {
        RequireNonEmpty(nameof(id), id);
        var node = await RequestAsync(
            HttpMethod.Get, $"/api/v1/branches/{Uri.EscapeDataString(id)}", null, ct)
            .ConfigureAwait(false);
        return ParseView(node) ?? throw new MoonshotHttpException(200, "empty body");
    }

    /// <summary>Deletes a branch.</summary>
    public async Task DeleteAsync(string id, CancellationToken ct = default)
    {
        RequireNonEmpty(nameof(id), id);
        await RequestAsync(
            HttpMethod.Delete, $"/api/v1/branches/{Uri.EscapeDataString(id)}", null, ct)
            .ConfigureAwait(false);
    }

    /// <summary>Appends a single message to a branch.</summary>
    public async Task<BranchMessage> AppendAsync(
        string id, string? key, string value, CancellationToken ct = default)
    {
        RequireNonEmpty(nameof(id), id);
        var body = new Dictionary<string, object?> { ["value"] = value };
        if (key is not null) body["key"] = key;
        var node = await RequestAsync(
            HttpMethod.Post, $"/api/v1/branches/{Uri.EscapeDataString(id)}/messages", body, ct)
            .ConfigureAwait(false);
        if (node is null) throw new MoonshotHttpException(200, "empty body");
        return JsonSerializer.Deserialize<BranchMessage>(node.ToJsonString())!;
    }

    /// <summary>Reads messages from a branch.</summary>
    public async Task<IReadOnlyList<BranchMessage>> MessagesAsync(
        string id, int? limit = null, CancellationToken ct = default)
    {
        RequireNonEmpty(nameof(id), id);
        var path = $"/api/v1/branches/{Uri.EscapeDataString(id)}/messages"
            + (limit.HasValue ? $"?limit={limit.Value}" : "");
        var node = await RequestAsync(HttpMethod.Get, path, null, ct).ConfigureAwait(false);
        return ParseMessages(node);
    }

    private static BranchView? ParseView(JsonNode? node)
        => node is null ? null : JsonSerializer.Deserialize<BranchView>(node.ToJsonString());

    private static List<BranchView> ParseList(JsonNode? node)
    {
        var arr = ToArray(node, "items");
        var list = new List<BranchView>(arr.Count);
        foreach (var item in arr)
        {
            try { list.Add(JsonSerializer.Deserialize<BranchView>(item.ToJsonString())!); }
            catch (JsonException) { /* tolerant */ }
        }
        return list;
    }

    private static List<BranchMessage> ParseMessages(JsonNode? node)
    {
        var arr = ToArray(node, "messages");
        var list = new List<BranchMessage>(arr.Count);
        foreach (var item in arr)
        {
            try { list.Add(JsonSerializer.Deserialize<BranchMessage>(item.ToJsonString())!); }
            catch (JsonException) { /* tolerant */ }
        }
        return list;
    }

    private static List<JsonNode> ToArray(JsonNode? node, string wrapKey)
    {
        if (node is JsonArray a)
        {
            return a.Where(n => n is not null).Select(n => n!).ToList();
        }
        if (node is JsonObject o && o.TryGetPropertyValue(wrapKey, out var inner) && inner is JsonArray arr)
        {
            return arr.Where(n => n is not null).Select(n => n!).ToList();
        }
        return new List<JsonNode>();
    }
}

// ---------------------------------------------------------------------------
// M4 — Contracts validation
// ---------------------------------------------------------------------------

/// <summary>Single field validation failure.</summary>
public sealed record ValidationFailure(
    [property: JsonPropertyName("field_path")] string FieldPath,
    [property: JsonPropertyName("expected")] string? Expected,
    [property: JsonPropertyName("actual")] string? Actual,
    [property: JsonPropertyName("message")] string? Message);

/// <summary>Outcome of a validation request.</summary>
public sealed record ValidationResult(
    bool Valid,
    long? SchemaId,
    IReadOnlyList<ValidationFailure> Errors);

/// <summary>Client for the M4 contracts validation endpoint.</summary>
public sealed class ContractsClient : MoonshotClientBase
{
    /// <summary>Constructs from a configured HTTP client.</summary>
    public ContractsClient(HttpClient http) : base(http) { }
    /// <summary>Constructs from a base URL.</summary>
    public ContractsClient(string baseUrl, TimeSpan? timeout = null) : base(baseUrl, timeout) { }

    /// <summary>Validates <paramref name="value"/> against <paramref name="contract"/>.</summary>
    public async Task<ValidationResult> ValidateAsync(
        IReadOnlyDictionary<string, object?> contract, object? value, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(contract);
        var body = new Dictionary<string, object?>
        {
            ["contract"] = contract,
        };
        switch (value)
        {
            case byte[] b: body["value_string"] = Encoding.UTF8.GetString(b); break;
            case string s: body["value_string"] = s; break;
            default: body["value"] = value; break;
        }

        using var req = new HttpRequestMessage(HttpMethod.Post, "/api/v1/contracts/validate")
        {
            Content = JsonContent.Create(body, options: JsonOpts),
        };
        using var resp = await Http.SendAsync(req, ct).ConfigureAwait(false);
        var raw = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        var status = (int)resp.StatusCode;
        if (status != 200 && status != 400)
        {
            throw new MoonshotHttpException(status, raw);
        }
        var node = string.IsNullOrEmpty(raw) ? null : JsonNode.Parse(raw);
        long? schemaId = null;
        var errors = new List<ValidationFailure>();
        if (node is JsonObject o)
        {
            if (o["schema_id"] is JsonValue v && v.TryGetValue<long>(out var sid)) schemaId = sid;
            if (o["errors"] is JsonArray arr)
            {
                foreach (var item in arr)
                {
                    if (item is null) continue;
                    try { errors.Add(JsonSerializer.Deserialize<ValidationFailure>(item.ToJsonString())!); }
                    catch (JsonException) { /* tolerant */ }
                }
            }
        }
        return new ValidationResult(status == 200, schemaId, errors);
    }
}

// ---------------------------------------------------------------------------
// M4 — Attestation
// ---------------------------------------------------------------------------

/// <summary>Signed attestation envelope.</summary>
public sealed record SignedAttestation(
    [property: JsonPropertyName("key_id")] string KeyId,
    [property: JsonPropertyName("algorithm")] string Algorithm,
    [property: JsonPropertyName("timestamp_ms")] long TimestampMs,
    [property: JsonPropertyName("payload_sha256")] string PayloadSha256,
    [property: JsonPropertyName("signature_b64")] string SignatureB64,
    [property: JsonPropertyName("header_name")] string HeaderName,
    [property: JsonPropertyName("header_value")] string HeaderValue);

/// <summary>Inputs for signing.</summary>
public sealed record SignParams(
    string Topic,
    int Partition,
    long Offset,
    long TimestampMs,
    byte[]? Key = null,
    byte[]? Value = null,
    string? ValueString = null,
    string? KeyId = null,
    string? Algorithm = null);

/// <summary>Inputs for verification.</summary>
public sealed record VerifyParams(
    string Topic,
    int Partition,
    long Offset,
    long TimestampMs,
    string SignatureB64,
    byte[]? Key = null,
    byte[]? Value = null,
    string? ValueString = null,
    string? KeyId = null,
    string? Algorithm = null);

/// <summary>HTTP client for sign/verify against the broker.</summary>
public sealed class AttestationClient : MoonshotClientBase
{
    /// <summary>HTTP header name carrying broker attestations.</summary>
    public const string Header = "streamline-attest";

    private readonly string _defaultKeyId;
    private readonly string _defaultAlgorithm;

    /// <summary>Constructs from a configured HTTP client.</summary>
    public AttestationClient(HttpClient http, string keyId = "broker-0", string algorithm = "ed25519")
        : base(http)
    {
        _defaultKeyId = keyId;
        _defaultAlgorithm = algorithm;
    }

    /// <summary>Constructs from a base URL.</summary>
    public AttestationClient(string baseUrl, TimeSpan? timeout = null,
        string keyId = "broker-0", string algorithm = "ed25519")
        : base(baseUrl, timeout)
    {
        _defaultKeyId = keyId;
        _defaultAlgorithm = algorithm;
    }

    /// <summary>Asks the broker to sign the supplied payload.</summary>
    public async Task<SignedAttestation> SignAsync(SignParams p, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(p);
        if (p.Value is not null && p.ValueString is not null)
        {
            throw new ArgumentException("set either Value or ValueString, not both", nameof(p));
        }
        RequireNonEmpty("Topic", p.Topic);
        var body = BuildBody(p.Topic, p.Partition, p.Offset, p.TimestampMs, p.Key, p.Value, p.ValueString,
            p.KeyId, p.Algorithm);
        var node = await RequestAsync(HttpMethod.Post, "/api/v1/attest/sign", body, ct)
            .ConfigureAwait(false)
            ?? throw new MoonshotHttpException(200, "empty body");
        return JsonSerializer.Deserialize<SignedAttestation>(node.ToJsonString())!;
    }

    /// <summary>Asks the broker to verify a signature; returns the broker's <c>valid</c> flag.</summary>
    public async Task<bool> VerifyAsync(VerifyParams p, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(p);
        if (p.Value is not null && p.ValueString is not null)
        {
            throw new ArgumentException("set either Value or ValueString, not both", nameof(p));
        }
        RequireNonEmpty("Topic", p.Topic);
        RequireNonEmpty("SignatureB64", p.SignatureB64);
        var body = BuildBody(p.Topic, p.Partition, p.Offset, p.TimestampMs, p.Key, p.Value, p.ValueString,
            p.KeyId, p.Algorithm);
        body["signature_b64"] = p.SignatureB64;
        var node = await RequestAsync(HttpMethod.Post, "/api/v1/attest/verify", body, ct)
            .ConfigureAwait(false);
        return node is JsonObject o
            && o["valid"] is JsonValue v
            && v.TryGetValue<bool>(out var ok)
            && ok;
    }

    private Dictionary<string, object?> BuildBody(
        string topic, int partition, long offset, long timestampMs,
        byte[]? key, byte[]? value, string? valueString, string? keyId, string? algorithm)
    {
        var body = new Dictionary<string, object?>
        {
            ["topic"] = topic,
            ["partition"] = partition,
            ["offset"] = offset,
            ["timestamp_ms"] = timestampMs,
            ["key_id"] = keyId ?? _defaultKeyId,
            ["algorithm"] = algorithm ?? _defaultAlgorithm,
        };
        if (key is not null) body["key_b64"] = Convert.ToBase64String(key);
        if (value is not null) body["value_b64"] = Convert.ToBase64String(value);
        if (valueString is not null) body["value"] = valueString;
        return body;
    }
}

// ---------------------------------------------------------------------------
// M2 — Semantic search
// ---------------------------------------------------------------------------

/// <summary>One semantic search hit.</summary>
public sealed record SearchHit(
    [property: JsonPropertyName("partition")] int Partition,
    [property: JsonPropertyName("offset")] long Offset,
    [property: JsonPropertyName("score")] double Score,
    [property: JsonPropertyName("key")] string? Key,
    [property: JsonPropertyName("value")] string? Value,
    [property: JsonPropertyName("timestamp_ms")] long? TimestampMs);

/// <summary>Search response.</summary>
public sealed record SearchResult(
    [property: JsonPropertyName("hits")] IReadOnlyList<SearchHit> Hits,
    [property: JsonPropertyName("took_ms")] long TookMs,
    [property: JsonPropertyName("total")] long? Total);

/// <summary>Optional search parameters.</summary>
public sealed record SearchOptions(int K = 10, IReadOnlyDictionary<string, object?>? Filter = null);

/// <summary>Client for the M2 semantic-search endpoint.</summary>
public sealed class SemanticSearchClient : MoonshotClientBase
{
    /// <summary>Constructs from a configured HTTP client.</summary>
    public SemanticSearchClient(HttpClient http) : base(http) { }
    /// <summary>Constructs from a base URL.</summary>
    public SemanticSearchClient(string baseUrl, TimeSpan? timeout = null) : base(baseUrl, timeout) { }

    /// <summary>Runs a semantic search over <paramref name="topic"/>.</summary>
    public async Task<SearchResult> SearchAsync(
        string topic, string query, SearchOptions? opts = null, CancellationToken ct = default)
    {
        RequireNonEmpty(nameof(topic), topic);
        RequireNonEmpty(nameof(query), query);
        opts ??= new SearchOptions();
        if (opts.K <= 0 || opts.K > 1000)
        {
            throw new ArgumentOutOfRangeException(nameof(opts), "K must be in 1..=1000");
        }
        var body = new Dictionary<string, object?> { ["query"] = query, ["k"] = opts.K };
        if (opts.Filter is not null) body["filter"] = opts.Filter;
        var node = await RequestAsync(
            HttpMethod.Post, $"/api/v1/topics/{Uri.EscapeDataString(topic)}/search", body, ct)
            .ConfigureAwait(false)
            ?? throw new MoonshotHttpException(200, "empty body");
        return JsonSerializer.Deserialize<SearchResult>(node.ToJsonString())!;
    }
}

// ---------------------------------------------------------------------------
// M1 — Memory
// ---------------------------------------------------------------------------

/// <summary>Memory entry kind.</summary>
public enum MemoryKind
{
    /// <summary>Episodic observation.</summary>
    Observation,
    /// <summary>Semantic fact.</summary>
    Fact,
    /// <summary>Procedural skill.</summary>
    Procedure,
}

internal static class MemoryKindExtensions
{
    public static string Wire(this MemoryKind k) => k switch
    {
        MemoryKind.Observation => "observation",
        MemoryKind.Fact => "fact",
        MemoryKind.Procedure => "procedure",
        _ => throw new ArgumentOutOfRangeException(nameof(k)),
    };
}

/// <summary>Where a written memory landed.</summary>
public sealed record WrittenEntry(
    [property: JsonPropertyName("topic")] string Topic,
    [property: JsonPropertyName("offset")] long Offset);

/// <summary>Single recall hit.</summary>
public sealed record RecalledMemory(
    [property: JsonPropertyName("tier")] string Tier,
    [property: JsonPropertyName("topic")] string Topic,
    [property: JsonPropertyName("offset")] long Offset,
    [property: JsonPropertyName("content")] string Content,
    [property: JsonPropertyName("score")] double Score,
    [property: JsonPropertyName("timestamp_ms")] long? TimestampMs);

/// <summary>Inputs for remember.</summary>
public sealed record RememberParams(
    string AgentId,
    MemoryKind Kind,
    string Content,
    double? Salience = null,
    string? Skill = null,
    IReadOnlyDictionary<string, object?>? Metadata = null);

/// <summary>Inputs for recall.</summary>
public sealed record RecallParams(
    string AgentId,
    string Query,
    int KEpisodic = 0,
    int KSemantic = 0);

/// <summary>HTTP client for the M1 memory endpoints.</summary>
public sealed class MemoryClient : MoonshotClientBase
{
    /// <summary>Constructs from a configured HTTP client.</summary>
    public MemoryClient(HttpClient http) : base(http) { }
    /// <summary>Constructs from a base URL.</summary>
    public MemoryClient(string baseUrl, TimeSpan? timeout = null) : base(baseUrl, timeout) { }

    /// <summary>Persists a memory.</summary>
    public async Task<IReadOnlyList<WrittenEntry>> RememberAsync(
        RememberParams p, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(p);
        RequireNonEmpty("AgentId", p.AgentId);
        RequireNonEmpty("Content", p.Content);
        if (p.Kind == MemoryKind.Procedure && string.IsNullOrWhiteSpace(p.Skill))
        {
            throw new ArgumentException("skill is required when kind=procedure", nameof(p));
        }
        if (p.Salience is double s && (s < 0 || s > 1))
        {
            throw new ArgumentOutOfRangeException(nameof(p), "salience must be in [0,1]");
        }
        var body = new Dictionary<string, object?>
        {
            ["agent_id"] = p.AgentId,
            ["kind"] = p.Kind.Wire(),
            ["content"] = p.Content,
        };
        if (p.Salience.HasValue) body["salience"] = p.Salience.Value;
        if (p.Skill is not null) body["skill"] = p.Skill;
        if (p.Metadata is not null) body["metadata"] = p.Metadata;
        var node = await RequestAsync(HttpMethod.Post, "/api/v1/memory/remember", body, ct)
            .ConfigureAwait(false);
        var list = new List<WrittenEntry>();
        if (node is JsonObject o && o["written"] is JsonArray arr)
        {
            foreach (var item in arr)
            {
                if (item is null) continue;
                try { list.Add(JsonSerializer.Deserialize<WrittenEntry>(item.ToJsonString())!); }
                catch (JsonException) { /* tolerant */ }
            }
        }
        return list;
    }

    /// <summary>Recalls memories matching the query.</summary>
    public async Task<IReadOnlyList<RecalledMemory>> RecallAsync(
        RecallParams p, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(p);
        RequireNonEmpty("AgentId", p.AgentId);
        RequireNonEmpty("Query", p.Query);
        var body = new Dictionary<string, object?>
        {
            ["agent_id"] = p.AgentId,
            ["query"] = p.Query,
        };
        if (p.KEpisodic > 0) body["k_episodic"] = p.KEpisodic;
        if (p.KSemantic > 0) body["k_semantic"] = p.KSemantic;
        var node = await RequestAsync(HttpMethod.Post, "/api/v1/memory/recall", body, ct)
            .ConfigureAwait(false);
        var list = new List<RecalledMemory>();
        if (node is JsonObject o && o["hits"] is JsonArray arr)
        {
            foreach (var item in arr)
            {
                if (item is null) continue;
                try { list.Add(JsonSerializer.Deserialize<RecalledMemory>(item.ToJsonString())!); }
                catch (JsonException) { /* tolerant */ }
            }
        }
        return list;
    }
}
