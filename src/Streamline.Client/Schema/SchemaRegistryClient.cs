using System.Collections.Concurrent;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Streamline.Client.Schema;

/// <summary>
/// Production-grade HTTP client for the Streamline Schema Registry API.
/// Provides schema registration, retrieval, compatibility checks, and
/// subject management with built-in caching and DI support.
/// </summary>
/// <remarks>
/// This client caches schemas by ID and by subject+version to reduce
/// network round-trips. The cache is bounded by <see cref="SchemaRegistryOptions.CacheCapacity"/>.
/// </remarks>
public sealed class SchemaRegistryClient : ISchemaRegistryClient
{
    private readonly HttpClient _httpClient;
    private readonly bool _ownsHttpClient;
    private readonly ILogger<SchemaRegistryClient> _logger;
    private readonly int _cacheCapacity;

    private readonly ConcurrentDictionary<int, SchemaInfo> _cacheById = new();
    private readonly ConcurrentDictionary<string, SchemaInfo> _cacheBySubjectVersion = new();

    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
    };

    private bool _disposed;

    /// <summary>
    /// Creates a new Schema Registry client with the specified base URL.
    /// </summary>
    /// <param name="baseUrl">Base URL of the schema registry (e.g., "http://localhost:9094").</param>
    public SchemaRegistryClient(string baseUrl)
        : this(new SchemaRegistryOptions { BaseUrl = baseUrl })
    {
    }

    /// <summary>
    /// Creates a new Schema Registry client with the specified options.
    /// </summary>
    /// <param name="options">Schema registry configuration options.</param>
    /// <param name="logger">Optional logger instance.</param>
    public SchemaRegistryClient(
        SchemaRegistryOptions options,
        ILogger<SchemaRegistryClient>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        _logger = logger ?? NullLogger<SchemaRegistryClient>.Instance;
        _cacheCapacity = options.CacheCapacity;
        _ownsHttpClient = true;

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(options.BaseUrl.TrimEnd('/')),
            Timeout = options.RequestTimeout,
        };

        if (options.AuthToken is not null)
        {
            _httpClient.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", options.AuthToken);
        }

        _logger.LogDebug("Schema registry client initialized with base URL: {BaseUrl}", options.BaseUrl);
    }

    /// <summary>
    /// Creates a new Schema Registry client using <see cref="IOptions{TOptions}"/> for DI integration.
    /// </summary>
    /// <param name="options">The options accessor.</param>
    /// <param name="logger">Optional logger instance.</param>
    public SchemaRegistryClient(
        IOptions<SchemaRegistryOptions> options,
        ILogger<SchemaRegistryClient>? logger = null)
        : this(options?.Value ?? throw new ArgumentNullException(nameof(options)), logger)
    {
    }

    /// <summary>
    /// Creates a new Schema Registry client with a pre-configured <see cref="HttpClient"/>
    /// (useful for testing or custom HTTP pipeline configuration).
    /// </summary>
    /// <param name="httpClient">A pre-configured HttpClient instance.</param>
    /// <param name="cacheCapacity">Maximum number of cached schemas. Defaults to 1000.</param>
    /// <param name="logger">Optional logger instance.</param>
    public SchemaRegistryClient(
        HttpClient httpClient,
        int cacheCapacity = 1000,
        ILogger<SchemaRegistryClient>? logger = null)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _ownsHttpClient = false;
        _cacheCapacity = cacheCapacity;
        _logger = logger ?? NullLogger<SchemaRegistryClient>.Instance;
    }

    /// <inheritdoc />
    public async Task<int> RegisterSchemaAsync(
        string subject,
        string schema,
        SchemaFormat format,
        CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);

        _logger.LogDebug("Registering schema for subject {Subject} with format {Format}", subject, format);

        var requestBody = new
        {
            schema,
            schemaType = format.ToString().ToUpperInvariant(),
        };

        var response = await SendAsync(
            HttpMethod.Post,
            $"/api/schemas/subjects/{Uri.EscapeDataString(subject)}/versions",
            cancellationToken,
            requestBody);

        var result = await DeserializeAsync<JsonElement>(response, cancellationToken);
        var schemaId = result.GetProperty("id").GetInt32();

        _logger.LogInformation("Registered schema for subject {Subject}: ID={SchemaId}", subject, schemaId);
        return schemaId;
    }

    /// <inheritdoc />
    public async Task<SchemaInfo> GetSchemaAsync(
        string subject,
        int version,
        CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(version);

        var cacheKey = $"{subject}:{version}";
        if (_cacheBySubjectVersion.TryGetValue(cacheKey, out var cached))
        {
            _logger.LogDebug("Cache hit for subject {Subject} version {Version}", subject, version);
            return cached;
        }

        _logger.LogDebug("Fetching schema for subject {Subject} version {Version}", subject, version);

        var response = await SendAsync(
            HttpMethod.Get,
            $"/api/schemas/subjects/{Uri.EscapeDataString(subject)}/versions/{version}",
            cancellationToken);

        var schemaInfo = await DeserializeAsync<SchemaInfo>(response, cancellationToken)
            ?? throw new StreamlineException(
                $"Empty response when fetching schema for subject '{subject}' version {version}",
                StreamlineErrorCode.Serialization);

        CacheSchema(schemaInfo, cacheKey);
        return schemaInfo;
    }

    /// <inheritdoc />
    public async Task<SchemaInfo> GetLatestSchemaAsync(
        string subject,
        CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);

        _logger.LogDebug("Fetching latest schema for subject {Subject}", subject);

        var response = await SendAsync(
            HttpMethod.Get,
            $"/api/schemas/subjects/{Uri.EscapeDataString(subject)}/versions/latest",
            cancellationToken);

        var schemaInfo = await DeserializeAsync<SchemaInfo>(response, cancellationToken)
            ?? throw new StreamlineException(
                $"Empty response when fetching latest schema for subject '{subject}'",
                StreamlineErrorCode.Serialization);

        var cacheKey = $"{subject}:{schemaInfo.Version}";
        CacheSchema(schemaInfo, cacheKey);
        return schemaInfo;
    }

    /// <inheritdoc />
    public async Task<SchemaInfo> GetSchemaByIdAsync(
        int id,
        CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(id);

        if (_cacheById.TryGetValue(id, out var cached))
        {
            _logger.LogDebug("Cache hit for schema ID {SchemaId}", id);
            return cached;
        }

        _logger.LogDebug("Fetching schema by ID {SchemaId}", id);

        var response = await SendAsync(
            HttpMethod.Get,
            $"/api/schemas/ids/{id}",
            cancellationToken);

        var schemaInfo = await DeserializeAsync<SchemaInfo>(response, cancellationToken)
            ?? throw new StreamlineException(
                $"Empty response when fetching schema ID {id}",
                StreamlineErrorCode.Serialization);

        CacheSchema(schemaInfo, cacheKey: null);
        return schemaInfo;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListSubjectsAsync(
        CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();

        _logger.LogDebug("Listing all subjects");

        var response = await SendAsync(
            HttpMethod.Get,
            "/api/schemas/subjects",
            cancellationToken);

        return await DeserializeAsync<List<string>>(response, cancellationToken)
            ?? [];
    }

    /// <inheritdoc />
    public async Task DeleteSubjectAsync(
        string subject,
        CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);

        _logger.LogInformation("Deleting subject {Subject}", subject);

        await SendAsync(
            HttpMethod.Delete,
            $"/api/schemas/subjects/{Uri.EscapeDataString(subject)}",
            cancellationToken);

        // Evict all cached entries for this subject
        var keysToRemove = _cacheBySubjectVersion.Keys
            .Where(k => k.StartsWith($"{subject}:", StringComparison.Ordinal))
            .ToList();

        foreach (var key in keysToRemove)
        {
            if (_cacheBySubjectVersion.TryRemove(key, out var removed))
            {
                _cacheById.TryRemove(removed.Id, out _);
            }
        }
    }

    /// <inheritdoc />
    public async Task<bool> CheckCompatibilityAsync(
        string subject,
        string schema,
        SchemaFormat format,
        CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);
        ArgumentException.ThrowIfNullOrWhiteSpace(schema);

        _logger.LogDebug("Checking compatibility for subject {Subject}", subject);

        var requestBody = new
        {
            schema,
            schemaType = format.ToString().ToUpperInvariant(),
        };

        var response = await SendAsync(
            HttpMethod.Post,
            $"/api/schemas/compatibility/subjects/{Uri.EscapeDataString(subject)}/versions/latest",
            cancellationToken,
            requestBody);

        var result = await DeserializeAsync<JsonElement>(response, cancellationToken);
        var isCompatible = result.GetProperty("is_compatible").GetBoolean();

        _logger.LogDebug("Compatibility check for subject {Subject}: {Result}", subject, isCompatible);
        return isCompatible;
    }

    /// <inheritdoc />
    public async Task<CompatibilityLevel> GetCompatibilityLevelAsync(
        string subject,
        CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);

        _logger.LogDebug("Getting compatibility level for subject {Subject}", subject);

        var response = await SendAsync(
            HttpMethod.Get,
            $"/api/schemas/config/{Uri.EscapeDataString(subject)}",
            cancellationToken);

        var result = await DeserializeAsync<JsonElement>(response, cancellationToken);
        var levelString = result.GetProperty("compatibilityLevel").GetString()
            ?? throw new StreamlineException(
                $"Missing compatibility level in response for subject '{subject}'",
                StreamlineErrorCode.Serialization);

        if (!Enum.TryParse<CompatibilityLevel>(levelString, ignoreCase: true, out var level))
        {
            throw new StreamlineException(
                $"Unknown compatibility level '{levelString}' for subject '{subject}'",
                StreamlineErrorCode.Serialization);
        }

        return level;
    }

    /// <inheritdoc />
    public async Task SetCompatibilityLevelAsync(
        string subject,
        CompatibilityLevel level,
        CancellationToken cancellationToken = default)
    {
        EnsureNotDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);

        _logger.LogInformation("Setting compatibility level for subject {Subject} to {Level}", subject, level);

        var requestBody = new { compatibility = level.ToString().ToUpperInvariant() };

        await SendAsync(
            HttpMethod.Put,
            $"/api/schemas/config/{Uri.EscapeDataString(subject)}",
            cancellationToken,
            requestBody);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _cacheById.Clear();
            _cacheBySubjectVersion.Clear();

            if (_ownsHttpClient)
            {
                _httpClient.Dispose();
            }

            _logger.LogDebug("Schema registry client disposed");
        }

        return ValueTask.CompletedTask;
    }

    private void CacheSchema(SchemaInfo schemaInfo, string? cacheKey)
    {
        if (_cacheCapacity <= 0)
            return;

        // Evict oldest entries if at capacity (simple eviction: clear half)
        if (_cacheById.Count >= _cacheCapacity)
        {
            var keysToRemove = _cacheById.Keys.Take(_cacheCapacity / 2).ToList();
            foreach (var key in keysToRemove)
            {
                _cacheById.TryRemove(key, out _);
            }

            var subjectKeysToRemove = _cacheBySubjectVersion.Keys.Take(_cacheCapacity / 2).ToList();
            foreach (var key in subjectKeysToRemove)
            {
                _cacheBySubjectVersion.TryRemove(key, out _);
            }
        }

        _cacheById.TryAdd(schemaInfo.Id, schemaInfo);

        if (cacheKey is not null)
        {
            _cacheBySubjectVersion.TryAdd(cacheKey, schemaInfo);
        }
    }

    private async Task<HttpResponseMessage> SendAsync(
        HttpMethod method,
        string path,
        CancellationToken cancellationToken,
        object? body = null)
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
            throw new StreamlineConnectionException(
                $"Schema registry request failed: {ex.Message}", ex);
        }
        catch (TaskCanceledException ex) when (!cancellationToken.IsCancellationRequested)
        {
            throw new StreamlineTimeoutException(
                $"Schema registry request timed out: {path}", ex);
        }

        if (response.IsSuccessStatusCode)
            return response;

        var errorBody = await response.Content.ReadAsStringAsync(cancellationToken);

        throw response.StatusCode switch
        {
            System.Net.HttpStatusCode.NotFound => new StreamlineException(
                $"Schema registry resource not found: {errorBody}",
                StreamlineErrorCode.Unknown,
                hint: "Check that the subject or schema ID exists"),
            System.Net.HttpStatusCode.Unauthorized => new StreamlineAuthenticationException(
                $"Schema registry unauthorized: {errorBody}"),
            System.Net.HttpStatusCode.Forbidden => new StreamlineAuthorizationException(
                $"Schema registry forbidden: {errorBody}"),
            System.Net.HttpStatusCode.Conflict => new StreamlineException(
                $"Schema registry conflict: {errorBody}",
                StreamlineErrorCode.Serialization,
                hint: "The schema may be incompatible with the current compatibility settings"),
            System.Net.HttpStatusCode.UnprocessableEntity => new StreamlineException(
                $"Invalid schema: {errorBody}",
                StreamlineErrorCode.Serialization,
                hint: "Check that the schema text is valid for the specified format"),
            _ => new StreamlineException(
                $"Schema registry HTTP {(int)response.StatusCode}: {errorBody}",
                StreamlineErrorCode.Connection),
        };
    }

    private async Task<T?> DeserializeAsync<T>(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        return await JsonSerializer.DeserializeAsync<T>(stream, _jsonOptions, cancellationToken);
    }

    private void EnsureNotDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}
