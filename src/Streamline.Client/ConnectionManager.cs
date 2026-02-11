using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Streamline.Client;

/// <summary>
/// Connection states for the <see cref="ConnectionManager"/>.
/// </summary>
public enum ConnectionState
{
    /// <summary>Not connected to the server.</summary>
    Disconnected,
    /// <summary>Connected and healthy.</summary>
    Connected,
    /// <summary>Attempting to reconnect after a failure.</summary>
    Reconnecting,
}

/// <summary>
/// Manages HTTP connections to the Streamline server, including health checks
/// and automatic reconnection.
/// </summary>
public sealed class ConnectionManager : IAsyncDisposable
{
    private readonly StreamlineOptions _options;
    private readonly ILogger _logger;
    private readonly HttpClient _httpClient;
    private readonly bool _ownsHttpClient;
    private readonly RetryPolicy _retryPolicy;
    private readonly CancellationTokenSource _cts = new();
    private readonly object _stateLock = new();

    private volatile ConnectionState _state = ConnectionState.Disconnected;
    private Task? _healthCheckTask;

    /// <summary>
    /// Raised when the connection state changes.
    /// </summary>
    public event Action<ConnectionState>? StateChanged;

    /// <summary>
    /// The current connection state.
    /// </summary>
    public ConnectionState State => _state;

    /// <summary>
    /// The health check interval. Default is 30 seconds.
    /// </summary>
    public TimeSpan HealthCheckInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Creates a new connection manager with an internally-owned <see cref="HttpClient"/>.
    /// </summary>
    public ConnectionManager(StreamlineOptions options, ILogger? logger = null)
        : this(options ?? throw new ArgumentNullException(nameof(options)),
               CreateDefaultHttpClient(options), ownsHttpClient: true, logger)
    {
    }

    /// <summary>
    /// Creates a new connection manager with an externally-provided <see cref="HttpClient"/>.
    /// </summary>
    public ConnectionManager(
        StreamlineOptions options,
        HttpClient httpClient,
        ILogger? logger = null)
        : this(options, httpClient, ownsHttpClient: false, logger)
    {
    }

    private ConnectionManager(
        StreamlineOptions options,
        HttpClient httpClient,
        bool ownsHttpClient,
        ILogger? logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _ownsHttpClient = ownsHttpClient;
        _logger = logger ?? NullLogger.Instance;
        _retryPolicy = new RetryPolicy(new RetryPolicyOptions
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(500),
            MaxDelay = TimeSpan.FromSeconds(5),
        }, _logger);
    }

    /// <summary>
    /// Establishes the initial connection and starts periodic health checks.
    /// </summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (_state == ConnectionState.Connected)
            return;

        await PerformHealthCheckAsync(cancellationToken).ConfigureAwait(false);
        StartHealthCheckLoop();
    }

    /// <summary>
    /// Sends a request through the managed connection with retry logic.
    /// </summary>
    public async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken = default)
    {
        return await _retryPolicy.ExecuteAsync(async () =>
        {
            if (_state == ConnectionState.Disconnected)
            {
                throw new StreamlineConnectionException(
                    "Not connected to Streamline server. Call ConnectAsync first.");
            }

            try
            {
                var response = await _httpClient.SendAsync(request, cancellationToken)
                    .ConfigureAwait(false);
                return response;
            }
            catch (HttpRequestException ex)
            {
                TransitionState(ConnectionState.Reconnecting);
                throw new StreamlineConnectionException(
                    $"Request failed: {ex.Message}", ex);
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Performs a single health check against the /health endpoint.
    /// </summary>
    public async ValueTask<bool> CheckHealthAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, "/health");
            var response = await _httpClient.SendAsync(request, cancellationToken)
                .ConfigureAwait(false);
            var healthy = response.IsSuccessStatusCode;

            TransitionState(healthy ? ConnectionState.Connected : ConnectionState.Disconnected);

            return healthy;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "Health check failed");
            TransitionState(ConnectionState.Disconnected);
            return false;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_cts.IsCancellationRequested)
            return;

        await _cts.CancelAsync().ConfigureAwait(false);

        if (_healthCheckTask is not null)
        {
            try
            {
                await _healthCheckTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown.
            }
        }

        if (_ownsHttpClient)
            _httpClient.Dispose();

        _cts.Dispose();

        _logger.LogInformation("Connection manager disposed");
    }

    private async Task PerformHealthCheckAsync(CancellationToken cancellationToken)
    {
        var healthy = await CheckHealthAsync(cancellationToken).ConfigureAwait(false);

        if (!healthy)
        {
            await _retryPolicy.ExecuteAsync(async () =>
            {
                var ok = await CheckHealthAsync(cancellationToken).ConfigureAwait(false);
                if (!ok)
                    throw new StreamlineConnectionException("Health check failed during connect");
                return ok;
            }, cancellationToken).ConfigureAwait(false);
        }
    }

    private void StartHealthCheckLoop()
    {
        if (_healthCheckTask is not null)
            return;

        _healthCheckTask = Task.Run(async () =>
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(HealthCheckInterval, _cts.Token).ConfigureAwait(false);
                    await CheckHealthAsync(_cts.Token).ConfigureAwait(false);

                    if (_state == ConnectionState.Disconnected)
                    {
                        TransitionState(ConnectionState.Reconnecting);
                        await PerformHealthCheckAsync(_cts.Token).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Health check loop encountered an error");
                }
            }
        }, _cts.Token);
    }

    private void TransitionState(ConnectionState newState)
    {
        lock (_stateLock)
        {
            if (_state == newState)
                return;

            var oldState = _state;
            _state = newState;
            _logger.LogInformation("Connection state changed: {OldState} -> {NewState}",
                oldState, newState);
        }

        StateChanged?.Invoke(newState);
    }

    private static HttpClient CreateDefaultHttpClient(StreamlineOptions options)
    {
        var handler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            MaxConnectionsPerServer = options.ConnectionPoolSize,
            ConnectTimeout = options.ConnectTimeout,
        };

        // Parse the first bootstrap server for the HTTP API base address (port 9094)
        var server = options.BootstrapServers.Split(',')[0].Trim();
        var host = server.Contains(':') ? server[..server.LastIndexOf(':')] : server;

        return new HttpClient(handler)
        {
            BaseAddress = new Uri($"http://{host}:9094"),
            Timeout = options.RequestTimeout,
        };
    }
}
