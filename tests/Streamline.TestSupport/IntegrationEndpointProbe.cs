using System.Net.Sockets;

namespace Streamline.TestSupport;

/// <summary>
/// Thrown when integration tests are explicitly enabled but the configured Streamline
/// endpoints cannot be reached within the readiness budget.
/// </summary>
public sealed class StreamlineIntegrationUnavailableException : Exception
{
    /// <summary>Creates the exception with the given message.</summary>
    /// <param name="message">A description of what could not be reached.</param>
    public StreamlineIntegrationUnavailableException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with the given message and cause.</summary>
    /// <param name="message">A description of what could not be reached.</param>
    /// <param name="innerException">The underlying transport failure.</param>
    public StreamlineIntegrationUnavailableException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Bounded reachability probes for the Streamline Kafka-protocol and HTTP endpoints.
/// Every probe is time-boxed so an unreachable endpoint fails fast rather than
/// retrying <c>localhost</c> indefinitely.
/// </summary>
public static class IntegrationEndpointProbe
{
    private static readonly string[] HealthPaths = ["/health/live", "/health"];

    /// <summary>
    /// Attempts a TCP connection to the first broker in <paramref name="bootstrapServers"/>.
    /// </summary>
    /// <param name="bootstrapServers">Comma-separated <c>host:port</c> list.</param>
    /// <param name="timeout">Upper bound on the probe.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> when the port accepts a connection within the budget.</returns>
    public static async Task<bool> IsBrokerReachableAsync(
        string bootstrapServers,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        var (host, port) = StreamlineTestEnvironment.ParseFirstBroker(bootstrapServers);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);

        try
        {
            using var socket = new TcpClient();
            await socket.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);
            return socket.Connected;
        }
        catch (Exception ex) when (ex is SocketException or OperationCanceledException or ArgumentException)
        {
            return false;
        }
    }

    /// <summary>
    /// Issues a bounded health request against the HTTP management endpoint.
    /// </summary>
    /// <param name="httpBaseUrl">Base URL of the HTTP API.</param>
    /// <param name="timeout">Upper bound on the probe.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true"/> when a health endpoint responds successfully within the budget.</returns>
    public static async Task<bool> IsHttpReachableAsync(
        string httpBaseUrl,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        if (!Uri.TryCreate(httpBaseUrl, UriKind.Absolute, out var baseUri))
            return false;

        using var http = new HttpClient { BaseAddress = baseUri, Timeout = timeout };

        foreach (var path in HealthPaths)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(timeout);

            try
            {
                using var response = await http.GetAsync(path, cts.Token).ConfigureAwait(false);
                if (response.IsSuccessStatusCode)
                    return true;
            }
            catch (Exception ex) when (ex is HttpRequestException or OperationCanceledException)
            {
                // Try the next candidate path, then give up within the budget.
            }
        }

        return false;
    }

    /// <summary>
    /// Verifies that both endpoints are reachable, throwing an actionable exception otherwise.
    /// </summary>
    /// <param name="bootstrapServers">Comma-separated <c>host:port</c> list.</param>
    /// <param name="httpBaseUrl">Base URL of the HTTP API.</param>
    /// <param name="timeout">Upper bound applied to each probe.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="StreamlineIntegrationUnavailableException">
    /// Thrown when either endpoint does not respond within the budget.
    /// </exception>
    public static async Task EnsureReachableAsync(
        string bootstrapServers,
        string httpBaseUrl,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        var unreachable = new List<string>();

        if (!await IsBrokerReachableAsync(bootstrapServers, timeout, cancellationToken).ConfigureAwait(false))
            unreachable.Add($"Kafka protocol endpoint '{bootstrapServers}'");

        if (!await IsHttpReachableAsync(httpBaseUrl, timeout, cancellationToken).ConfigureAwait(false))
            unreachable.Add($"HTTP endpoint '{httpBaseUrl}'");

        if (unreachable.Count == 0)
            return;

        throw new StreamlineIntegrationUnavailableException(
            $"{StreamlineTestEnvironment.IntegrationVariable} is enabled but {string.Join(" and ", unreachable)} " +
            $"did not respond within {timeout.TotalSeconds:0.#}s. {StreamlineTestEnvironment.SetupHint}");
    }
}
