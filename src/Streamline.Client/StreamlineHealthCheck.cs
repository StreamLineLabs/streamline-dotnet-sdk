using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Streamline.Client;

/// <summary>
/// Health check for Streamline connectivity.
/// Reports Healthy when the client can reach the broker, Unhealthy otherwise.
/// </summary>
public class StreamlineHealthCheck : IHealthCheck
{
    private readonly IStreamlineClient _client;

    public StreamlineHealthCheck(IStreamlineClient client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var isHealthy = await _client.IsHealthyAsync(cancellationToken);
            return isHealthy
                ? HealthCheckResult.Healthy("Streamline broker is reachable")
                : HealthCheckResult.Unhealthy("Streamline broker is not reachable");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Streamline health check failed", ex);
        }
    }
}
