using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Streamline.Client;

/// <summary>
/// Extension methods for configuring Streamline in dependency injection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds Streamline client to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration action.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddStreamline(
        this IServiceCollection services,
        Action<StreamlineOptions> configure)
    {
        services.Configure(configure);

        services.AddSingleton<IStreamlineClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<StreamlineOptions>>().Value;
            var logger = sp.GetRequiredService<ILogger<StreamlineClient>>();
            return new StreamlineClient(options, logger);
        });

        return services;
    }

    /// <summary>
    /// Adds Streamline client with options from configuration.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="bootstrapServers">Bootstrap servers.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddStreamline(
        this IServiceCollection services,
        string bootstrapServers)
    {
        return services.AddStreamline(options =>
        {
            options.BootstrapServers = bootstrapServers;
        });
    }

    /// <summary>
    /// Adds the Streamline admin client to the service collection.
    /// Uses the <see cref="AdminOptions"/> from the configured <see cref="StreamlineOptions"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddStreamlineAdmin(
        this IServiceCollection services)
    {
        services.AddSingleton<IAdminClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<StreamlineOptions>>().Value;
            var httpClient = new HttpClient
            {
                BaseAddress = new Uri(options.Admin.HttpBaseUrl),
                Timeout = options.Admin.Timeout,
            };
            if (options.Admin.AuthToken is not null)
            {
                httpClient.DefaultRequestHeaders.Authorization =
                    new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", options.Admin.AuthToken);
            }
            return new AdminClient(httpClient);
        });

        return services;
    }

    /// <summary>
    /// Adds the Streamline admin client with explicit configuration.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="httpBaseUrl">Base URL of the HTTP API (e.g., "http://localhost:9094").</param>
    /// <param name="authToken">Optional bearer token.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddStreamlineAdmin(
        this IServiceCollection services,
        string httpBaseUrl,
        string? authToken = null)
    {
        services.AddSingleton<IAdminClient>(sp =>
        {
            return new AdminClient(httpBaseUrl, authToken);
        });

        return services;
    }
}
