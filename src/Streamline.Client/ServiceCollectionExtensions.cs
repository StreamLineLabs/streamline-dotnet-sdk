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
}
