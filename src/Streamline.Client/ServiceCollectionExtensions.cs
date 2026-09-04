using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Streamline.Client.Schema;

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
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);

        services.AddSingleton<IStreamlineClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<StreamlineOptions>>().Value;
            var logger = sp.GetService<ILogger<StreamlineClient>>();
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
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(bootstrapServers);
        return services.AddStreamline(options =>
        {
            options.BootstrapServers = bootstrapServers;
        });
    }

    /// <summary>
    /// Adds the Streamline admin client to the service collection.
    /// Uses the <see cref="AdminOptions"/> from the configured <see cref="StreamlineOptions"/>.
    /// </summary>
    /// <remarks>
    /// The <see cref="AdminClient"/> is constructed via the base-URL constructor so it
    /// creates and owns its own <see cref="HttpClient"/>. That client is not registered
    /// with DI or an <c>IHttpClientFactory</c>, so nothing else can hold a
    /// reference to it; <see cref="AdminClient"/> disposing it when the container
    /// disposes the singleton is what prevents the handle (and its socket/connection
    /// pool) from leaking for the lifetime of the process.
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddStreamlineAdmin(
        this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddSingleton<IAdminClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<StreamlineOptions>>().Value;
            return new AdminClient(options.Admin.HttpBaseUrl, options.Admin.AuthToken, options.Admin.Timeout);
        });

        return services;
    }

    /// <summary>
    /// Adds the Streamline admin client with explicit configuration.
    /// </summary>
    /// <remarks>
    /// Uses the <see cref="AdminClient(string, string?)"/> constructor, which creates
    /// and owns its own <see cref="HttpClient"/>; that client is disposed along with
    /// the <see cref="AdminClient"/> singleton when the container is disposed.
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="httpBaseUrl">Base URL of the HTTP API (e.g., "http://localhost:9094").</param>
    /// <param name="authToken">Optional bearer token.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddStreamlineAdmin(
        this IServiceCollection services,
        string httpBaseUrl,
        string? authToken = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(httpBaseUrl);
        services.AddSingleton<IAdminClient>(_ => new AdminClient(httpBaseUrl, authToken));

        return services;
    }

    /// <summary>
    /// Adds the Streamline Schema Registry client to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration action for <see cref="SchemaRegistryOptions"/>.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddStreamlineSchemaRegistry(
        this IServiceCollection services,
        Action<SchemaRegistryOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        services.Configure(configure);

        services.AddSingleton<ISchemaRegistryClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<SchemaRegistryOptions>>();
            var logger = sp.GetRequiredService<ILogger<SchemaRegistryClient>>();
            return new SchemaRegistryClient(options, logger);
        });

        return services;
    }

    /// <summary>
    /// Adds the Streamline Schema Registry client with the specified base URL.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="baseUrl">Base URL of the schema registry (e.g., "http://localhost:9094").</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddStreamlineSchemaRegistry(
        this IServiceCollection services,
        string baseUrl)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(baseUrl);
        return services.AddStreamlineSchemaRegistry(options =>
        {
            options.BaseUrl = baseUrl;
        });
    }

    /// <summary>
    /// Adds a Streamline health check to the service collection.
    /// Registers <see cref="StreamlineHealthCheck"/> under the name "streamline".
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddStreamlineHealthChecks(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddHealthChecks()
            .AddCheck<StreamlineHealthCheck>("streamline");
        return services;
    }
}
