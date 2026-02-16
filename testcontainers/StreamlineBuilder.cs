using Docker.DotNet.Models;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;

namespace Streamline.TestContainers;

/// <summary>
/// Builder for configuring and creating <see cref="StreamlineContainer"/> instances.
/// </summary>
/// <remarks>
/// <para>
/// This builder extends the Testcontainers <see cref="ContainerBuilder{TBuilderEntity, TContainerEntity, TConfigurationEntity}"/>
/// to provide a fluent API for configuring Streamline containers.
/// </para>
/// <para>
/// By default, the builder configures the container with:
/// <list type="bullet">
///   <item><description>Image: <c>ghcr.io/streamlinelabs/streamline:latest</c></description></item>
///   <item><description>Kafka port: 9092 (exposed)</description></item>
///   <item><description>HTTP port: 9094 (exposed)</description></item>
///   <item><description>Health check wait strategy on the HTTP /health endpoint</description></item>
///   <item><description>30-second startup timeout</description></item>
/// </list>
/// </para>
/// </remarks>
/// <example>
/// Basic usage:
/// <code>
/// await using var container = new StreamlineBuilder().Build();
/// await container.StartAsync();
/// var bootstrapServers = container.GetBootstrapServers();
/// </code>
/// </example>
/// <example>
/// With configuration:
/// <code>
/// await using var container = new StreamlineBuilder()
///     .WithImage("ghcr.io/streamlinelabs/streamline:0.2.0")
///     .WithDebugLogging()
///     .WithPlayground()
///     .Build();
/// await container.StartAsync();
/// </code>
/// </example>
public sealed class StreamlineBuilder : ContainerBuilder<StreamlineBuilder, StreamlineContainer, StreamlineConfiguration>
{
    /// <summary>
    /// The default Docker image for Streamline.
    /// </summary>
    public const string DefaultImage = "ghcr.io/streamlinelabs/streamline";

    /// <summary>
    /// The default Docker image tag.
    /// </summary>
    public const string DefaultTag = "latest";

    /// <summary>
    /// The default startup timeout.
    /// </summary>
    public static readonly TimeSpan DefaultStartupTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamlineBuilder"/> class with default configuration.
    /// </summary>
    public StreamlineBuilder()
        : this(new StreamlineConfiguration())
    {
        DockerResourceConfiguration = Init().DockerResourceConfiguration;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamlineBuilder"/> class with a specific configuration.
    /// </summary>
    /// <param name="resourceConfiguration">The Docker resource configuration.</param>
    private StreamlineBuilder(StreamlineConfiguration resourceConfiguration)
        : base(resourceConfiguration)
    {
        DockerResourceConfiguration = resourceConfiguration;
    }

    /// <inheritdoc />
    protected override StreamlineConfiguration DockerResourceConfiguration { get; }

    /// <summary>
    /// Sets the Docker image tag to use for the Streamline container.
    /// </summary>
    /// <param name="tag">The image tag (e.g., "0.2.0", "latest").</param>
    /// <returns>A configured <see cref="StreamlineBuilder"/>.</returns>
    public StreamlineBuilder WithTag(string tag)
    {
        return WithImage($"{DefaultImage}:{tag}");
    }

    /// <summary>
    /// Sets the log level for the Streamline server.
    /// </summary>
    /// <param name="level">The log level: trace, debug, info, warn, or error.</param>
    /// <returns>A configured <see cref="StreamlineBuilder"/>.</returns>
    public StreamlineBuilder WithLogLevel(string level)
    {
        return WithEnvironment("STREAMLINE_LOG_LEVEL", level);
    }

    /// <summary>
    /// Enables debug logging for the Streamline server.
    /// </summary>
    /// <returns>A configured <see cref="StreamlineBuilder"/>.</returns>
    public StreamlineBuilder WithDebugLogging()
    {
        return WithLogLevel("debug");
    }

    /// <summary>
    /// Enables trace logging for the Streamline server.
    /// </summary>
    /// <returns>A configured <see cref="StreamlineBuilder"/>.</returns>
    public StreamlineBuilder WithTraceLogging()
    {
        return WithLogLevel("trace");
    }

    /// <summary>
    /// Enables in-memory storage mode (no disk persistence).
    /// </summary>
    /// <remarks>
    /// When enabled, all data is stored in memory and will be lost when the container stops.
    /// This is useful for faster tests that do not need data persistence.
    /// </remarks>
    /// <returns>A configured <see cref="StreamlineBuilder"/>.</returns>
    public StreamlineBuilder WithInMemory()
    {
        return WithEnvironment("STREAMLINE_IN_MEMORY", "true");
    }

    /// <summary>
    /// Enables playground mode (pre-loaded demo topics and sample data).
    /// </summary>
    /// <returns>A configured <see cref="StreamlineBuilder"/>.</returns>
    public StreamlineBuilder WithPlayground()
    {
        return WithEnvironment("STREAMLINE_PLAYGROUND", "true");
    }

    /// <summary>
    /// Sets a Streamline server environment variable.
    /// </summary>
    /// <param name="name">The environment variable name (without the STREAMLINE_ prefix).</param>
    /// <param name="value">The environment variable value.</param>
    /// <returns>A configured <see cref="StreamlineBuilder"/>.</returns>
    public StreamlineBuilder WithStreamlineEnv(string name, string value)
    {
        return WithEnvironment($"STREAMLINE_{name}", value);
    }

    /// <summary>
    /// Sets the startup timeout for waiting on the container to become ready.
    /// </summary>
    /// <param name="timeout">The startup timeout.</param>
    /// <returns>A configured <see cref="StreamlineBuilder"/>.</returns>
    public StreamlineBuilder WithStartupTimeout(TimeSpan timeout)
    {
        return WithWaitStrategy(
            Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(request =>
                    request
                        .ForPort(StreamlineContainer.HttpPort)
                        .ForPath("/health")
                        .ForStatusCode(System.Net.HttpStatusCode.OK))
                .AddCustomWaitStrategy(new WaitUntilPortIsAvailable(StreamlineContainer.KafkaPort)));
    }

    /// <inheritdoc />
    public override StreamlineContainer Build()
    {
        Validate();
        return new StreamlineContainer(DockerResourceConfiguration);
    }

    /// <inheritdoc />
    protected override StreamlineBuilder Init()
    {
        return base.Init()
            .WithImage($"{DefaultImage}:{DefaultTag}")
            .WithPortBinding(StreamlineContainer.KafkaPort, true)
            .WithPortBinding(StreamlineContainer.HttpPort, true)
            .WithEnvironment("STREAMLINE_LISTEN_ADDR", $"0.0.0.0:{StreamlineContainer.KafkaPort}")
            .WithEnvironment("STREAMLINE_HTTP_ADDR", $"0.0.0.0:{StreamlineContainer.HttpPort}")
            .WithWaitStrategy(
                Wait.ForUnixContainer()
                    .UntilHttpRequestIsSucceeded(request =>
                        request
                            .ForPort(StreamlineContainer.HttpPort)
                            .ForPath("/health")
                            .ForStatusCode(System.Net.HttpStatusCode.OK)));
    }

    /// <inheritdoc />
    protected override StreamlineBuilder Clone(IContainerConfiguration resourceConfiguration)
    {
        return Merge(DockerResourceConfiguration, new StreamlineConfiguration(resourceConfiguration));
    }

    /// <inheritdoc />
    protected override StreamlineBuilder Clone(IResourceConfiguration<CreateContainerParameters> resourceConfiguration)
    {
        return Merge(DockerResourceConfiguration, new StreamlineConfiguration(resourceConfiguration));
    }

    /// <inheritdoc />
    protected override StreamlineBuilder Merge(StreamlineConfiguration oldValue, StreamlineConfiguration newValue)
    {
        return new StreamlineBuilder(new StreamlineConfiguration(oldValue, newValue));
    }
}

/// <summary>
/// Custom wait strategy that waits until a specific port is available on the container.
/// </summary>
internal sealed class WaitUntilPortIsAvailable : IWaitUntil
{
    private readonly int _port;

    /// <summary>
    /// Initializes a new instance of the <see cref="WaitUntilPortIsAvailable"/> class.
    /// </summary>
    /// <param name="port">The port to wait for.</param>
    public WaitUntilPortIsAvailable(int port)
    {
        _port = port;
    }

    /// <inheritdoc />
    public async Task<bool> UntilAsync(IContainer container)
    {
        try
        {
            var mappedPort = container.GetMappedPublicPort(_port);
            using var tcpClient = new System.Net.Sockets.TcpClient();
            await tcpClient.ConnectAsync(container.Hostname, mappedPort).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
