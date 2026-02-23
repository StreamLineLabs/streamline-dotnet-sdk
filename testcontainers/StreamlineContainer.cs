using System.Net.Http;
using DotNet.Testcontainers.Containers;

namespace Streamline.TestContainers;

/// <summary>
/// A running Streamline container for integration testing.
/// </summary>
/// <remarks>
/// <para>
/// Streamline is a Kafka-compatible streaming platform that provides a lightweight,
/// single-binary alternative to Apache Kafka for development and testing.
/// </para>
/// <para>
/// Use <see cref="StreamlineBuilder"/> to configure and create instances of this container.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// await using var container = new StreamlineBuilder().Build();
/// await container.StartAsync();
///
/// var bootstrapServers = container.GetBootstrapServers();
/// // Use with any Kafka client
/// </code>
/// </example>
public sealed class StreamlineContainer : DockerContainer, IAsyncDisposable
{
    /// <summary>
    /// The Kafka protocol port (9092).
    /// </summary>
    public const int KafkaPort = 9092;

    /// <summary>
    /// The HTTP API port (9094).
    /// </summary>
    public const int HttpPort = 9094;

    private readonly StreamlineConfiguration _configuration;

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamlineContainer"/> class.
    /// </summary>
    /// <param name="configuration">The container configuration.</param>
    internal StreamlineContainer(StreamlineConfiguration configuration)
        : base(configuration)
    {
        _configuration = configuration;
    }

    /// <summary>
    /// Gets the Kafka bootstrap servers connection string.
    /// </summary>
    /// <remarks>
    /// Use this value for the <c>bootstrap.servers</c> property in Kafka clients.
    /// The returned string is in the format <c>host:port</c>, where the port is the
    /// mapped port on the host machine.
    /// </remarks>
    /// <returns>The bootstrap servers string in the format "host:port".</returns>
    public string GetBootstrapServers()
    {
        return $"{Hostname}:{GetMappedPublicPort(KafkaPort)}";
    }

    /// <summary>
    /// Gets the HTTP API base URL.
    /// </summary>
    /// <remarks>
    /// The HTTP API provides endpoints for health checks, metrics, and server administration.
    /// </remarks>
    /// <returns>The HTTP API base URL (e.g., "http://localhost:32768").</returns>
    public string GetHttpUrl()
    {
        return $"http://{Hostname}:{GetMappedPublicPort(HttpPort)}";
    }

    /// <summary>
    /// Gets the health check endpoint URL.
    /// </summary>
    /// <returns>The full URL for the health endpoint.</returns>
    public string GetHealthUrl()
    {
        return $"{GetHttpUrl()}/health";
    }

    /// <summary>
    /// Gets the Prometheus metrics endpoint URL.
    /// </summary>
    /// <returns>The full URL for the metrics endpoint.</returns>
    public string GetMetricsUrl()
    {
        return $"{GetHttpUrl()}/metrics";
    }

    /// <summary>
    /// Gets the server info endpoint URL.
    /// </summary>
    /// <returns>The full URL for the info endpoint.</returns>
    public string GetInfoUrl()
    {
        return $"{GetHttpUrl()}/info";
    }

    /// <summary>
    /// Creates a topic with the specified name and number of partitions.
    /// </summary>
    /// <remarks>
    /// Streamline supports auto-topic creation, so topics are created automatically when
    /// a producer first writes to them. Use this method if you need to pre-create topics
    /// with specific configurations.
    /// </remarks>
    /// <param name="topicName">The name of the topic to create.</param>
    /// <param name="partitions">The number of partitions (default: 1).</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when the topic has been created.</returns>
    /// <exception cref="InvalidOperationException">Thrown when topic creation fails.</exception>
    public async Task CreateTopicAsync(string topicName, int partitions = 1, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ArgumentOutOfRangeException.ThrowIfLessThan(partitions, 1);

        var result = await ExecAsync(
            new[]
            {
                "streamline-cli",
                "topics", "create", topicName,
                "--partitions", partitions.ToString()
            },
            cancellationToken
        ).ConfigureAwait(false);

        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Failed to create topic '{topicName}': {result.Stderr}");
        }
    }

    /// <summary>
    /// Creates multiple topics at once.
    /// </summary>
    /// <param name="topics">A dictionary mapping topic names to their partition counts.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when all topics have been created.</returns>
    /// <exception cref="InvalidOperationException">Thrown when any topic creation fails.</exception>
    public async Task CreateTopicsAsync(IDictionary<string, int> topics, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topics);

        foreach (var (name, partitions) in topics)
        {
            await CreateTopicAsync(name, partitions, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Produces a single message to a topic using the Streamline CLI.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="value">The message value.</param>
    /// <param name="key">An optional message key.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when the message has been produced.</returns>
    /// <exception cref="InvalidOperationException">Thrown when message production fails.</exception>
    public async Task ProduceMessageAsync(string topic, string value, string? key = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(value);

        var args = new List<string> { "streamline-cli", "produce", topic, "-m", value };
        if (!string.IsNullOrEmpty(key))
        {
            args.Add("-k");
            args.Add(key);
        }

        var result = await ExecAsync(args, cancellationToken).ConfigureAwait(false);

        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Failed to produce message to '{topic}': {result.Stderr}");
        }
    }

    /// <summary>
    /// Asserts that the container is healthy by calling the health endpoint.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when the health check passes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the health check fails.</exception>
    public async Task AssertHealthyAsync(CancellationToken cancellationToken = default)
    {
        using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var response = await httpClient.GetAsync(GetHealthUrl(), cancellationToken).ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException(
                $"Health check failed with status code {(int)response.StatusCode}");
        }
    }

    /// <summary>
    /// Asserts that a topic exists on the server.
    /// </summary>
    /// <param name="topicName">The topic name to verify.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when the assertion passes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the topic does not exist.</exception>
    public async Task AssertTopicExistsAsync(string topicName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);

        var result = await ExecAsync(
            new[] { "streamline-cli", "topics", "describe", topicName },
            cancellationToken
        ).ConfigureAwait(false);

        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException($"Topic '{topicName}' does not exist");
        }
    }

    /// <summary>
    /// Waits until all specified topics exist, polling at regular intervals.
    /// </summary>
    /// <param name="topics">The list of topic names to wait for.</param>
    /// <param name="timeout">The maximum time to wait (default: 10 seconds).</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that completes when all topics exist.</returns>
    /// <exception cref="TimeoutException">Thrown when the topics are not available within the timeout.</exception>
    public async Task WaitForTopicsAsync(IReadOnlyList<string> topics, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topics);

        var effectiveTimeout = timeout ?? TimeSpan.FromSeconds(10);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(effectiveTimeout);

        while (!cts.Token.IsCancellationRequested)
        {
            var allExist = true;
            foreach (var topic in topics)
            {
                var result = await ExecAsync(
                    new[] { "streamline-cli", "topics", "describe", topic },
                    cts.Token
                ).ConfigureAwait(false);

                if (result.ExitCode != 0)
                {
                    allExist = false;
                    break;
                }
            }

            if (allExist)
            {
                return;
            }

            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(200), cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cts.Token.IsCancellationRequested)
            {
                break;
            }
        }

        throw new TimeoutException(
            $"Topics [{string.Join(", ", topics)}] not available within {effectiveTimeout}");
    }

    /// <summary>
    /// Creates a <see cref="Streamline.Client.StreamlineClient"/> pre-configured to connect to this container.
    /// </summary>
    /// <returns>A new <see cref="Streamline.Client.StreamlineClient"/> instance.</returns>
    public Client.StreamlineClient CreateClient()
    {
        return new Client.StreamlineClient(GetBootstrapServers());
    }

    /// <summary>
    /// Creates a <see cref="Streamline.Client.StreamlineClient"/> pre-configured to connect to this container
    /// with custom options.
    /// </summary>
    /// <param name="configureOptions">An action to further configure the client options.</param>
    /// <returns>A new <see cref="Streamline.Client.StreamlineClient"/> instance.</returns>
    public Client.StreamlineClient CreateClient(Action<Client.StreamlineOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(configureOptions);

        var options = new Client.StreamlineOptions
        {
            BootstrapServers = GetBootstrapServers()
        };
        configureOptions(options);
        return new Client.StreamlineClient(options);
    }

    // -------------------------------------------------------------------------
    // Enhanced capabilities: batch produce, consumer groups, migration helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Produces a batch of messages to a topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="messages">The messages to produce.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    public async Task ProduceMessagesAsync(string topic, IEnumerable<string> messages, CancellationToken cancellationToken = default)
    {
        foreach (var message in messages)
        {
            await ProduceMessageAsync(topic, message, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Produces a batch of keyed messages to a topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="messages">Dictionary of key to value.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    public async Task ProduceKeyedMessagesAsync(string topic, IDictionary<string, string> messages, CancellationToken cancellationToken = default)
    {
        foreach (var (key, value) in messages)
        {
            await ProduceMessageAsync(topic, value, key, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Asserts that a consumer group exists.
    /// </summary>
    /// <param name="groupId">The consumer group ID.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <exception cref="InvalidOperationException">Thrown when the group does not exist.</exception>
    public async Task AssertConsumerGroupExistsAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);

        var result = await ExecAsync(
            new[] { "streamline-cli", "groups", "describe", groupId },
            cancellationToken
        ).ConfigureAwait(false);

        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException($"Consumer group '{groupId}' does not exist");
        }
    }

    /// <summary>
    /// Gets cluster information from the HTTP API.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>The cluster info as a JSON string.</returns>
    public async Task<string> GetClusterInfoAsync(CancellationToken cancellationToken = default)
    {
        using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var response = await httpClient.GetAsync(GetInfoUrl(), cancellationToken).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a StreamlineBuilder pre-configured as a drop-in Kafka replacement.
    /// Useful for migrating from Kafka-based tests.
    /// </summary>
    /// <example>
    /// <code>
    /// // Before (Kafka):
    /// // await using var kafka = new KafkaBuilder().Build();
    ///
    /// // After (Streamline):
    /// await using var container = StreamlineContainer.AsKafkaReplacement();
    /// await container.StartAsync();
    /// var servers = container.GetBootstrapServers();
    /// </code>
    /// </example>
    /// <returns>A StreamlineContainer configured for Kafka compatibility.</returns>
    public static StreamlineContainer AsKafkaReplacement()
    {
        return new StreamlineBuilder()
            .WithInMemory()
            .WithStreamlineEnv("STREAMLINE_AUTO_CREATE_TOPICS", "true")
            .WithStreamlineEnv("STREAMLINE_DEFAULT_PARTITIONS", "1")
            .Build();
    }
}
