using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Streamline.Client;

/// <summary>
/// Main entry point for the Streamline .NET client.
/// </summary>
/// <example>
/// <code>
/// var client = new StreamlineClient("localhost:9092");
/// await client.ProduceAsync("my-topic", "key", "value");
/// </code>
/// </example>
public class StreamlineClient : IStreamlineClient, IAsyncDisposable
{
    private readonly StreamlineOptions _options;
    private readonly ILogger<StreamlineClient> _logger;
    private readonly RetryPolicy _retryPolicy;
    private readonly ConnectionManager _connectionManager;
    private bool _disposed;

    /// <summary>
    /// Creates a new Streamline client with the specified bootstrap servers.
    /// </summary>
    /// <param name="bootstrapServers">Comma-separated list of host:port pairs.</param>
    public StreamlineClient(string bootstrapServers)
        : this(new StreamlineOptions { BootstrapServers = bootstrapServers })
    {
    }

    /// <summary>
    /// Creates a new Streamline client with the specified options.
    /// </summary>
    /// <param name="options">Client configuration options.</param>
    /// <param name="logger">Optional logger instance.</param>
    public StreamlineClient(StreamlineOptions options, ILogger<StreamlineClient>? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? NullLogger<StreamlineClient>.Instance;

        _retryPolicy = new RetryPolicy(new RetryPolicyOptions
        {
            MaxRetries = options.Producer.Retries,
            BaseDelay = TimeSpan.FromMilliseconds(options.Producer.RetryBackoffMs),
        }, _logger);

        _connectionManager = new ConnectionManager(options, _logger);

        _logger.LogInformation("Streamline client initialized with bootstrap servers: {Servers}",
            _options.BootstrapServers);
    }

    /// <summary>
    /// Creates a new Streamline client with explicit retry policy and connection manager.
    /// </summary>
    internal StreamlineClient(
        StreamlineOptions options,
        RetryPolicy retryPolicy,
        ConnectionManager connectionManager,
        ILogger<StreamlineClient>? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _retryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _logger = logger ?? NullLogger<StreamlineClient>.Instance;
    }

    /// <summary>
    /// The retry policy used by this client.
    /// </summary>
    public RetryPolicy RetryPolicy => _retryPolicy;

    /// <summary>
    /// The connection manager used by this client.
    /// </summary>
    public ConnectionManager ConnectionManager => _connectionManager;

    /// <summary>
    /// Produces a message to the specified topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="key">The message key.</param>
    /// <param name="value">The message value.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Record metadata including partition and offset.</returns>
    public async Task<RecordMetadata> ProduceAsync(
        string topic,
        string? key,
        string value,
        CancellationToken cancellationToken = default)
    {
        return await ProduceAsync(topic, key, value, null, cancellationToken);
    }

    /// <summary>
    /// Produces a message to the specified topic with headers.
    /// </summary>
    public async Task<RecordMetadata> ProduceAsync(
        string topic,
        string? key,
        string value,
        Headers? headers,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        return await _retryPolicy.ExecuteAsync(async () =>
        {
            _logger.LogDebug("Producing message to topic {Topic}", topic);

            // TODO: Implement actual Kafka protocol message sending
            await Task.Delay(1, cancellationToken);

            return new RecordMetadata(
                Topic: topic,
                Partition: 0,
                Offset: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Timestamp: DateTimeOffset.UtcNow);
        }, cancellationToken);
    }

    /// <summary>
    /// Creates a producer with default configuration.
    /// </summary>
    public IProducer<TKey, TValue> CreateProducer<TKey, TValue>()
    {
        return CreateProducer<TKey, TValue>(new ProducerOptions());
    }

    /// <summary>
    /// Creates a producer with custom configuration.
    /// </summary>
    public IProducer<TKey, TValue> CreateProducer<TKey, TValue>(ProducerOptions options)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new Producer<TKey, TValue>(_options, options, _logger);
    }

    /// <summary>
    /// Creates a consumer for the specified topic and group.
    /// </summary>
    public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(string topic, string groupId)
    {
        return CreateConsumer<TKey, TValue>(topic, new ConsumerOptions { GroupId = groupId });
    }

    /// <summary>
    /// Creates a consumer with custom configuration.
    /// </summary>
    public IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(string topic, ConsumerOptions options)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new Consumer<TKey, TValue>(_options, topic, options, _logger);
    }

    /// <summary>
    /// Checks if the client is connected and healthy.
    /// </summary>
    public async Task<bool> IsHealthyAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed)
            return false;

        return await _connectionManager.CheckHealthAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            await _connectionManager.DisposeAsync();
            _logger.LogInformation("Streamline client disposed");
        }
        GC.SuppressFinalize(this);
    }
}
