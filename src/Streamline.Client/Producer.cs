using Microsoft.Extensions.Logging;

namespace Streamline.Client;

/// <summary>
/// Interface for producing messages.
/// </summary>
public interface IProducer<TKey, TValue> : IAsyncDisposable
{
    /// <summary>
    /// Sends a message to a topic.
    /// </summary>
    Task<RecordMetadata> SendAsync(
        string topic,
        TKey? key,
        TValue value,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a message with headers.
    /// </summary>
    Task<RecordMetadata> SendAsync(
        string topic,
        TKey? key,
        TValue value,
        Headers? headers,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Flushes any buffered messages.
    /// </summary>
    Task FlushAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Asynchronous producer for Streamline.
/// </summary>
internal class Producer<TKey, TValue> : IProducer<TKey, TValue>
{
    private readonly StreamlineOptions _clientOptions;
    private readonly ProducerOptions _options;
    private readonly ILogger _logger;
    private bool _disposed;

    public Producer(StreamlineOptions clientOptions, ProducerOptions options, ILogger logger)
    {
        _clientOptions = clientOptions;
        _options = options;
        _logger = logger;
    }

    public async Task<RecordMetadata> SendAsync(
        string topic,
        TKey? key,
        TValue value,
        CancellationToken cancellationToken = default)
    {
        return await SendAsync(topic, key, value, null, cancellationToken);
    }

    public async Task<RecordMetadata> SendAsync(
        string topic,
        TKey? key,
        TValue value,
        Headers? headers,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _logger.LogDebug("Sending message to topic {Topic}", topic);

        // TODO: Implement actual Kafka protocol message sending
        await Task.Delay(1, cancellationToken);

        return new RecordMetadata(
            Topic: topic,
            Partition: 0,
            Offset: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Timestamp: DateTimeOffset.UtcNow);
    }

    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _logger.LogDebug("Flushing producer");
        await Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _logger.LogDebug("Producer disposed");
        }
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// Metadata for a produced record.
/// </summary>
/// <param name="Topic">The topic name.</param>
/// <param name="Partition">The partition number.</param>
/// <param name="Offset">The offset of the record.</param>
/// <param name="Timestamp">The timestamp of the record.</param>
public record RecordMetadata(
    string Topic,
    int Partition,
    long Offset,
    DateTimeOffset Timestamp);
