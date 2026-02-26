using System.Text;
using Confluent.Kafka;
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
/// Asynchronous producer for Streamline, backed by Confluent.Kafka for wire protocol compatibility.
/// </summary>
internal class Producer<TKey, TValue> : IProducer<TKey, TValue>
{
    private readonly StreamlineOptions _clientOptions;
    private readonly ProducerOptions _options;
    private readonly ILogger _logger;
    private readonly IProducer<byte[], byte[]> _kafkaProducer;
    private bool _disposed;

    public Producer(StreamlineOptions clientOptions, ProducerOptions options, ILogger logger)
    {
        _clientOptions = clientOptions;
        _options = options;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = clientOptions.BootstrapServers,
            Acks = Acks.All,
            MessageSendMaxRetries = 3,
            BatchSize = options.BatchSize,
            LingerMs = options.LingerMs,
        };

        _kafkaProducer = new ProducerBuilder<byte[], byte[]>(config).Build();
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

        var keyBytes = key != null ? SerializeToBytes(key) : null;
        var valueBytes = SerializeToBytes(value);

        var message = new Message<byte[], byte[]>
        {
            Key = keyBytes,
            Value = valueBytes,
        };

        if (headers is not null && !headers.IsEmpty)
        {
            message.Headers = new Confluent.Kafka.Headers();
            foreach (var header in headers)
            {
                message.Headers.Add(header.Key, header.Value);
            }
        }

        var result = await _kafkaProducer.ProduceAsync(topic, message, cancellationToken);

        return new RecordMetadata(
            Topic: result.Topic,
            Partition: result.Partition.Value,
            Offset: result.Offset.Value,
            Timestamp: result.Timestamp.UtcDateTime);
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _kafkaProducer.Flush(cancellationToken);
        _logger.LogDebug("Producer flushed");
        return Task.CompletedTask;
    }

    private static byte[]? SerializeToBytes<T>(T? obj)
    {
        if (obj is null) return null;
        if (obj is byte[] bytes) return bytes;
        if (obj is string s) return Encoding.UTF8.GetBytes(s);
        return Encoding.UTF8.GetBytes(obj.ToString() ?? string.Empty);
    }

    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _kafkaProducer.Dispose();
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

// add batch producer API for bulk publishing
