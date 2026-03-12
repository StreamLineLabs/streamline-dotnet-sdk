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

    /// <summary>
    /// Sends a batch of messages to a topic.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="messages">Sequence of (key, value) pairs to send.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Metadata for each produced record.</returns>
    Task<IReadOnlyList<RecordMetadata>> SendBatchAsync(
        string topic,
        IEnumerable<(TKey? Key, TValue Value)> messages,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Asynchronous producer for Streamline, backed by Confluent.Kafka for wire protocol compatibility.
/// </summary>
internal class Producer<TKey, TValue> : IProducer<TKey, TValue>
{
    private readonly StreamlineOptions _clientOptions;
    private readonly ProducerOptions _options;
    private readonly ILogger _logger;
    private readonly Confluent.Kafka.IProducer<byte[], byte[]> _kafkaProducer;
    private readonly CircuitBreaker? _circuitBreaker;
    private bool _disposed;
    private bool _inTransaction;
    private readonly List<(string Topic, TKey? Key, TValue Value, TaskCompletionSource<RecordMetadata> Tcs)> _transactionBuffer = new();

    public Producer(StreamlineOptions clientOptions, ProducerOptions options, ILogger logger, CircuitBreaker? circuitBreaker = null)
    {
        _clientOptions = clientOptions;
        _options = options;
        _logger = logger;
        _circuitBreaker = circuitBreaker;

        var config = new ProducerConfig
        {
            BootstrapServers = clientOptions.BootstrapServers,
            Acks = Acks.All,
            MessageSendMaxRetries = options.Retries,
            RetryBackoffMs = options.RetryBackoffMs,
            BatchSize = options.BatchSize,
            LingerMs = options.LingerMs,
            CompressionType = MapCompressionType(options.CompressionType),
            EnableIdempotence = options.Idempotent,
            SecurityProtocol = MapSecurityProtocol(clientOptions.SecurityProtocol),
        };

        if (clientOptions.Tls is { } tls)
        {
            if (tls.CaCertificatePath is not null)
                config.SslCaLocation = tls.CaCertificatePath;
            if (tls.ClientCertificatePath is not null)
                config.SslCertificateLocation = tls.ClientCertificatePath;
            if (tls.ClientKeyPath is not null)
                config.SslKeyLocation = tls.ClientKeyPath;
            if (tls.SkipCertificateVerification)
                config.EnableSslCertificateVerification = false;
        }

        if (clientOptions.Sasl is { } sasl)
        {
            config.SaslMechanism = MapSaslMechanism(sasl.Mechanism);
            config.SaslUsername = sasl.Username;
            config.SaslPassword = sasl.Password;
        }

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

        if (_circuitBreaker is not null && !_circuitBreaker.Allow())
        {
            throw new StreamlineException(
                "Circuit breaker is open — too many recent failures",
                isRetryable: true,
                hint: "The client detected repeated failures and is temporarily pausing requests.");
        }

        _logger.LogDebug("Sending message to topic {Topic}", topic);

        var keyBytes = key != null ? SerializeToBytes(key) : null;
        var valueBytes = SerializeToBytes(value);

        var message = new Message<byte[], byte[]>
        {
            Key = keyBytes!,
            Value = valueBytes!,
        };

        if (headers is not null && !headers.IsEmpty)
        {
            message.Headers = new Confluent.Kafka.Headers();
            foreach (var header in headers)
            {
                message.Headers.Add(header.Key, header.Value);
            }
        }

        try
        {
            var result = await _kafkaProducer.ProduceAsync(topic, message, cancellationToken);
            _circuitBreaker?.RecordSuccess();

            return new RecordMetadata(
                Topic: result.Topic,
                Partition: result.Partition.Value,
                Offset: result.Offset.Value,
                Timestamp: result.Timestamp.UtcDateTime);
        }
        catch
        {
            _circuitBreaker?.RecordFailure();
            throw;
        }
    }

    public async Task<IReadOnlyList<RecordMetadata>> SendBatchAsync(
        string topic,
        IEnumerable<(TKey? Key, TValue Value)> messages,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_circuitBreaker is not null && !_circuitBreaker.Allow())
        {
            throw new StreamlineException(
                "Circuit breaker is open — too many recent failures",
                isRetryable: true,
                hint: "The client detected repeated failures and is temporarily pausing requests.");
        }

        var results = new List<RecordMetadata>();
        var tasks = new List<Task<DeliveryResult<byte[], byte[]>>>();

        foreach (var (key, value) in messages)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var keyBytes = key != null ? SerializeToBytes(key) : null;
            var valueBytes = SerializeToBytes(value);

            var message = new Message<byte[], byte[]>
            {
                Key = keyBytes!,
                Value = valueBytes!,
            };

            tasks.Add(_kafkaProducer.ProduceAsync(topic, message, cancellationToken));
        }

        _logger.LogDebug("Sending batch of {Count} messages to topic {Topic}", tasks.Count, topic);

        try
        {
            var deliveryResults = await Task.WhenAll(tasks);

            foreach (var result in deliveryResults)
            {
                results.Add(new RecordMetadata(
                    Topic: result.Topic,
                    Partition: result.Partition.Value,
                    Offset: result.Offset.Value,
                    Timestamp: result.Timestamp.UtcDateTime));
            }

            _circuitBreaker?.RecordSuccess();
            return results;
        }
        catch
        {
            _circuitBreaker?.RecordFailure();
            throw;
        }
    }

    /// <summary>Begin a new transaction.</summary>
    public void BeginTransaction()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_inTransaction) throw new InvalidOperationException("Transaction already in progress");
        _inTransaction = true;
        _transactionBuffer.Clear();
    }

    /// <summary>Buffer a message within the current transaction.</summary>
    public Task<RecordMetadata> SendTransactionalAsync(string topic, TKey? key, TValue value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_inTransaction) throw new InvalidOperationException("No transaction in progress");
        var tcs = new TaskCompletionSource<RecordMetadata>();
        _transactionBuffer.Add((topic, key, value, tcs));
        return tcs.Task;
    }

    /// <summary>Commit the transaction, sending all buffered records.</summary>
    public async Task<IReadOnlyList<RecordMetadata>> CommitTransactionAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_inTransaction) throw new InvalidOperationException("No transaction in progress");
        try
        {
            var results = new List<RecordMetadata>();
            foreach (var (topic, key, value, tcs) in _transactionBuffer)
            {
                var result = await SendAsync(topic, key, value, ct).ConfigureAwait(false);
                tcs.TrySetResult(result);
                results.Add(result);
            }
            return results;
        }
        finally
        {
            _inTransaction = false;
            _transactionBuffer.Clear();
        }
    }

    /// <summary>Abort the transaction, discarding all buffered records.</summary>
    public void AbortTransaction()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_inTransaction) throw new InvalidOperationException("No transaction in progress");
        foreach (var (_, _, _, tcs) in _transactionBuffer)
            tcs.TrySetCanceled();
        _inTransaction = false;
        _transactionBuffer.Clear();
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _kafkaProducer.Flush(cancellationToken);
        _logger.LogDebug("Producer flushed");
        return Task.CompletedTask;
    }

    private static Confluent.Kafka.CompressionType MapCompressionType(CompressionType ct) => ct switch
    {
        CompressionType.Gzip => Confluent.Kafka.CompressionType.Gzip,
        CompressionType.Lz4 => Confluent.Kafka.CompressionType.Lz4,
        CompressionType.Snappy => Confluent.Kafka.CompressionType.Snappy,
        CompressionType.Zstd => Confluent.Kafka.CompressionType.Zstd,
        _ => Confluent.Kafka.CompressionType.None,
    };

    private static Confluent.Kafka.SecurityProtocol MapSecurityProtocol(SecurityProtocol sp) => sp switch
    {
        SecurityProtocol.Ssl => Confluent.Kafka.SecurityProtocol.Ssl,
        SecurityProtocol.SaslPlaintext => Confluent.Kafka.SecurityProtocol.SaslPlaintext,
        SecurityProtocol.SaslSsl => Confluent.Kafka.SecurityProtocol.SaslSsl,
        _ => Confluent.Kafka.SecurityProtocol.Plaintext,
    };

    private static Confluent.Kafka.SaslMechanism MapSaslMechanism(SaslMechanism sm) => sm switch
    {
        SaslMechanism.ScramSha256 => Confluent.Kafka.SaslMechanism.ScramSha256,
        SaslMechanism.ScramSha512 => Confluent.Kafka.SaslMechanism.ScramSha512,
        _ => Confluent.Kafka.SaslMechanism.Plain,
    };

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
