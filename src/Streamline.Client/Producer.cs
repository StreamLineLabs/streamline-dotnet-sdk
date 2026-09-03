using System.Text;
using System.Runtime.ExceptionServices;
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
/// Producer capability for client-buffered transactions.
/// </summary>
/// <remarks>
/// These transactions are not broker transactions. A commit sends buffered records
/// in order, but a send failure can occur after earlier records were delivered.
/// Aborting only discards records that have not started sending.
/// </remarks>
public interface ITransactionalProducer<TKey, TValue> : IProducer<TKey, TValue>
{
    /// <summary>
    /// Begins a client-buffered transaction.
    /// </summary>
    void BeginTransaction();

    /// <summary>
    /// Buffers a record for the active transaction.
    /// </summary>
    /// <remarks>
    /// The returned task completes when the transaction commits. Do not await it
    /// before calling <see cref="CommitTransactionAsync"/>.
    /// </remarks>
    /// <param name="topic">The topic name.</param>
    /// <param name="key">The record key.</param>
    /// <param name="value">The record value.</param>
    /// <returns>A task that resolves to delivery metadata after commit.</returns>
    Task<RecordMetadata> SendTransactionalAsync(string topic, TKey? key, TValue value);

    /// <summary>
    /// Buffers a record with headers for the active transaction.
    /// </summary>
    /// <remarks>
    /// The returned task completes when the transaction commits. Do not await it
    /// before calling <see cref="CommitTransactionAsync"/>.
    /// </remarks>
    /// <param name="topic">The topic name.</param>
    /// <param name="key">The record key.</param>
    /// <param name="value">The record value.</param>
    /// <param name="headers">Optional record headers.</param>
    /// <returns>A task that resolves to delivery metadata after commit.</returns>
    Task<RecordMetadata> SendTransactionalAsync(
        string topic,
        TKey? key,
        TValue value,
        Headers? headers);

    /// <summary>
    /// Sends the buffered records in order.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Delivery metadata for records sent before the commit completed.</returns>
    Task<IReadOnlyList<RecordMetadata>> CommitTransactionAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Aborts the active transaction and cancels its pending delivery tasks.
    /// </summary>
    void AbortTransaction();
}

/// <summary>
/// Asynchronous producer for Streamline, backed by Confluent.Kafka for wire protocol compatibility.
/// </summary>
internal class Producer<TKey, TValue> : ITransactionalProducer<TKey, TValue>
{
    private readonly StreamlineOptions _clientOptions;
    private readonly ProducerOptions _options;
    private readonly ILogger _logger;
    private readonly Lazy<Confluent.Kafka.IProducer<byte[], byte[]>> _kafkaProducerFactory;
    private readonly CircuitBreaker? _circuitBreaker;
    private readonly object _handleLock = new();
    private bool _disposed;
    private Task? _disposeTask;
    private readonly object _transactionLock = new();
    private bool _inTransaction;
    private bool _transactionCommitInProgress;
    private readonly List<(
        string Topic,
        TKey? Key,
        TValue Value,
        Headers? Headers,
        TaskCompletionSource<RecordMetadata> Completion)> _transactionBuffer = new();

    /// <summary>
    /// The underlying librdkafka producer, created on first use so that constructing a
    /// producer never opens a connection.
    /// </summary>
    /// <remarks>
    /// Creation is serialised with disposal: without the lock a caller that passed the
    /// <c>_disposed</c> check could build a fresh native handle after
    /// <see cref="DisposeAsync"/> had already decided there was nothing to clean up,
    /// leaking the handle and its broker threads.
    /// </remarks>
    private Confluent.Kafka.IProducer<byte[], byte[]> KafkaProducer
    {
        get
        {
            lock (_handleLock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                return _kafkaProducerFactory.Value;
            }
        }
    }

    /// <summary>
    /// Returns the librdkafka producer only if it has already been created, without
    /// forcing creation.
    /// </summary>
    private bool TryGetExistingHandle(out Confluent.Kafka.IProducer<byte[], byte[]> producer)
    {
        lock (_handleLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_kafkaProducerFactory.IsValueCreated)
            {
                producer = _kafkaProducerFactory.Value;
                return true;
            }
        }

        producer = null!;
        return false;
    }

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

        KafkaTimeouts.Apply(config, clientOptions);

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

        _kafkaProducerFactory = new Lazy<Confluent.Kafka.IProducer<byte[], byte[]>>(
            () => new ProducerBuilder<byte[], byte[]>(config).Build(),
            LazyThreadSafetyMode.ExecutionAndPublication);
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
        TopicNameValidator.Validate(topic);
        cancellationToken.ThrowIfCancellationRequested();

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
            var result = await KafkaProducer.ProduceAsync(topic, message, cancellationToken);
            _circuitBreaker?.RecordSuccess();

            return new RecordMetadata(
                Topic: result.Topic,
                Partition: result.Partition.Value,
                Offset: result.Offset.Value,
                Timestamp: result.Timestamp.UtcDateTime);
        }
        catch (KafkaException ex) when (IsAuthenticationError(ex.Error.Code))
        {
            _circuitBreaker?.RecordFailure();
            throw CreateAuthenticationException(ex);
        }
        catch (KafkaException ex) when (IsAuthorizationError(ex.Error.Code))
        {
            _circuitBreaker?.RecordFailure();
            throw CreateAuthorizationException(ex);
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
        TopicNameValidator.Validate(topic);
        cancellationToken.ThrowIfCancellationRequested();

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

            tasks.Add(KafkaProducer.ProduceAsync(topic, message, cancellationToken));
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
        lock (_transactionLock)
        {
            if (_inTransaction || _transactionCommitInProgress)
                throw new InvalidOperationException("Transaction already in progress");

            _inTransaction = true;
            _transactionBuffer.Clear();
        }
    }

    /// <summary>Buffer a message within the current transaction.</summary>
    public Task<RecordMetadata> SendTransactionalAsync(string topic, TKey? key, TValue value)
    {
        return SendTransactionalAsync(topic, key, value, headers: null);
    }

    /// <summary>Buffer a message with headers within the current transaction.</summary>
    public Task<RecordMetadata> SendTransactionalAsync(
        string topic,
        TKey? key,
        TValue value,
        Headers? headers)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        TopicNameValidator.Validate(topic);

        lock (_transactionLock)
        {
            if (!_inTransaction)
                throw new InvalidOperationException("No transaction in progress");

            var completion = new TaskCompletionSource<RecordMetadata>(
                TaskCreationOptions.RunContinuationsAsynchronously);
            _transactionBuffer.Add((topic, key, value, headers, completion));
            return completion.Task;
        }
    }

    /// <summary>Commit the transaction, sending all buffered records.</summary>
    public async Task<IReadOnlyList<RecordMetadata>> CommitTransactionAsync(
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        List<(
            string Topic,
            TKey? Key,
            TValue Value,
            Headers? Headers,
            TaskCompletionSource<RecordMetadata> Completion)> pending;

        lock (_transactionLock)
        {
            if (!_inTransaction)
                throw new InvalidOperationException("No transaction in progress");

            _inTransaction = false;
            _transactionCommitInProgress = true;
            pending = [.. _transactionBuffer];
            _transactionBuffer.Clear();
        }

        var results = new List<RecordMetadata>();
        Exception? failure = null;
        try
        {
            for (var index = 0; index < pending.Count; index++)
            {
                var item = pending[index];
                var result = await SendAsync(
                    item.Topic,
                    item.Key,
                    item.Value,
                    item.Headers,
                    cancellationToken).ConfigureAwait(false);
                results.Add(result);
            }
        }
        catch (Exception ex)
        {
            failure = ex;
        }
        finally
        {
            lock (_transactionLock)
            {
                _transactionCommitInProgress = false;
            }

            for (var index = 0; index < results.Count; index++)
                pending[index].Completion.TrySetResult(results[index]);

            if (failure is OperationCanceledException && cancellationToken.IsCancellationRequested)
            {
                CancelPending(pending, results.Count, cancellationToken);
            }
            else if (failure is not null)
            {
                FailPending(pending, results.Count, failure);
            }
        }

        if (failure is not null)
            ExceptionDispatchInfo.Capture(failure).Throw();

        return results;
    }

    /// <summary>Abort the transaction, discarding all buffered records.</summary>
    public void AbortTransaction()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        List<TaskCompletionSource<RecordMetadata>> pending;

        lock (_transactionLock)
        {
            if (!_inTransaction)
                throw new InvalidOperationException("No transaction in progress");

            pending = _transactionBuffer.Select(item => item.Completion).ToList();
            _inTransaction = false;
            _transactionBuffer.Clear();
        }

        foreach (var completion in pending)
            completion.TrySetCanceled();
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();

        // Nothing was ever produced, so there is no handle and nothing to flush.
        if (!TryGetExistingHandle(out var producer))
            return Task.CompletedTask;

        // Flush in slices instead of one unbounded call: Flush(CancellationToken) blocks
        // until every outstanding message is delivered, which never happens without a
        // broker. Slicing keeps the total bounded while still honouring cancellation.
        var deadline = DateTime.UtcNow.Add(KafkaTimeouts.FlushTimeout(_clientOptions));
        var slice = TimeSpan.FromMilliseconds(100);
        int remaining;

        do
        {
            cancellationToken.ThrowIfCancellationRequested();
            remaining = producer.Flush(slice);
        }
        while (remaining > 0 && DateTime.UtcNow < deadline);

        if (remaining > 0)
        {
            // FlushAsync is a durability barrier: reporting success with undelivered
            // messages would silently lose data.
            throw new StreamlineTimeoutException(
                $"Producer flush timed out with {remaining} message(s) still in flight");
        }

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
        lock (_handleLock)
        {
            if (_disposeTask is not null)
                return new ValueTask(_disposeTask);

            _disposed = true;
            CancelBufferedTransaction();

            // Read the handle under the same lock that guards creation, so a concurrent
            // first use cannot build a handle that this dispose would then miss.
            if (!_kafkaProducerFactory.IsValueCreated)
            {
                _logger.LogDebug("Producer disposed");
                _disposeTask = Task.CompletedTask;
                return new ValueTask(_disposeTask);
            }

            _disposeTask = DisposeProducerAsync(_kafkaProducerFactory.Value);
            return new ValueTask(_disposeTask);
        }
    }

    private void CancelBufferedTransaction()
    {
        List<TaskCompletionSource<RecordMetadata>> pending;
        lock (_transactionLock)
        {
            pending = _transactionBuffer.Select(item => item.Completion).ToList();
            _inTransaction = false;
            _transactionBuffer.Clear();
        }

        foreach (var completion in pending)
            completion.TrySetCanceled();
    }

    private static void CancelPending(
        List<(
            string Topic,
            TKey? Key,
            TValue Value,
            Headers? Headers,
            TaskCompletionSource<RecordMetadata> Completion)> pending,
        int startIndex,
        CancellationToken cancellationToken)
    {
        for (var index = startIndex; index < pending.Count; index++)
            pending[index].Completion.TrySetCanceled(cancellationToken);
    }

    private static void FailPending(
        List<(
            string Topic,
            TKey? Key,
            TValue Value,
            Headers? Headers,
            TaskCompletionSource<RecordMetadata> Completion)> pending,
        int startIndex,
        Exception exception)
    {
        for (var index = startIndex; index < pending.Count; index++)
            pending[index].Completion.TrySetException(exception);
    }

    private static bool IsAuthenticationError(ErrorCode errorCode)
    {
        return errorCode is ErrorCode.SaslAuthenticationFailed or ErrorCode.Local_Authentication;
    }

    private static bool IsAuthorizationError(ErrorCode errorCode)
    {
        return errorCode is ErrorCode.TopicAuthorizationFailed or ErrorCode.ClusterAuthorizationFailed;
    }

    private static StreamlineAuthenticationException CreateAuthenticationException(KafkaException exception)
    {
        return new StreamlineAuthenticationException(
            $"Authentication failed: {exception.Error.Reason}",
            exception);
    }

    private static StreamlineAuthorizationException CreateAuthorizationException(KafkaException exception)
    {
        return new StreamlineAuthorizationException(
            $"Producer authorization failed: {exception.Error.Reason}",
            exception);
    }

    private async Task DisposeProducerAsync(
        Confluent.Kafka.IProducer<byte[], byte[]> producer)
    {
        try
        {
            await Task.Run(() =>
                producer.Flush(KafkaTimeouts.ShutdownTimeout(_clientOptions)))
                .ConfigureAwait(false);
        }
        catch (KafkaException ex)
        {
            _logger.LogDebug(ex, "Producer flush failed during dispose");
        }
        finally
        {
            producer.Dispose();
            _logger.LogDebug("Producer disposed");
        }
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
