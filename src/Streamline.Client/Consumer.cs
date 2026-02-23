using System.Text;
using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Streamline.Client;

/// <summary>
/// Interface for consuming messages.
/// </summary>
public interface IConsumer<TKey, TValue> : IAsyncDisposable
{
    /// <summary>
    /// Subscribes to the topic.
    /// </summary>
    Task SubscribeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Consumes messages as an async enumerable.
    /// </summary>
    IAsyncEnumerable<ConsumerRecord<TKey, TValue>> ConsumeAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Polls for records with a timeout.
    /// </summary>
    Task<IReadOnlyList<ConsumerRecord<TKey, TValue>>> PollAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the current offsets.
    /// </summary>
    Task CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Seeks to the beginning.
    /// </summary>
    Task SeekToBeginningAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Seeks to the end.
    /// </summary>
    Task SeekToEndAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Seeks to a specific offset.
    /// </summary>
    Task SeekAsync(int partition, long offset, CancellationToken cancellationToken = default);
}

/// <summary>
/// Asynchronous consumer for Streamline, backed by Confluent.Kafka.
/// </summary>
internal class Consumer<TKey, TValue> : IConsumer<TKey, TValue>
{
    private readonly StreamlineOptions _clientOptions;
    private readonly string _topic;
    private readonly ConsumerOptions _options;
    private readonly ILogger _logger;
    private readonly IConsumer<byte[], byte[]> _kafkaConsumer;
    private bool _subscribed;
    private bool _disposed;

    public Consumer(
        StreamlineOptions clientOptions,
        string topic,
        ConsumerOptions options,
        ILogger logger)
    {
        _clientOptions = clientOptions;
        _topic = topic;
        _options = options;
        _logger = logger;

        var config = new Confluent.Kafka.ConsumerConfig
        {
            BootstrapServers = clientOptions.BootstrapServers,
            GroupId = options.GroupId ?? $"streamline-dotnet-{Guid.NewGuid():N}",
            AutoOffsetReset = options.AutoOffsetReset == Client.AutoOffsetReset.Earliest
                ? Confluent.Kafka.AutoOffsetReset.Earliest
                : Confluent.Kafka.AutoOffsetReset.Latest,
            EnableAutoCommit = options.EnableAutoCommit,
        };

        _kafkaConsumer = new ConsumerBuilder<byte[], byte[]>(config).Build();
    }

    public Task SubscribeAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_subscribed)
        {
            _logger.LogInformation("Subscribing to topic {Topic}", _topic);
            _kafkaConsumer.Subscribe(_topic);
            _subscribed = true;
        }

        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<ConsumerRecord<TKey, TValue>> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_subscribed)
        {
            throw new InvalidOperationException("Consumer is not subscribed");
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            var records = await PollAsync(TimeSpan.FromMilliseconds(100), cancellationToken);
            foreach (var record in records)
            {
                yield return record;
            }
        }
    }

    public Task<IReadOnlyList<ConsumerRecord<TKey, TValue>>> PollAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_subscribed)
        {
            throw new InvalidOperationException("Consumer is not subscribed");
        }

        var results = new List<ConsumerRecord<TKey, TValue>>();
        var deadline = DateTime.UtcNow.Add(timeout);

        while (DateTime.UtcNow < deadline && !cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = _kafkaConsumer.Consume(TimeSpan.FromMilliseconds(50));
                if (result?.Message == null) continue;

                var key = result.Message.Key != null
                    ? (TKey)(object)Encoding.UTF8.GetString(result.Message.Key)
                    : default;
                var value = result.Message.Value != null
                    ? (TValue)(object)Encoding.UTF8.GetString(result.Message.Value)
                    : default!;

                var headers = new Client.Headers();
                if (result.Message.Headers != null)
                {
                    foreach (var header in result.Message.Headers)
                    {
                        headers.Add(header.Key, header.GetValueBytes());
                    }
                }

                results.Add(new ConsumerRecord<TKey, TValue>(
                    result.Topic,
                    result.Partition.Value,
                    result.Offset.Value,
                    result.Message.Timestamp.UtcDateTime,
                    key,
                    value,
                    headers));
            }
            catch (ConsumeException ex)
            {
                _logger.LogWarning("Consume error: {Error}", ex.Error.Reason);
            }
        }

        return Task.FromResult<IReadOnlyList<ConsumerRecord<TKey, TValue>>>(results);
    }

    public Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _kafkaConsumer.Commit();
        _logger.LogDebug("Offsets committed");
        return Task.CompletedTask;
    }

    public Task SeekToBeginningAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var assignment = _kafkaConsumer.Assignment;
        var offsets = assignment.Select(tp => new TopicPartitionOffset(tp, Confluent.Kafka.Offset.Beginning)).ToList();
        foreach (var tpo in offsets)
        {
            _kafkaConsumer.Seek(tpo);
        }
        _logger.LogDebug("Seeking to beginning");
        return Task.CompletedTask;
    }

    public Task SeekToEndAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var assignment = _kafkaConsumer.Assignment;
        var offsets = assignment.Select(tp => new TopicPartitionOffset(tp, Confluent.Kafka.Offset.End)).ToList();
        foreach (var tpo in offsets)
        {
            _kafkaConsumer.Seek(tpo);
        }
        _logger.LogDebug("Seeking to end");
        return Task.CompletedTask;
    }

    public Task SeekAsync(int partition, long offset, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _kafkaConsumer.Seek(new TopicPartitionOffset(_topic, partition, offset));
        _logger.LogDebug("Seeking partition {Partition} to offset {Offset}", partition, offset);
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _kafkaConsumer.Close();
            _kafkaConsumer.Dispose();
            _disposed = true;
            _logger.LogInformation("Consumer disposed");
        }
        return ValueTask.CompletedTask;
    }
}

/// <summary>
/// A record received from a consumer poll.
/// </summary>
public record ConsumerRecord<TKey, TValue>(
    string Topic,
    int Partition,
    long Offset,
    DateTimeOffset Timestamp,
    TKey? Key,
    TValue Value,
    Headers Headers);
