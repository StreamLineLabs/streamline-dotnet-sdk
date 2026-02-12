using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;

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
/// Asynchronous consumer for Streamline.
/// </summary>
internal class Consumer<TKey, TValue> : IConsumer<TKey, TValue>
{
    private readonly StreamlineOptions _clientOptions;
    private readonly string _topic;
    private readonly ConsumerOptions _options;
    private readonly ILogger _logger;
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
    }

    public async Task SubscribeAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_subscribed)
        {
            _logger.LogInformation("Subscribing to topic {Topic}", _topic);
            // TODO: Implement subscription
            _subscribed = true;
            await Task.CompletedTask;
        }
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

    public async Task<IReadOnlyList<ConsumerRecord<TKey, TValue>>> PollAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_subscribed)
        {
            throw new InvalidOperationException("Consumer is not subscribed");
        }

        _logger.LogTrace("Polling for records with timeout {Timeout}", timeout);

        // TODO: Implement actual Kafka protocol fetch
        await Task.Delay(timeout, cancellationToken);
        return Array.Empty<ConsumerRecord<TKey, TValue>>();
    }

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _logger.LogDebug("Committing offsets");
        await Task.CompletedTask;
    }

    public async Task SeekToBeginningAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _logger.LogDebug("Seeking to beginning");
        await Task.CompletedTask;
    }

    public async Task SeekToEndAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _logger.LogDebug("Seeking to end");
        await Task.CompletedTask;
    }

    public async Task SeekAsync(int partition, long offset, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _logger.LogDebug("Seeking partition {Partition} to offset {Offset}", partition, offset);
        await Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
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
