using System.Globalization;
using System.Runtime.CompilerServices;

namespace Streamline.Client.Telemetry;

/// <summary>
/// A consumer wrapper that adds OpenTelemetry tracing to poll and consume operations.
///
/// <para>
/// <see cref="PollAsync"/> is wrapped with a consume activity.
/// <see cref="ConsumeAsync"/> traces each yielded record with a process activity
/// linked to the producer via header context extraction.
/// All other methods (subscribe, commit, seek) are pure delegation.
/// </para>
///
/// <example>
/// <code>
/// await using var consumer = client.CreateConsumer&lt;string, string&gt;("orders");
/// await using var traced = new TracedConsumer&lt;string, string&gt;(consumer, "orders");
///
/// await traced.SubscribeAsync();
/// var records = await traced.PollAsync(TimeSpan.FromSeconds(1));
/// </code>
/// </example>
/// </summary>
/// <typeparam name="TKey">The message key type.</typeparam>
/// <typeparam name="TValue">The message value type.</typeparam>
public class TracedConsumer<TKey, TValue> : IConsumer<TKey, TValue>
{
    private readonly IConsumer<TKey, TValue> _consumer;
    private readonly string _topic;

    /// <summary>
    /// Wraps an existing consumer with tracing.
    /// </summary>
    /// <param name="consumer">The underlying consumer to wrap.</param>
    /// <param name="topic">The topic name, used for trace span naming.</param>
    public TracedConsumer(IConsumer<TKey, TValue> consumer, string topic)
    {
        _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        _topic = topic ?? throw new ArgumentNullException(nameof(topic));
    }

    /// <inheritdoc />
    public Task SubscribeAsync(CancellationToken cancellationToken = default)
        => _consumer.SubscribeAsync(cancellationToken);

    /// <inheritdoc />
    /// <remarks>
    /// Each yielded record is wrapped in a process activity that extracts parent
    /// trace context from the record's headers, enabling end-to-end distributed
    /// tracing from producer to consumer.
    /// </remarks>
    public async IAsyncEnumerable<ConsumerRecord<TKey, TValue>> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var record in _consumer.ConsumeAsync(cancellationToken).ConfigureAwait(false))
        {
            var parentContext = StreamlineActivitySource.ExtractContext(record.Headers);
            using var activity = StreamlineActivitySource.StartProcess(
                record.Topic, record.Partition, record.Offset, parentContext);
            yield return record;
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ConsumerRecord<TKey, TValue>>> PollAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        using var activity = StreamlineActivitySource.StartConsume(_topic);
        try
        {
            var records = await _consumer.PollAsync(timeout, cancellationToken)
                .ConfigureAwait(false);
            activity?.SetTag("messaging.batch.message_count",
                records.Count.ToString(CultureInfo.InvariantCulture));
            return records;
        }
        catch (Exception ex)
        {
            StreamlineActivitySource.RecordError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public Task CommitAsync(CancellationToken cancellationToken = default)
        => _consumer.CommitAsync(cancellationToken);

    /// <inheritdoc />
    public Task SeekToBeginningAsync(CancellationToken cancellationToken = default)
        => _consumer.SeekToBeginningAsync(cancellationToken);

    /// <inheritdoc />
    public Task SeekToEndAsync(CancellationToken cancellationToken = default)
        => _consumer.SeekToEndAsync(cancellationToken);

    /// <inheritdoc />
    public Task SeekAsync(int partition, long offset, CancellationToken cancellationToken = default)
        => _consumer.SeekAsync(partition, offset, cancellationToken);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _consumer.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
}
