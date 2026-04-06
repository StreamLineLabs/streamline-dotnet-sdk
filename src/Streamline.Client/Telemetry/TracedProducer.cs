using System.Globalization;

namespace Streamline.Client.Telemetry;

/// <summary>
/// A producer wrapper that adds OpenTelemetry tracing to send operations.
///
/// <para>
/// Delegates all methods to the underlying <see cref="IProducer{TKey, TValue}"/>.
/// Only <see cref="SendAsync(string, TKey?, TValue, CancellationToken)"/>,
/// <see cref="SendAsync(string, TKey?, TValue, Headers?, CancellationToken)"/>,
/// and <see cref="SendBatchAsync"/> are traced with produce activities.
/// <see cref="FlushAsync"/> and <see cref="DisposeAsync"/> are pure delegation.
/// </para>
///
/// <example>
/// <code>
/// await using var producer = client.CreateProducer&lt;string, string&gt;();
/// await using var traced = new TracedProducer&lt;string, string&gt;(producer);
///
/// var metadata = await traced.SendAsync("orders", "key-1", "order-data");
/// </code>
/// </example>
/// </summary>
/// <typeparam name="TKey">The message key type.</typeparam>
/// <typeparam name="TValue">The message value type.</typeparam>
public class TracedProducer<TKey, TValue> : IProducer<TKey, TValue>
{
    private readonly IProducer<TKey, TValue> _producer;

    /// <summary>
    /// Wraps an existing producer with tracing.
    /// </summary>
    /// <param name="producer">The underlying producer to wrap.</param>
    public TracedProducer(IProducer<TKey, TValue> producer)
    {
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
    }

    /// <inheritdoc />
    public async Task<RecordMetadata> SendAsync(
        string topic,
        TKey? key,
        TValue value,
        CancellationToken cancellationToken = default)
    {
        return await SendAsync(topic, key, value, null, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<RecordMetadata> SendAsync(
        string topic,
        TKey? key,
        TValue value,
        Headers? headers,
        CancellationToken cancellationToken = default)
    {
        using var activity = StreamlineActivitySource.StartProduce(topic);
        try
        {
            var result = await _producer.SendAsync(topic, key, value, headers, cancellationToken)
                .ConfigureAwait(false);
            activity?.SetTag("messaging.destination.partition.id",
                result.Partition.ToString(CultureInfo.InvariantCulture));
            activity?.SetTag("messaging.message.id",
                result.Offset.ToString(CultureInfo.InvariantCulture));
            return result;
        }
        catch (Exception ex)
        {
            StreamlineActivitySource.RecordError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<RecordMetadata>> SendBatchAsync(
        string topic,
        IEnumerable<(TKey? Key, TValue Value)> messages,
        CancellationToken cancellationToken = default)
    {
        using var activity = StreamlineActivitySource.StartProduce(topic);
        try
        {
            var results = await _producer.SendBatchAsync(topic, messages, cancellationToken)
                .ConfigureAwait(false);
            activity?.SetTag("messaging.batch.message_count",
                results.Count.ToString(CultureInfo.InvariantCulture));
            return results;
        }
        catch (Exception ex)
        {
            StreamlineActivitySource.RecordError(activity, ex);
            throw;
        }
    }

    /// <inheritdoc />
    public Task FlushAsync(CancellationToken cancellationToken = default)
        => _producer.FlushAsync(cancellationToken);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _producer.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
}
