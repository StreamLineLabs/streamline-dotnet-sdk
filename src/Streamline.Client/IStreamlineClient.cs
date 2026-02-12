namespace Streamline.Client;

/// <summary>
/// Interface for the Streamline client.
/// </summary>
public interface IStreamlineClient : IAsyncDisposable
{
    /// <summary>
    /// Produces a message to the specified topic.
    /// </summary>
    Task<RecordMetadata> ProduceAsync(
        string topic,
        string? key,
        string value,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Produces a message to the specified topic with headers.
    /// </summary>
    Task<RecordMetadata> ProduceAsync(
        string topic,
        string? key,
        string value,
        Headers? headers,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a producer with default configuration.
    /// </summary>
    IProducer<TKey, TValue> CreateProducer<TKey, TValue>();

    /// <summary>
    /// Creates a producer with custom configuration.
    /// </summary>
    IProducer<TKey, TValue> CreateProducer<TKey, TValue>(ProducerOptions options);

    /// <summary>
    /// Creates a consumer for the specified topic and group.
    /// </summary>
    IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(string topic, string groupId);

    /// <summary>
    /// Creates a consumer with custom configuration.
    /// </summary>
    IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(string topic, ConsumerOptions options);

    /// <summary>
    /// Checks if the client is connected and healthy.
    /// </summary>
    Task<bool> IsHealthyAsync(CancellationToken cancellationToken = default);
}
