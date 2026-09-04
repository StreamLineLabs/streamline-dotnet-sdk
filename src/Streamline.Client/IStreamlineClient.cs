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
    /// Creates a producer that supports client-buffered transactions.
    /// </summary>
    /// <typeparam name="TKey">Record key type.</typeparam>
    /// <typeparam name="TValue">Record value type.</typeparam>
    /// <returns>A transactional producer.</returns>
    ITransactionalProducer<TKey, TValue> CreateTransactionalProducer<TKey, TValue>()
    {
        throw new NotSupportedException(
            "This IStreamlineClient implementation does not provide client-buffered transactions.");
    }

    /// <summary>
    /// Creates a producer with custom configuration that supports client-buffered transactions.
    /// </summary>
    /// <param name="options">Producer configuration.</param>
    /// <typeparam name="TKey">Record key type.</typeparam>
    /// <typeparam name="TValue">Record value type.</typeparam>
    /// <returns>A transactional producer.</returns>
    ITransactionalProducer<TKey, TValue> CreateTransactionalProducer<TKey, TValue>(
        ProducerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        throw new NotSupportedException(
            "This IStreamlineClient implementation does not provide client-buffered transactions.");
    }

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

    /// <summary>
    /// Creates an admin client for topic, consumer group, and server management
    /// via the HTTP REST API, using the configured HTTP base URL.
    /// </summary>
    IAdminClient CreateAdmin();

    /// <summary>
    /// Creates an admin client with an explicit HTTP base URL.
    /// </summary>
    /// <param name="httpBaseUrl">Base URL of the HTTP API.</param>
    IAdminClient CreateAdmin(string httpBaseUrl);

    /// <summary>
    /// Creates an admin client with an explicit HTTP base URL and bearer token.
    /// </summary>
    /// <remarks>
    /// Implementations that do not support explicit credentials fail closed instead
    /// of forwarding a token to an unknown destination.
    /// </remarks>
    /// <param name="httpBaseUrl">Base URL of the HTTP API.</param>
    /// <param name="authToken">Bearer token to send to that host, or null for none.</param>
    IAdminClient CreateAdmin(string httpBaseUrl, string? authToken)
    {
        if (authToken is not null)
        {
            throw new NotSupportedException(
                "This IStreamlineClient implementation does not support explicit admin credentials.");
        }

        return CreateAdmin(httpBaseUrl);
    }
}
