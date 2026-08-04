using Confluent.Kafka;

namespace Streamline.Client;

/// <summary>
/// Translates <see cref="StreamlineOptions"/> timeouts into librdkafka settings,
/// clamping each value into the range librdkafka accepts.
///
/// <para>
/// Every producer and consumer created by this SDK gets an explicit, bounded
/// delivery and socket timeout. Without them librdkafka falls back to a
/// five-minute <c>message.timeout.ms</c>, which makes a call against an
/// unreachable broker look like a hang.
/// </para>
/// </summary>
internal static class KafkaTimeouts
{
    /// <summary>Lower bound librdkafka accepts for <c>socket.connection.setup.timeout.ms</c>.</summary>
    private const int MinConnectionSetupMs = 1_000;

    /// <summary>Lower bound applied to <c>message.timeout.ms</c>; 0 would mean "never expire".</summary>
    private const int MinMessageTimeoutMs = 1_000;

    /// <summary>librdkafka accepts <c>socket.timeout.ms</c> in [10, 300000].</summary>
    private const int MinSocketTimeoutMs = 10;

    /// <summary>librdkafka accepts <c>socket.timeout.ms</c> in [10, 300000].</summary>
    private const int MaxSocketTimeoutMs = 300_000;

    /// <summary>Maximum time disposal spends draining queued producer messages.</summary>
    private const int MaxShutdownTimeoutMs = 1_000;

    /// <summary>
    /// Applies bounded connection and delivery timeouts to a producer configuration.
    /// </summary>
    /// <param name="config">The configuration to mutate.</param>
    /// <param name="options">The client options supplying the timeout budget.</param>
    internal static void Apply(ProducerConfig config, StreamlineOptions options)
    {
        config.MessageTimeoutMs = MessageTimeoutMs(options);
        config.SocketTimeoutMs = SocketTimeoutMs(options);
        config.SocketConnectionSetupTimeoutMs = ConnectionSetupTimeoutMs(options);
    }

    /// <summary>
    /// Applies bounded connection and socket timeouts to a consumer configuration.
    /// </summary>
    /// <param name="config">The configuration to mutate.</param>
    /// <param name="options">The client options supplying the timeout budget.</param>
    internal static void Apply(ConsumerConfig config, StreamlineOptions options)
    {
        config.SocketTimeoutMs = SocketTimeoutMs(options);
        config.SocketConnectionSetupTimeoutMs = ConnectionSetupTimeoutMs(options);
    }

    /// <summary>
    /// The upper bound on how long a produce call may take, including retries.
    /// </summary>
    /// <param name="options">The client options supplying the timeout budget.</param>
    /// <returns>A bounded <c>message.timeout.ms</c> value.</returns>
    internal static int MessageTimeoutMs(StreamlineOptions options) =>
        Clamp(options.RequestTimeout, MinMessageTimeoutMs, int.MaxValue);

    /// <summary>The configured delivery budget used by an explicit flush.</summary>
    internal static TimeSpan FlushTimeout(StreamlineOptions options) =>
        TimeSpan.FromMilliseconds(MessageTimeoutMs(options));

    /// <summary>
    /// The bound applied when flushing or closing a client so shutdown cannot hang.
    /// </summary>
    /// <param name="options">The client options supplying the timeout budget.</param>
    /// <returns>A bounded shutdown budget.</returns>
    internal static TimeSpan ShutdownTimeout(StreamlineOptions options) =>
        TimeSpan.FromMilliseconds(Math.Min(MessageTimeoutMs(options), MaxShutdownTimeoutMs));

    private static int SocketTimeoutMs(StreamlineOptions options) =>
        Clamp(options.RequestTimeout, MinSocketTimeoutMs, MaxSocketTimeoutMs);

    private static int ConnectionSetupTimeoutMs(StreamlineOptions options) =>
        Clamp(options.ConnectTimeout, MinConnectionSetupMs, int.MaxValue);

    private static int Clamp(TimeSpan value, int minMs, int maxMs)
    {
        var ms = value.TotalMilliseconds;
        if (double.IsNaN(ms) || ms <= minMs)
            return minMs;

        return ms >= maxMs ? maxMs : (int)ms;
    }
}
