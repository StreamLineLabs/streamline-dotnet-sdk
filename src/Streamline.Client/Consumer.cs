using System.Net.Http.Json;
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

    /// <summary>
    /// Searches a topic using semantic search via the HTTP API.
    /// </summary>
    /// <param name="topic">Topic to search.</param>
    /// <param name="query">Free-text search query.</param>
    /// <param name="k">Maximum number of results (default 10).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A list of search results ordered by descending score.</returns>
    Task<IReadOnlyList<SearchResult>> SearchAsync(
        string topic,
        string query,
        int k = 10,
        CancellationToken cancellationToken = default);
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
    private readonly Confluent.Kafka.IConsumer<byte[], byte[]> _kafkaConsumer;
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
            AutoCommitIntervalMs = (int)options.AutoCommitInterval.TotalMilliseconds,
            SessionTimeoutMs = (int)options.SessionTimeout.TotalMilliseconds,
            HeartbeatIntervalMs = (int)options.HeartbeatInterval.TotalMilliseconds,
            MaxPollIntervalMs = options.MaxPollRecords > 0 ? 300000 : 300000,
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

        _kafkaConsumer = new ConsumerBuilder<byte[], byte[]>(config)
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                if (_options.OnPartitionsAssigned is { } handler)
                {
                    var infos = partitions
                        .Select(tp => new TopicPartitionInfo(tp.Topic, tp.Partition.Value))
                        .ToList();
                    handler(infos);
                }
                _logger.LogInformation("Partitions assigned: {Partitions}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}-{p.Partition.Value}")));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                if (_options.OnPartitionsRevoked is { } handler)
                {
                    var infos = partitions
                        .Select(tpo => new TopicPartitionOffsetInfo(
                            tpo.Topic, tpo.Partition.Value, tpo.Offset.Value))
                        .ToList();
                    handler(infos);
                }
                _logger.LogInformation("Partitions revoked: {Partitions}",
                    string.Join(", ", partitions.Select(p => $"{p.Topic}-{p.Partition.Value}")));
            })
            .Build();
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

    /// <summary>
    /// Validates that the given offset is within valid range.
    /// </summary>
    /// <inheritdoc />
    public async Task<IReadOnlyList<SearchResult>> SearchAsync(
        string topic,
        string query,
        int k = 10,
        CancellationToken cancellationToken = default)
    {
        var host = _clientOptions.BootstrapServers.Split(',')[0].Split(':')[0];
        var baseUrl = $"http://{host}:9094";

        using var httpClient = new HttpClient { BaseAddress = new Uri(baseUrl) };
        var request = new { query, k };
        var response = await httpClient.PostAsJsonAsync(
            $"/api/v1/topics/{Uri.EscapeDataString(topic)}/search",
            request,
            cancellationToken);

        response.EnsureSuccessStatusCode();

        var data = await response.Content.ReadFromJsonAsync<SearchApiResponse>(
            cancellationToken: cancellationToken)
            ?? throw new InvalidOperationException("Empty search response");

        return data.Hits;
    }

    private static void ValidateOffset(long offset)
    {
        if (offset < -2)
        {
            throw new ArgumentOutOfRangeException(
                nameof(offset),
                offset,
                "Offset must be >= -2 (earliest=-2, latest=-1, or a specific offset >= 0)");
        }
    }
}

/// <summary>
/// A single search result from a topic.
/// </summary>
/// <param name="Partition">Partition of the matching record.</param>
/// <param name="Offset">Offset of the matching record.</param>
/// <param name="Score">Similarity score (higher = more relevant).</param>
/// <param name="Value">Record value, if returned by the server.</param>
public record SearchResult(
    [property: System.Text.Json.Serialization.JsonPropertyName("partition")] int Partition,
    [property: System.Text.Json.Serialization.JsonPropertyName("offset")] long Offset,
    [property: System.Text.Json.Serialization.JsonPropertyName("score")] double Score,
    [property: System.Text.Json.Serialization.JsonPropertyName("value")] System.Text.Json.JsonElement? Value = null);

/// <summary>
/// Internal response from the search API.
/// </summary>
internal record SearchApiResponse(
    [property: System.Text.Json.Serialization.JsonPropertyName("hits")] SearchResult[] Hits,
    [property: System.Text.Json.Serialization.JsonPropertyName("took_ms")] long TookMs);

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
