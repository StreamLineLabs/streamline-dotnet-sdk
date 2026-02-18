using BenchmarkDotNet.Attributes;
using Streamline.Client;

namespace Streamline.Client.Benchmarks;

/// <summary>
/// Benchmarks for client construction, configuration, and record creation.
/// </summary>
[MemoryDiagnoser]
public class ClientBenchmarks
{
    private StreamlineOptions _options = null!;

    [GlobalSetup]
    public void Setup()
    {
        _options = new StreamlineOptions
        {
            BootstrapServers = "localhost:9092",
            ConnectionPoolSize = 4,
            ConnectTimeout = TimeSpan.FromSeconds(30),
            RequestTimeout = TimeSpan.FromSeconds(30),
        };
    }

    [Benchmark]
    public StreamlineClient CreateClient_WithConnectionString()
    {
        return new StreamlineClient("localhost:9092");
    }

    [Benchmark]
    public StreamlineClient CreateClient_WithOptions()
    {
        return new StreamlineClient(_options);
    }

    [Benchmark]
    public StreamlineOptions CreateOptions_Default()
    {
        return new StreamlineOptions();
    }

    [Benchmark]
    public StreamlineOptions CreateOptions_FullyConfigured()
    {
        return new StreamlineOptions
        {
            BootstrapServers = "broker1:9092,broker2:9092,broker3:9092",
            ConnectionPoolSize = 8,
            ConnectTimeout = TimeSpan.FromSeconds(10),
            RequestTimeout = TimeSpan.FromSeconds(15),
            Producer = new ProducerOptions
            {
                BatchSize = 32768,
                LingerMs = 5,
                MaxRequestSize = 2097152,
                CompressionType = CompressionType.Lz4,
                Retries = 5,
                RetryBackoffMs = 200,
                Idempotent = true,
            },
            Consumer = new ConsumerOptions
            {
                GroupId = "benchmark-group",
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = false,
                AutoCommitInterval = TimeSpan.FromSeconds(10),
                SessionTimeout = TimeSpan.FromSeconds(45),
                HeartbeatInterval = TimeSpan.FromSeconds(5),
                MaxPollRecords = 1000,
            },
        };
    }

    [Benchmark]
    public ProducerOptions CreateProducerOptions()
    {
        return new ProducerOptions
        {
            BatchSize = 32768,
            LingerMs = 5,
            CompressionType = CompressionType.Snappy,
            Retries = 5,
        };
    }

    [Benchmark]
    public ConsumerOptions CreateConsumerOptions()
    {
        return new ConsumerOptions
        {
            GroupId = "my-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            MaxPollRecords = 500,
        };
    }

    [Benchmark]
    public RecordMetadata CreateRecordMetadata()
    {
        return new RecordMetadata(
            Topic: "benchmark-topic",
            Partition: 0,
            Offset: 12345L,
            Timestamp: DateTimeOffset.UtcNow);
    }

    [Benchmark]
    public ConsumerRecord<string, string> CreateConsumerRecord()
    {
        return new ConsumerRecord<string, string>(
            Topic: "benchmark-topic",
            Partition: 0,
            Offset: 12345L,
            Timestamp: DateTimeOffset.UtcNow,
            Key: "benchmark-key",
            Value: "benchmark-value",
            Headers: new Headers());
    }

    [Benchmark]
    public StreamlineException CreateException_Base()
    {
        return new StreamlineException(
            "Test error",
            StreamlineErrorCode.Unknown,
            isRetryable: true,
            hint: "Try again");
    }

    [Benchmark]
    public StreamlineConnectionException CreateException_Connection()
    {
        return new StreamlineConnectionException("Connection refused");
    }

    [Benchmark]
    public StreamlineTopicNotFoundException CreateException_TopicNotFound()
    {
        return new StreamlineTopicNotFoundException("missing-topic");
    }
}
