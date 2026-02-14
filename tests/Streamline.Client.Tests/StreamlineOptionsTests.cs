using Streamline.Client;
using Xunit;

namespace Streamline.Client.Tests;

public class StreamlineOptionsTests
{
    [Fact]
    public void DefaultOptions_HasSensibleDefaults()
    {
        var options = new StreamlineOptions();

        Assert.Equal("localhost:9092", options.BootstrapServers);
        Assert.Equal(4, options.ConnectionPoolSize);
        Assert.Equal(TimeSpan.FromSeconds(30), options.ConnectTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), options.RequestTimeout);
    }

    [Fact]
    public void ProducerOptions_HasSensibleDefaults()
    {
        var options = new ProducerOptions();

        Assert.Equal(16384, options.BatchSize);
        Assert.Equal(1, options.LingerMs);
        Assert.Equal(1048576, options.MaxRequestSize);
        Assert.Equal(CompressionType.None, options.CompressionType);
        Assert.Equal(3, options.Retries);
        Assert.Equal(100, options.RetryBackoffMs);
        Assert.False(options.Idempotent);
    }

    [Fact]
    public void ConsumerOptions_HasSensibleDefaults()
    {
        var options = new ConsumerOptions();

        Assert.Null(options.GroupId);
        Assert.Equal(AutoOffsetReset.Earliest, options.AutoOffsetReset);
        Assert.True(options.EnableAutoCommit);
        Assert.Equal(TimeSpan.FromSeconds(5), options.AutoCommitInterval);
        Assert.Equal(TimeSpan.FromSeconds(30), options.SessionTimeout);
        Assert.Equal(TimeSpan.FromSeconds(3), options.HeartbeatInterval);
        Assert.Equal(500, options.MaxPollRecords);
    }

    [Fact]
    public void Options_CanBeCustomized()
    {
        var options = new StreamlineOptions
        {
            BootstrapServers = "broker1:9092,broker2:9092",
            ConnectionPoolSize = 8,
            ConnectTimeout = TimeSpan.FromSeconds(60),
            RequestTimeout = TimeSpan.FromSeconds(15),
            Producer = new ProducerOptions
            {
                BatchSize = 32768,
                CompressionType = CompressionType.Zstd,
                Retries = 5,
            },
            Consumer = new ConsumerOptions
            {
                GroupId = "my-group",
                AutoOffsetReset = AutoOffsetReset.Latest,
                MaxPollRecords = 1000,
            },
        };

        Assert.Equal("broker1:9092,broker2:9092", options.BootstrapServers);
        Assert.Equal(8, options.ConnectionPoolSize);
        Assert.Equal(32768, options.Producer.BatchSize);
        Assert.Equal(CompressionType.Zstd, options.Producer.CompressionType);
        Assert.Equal("my-group", options.Consumer.GroupId);
        Assert.Equal(AutoOffsetReset.Latest, options.Consumer.AutoOffsetReset);
    }
}
