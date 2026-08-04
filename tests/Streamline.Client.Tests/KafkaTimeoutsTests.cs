using Xunit;

namespace Streamline.Client.Tests;

public class KafkaTimeoutsTests
{
    [Fact]
    public void ShutdownTimeout_IsIndependentOfLongRequestTimeout()
    {
        var options = new StreamlineOptions
        {
            RequestTimeout = TimeSpan.FromSeconds(30),
        };

        Assert.Equal(TimeSpan.FromSeconds(1), KafkaTimeouts.ShutdownTimeout(options));
    }

    [Fact]
    public void MessageTimeout_StillUsesRequestTimeout()
    {
        var options = new StreamlineOptions
        {
            RequestTimeout = TimeSpan.FromSeconds(30),
        };

        Assert.Equal(30_000, KafkaTimeouts.MessageTimeoutMs(options));
    }

    [Fact]
    public void FlushTimeout_UsesRequestTimeout()
    {
        var options = new StreamlineOptions
        {
            RequestTimeout = TimeSpan.FromSeconds(30),
        };

        Assert.Equal(TimeSpan.FromSeconds(30), KafkaTimeouts.FlushTimeout(options));
    }
}
