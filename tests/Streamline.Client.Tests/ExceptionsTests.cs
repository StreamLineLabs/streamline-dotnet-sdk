using Streamline.Client;
using Xunit;

namespace Streamline.Client.Tests;

public class ExceptionsTests
{
    [Fact]
    public void StreamlineException_SetsProperties()
    {
        var ex = new StreamlineException("test error", StreamlineErrorCode.Connection, true, "try again");

        Assert.Equal("test error", ex.Message);
        Assert.Equal(StreamlineErrorCode.Connection, ex.ErrorCode);
        Assert.True(ex.IsRetryable);
        Assert.Equal("try again", ex.Hint);
        Assert.Null(ex.InnerException);
    }

    [Fact]
    public void StreamlineException_WrapsInnerException()
    {
        var inner = new InvalidOperationException("underlying cause");
        var ex = new StreamlineException("wrapper", innerException: inner);

        Assert.Equal(inner, ex.InnerException);
        Assert.Equal(StreamlineErrorCode.Unknown, ex.ErrorCode);
    }

    [Fact]
    public void ConnectionException_IsRetryable()
    {
        var ex = new StreamlineConnectionException("cannot connect");

        Assert.Equal(StreamlineErrorCode.Connection, ex.ErrorCode);
        Assert.True(ex.IsRetryable);
        Assert.NotNull(ex.Hint);
    }

    [Fact]
    public void AuthenticationException_IsNotRetryable()
    {
        var ex = new StreamlineAuthenticationException("bad credentials");

        Assert.Equal(StreamlineErrorCode.Authentication, ex.ErrorCode);
        Assert.False(ex.IsRetryable);
    }

    [Fact]
    public void AuthorizationException_IsNotRetryable()
    {
        var ex = new StreamlineAuthorizationException("access denied");

        Assert.Equal(StreamlineErrorCode.Authorization, ex.ErrorCode);
        Assert.False(ex.IsRetryable);
    }

    [Fact]
    public void TopicNotFoundException_ContainsTopic()
    {
        var ex = new StreamlineTopicNotFoundException("events");

        Assert.Equal("events", ex.Topic);
        Assert.Equal(StreamlineErrorCode.TopicNotFound, ex.ErrorCode);
        Assert.Contains("events", ex.Message);
        Assert.Contains("events", ex.Hint);
        Assert.False(ex.IsRetryable);
    }

    [Fact]
    public void TimeoutException_IsRetryable()
    {
        var ex = new StreamlineTimeoutException("fetch timed out");

        Assert.Equal(StreamlineErrorCode.Timeout, ex.ErrorCode);
        Assert.True(ex.IsRetryable);
    }

    [Fact]
    public void ProducerException_IsRetryable()
    {
        var ex = new StreamlineProducerException("send failed");

        Assert.Equal(StreamlineErrorCode.Producer, ex.ErrorCode);
        Assert.True(ex.IsRetryable);
    }

    [Fact]
    public void ConsumerException_IsRetryable()
    {
        var ex = new StreamlineConsumerException("poll failed");

        Assert.Equal(StreamlineErrorCode.Consumer, ex.ErrorCode);
        Assert.True(ex.IsRetryable);
    }

    [Fact]
    public void SerializationException_IsNotRetryable()
    {
        var ex = new StreamlineSerializationException("invalid JSON");

        Assert.Equal(StreamlineErrorCode.Serialization, ex.ErrorCode);
        Assert.False(ex.IsRetryable);
    }

    [Fact]
    public void ConfigurationException_IsNotRetryable()
    {
        var ex = new StreamlineConfigurationException("missing bootstrap servers");

        Assert.Equal(StreamlineErrorCode.Configuration, ex.ErrorCode);
        Assert.False(ex.IsRetryable);
    }

    [Fact]
    public void ExceptionHierarchy_InheritsFromStreamlineException()
    {
        Assert.IsAssignableFrom<StreamlineException>(new StreamlineConnectionException(""));
        Assert.IsAssignableFrom<StreamlineException>(new StreamlineAuthenticationException(""));
        Assert.IsAssignableFrom<StreamlineException>(new StreamlineAuthorizationException(""));
        Assert.IsAssignableFrom<StreamlineException>(new StreamlineTopicNotFoundException("t"));
        Assert.IsAssignableFrom<StreamlineException>(new StreamlineTimeoutException(""));
        Assert.IsAssignableFrom<StreamlineException>(new StreamlineProducerException(""));
        Assert.IsAssignableFrom<StreamlineException>(new StreamlineConsumerException(""));
        Assert.IsAssignableFrom<StreamlineException>(new StreamlineSerializationException(""));
        Assert.IsAssignableFrom<StreamlineException>(new StreamlineConfigurationException(""));
    }
}
