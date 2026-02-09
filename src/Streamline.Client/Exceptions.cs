namespace Streamline.Client;

/// <summary>
/// Error codes for categorizing Streamline errors.
/// </summary>
public enum StreamlineErrorCode
{
    /// <summary>Unknown error.</summary>
    Unknown,
    /// <summary>Connection failure.</summary>
    Connection,
    /// <summary>Authentication failure.</summary>
    Authentication,
    /// <summary>Authorization/ACL failure.</summary>
    Authorization,
    /// <summary>Topic not found.</summary>
    TopicNotFound,
    /// <summary>Operation timed out.</summary>
    Timeout,
    /// <summary>Producer operation error.</summary>
    Producer,
    /// <summary>Consumer operation error.</summary>
    Consumer,
    /// <summary>Serialization/deserialization error.</summary>
    Serialization,
    /// <summary>Invalid configuration.</summary>
    Configuration,
}

/// <summary>
/// Base exception for all Streamline SDK errors.
/// </summary>
public class StreamlineException : Exception
{
    /// <summary>
    /// The error code indicating the type of failure.
    /// </summary>
    public StreamlineErrorCode ErrorCode { get; }

    /// <summary>
    /// Whether the operation that caused this error can be retried.
    /// </summary>
    public bool IsRetryable { get; }

    /// <summary>
    /// An optional hint for resolving the error.
    /// </summary>
    public string? Hint { get; }

    /// <summary>Initializes a new instance of <see cref="StreamlineException"/>.</summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The error code.</param>
    /// <param name="isRetryable">Whether the operation can be retried.</param>
    /// <param name="hint">An optional hint for resolving the error.</param>
    /// <param name="innerException">The inner exception, if any.</param>
    public StreamlineException(string message, StreamlineErrorCode errorCode = StreamlineErrorCode.Unknown, bool isRetryable = false, string? hint = null, Exception? innerException = null)
        : base(message, innerException)
    {
        ErrorCode = errorCode;
        IsRetryable = isRetryable;
        Hint = hint;
    }
}

/// <summary>
/// Thrown when a connection to the Streamline server fails.
/// </summary>
public class StreamlineConnectionException : StreamlineException
{
    /// <inheritdoc />
    public StreamlineConnectionException(string message, Exception? innerException = null)
        : base(message, StreamlineErrorCode.Connection, isRetryable: true,
               hint: "Check that Streamline server is running and accessible", innerException: innerException)
    {
    }
}

/// <summary>
/// Thrown when authentication fails.
/// </summary>
public class StreamlineAuthenticationException : StreamlineException
{
    /// <inheritdoc />
    public StreamlineAuthenticationException(string message, Exception? innerException = null)
        : base(message, StreamlineErrorCode.Authentication, isRetryable: false,
               hint: "Verify your credentials and authentication mechanism", innerException: innerException)
    {
    }
}

/// <summary>
/// Thrown when authorization (ACL check) fails.
/// </summary>
public class StreamlineAuthorizationException : StreamlineException
{
    /// <inheritdoc />
    public StreamlineAuthorizationException(string message, Exception? innerException = null)
        : base(message, StreamlineErrorCode.Authorization, isRetryable: false,
               hint: "Check ACL permissions for this operation", innerException: innerException)
    {
    }
}

/// <summary>
/// Thrown when a requested topic does not exist.
/// </summary>
public class StreamlineTopicNotFoundException : StreamlineException
{
    /// <summary>
    /// The topic that was not found.
    /// </summary>
    public string Topic { get; }

    /// <summary>Initializes a new instance for the specified topic.</summary>
    /// <param name="topic">The topic that was not found.</param>
    public StreamlineTopicNotFoundException(string topic)
        : base($"Topic not found: {topic}", StreamlineErrorCode.TopicNotFound, isRetryable: false,
               hint: $"Create the topic with: streamline-cli topics create {topic}")
    {
        Topic = topic;
    }
}

/// <summary>
/// Thrown when an operation times out.
/// </summary>
public class StreamlineTimeoutException : StreamlineException
{
    /// <inheritdoc />
    public StreamlineTimeoutException(string message, Exception? innerException = null)
        : base(message, StreamlineErrorCode.Timeout, isRetryable: true,
               hint: "Consider increasing timeout settings or checking server load", innerException: innerException)
    {
    }
}

/// <summary>
/// Thrown when a producer operation fails.
/// </summary>
public class StreamlineProducerException : StreamlineException
{
    /// <inheritdoc />
    public StreamlineProducerException(string message, Exception? innerException = null)
        : base(message, StreamlineErrorCode.Producer, isRetryable: true, innerException: innerException)
    {
    }
}

/// <summary>
/// Thrown when a consumer operation fails.
/// </summary>
public class StreamlineConsumerException : StreamlineException
{
    /// <inheritdoc />
    public StreamlineConsumerException(string message, Exception? innerException = null)
        : base(message, StreamlineErrorCode.Consumer, isRetryable: true, innerException: innerException)
    {
    }
}

/// <summary>
/// Thrown when serialization or deserialization fails.
/// </summary>
public class StreamlineSerializationException : StreamlineException
{
    /// <inheritdoc />
    public StreamlineSerializationException(string message, Exception? innerException = null)
        : base(message, StreamlineErrorCode.Serialization, isRetryable: false, innerException: innerException)
    {
    }
}

/// <summary>
/// Thrown when a configuration is invalid.
/// </summary>
public class StreamlineConfigurationException : StreamlineException
{
    /// <inheritdoc />
    public StreamlineConfigurationException(string message)
        : base(message, StreamlineErrorCode.Configuration, isRetryable: false)
    {
    }
}
