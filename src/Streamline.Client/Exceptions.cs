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
    /// <summary>Contract violation.</summary>
    ContractViolation,
    /// <summary>Attestation verification failed.</summary>
    AttestationFailed,
    /// <summary>Memory access denied.</summary>
    MemoryAccessDenied,
    /// <summary>Branch quota exceeded.</summary>
    BranchQuotaExceeded,
    /// <summary>Semantic search unavailable.</summary>
    SemanticSearchUnavailable,
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
    /// <param name="message">Optional custom message.</param>
    public StreamlineTopicNotFoundException(string topic, string? message = null)
        : base(message ?? $"Topic not found: {topic}", StreamlineErrorCode.TopicNotFound, isRetryable: false,
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

/// <summary>
/// Thrown when a record violates a topic's data contract.
/// </summary>
public class StreamlineContractViolationException : StreamlineException
{
    /// <summary>The topic where the contract was violated.</summary>
    public string Topic { get; }

    /// <inheritdoc />
    public StreamlineContractViolationException(string topic, string details)
        : base($"Contract violation on topic '{topic}': {details}", StreamlineErrorCode.ContractViolation, isRetryable: false,
               hint: "Validate the record against the topic's registered schema")
    {
        Topic = topic;
    }
}

/// <summary>
/// Thrown when attestation signature verification fails.
/// </summary>
public class StreamlineAttestationException : StreamlineException
{
    /// <inheritdoc />
    public StreamlineAttestationException(string message, Exception? innerException = null)
        : base(message, StreamlineErrorCode.AttestationFailed, isRetryable: false,
               hint: "Check the signing key and attestation configuration", innerException: innerException)
    {
    }
}

/// <summary>
/// Thrown when an agent lacks permission to access memory.
/// </summary>
public class StreamlineMemoryAccessDeniedException : StreamlineException
{
    /// <inheritdoc />
    public StreamlineMemoryAccessDeniedException(string agent)
        : base($"Memory access denied for agent: {agent}", StreamlineErrorCode.MemoryAccessDenied, isRetryable: false,
               hint: "Verify agent permissions for memory operations")
    {
    }
}

/// <summary>
/// Thrown when a branch exceeds its storage or lifetime quota.
/// </summary>
public class StreamlineBranchQuotaExceededException : StreamlineException
{
    /// <inheritdoc />
    public StreamlineBranchQuotaExceededException(string branch, string details)
        : base($"Branch quota exceeded for '{branch}': {details}", StreamlineErrorCode.BranchQuotaExceeded, isRetryable: false,
               hint: "Increase branch quotas or clean up unused branches")
    {
    }
}

/// <summary>
/// Thrown when semantic search is unavailable (embedding provider down).
/// </summary>
public class StreamlineSemanticSearchUnavailableException : StreamlineException
{
    /// <inheritdoc />
    public StreamlineSemanticSearchUnavailableException(string message, Exception? innerException = null)
        : base(message, StreamlineErrorCode.SemanticSearchUnavailable, isRetryable: true,
               hint: "Check embedding provider connectivity and configuration", innerException: innerException)
    {
    }
}


    /// <summary>
    /// Centralized error message constants for consistent messaging.
    /// </summary>
    internal static class ErrorMessages
    {
        public const string ConnectionFailed = "Failed to connect to Streamline broker";
        public const string AuthenticationFailed = "Authentication failed. Check credentials and SASL configuration";
        public const string TopicNotFound = "The specified topic does not exist";
        public const string ProducerClosed = "Cannot produce: producer has been closed";
        public const string ConsumerClosed = "Cannot consume: consumer has been closed";
        public const string InvalidConfiguration = "Invalid client configuration";
        public const string SerializationFailed = "Failed to serialize/deserialize message";
    }
