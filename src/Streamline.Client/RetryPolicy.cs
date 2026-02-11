using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Streamline.Client;

/// <summary>
/// Configuration for <see cref="RetryPolicy"/>.
/// </summary>
public sealed class RetryPolicyOptions
{
    /// <summary>
    /// Maximum number of retry attempts. Default is 3.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Base delay between retries. Default is 100ms.
    /// </summary>
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Maximum delay between retries. Default is 10s.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Multiplier applied to the delay on each subsequent retry. Default is 2.0.
    /// </summary>
    public double Multiplier { get; set; } = 2.0;

    /// <summary>
    /// Predicate that determines whether an exception is retryable.
    /// When null, defaults to retrying <see cref="StreamlineException"/> instances
    /// where <see cref="StreamlineException.IsRetryable"/> is true,
    /// plus <see cref="HttpRequestException"/> and <see cref="TaskCanceledException"/>.
    /// </summary>
    public Func<Exception, bool>? IsRetryable { get; set; }
}

/// <summary>
/// Provides retry logic with exponential backoff and jitter for Streamline operations.
/// </summary>
public sealed class RetryPolicy
{
    private readonly RetryPolicyOptions _options;
    private readonly ILogger _logger;
    private readonly Func<Exception, bool> _isRetryable;

    /// <summary>
    /// Creates a new retry policy with default options.
    /// </summary>
    public RetryPolicy()
        : this(new RetryPolicyOptions())
    {
    }

    /// <summary>
    /// Creates a new retry policy with the specified options.
    /// </summary>
    public RetryPolicy(RetryPolicyOptions options, ILogger? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? NullLogger.Instance;
        _isRetryable = options.IsRetryable ?? DefaultIsRetryable;

        if (options.MaxRetries < 0)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxRetries must be non-negative.");
        if (options.BaseDelay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "BaseDelay must be non-negative.");
        if (options.MaxDelay < options.BaseDelay)
            throw new ArgumentOutOfRangeException(nameof(options), "MaxDelay must be >= BaseDelay.");
        if (options.Multiplier < 1.0)
            throw new ArgumentOutOfRangeException(nameof(options), "Multiplier must be >= 1.0.");
    }

    /// <summary>
    /// Executes an async operation with retry logic.
    /// </summary>
    /// <typeparam name="T">The return type of the operation.</typeparam>
    /// <param name="operation">The operation to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the operation.</returns>
    /// <exception cref="StreamlineException">
    /// Thrown when all retry attempts are exhausted or a non-retryable exception occurs.
    /// </exception>
    public async Task<T> ExecuteAsync<T>(
        Func<Task<T>> operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        var attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                attempt++;

                if (!_isRetryable(ex) || attempt > _options.MaxRetries)
                {
                    _logger.LogWarning(ex,
                        "Operation failed after {Attempt} attempt(s), not retrying: {Message}",
                        attempt, ex.Message);
                    throw;
                }

                var delay = ComputeDelay(attempt);

                _logger.LogWarning(ex,
                    "Operation failed on attempt {Attempt}/{MaxRetries}, retrying in {Delay}ms: {Message}",
                    attempt, _options.MaxRetries, delay.TotalMilliseconds, ex.Message);

                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Executes an async operation (returning void) with retry logic.
    /// </summary>
    /// <param name="operation">The operation to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task ExecuteAsync(
        Func<Task> operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);

        await ExecuteAsync(async () =>
        {
            await operation().ConfigureAwait(false);
            return true;
        }, cancellationToken).ConfigureAwait(false);
    }

    internal TimeSpan ComputeDelay(int attempt)
    {
        // Exponential backoff: baseDelay * multiplier^(attempt-1)
        var exponentialMs = _options.BaseDelay.TotalMilliseconds
                           * Math.Pow(_options.Multiplier, attempt - 1);

        // Cap at max delay
        var cappedMs = Math.Min(exponentialMs, _options.MaxDelay.TotalMilliseconds);

        // Add jitter: ±25% of the computed delay
        var jitterFactor = 0.75 + (Random.Shared.NextDouble() * 0.5);
        var delayMs = cappedMs * jitterFactor;

        return TimeSpan.FromMilliseconds(Math.Max(0, delayMs));
    }

    private static bool DefaultIsRetryable(Exception ex) => ex switch
    {
        StreamlineException se => se.IsRetryable,
        HttpRequestException => true,
        TaskCanceledException => false,
        _ => false,
    };
}
