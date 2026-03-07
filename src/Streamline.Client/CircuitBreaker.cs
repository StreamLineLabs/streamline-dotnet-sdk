using Microsoft.Extensions.Logging;

namespace Streamline.Client;

/// <summary>
/// Circuit breaker states.
/// </summary>
public enum CircuitState
{
    /// <summary>Requests flow normally.</summary>
    Closed,
    /// <summary>Requests are rejected immediately.</summary>
    Open,
    /// <summary>A limited number of probe requests are allowed.</summary>
    HalfOpen,
}

/// <summary>
/// Configuration for the circuit breaker.
/// </summary>
public class CircuitBreakerOptions
{
    /// <summary>Number of consecutive failures before opening the circuit (default: 5).</summary>
    public int FailureThreshold { get; set; } = 5;

    /// <summary>Consecutive successes in half-open state to close the circuit (default: 2).</summary>
    public int SuccessThreshold { get; set; } = 2;

    /// <summary>How long to wait before transitioning from open to half-open (default: 30s).</summary>
    public TimeSpan OpenTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Max probe requests allowed in half-open state (default: 3).</summary>
    public int HalfOpenMaxRequests { get; set; } = 3;
}

/// <summary>
/// Circuit breaker pattern for resilient Streamline operations.
/// <para>
/// Tracks failures and temporarily stops requests to a failing service,
/// allowing it time to recover before retrying. Models the standard
/// Closed → Open → Half-Open state machine.
/// </para>
/// </summary>
/// <example>
/// <code>
/// var breaker = new CircuitBreaker();
/// var result = await breaker.ExecuteAsync(async () => {
///     return await producer.SendAsync("topic", "key", "value");
/// });
/// </code>
/// </example>
public class CircuitBreaker
{
    private readonly CircuitBreakerOptions _options;
    private readonly ILogger? _logger;
    private readonly object _lock = new();

    private CircuitState _state = CircuitState.Closed;
    private int _failureCount;
    private int _successCount;
    private int _halfOpenCount;
    private DateTimeOffset _lastFailureAt = DateTimeOffset.MinValue;

    /// <summary>Event raised when the circuit state changes.</summary>
    public event Action<CircuitState, CircuitState>? OnStateChange;

    /// <summary>Initializes a new instance of the <see cref="CircuitBreaker"/> class.</summary>
    /// <param name="options">Configuration options for the circuit breaker. If null, default options are used.</param>
    /// <param name="logger">Optional logger for diagnostic output.</param>
    public CircuitBreaker(CircuitBreakerOptions? options = null, ILogger<CircuitBreaker>? logger = null)
    {
        _options = options ?? new CircuitBreakerOptions();
        _logger = logger;
    }

    /// <summary>Returns the current circuit state.</summary>
    public CircuitState State
    {
        get
        {
            lock (_lock)
            {
                CheckOpenTimeout();
                return _state;
            }
        }
    }

    /// <summary>Returns true if a request should be allowed through.</summary>
    public bool Allow()
    {
        lock (_lock)
        {
            return _state switch
            {
                CircuitState.Closed => true,
                CircuitState.Open when DateTimeOffset.UtcNow - _lastFailureAt >= _options.OpenTimeout =>
                    TransitionAndAllow(CircuitState.HalfOpen),
                CircuitState.Open => false,
                CircuitState.HalfOpen when _halfOpenCount < _options.HalfOpenMaxRequests =>
                    IncrementHalfOpenAndAllow(),
                CircuitState.HalfOpen => false,
                _ => true,
            };
        }
    }

    /// <summary>Record a successful operation.</summary>
    public void RecordSuccess()
    {
        lock (_lock)
        {
            switch (_state)
            {
                case CircuitState.Closed:
                    _failureCount = 0;
                    break;
                case CircuitState.HalfOpen:
                    _successCount++;
                    if (_successCount >= _options.SuccessThreshold)
                    {
                        Transition(CircuitState.Closed);
                    }
                    break;
            }
        }
    }

    /// <summary>Record a failed operation. Only retryable failures should be recorded.</summary>
    public void RecordFailure()
    {
        lock (_lock)
        {
            _lastFailureAt = DateTimeOffset.UtcNow;

            switch (_state)
            {
                case CircuitState.Closed:
                    _failureCount++;
                    if (_failureCount >= _options.FailureThreshold)
                    {
                        Transition(CircuitState.Open);
                    }
                    break;
                case CircuitState.HalfOpen:
                    Transition(CircuitState.Open);
                    break;
            }
        }
    }

    /// <summary>Reset the circuit breaker to closed state.</summary>
    public void Reset()
    {
        lock (_lock)
        {
            Transition(CircuitState.Closed);
        }
    }

    /// <summary>
    /// Execute an async operation through the circuit breaker.
    /// </summary>
    /// <typeparam name="T">Result type.</typeparam>
    /// <param name="action">The async operation to execute.</param>
    /// <returns>The result of the operation.</returns>
    /// <exception cref="StreamlineException">Thrown when the circuit is open.</exception>
    public async Task<T> ExecuteAsync<T>(Func<Task<T>> action)
    {
        if (!Allow())
        {
            _logger?.LogWarning("Circuit breaker is open, rejecting request");
            throw new StreamlineException(
                "Circuit breaker is open — too many recent failures",
                isRetryable: true,
                hint: "The client detected repeated failures and is temporarily pausing requests.");
        }

        try
        {
            var result = await action();
            RecordSuccess();
            return result;
        }
        catch (StreamlineException ex) when (ex.IsRetryable)
        {
            RecordFailure();
            throw;
        }
        catch (Exception)
        {
            RecordFailure();
            throw;
        }
    }

    /// <summary>
    /// Execute an async operation through the circuit breaker (no return value).
    /// </summary>
    public async Task ExecuteAsync(Func<Task> action)
    {
        await ExecuteAsync(async () =>
        {
            await action();
            return true;
        });
    }

    private void CheckOpenTimeout()
    {
        if (_state == CircuitState.Open && DateTimeOffset.UtcNow - _lastFailureAt >= _options.OpenTimeout)
        {
            Transition(CircuitState.HalfOpen);
        }
    }

    private bool TransitionAndAllow(CircuitState to)
    {
        Transition(to);
        _halfOpenCount = 1;
        return true;
    }

    private bool IncrementHalfOpenAndAllow()
    {
        _halfOpenCount++;
        return true;
    }

    private void Transition(CircuitState to)
    {
        var from = _state;
        if (from == to) return;

        _state = to;
        _failureCount = 0;
        _successCount = 0;
        _halfOpenCount = 0;

        _logger?.LogInformation("Circuit breaker transitioned from {From} to {To}", from, to);
        OnStateChange?.Invoke(from, to);
    }
}
