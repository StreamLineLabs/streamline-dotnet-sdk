using Streamline.Client;
using Xunit;

namespace Streamline.Client.Tests;

public class CircuitBreakerTests
{
    private static CircuitBreakerOptions QuickOptions(int failureThreshold = 3, int successThreshold = 1, int openTimeoutMs = 10000) => new()
    {
        FailureThreshold = failureThreshold,
        SuccessThreshold = successThreshold,
        OpenTimeout = TimeSpan.FromMilliseconds(openTimeoutMs),
        HalfOpenMaxRequests = 2,
    };

    [Fact]
    public void StartsClosed()
    {
        var cb = new CircuitBreaker();
        Assert.Equal(CircuitState.Closed, cb.State);
        Assert.True(cb.Allow());
    }

    [Fact]
    public void OpensAfterFailureThreshold()
    {
        var cb = new CircuitBreaker(QuickOptions(failureThreshold: 3));

        for (var i = 0; i < 3; i++)
        {
            cb.Allow();
            cb.RecordFailure();
        }

        Assert.Equal(CircuitState.Open, cb.State);
        Assert.False(cb.Allow());
    }

    [Fact]
    public void DoesNotOpenBelowThreshold()
    {
        var cb = new CircuitBreaker(QuickOptions(failureThreshold: 3));

        cb.Allow();
        cb.RecordFailure();
        cb.Allow();
        cb.RecordFailure();

        Assert.Equal(CircuitState.Closed, cb.State);
        Assert.True(cb.Allow());
    }

    [Fact]
    public async Task TransitionsToHalfOpenAfterTimeout()
    {
        var cb = new CircuitBreaker(QuickOptions(failureThreshold: 2, openTimeoutMs: 50));

        cb.Allow();
        cb.RecordFailure();
        cb.Allow();
        cb.RecordFailure();
        Assert.Equal(CircuitState.Open, cb.State);

        await Task.Delay(60);

        Assert.Equal(CircuitState.HalfOpen, cb.State);
        Assert.True(cb.Allow());
    }

    [Fact]
    public async Task ClosesOnHalfOpenSuccess()
    {
        var cb = new CircuitBreaker(QuickOptions(failureThreshold: 2, successThreshold: 1, openTimeoutMs: 50));

        cb.Allow();
        cb.RecordFailure();
        cb.Allow();
        cb.RecordFailure();

        await Task.Delay(60);

        cb.Allow();
        cb.RecordSuccess();

        Assert.Equal(CircuitState.Closed, cb.State);
    }

    [Fact]
    public async Task ReopensOnHalfOpenFailure()
    {
        var opts = QuickOptions(failureThreshold: 2, openTimeoutMs: 50);
        opts.SuccessThreshold = 2;
        var cb = new CircuitBreaker(opts);

        cb.Allow();
        cb.RecordFailure();
        cb.Allow();
        cb.RecordFailure();

        await Task.Delay(60);

        cb.Allow();
        cb.RecordFailure();

        Assert.Equal(CircuitState.Open, cb.State);
    }

    [Fact]
    public async Task HalfOpenLimitsProbeRequests()
    {
        var opts = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            SuccessThreshold = 1,
            OpenTimeout = TimeSpan.FromMilliseconds(50),
            HalfOpenMaxRequests = 2,
        };
        var cb = new CircuitBreaker(opts);

        cb.Allow();
        cb.RecordFailure();

        await Task.Delay(60);

        Assert.True(cb.Allow(), "first probe should be allowed");
        Assert.True(cb.Allow(), "second probe should be allowed");
        Assert.False(cb.Allow(), "third probe should be rejected");
    }

    [Fact]
    public void SuccessResetsFailureCount()
    {
        var cb = new CircuitBreaker(QuickOptions(failureThreshold: 3));

        cb.Allow();
        cb.RecordFailure();
        cb.Allow();
        cb.RecordFailure();
        cb.Allow();
        cb.RecordSuccess(); // resets count

        cb.Allow();
        cb.RecordFailure();
        cb.Allow();
        cb.RecordFailure();

        Assert.Equal(CircuitState.Closed, cb.State);
    }

    [Fact]
    public void ResetReturnsToClosed()
    {
        var cb = new CircuitBreaker(QuickOptions(failureThreshold: 1, openTimeoutMs: 600_000));

        cb.Allow();
        cb.RecordFailure();
        Assert.Equal(CircuitState.Open, cb.State);

        cb.Reset();

        Assert.Equal(CircuitState.Closed, cb.State);
        Assert.True(cb.Allow());
    }

    [Fact]
    public async Task StateChangeEventFires()
    {
        var transitions = new List<string>();
        var opts = new CircuitBreakerOptions
        {
            FailureThreshold = 1,
            SuccessThreshold = 1,
            OpenTimeout = TimeSpan.FromMilliseconds(50),
            HalfOpenMaxRequests = 1,
        };
        var cb = new CircuitBreaker(opts);
        cb.OnStateChange += (from, to) => transitions.Add($"{from}->{to}");

        cb.Allow();
        cb.RecordFailure(); // Closed -> Open

        await Task.Delay(60);
        cb.Allow(); // Open -> HalfOpen (via State check)
        cb.RecordSuccess(); // HalfOpen -> Closed

        Assert.Equal(3, transitions.Count);
        Assert.Equal("Closed->Open", transitions[0]);
        Assert.Equal("Open->HalfOpen", transitions[1]);
        Assert.Equal("HalfOpen->Closed", transitions[2]);
    }

    [Fact]
    public async Task ExecuteAsync_ReturnsResultOnSuccess()
    {
        var cb = new CircuitBreaker();

        var result = await cb.ExecuteAsync(() => Task.FromResult(42));

        Assert.Equal(42, result);
    }

    [Fact]
    public async Task ExecuteAsync_ThrowsWhenCircuitOpen()
    {
        var cb = new CircuitBreaker(QuickOptions(failureThreshold: 1, openTimeoutMs: 600_000));

        cb.Allow();
        cb.RecordFailure();

        var ex = await Assert.ThrowsAsync<StreamlineException>(
            () => cb.ExecuteAsync(() => Task.FromResult("unreachable")));

        Assert.Contains("Circuit breaker is open", ex.Message);
        Assert.True(ex.IsRetryable);
    }

    [Fact]
    public async Task ExecuteAsync_RecordsFailureOnRetryableException()
    {
        var cb = new CircuitBreaker(QuickOptions(failureThreshold: 2, openTimeoutMs: 600_000));

        for (var i = 0; i < 2; i++)
        {
            await Assert.ThrowsAsync<StreamlineConnectionException>(
                () => cb.ExecuteAsync<string>(() =>
                {
                    throw new StreamlineConnectionException("transient");
                }));
        }

        Assert.Equal(CircuitState.Open, cb.State);
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotTripOnNonRetryableException()
    {
        var cb = new CircuitBreaker(QuickOptions(failureThreshold: 2, openTimeoutMs: 600_000));

        for (var i = 0; i < 5; i++)
        {
            await Assert.ThrowsAsync<StreamlineAuthenticationException>(
                () => cb.ExecuteAsync<string>(() =>
                {
                    throw new StreamlineAuthenticationException("bad creds");
                }));
        }

        Assert.Equal(CircuitState.Closed, cb.State);
    }
}
