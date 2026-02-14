using Streamline.Client;
using Xunit;

namespace Streamline.Client.Tests;

public class RetryPolicyTests
{
    [Fact]
    public async Task ExecuteAsync_ReturnsResultOnSuccess()
    {
        var policy = new RetryPolicy();

        var result = await policy.ExecuteAsync(() => Task.FromResult(42));

        Assert.Equal(42, result);
    }

    [Fact]
    public async Task ExecuteAsync_RetriesOnRetryableException()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(1),
        });

        var attempts = 0;
        var result = await policy.ExecuteAsync(() =>
        {
            attempts++;
            if (attempts < 3)
                throw new StreamlineConnectionException("transient failure");
            return Task.FromResult("success");
        });

        Assert.Equal("success", result);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task ExecuteAsync_ThrowsImmediatelyOnNonRetryableException()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(1),
        });

        var attempts = 0;
        var ex = await Assert.ThrowsAsync<StreamlineAuthenticationException>(async () =>
        {
            await policy.ExecuteAsync<int>(() =>
            {
                attempts++;
                throw new StreamlineAuthenticationException("bad credentials");
            });
        });

        Assert.Equal(1, attempts);
        Assert.Equal(StreamlineErrorCode.Authentication, ex.ErrorCode);
    }

    [Fact]
    public async Task ExecuteAsync_ThrowsAfterMaxRetries()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions
        {
            MaxRetries = 2,
            BaseDelay = TimeSpan.FromMilliseconds(1),
        });

        var attempts = 0;
        await Assert.ThrowsAsync<StreamlineConnectionException>(async () =>
        {
            await policy.ExecuteAsync<int>(() =>
            {
                attempts++;
                throw new StreamlineConnectionException("always fails");
            });
        });

        Assert.Equal(3, attempts); // 1 initial + 2 retries
    }

    [Fact]
    public async Task ExecuteAsync_RespectsCustomRetryablePredicate()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions
        {
            MaxRetries = 3,
            BaseDelay = TimeSpan.FromMilliseconds(1),
            IsRetryable = ex => ex is InvalidOperationException,
        });

        var attempts = 0;
        var result = await policy.ExecuteAsync(() =>
        {
            attempts++;
            if (attempts < 2)
                throw new InvalidOperationException("retry me");
            return Task.FromResult("done");
        });

        Assert.Equal("done", result);
        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task ExecuteAsync_RespectsCancellationToken()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions
        {
            MaxRetries = 10,
            BaseDelay = TimeSpan.FromSeconds(10),
        });

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await policy.ExecuteAsync(() => Task.FromResult(1), cts.Token);
        });
    }

    [Fact]
    public async Task ExecuteAsync_VoidOverload_Works()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions
        {
            MaxRetries = 2,
            BaseDelay = TimeSpan.FromMilliseconds(1),
        });

        var executed = false;
        await policy.ExecuteAsync(() =>
        {
            executed = true;
            return Task.CompletedTask;
        });

        Assert.True(executed);
    }

    [Fact]
    public async Task ExecuteAsync_VoidOverload_RetriesOnFailure()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions
        {
            MaxRetries = 2,
            BaseDelay = TimeSpan.FromMilliseconds(1),
        });

        var attempts = 0;
        await policy.ExecuteAsync(() =>
        {
            attempts++;
            if (attempts < 2)
                throw new StreamlineTimeoutException("timeout");
            return Task.CompletedTask;
        });

        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task ExecuteAsync_RetriesHttpRequestException()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions
        {
            MaxRetries = 2,
            BaseDelay = TimeSpan.FromMilliseconds(1),
        });

        var attempts = 0;
        var result = await policy.ExecuteAsync(() =>
        {
            attempts++;
            if (attempts < 2)
                throw new HttpRequestException("network error");
            return Task.FromResult("ok");
        });

        Assert.Equal("ok", result);
        Assert.Equal(2, attempts);
    }

    [Fact]
    public void ComputeDelay_ReturnsPositiveValue()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions
        {
            BaseDelay = TimeSpan.FromMilliseconds(100),
            MaxDelay = TimeSpan.FromSeconds(10),
            Multiplier = 2.0,
        });

        var delay = policy.ComputeDelay(1);
        Assert.True(delay > TimeSpan.Zero);
        Assert.True(delay <= TimeSpan.FromMilliseconds(150)); // 100ms * 1.25 (max jitter)
    }

    [Fact]
    public void ComputeDelay_IncreasesByMultiplier()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions
        {
            BaseDelay = TimeSpan.FromMilliseconds(100),
            MaxDelay = TimeSpan.FromSeconds(10),
            Multiplier = 2.0,
        });

        // Run several times to avoid flakiness from jitter
        var sumDelay1 = 0.0;
        var sumDelay3 = 0.0;
        for (var i = 0; i < 100; i++)
        {
            sumDelay1 += policy.ComputeDelay(1).TotalMilliseconds;
            sumDelay3 += policy.ComputeDelay(3).TotalMilliseconds;
        }

        // Average delay at attempt 3 should be ~4x attempt 1 (multiplier^2)
        Assert.True(sumDelay3 / sumDelay1 > 2.0);
    }

    [Fact]
    public void ComputeDelay_CapsAtMaxDelay()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions
        {
            BaseDelay = TimeSpan.FromSeconds(1),
            MaxDelay = TimeSpan.FromSeconds(2),
            Multiplier = 10.0,
        });

        var delay = policy.ComputeDelay(5);
        // Max delay * max jitter factor (1.25)
        Assert.True(delay <= TimeSpan.FromMilliseconds(2500));
    }

    [Fact]
    public void Constructor_ThrowsOnNegativeMaxRetries()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new RetryPolicy(new RetryPolicyOptions { MaxRetries = -1 }));
    }

    [Fact]
    public void Constructor_ThrowsOnNegativeBaseDelay()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new RetryPolicy(new RetryPolicyOptions { BaseDelay = TimeSpan.FromMilliseconds(-1) }));
    }

    [Fact]
    public void Constructor_ThrowsWhenMaxDelayLessThanBaseDelay()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new RetryPolicy(new RetryPolicyOptions
            {
                BaseDelay = TimeSpan.FromSeconds(5),
                MaxDelay = TimeSpan.FromSeconds(1),
            }));
    }

    [Fact]
    public void Constructor_ThrowsOnMultiplierLessThanOne()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new RetryPolicy(new RetryPolicyOptions { Multiplier = 0.5 }));
    }

    [Fact]
    public void Constructor_ThrowsOnNullOptions()
    {
        Assert.Throws<ArgumentNullException>(() => new RetryPolicy(null!));
    }

    [Fact]
    public async Task ExecuteAsync_ThrowsOnNullOperation()
    {
        var policy = new RetryPolicy();
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            policy.ExecuteAsync<int>(null!));
    }

    [Fact]
    public void Constructor_AllowsZeroMaxRetries()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions { MaxRetries = 0 });
        Assert.NotNull(policy);
    }

    [Fact]
    public async Task ExecuteAsync_ZeroRetriesMeansNoRetry()
    {
        var policy = new RetryPolicy(new RetryPolicyOptions { MaxRetries = 0 });

        var attempts = 0;
        await Assert.ThrowsAsync<StreamlineConnectionException>(async () =>
        {
            await policy.ExecuteAsync<int>(() =>
            {
                attempts++;
                throw new StreamlineConnectionException("fail");
            });
        });

        Assert.Equal(1, attempts);
    }
}
