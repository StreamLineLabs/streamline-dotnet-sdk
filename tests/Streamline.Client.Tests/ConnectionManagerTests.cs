using Streamline.Client;
using Xunit;

namespace Streamline.Client.Tests;

public class ConnectionManagerTests
{
    private static StreamlineOptions DefaultOptions => new()
    {
        BootstrapServers = "localhost:9092",
        ConnectionPoolSize = 2,
        ConnectTimeout = TimeSpan.FromMilliseconds(100),
        RequestTimeout = TimeSpan.FromMilliseconds(100),
    };

    [Fact]
    public void Constructor_SetsInitialStateToDisconnected()
    {
        using var handler = new MockHttpMessageHandler(_ =>
            new HttpResponseMessage(System.Net.HttpStatusCode.OK));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost:9094"),
        };
        var manager = new ConnectionManager(DefaultOptions, httpClient);

        Assert.Equal(ConnectionState.Disconnected, manager.State);
    }

    [Fact]
    public void Constructor_ThrowsOnNullOptions()
    {
        Assert.Throws<ArgumentNullException>(() => new ConnectionManager(null!));
    }

    [Fact]
    public void Constructor_ThrowsOnNullHttpClient()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new ConnectionManager(DefaultOptions, (HttpClient)null!));
    }

    [Fact]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        using var handler = new MockHttpMessageHandler(_ =>
            new HttpResponseMessage(System.Net.HttpStatusCode.OK));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost:9094"),
        };
        var manager = new ConnectionManager(DefaultOptions, httpClient);

        await manager.DisposeAsync();
        await manager.DisposeAsync(); // Should not throw
    }

    [Fact]
    public async Task StateChanged_EventFires()
    {
        using var handler = new MockHttpMessageHandler(_ =>
            new HttpResponseMessage(System.Net.HttpStatusCode.OK));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost:9094"),
        };
        var manager = new ConnectionManager(DefaultOptions, httpClient);

        var states = new List<ConnectionState>();
        manager.StateChanged += state => states.Add(state);

        var healthy = await manager.CheckHealthAsync();

        Assert.True(healthy);
        Assert.Contains(ConnectionState.Connected, states);

        await manager.DisposeAsync();
    }

    [Fact]
    public async Task CheckHealthAsync_ReturnsTrueWhenServerResponds()
    {
        using var handler = new MockHttpMessageHandler(_ =>
            new HttpResponseMessage(System.Net.HttpStatusCode.OK));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost:9094"),
        };
        var manager = new ConnectionManager(DefaultOptions, httpClient);

        var healthy = await manager.CheckHealthAsync();

        Assert.True(healthy);
        Assert.Equal(ConnectionState.Connected, manager.State);

        await manager.DisposeAsync();
    }

    [Fact]
    public async Task CheckHealthAsync_ReturnsFalseOnServerError()
    {
        using var handler = new MockHttpMessageHandler(_ =>
            new HttpResponseMessage(System.Net.HttpStatusCode.InternalServerError));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost:9094"),
        };
        var manager = new ConnectionManager(DefaultOptions, httpClient);

        var healthy = await manager.CheckHealthAsync();

        Assert.False(healthy);
        Assert.Equal(ConnectionState.Disconnected, manager.State);

        await manager.DisposeAsync();
    }

    [Fact]
    public async Task CheckHealthAsync_ReturnsFalseOnException()
    {
        using var handler = new MockHttpMessageHandler(_ =>
            throw new HttpRequestException("connection refused"));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost:9094"),
        };
        var manager = new ConnectionManager(DefaultOptions, httpClient);

        var healthy = await manager.CheckHealthAsync();

        Assert.False(healthy);
        Assert.Equal(ConnectionState.Disconnected, manager.State);

        await manager.DisposeAsync();
    }

    [Fact]
    public async Task ConnectAsync_TransitionsToConnected()
    {
        using var handler = new MockHttpMessageHandler(_ =>
            new HttpResponseMessage(System.Net.HttpStatusCode.OK));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost:9094"),
        };
        var manager = new ConnectionManager(DefaultOptions, httpClient);

        await manager.ConnectAsync();

        Assert.Equal(ConnectionState.Connected, manager.State);

        await manager.DisposeAsync();
    }

    [Fact]
    public async Task ConnectAsync_WhenAlreadyConnected_DoesNothing()
    {
        var callCount = 0;
        using var handler = new MockHttpMessageHandler(_ =>
        {
            Interlocked.Increment(ref callCount);
            return new HttpResponseMessage(System.Net.HttpStatusCode.OK);
        });
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost:9094"),
        };
        var manager = new ConnectionManager(DefaultOptions, httpClient);

        await manager.ConnectAsync();
        var countAfterFirst = callCount;
        await manager.ConnectAsync();

        Assert.Equal(countAfterFirst, callCount);

        await manager.DisposeAsync();
    }

    [Fact]
    public async Task HealthCheckInterval_DefaultIs30Seconds()
    {
        using var handler = new MockHttpMessageHandler(_ =>
            new HttpResponseMessage(System.Net.HttpStatusCode.OK));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost:9094"),
        };
        var manager = new ConnectionManager(DefaultOptions, httpClient);

        Assert.Equal(TimeSpan.FromSeconds(30), manager.HealthCheckInterval);

        await manager.DisposeAsync();
    }

    [Fact]
    public void ConnectionState_HasExpectedValues()
    {
        Assert.Equal(0, (int)ConnectionState.Disconnected);
        Assert.Equal(1, (int)ConnectionState.Connected);
        Assert.Equal(2, (int)ConnectionState.Reconnecting);
    }

    /// <summary>
    /// Simple mock HTTP message handler for unit testing.
    /// </summary>
    private sealed class MockHttpMessageHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _handler;

        public MockHttpMessageHandler(Func<HttpRequestMessage, HttpResponseMessage> handler)
        {
            _handler = handler;
        }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(_handler(request));
        }
    }
}
