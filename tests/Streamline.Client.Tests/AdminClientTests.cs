using System.Net;
using System.Text;
using System.Text.Json;
using Streamline.Client;
using Streamline.TestSupport;
using Xunit;

namespace Streamline.Client.Tests;

public class AdminClientTests
{
    // -- Helpers --

    private static HttpClient MockHttp(HttpStatusCode status, string body)
    {
        var handler = new MockHttpHandler(status, body);
        return new HttpClient(handler) { BaseAddress = new Uri(StreamlineTestEnvironment.UnitHttpBaseUrl) };
    }

    private static HttpClient MockHttp(Func<HttpRequestMessage, HttpResponseMessage> handler)
    {
        return new HttpClient(new DelegatingMockHandler(handler)) { BaseAddress = new Uri(StreamlineTestEnvironment.UnitHttpBaseUrl) };
    }

    // =========================================================================
    // ListTopics
    // =========================================================================

    [Fact]
    public async Task ListTopicsAsync_ReturnsParsedTopics()
    {
        var json = """[{"name":"events","partitions":3,"replication_factor":1,"message_count":100},{"name":"logs","partitions":1,"replication_factor":1,"message_count":50}]""";
        var http = MockHttp(HttpStatusCode.OK, json);
        await using var admin = new AdminClient(http);

        var topics = await admin.ListTopicsAsync();

        Assert.Equal(2, topics.Count);
        Assert.Equal("events", topics[0].Name);
        Assert.Equal(3, topics[0].Partitions);
        Assert.Equal(100, topics[0].MessageCount);
    }

    [Fact]
    public async Task ListTopicsAsync_EmptyArray()
    {
        var http = MockHttp(HttpStatusCode.OK, "[]");
        await using var admin = new AdminClient(http);

        var topics = await admin.ListTopicsAsync();

        Assert.Empty(topics);
    }

    // =========================================================================
    // DescribeTopic
    // =========================================================================

    [Fact]
    public async Task DescribeTopicAsync_ReturnsTopic()
    {
        var json = """{"name":"events","partitions":6,"replication_factor":3,"message_count":5000,"config":{"retention.ms":"86400000"}}""";
        var http = MockHttp(HttpStatusCode.OK, json);
        await using var admin = new AdminClient(http);

        var topic = await admin.DescribeTopicAsync("events");

        Assert.Equal("events", topic.Name);
        Assert.Equal(6, topic.Partitions);
        Assert.Equal(5000, topic.MessageCount);
        Assert.Equal("86400000", topic.Config!["retention.ms"]);
    }

    [Fact]
    public async Task DescribeTopicAsync_NotFound_ThrowsTopicNotFoundException()
    {
        var http = MockHttp(HttpStatusCode.NotFound, """{"error":"not found"}""");
        await using var admin = new AdminClient(http);

        await Assert.ThrowsAsync<StreamlineTopicNotFoundException>(
            () => admin.DescribeTopicAsync("nonexistent"));
    }

    // =========================================================================
    // CreateTopic
    // =========================================================================

    [Fact]
    public async Task CreateTopicAsync_SendsPostRequest()
    {
        HttpRequestMessage? captured = null;
        var http = MockHttp(req =>
        {
            captured = req;
            return new HttpResponseMessage(HttpStatusCode.Created)
            {
                Content = new StringContent("{}", Encoding.UTF8, "application/json")
            };
        });
        await using var admin = new AdminClient(http);

        await admin.CreateTopicAsync("new-topic", partitions: 3);

        Assert.NotNull(captured);
        Assert.Equal(HttpMethod.Post, captured!.Method);
        Assert.Contains("/v1/topics", captured.RequestUri!.ToString());
    }

    // =========================================================================
    // DeleteTopic
    // =========================================================================

    [Fact]
    public async Task DeleteTopicAsync_SendsDeleteRequest()
    {
        HttpRequestMessage? captured = null;
        var http = MockHttp(req =>
        {
            captured = req;
            return new HttpResponseMessage(HttpStatusCode.NoContent);
        });
        await using var admin = new AdminClient(http);

        await admin.DeleteTopicAsync("old-topic");

        Assert.Equal(HttpMethod.Delete, captured!.Method);
        Assert.Contains("old-topic", captured.RequestUri!.ToString());
    }

    // =========================================================================
    // ConsumerGroups
    // =========================================================================

    [Fact]
    public async Task ListConsumerGroupsAsync_ReturnsGroups()
    {
        var json = """[{"id":"group-1","state":"Stable","members":[{"id":"m1","client_id":"c1","host":"10.0.0.1"}]},{"id":"group-2","state":"Empty","members":[]}]""";
        var http = MockHttp(HttpStatusCode.OK, json);
        await using var admin = new AdminClient(http);

        var groups = await admin.ListConsumerGroupsAsync();

        Assert.Equal(2, groups.Count);
        Assert.Equal("group-1", groups[0].Id);
        Assert.Equal("Stable", groups[0].State);
        Assert.Equal("m1", Assert.Single(groups[0].Members!).Id);
        Assert.Empty(groups[1].Members!);
    }

    [Fact]
    public async Task DescribeConsumerGroupAsync_ReturnsDetails()
    {
        var json = """{"id":"cg-1","state":"Stable","members":[{"id":"m1","client_id":"c1","host":"10.0.0.1","assignments":["events-0"]}],"protocol":"range"}""";
        var http = MockHttp(HttpStatusCode.OK, json);
        await using var admin = new AdminClient(http);

        var group = await admin.DescribeConsumerGroupAsync("cg-1");

        Assert.Equal("cg-1", group.Id);
        Assert.Single(group.Members!);
        Assert.Equal("c1", group.Members![0].ClientId);
    }

    [Fact]
    public async Task DescribeConsumerGroupAsync_EscapesGroupIdPathSegment()
    {
        Uri? captured = null;
        var http = MockHttp(req =>
        {
            captured = req.RequestUri;
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"id":"group/with?reserved#chars","state":"Empty","members":[]}""",
                    Encoding.UTF8,
                    "application/json")
            };
        });
        await using var admin = new AdminClient(http);

        await admin.DescribeConsumerGroupAsync("group/with?reserved#chars");

        Assert.NotNull(captured);
        Assert.Equal(
            "/v1/consumer-groups/group%2Fwith%3Freserved%23chars",
            captured!.PathAndQuery);
    }

    [Fact]
    public async Task GetConsumerGroupTopicLagAsync_EscapesGroupIdPathSegment()
    {
        Uri? captured = null;
        var http = MockHttp(req =>
        {
            captured = req.RequestUri;
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(
                    """{"group_id":"group/with?reserved#chars","partitions":[],"total_lag":0}""",
                    Encoding.UTF8,
                    "application/json")
            };
        });
        await using var admin = new AdminClient(http);

        await admin.GetConsumerGroupTopicLagAsync(
            "group/with?reserved#chars",
            "orders.v1");

        Assert.NotNull(captured);
        Assert.Equal(
            "/v1/consumer-groups/group%2Fwith%3Freserved%23chars/lag/orders.v1",
            captured!.PathAndQuery);
    }

    [Fact]
    public async Task ConsumerGroupOperations_RejectWhitespaceGroupId()
    {
        var http = MockHttp(HttpStatusCode.OK, "{}");
        await using var admin = new AdminClient(http);

        await Assert.ThrowsAsync<ArgumentException>(
            () => admin.GetConsumerGroupLagAsync(" "));
    }

    // =========================================================================
    // Query
    // =========================================================================

    [Fact]
    public async Task QueryAsync_ReturnsResults()
    {
        var json = """{"columns":["key","value"],"rows":[["k1","v1"],["k2","v2"]],"row_count":2}""";
        var http = MockHttp(HttpStatusCode.OK, json);
        await using var admin = new AdminClient(http);

        var result = await admin.QueryAsync("SELECT * FROM events");

        Assert.Equal(2, result.Columns.Count);
        Assert.Equal(2, result.Rows.Count);
        Assert.Equal("k1", result.Rows[0][0]);
        Assert.Equal(2, result.RowCount);
    }

    [Fact]
    public async Task QueryAsync_EmptyResult()
    {
        var json = """{"columns":["key"],"rows":[],"row_count":0}""";
        var http = MockHttp(HttpStatusCode.OK, json);
        await using var admin = new AdminClient(http);

        var result = await admin.QueryAsync("SELECT * FROM empty");

        Assert.Empty(result.Rows);
        Assert.Equal(0, result.RowCount);
    }

    // =========================================================================
    // ServerInfo
    // =========================================================================

    [Fact]
    public async Task GetServerInfoAsync_ReturnsInfo()
    {
        var json = """{"version":"0.2.0","uptime":3600,"topic_count":5,"message_count":10000}""";
        var http = MockHttp(HttpStatusCode.OK, json);
        await using var admin = new AdminClient(http);

        var info = await admin.GetServerInfoAsync();

        Assert.Equal("0.2.0", info.Version);
        Assert.Equal(3600, info.Uptime);
        Assert.Equal(5, info.TopicCount);
    }

    // =========================================================================
    // Health
    // =========================================================================

    [Fact]
    public async Task IsHealthyAsync_ReturnsTrueOnSuccess()
    {
        var http = MockHttp(HttpStatusCode.OK, "ok");
        await using var admin = new AdminClient(http);

        Assert.True(await admin.IsHealthyAsync());
    }

    [Fact]
    public async Task IsHealthyAsync_ReturnsFalseOnError()
    {
        var http = MockHttp(HttpStatusCode.ServiceUnavailable, "");
        await using var admin = new AdminClient(http);

        Assert.False(await admin.IsHealthyAsync());
    }

    // =========================================================================
    // Error handling
    // =========================================================================

    [Fact]
    public async Task Unauthorized_ThrowsAuthenticationException()
    {
        var http = MockHttp(HttpStatusCode.Unauthorized, """{"error":"bad token"}""");
        await using var admin = new AdminClient(http);

        var ex = await Assert.ThrowsAsync<StreamlineAuthenticationException>(
            () => admin.ListTopicsAsync());
        Assert.Contains("Unauthorized", ex.Message);
    }

    [Fact]
    public async Task Forbidden_ThrowsAuthorizationException()
    {
        var http = MockHttp(HttpStatusCode.Forbidden, """{"error":"no access"}""");
        await using var admin = new AdminClient(http);

        await Assert.ThrowsAsync<StreamlineAuthorizationException>(
            () => admin.ListTopicsAsync());
    }

    [Fact]
    public async Task ServerError_ThrowsStreamlineException()
    {
        var http = MockHttp(HttpStatusCode.InternalServerError, """{"error":"boom"}""");
        await using var admin = new AdminClient(http);

        var ex = await Assert.ThrowsAsync<StreamlineException>(
            () => admin.ListTopicsAsync());
        Assert.Contains("500", ex.Message);
    }

    // =========================================================================
    // Auth header
    // =========================================================================

    [Fact]
    public async Task AuthToken_SentInHeader()
    {
        string? authHeader = null;
        var http = MockHttp(req =>
        {
            authHeader = req.Headers.Authorization?.ToString();
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent("[]", Encoding.UTF8, "application/json")
            };
        });
        http.DefaultRequestHeaders.Authorization =
            new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", "my-token");
        await using var admin = new AdminClient(http);

        await admin.ListTopicsAsync();

        Assert.Equal("Bearer my-token", authHeader);
    }

    // =========================================================================
    // Dispose / HttpClient ownership
    //
    // AdminClient has two families of constructors:
    //   - AdminClient(httpBaseUrl, ...)  -- creates its own HttpClient and must own
    //     (dispose) it, since nothing else can hold a reference to it.
    //   - AdminClient(httpClient)        -- receives a caller-supplied HttpClient (DI,
    //     testing, or a shared IHttpClientFactory-managed client) and must NOT dispose
    //     it, since the caller (or DI container) owns its lifetime.
    //
    // These tests exercise ownership purely through the public API: HttpClient throws
    // ObjectDisposedException from SendAsync as soon as it is disposed, *before* any
    // network I/O is attempted, so asserting on that exception is both a black-box and
    // a hermetic way to prove disposal without reflection or a live connection.
    // =========================================================================

    [Fact]
    public async Task DisposeAsync_DoesNotThrow()
    {
        var http = MockHttp(HttpStatusCode.OK, "[]");
        var admin = new AdminClient(http);
        await admin.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_SelfCreatedHttpClient_ThreeArgConstructor_IsDisposed()
    {
        // AdminClient(httpBaseUrl, authToken, timeout) creates and must own its HttpClient.
        var admin = new AdminClient(StreamlineTestEnvironment.UnitHttpBaseUrl, authToken: null, TimeSpan.FromSeconds(1));

        await admin.DisposeAsync();

        // A disposed HttpClient throws ObjectDisposedException synchronously from
        // SendAsync before attempting any network I/O, so this assertion never
        // touches the network even though UnitHttpBaseUrl cannot resolve.
        await Assert.ThrowsAsync<ObjectDisposedException>(() => admin.ListTopicsAsync());
    }

    [Fact]
    public async Task DisposeAsync_SelfCreatedHttpClient_TwoArgConstructor_IsDisposed()
    {
        // AdminClient(httpBaseUrl, authToken) is the overload used by
        // AddStreamlineAdmin(httpBaseUrl, authToken) and must also own its HttpClient.
        var admin = new AdminClient(StreamlineTestEnvironment.UnitHttpBaseUrl, authToken: "token");

        await admin.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => admin.ListTopicsAsync());
    }

    [Fact]
    public async Task DisposeAsync_SelfCreatedHttpClient_SingleArgConstructor_IsDisposed()
    {
        // AdminClient(httpBaseUrl) with all defaults must also own its HttpClient.
        var admin = new AdminClient(StreamlineTestEnvironment.UnitHttpBaseUrl);

        await admin.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => admin.ListTopicsAsync());
    }

    [Fact]
    public async Task DisposeAsync_InjectedHttpClient_IsNotDisposed()
    {
        // AdminClient(httpClient) must never dispose a caller-supplied HttpClient:
        // the caller (DI container, IHttpClientFactory, or test) owns its lifetime.
        var http = MockHttp(HttpStatusCode.OK, "[]");
        var admin = new AdminClient(http);

        await admin.DisposeAsync();

        // If the injected client had been disposed, this would throw
        // ObjectDisposedException instead of completing successfully.
        var topics = await admin.ListTopicsAsync();
        Assert.Empty(topics);

        // The caller can keep using the same HttpClient instance directly too.
        var response = await http.GetAsync(new Uri("/v1/topics", UriKind.Relative));
        Assert.True(response.IsSuccessStatusCode);
    }

    [Fact]
    public async Task DisposeAsync_InjectedHttpClient_SharedAcrossMultipleAdminClients_NotDoubleDisposedOrLeaked()
    {
        // A single DI-managed HttpClient can legitimately back more than one
        // AdminClient (e.g. re-resolved per scope). Disposing one AdminClient must
        // not affect the shared client or a sibling AdminClient using it.
        var http = MockHttp(HttpStatusCode.OK, "[]");
        var first = new AdminClient(http);
        var second = new AdminClient(http);

        await first.DisposeAsync();

        // The second AdminClient (and the underlying shared client) must remain usable.
        Assert.Empty(await second.ListTopicsAsync());

        await second.DisposeAsync();
        Assert.Empty(await new AdminClient(http).ListTopicsAsync());
    }

    [Fact]
    public async Task DisposeAsync_SelfCreatedHttpClient_CalledTwice_DoesNotThrow()
    {
        var admin = new AdminClient(StreamlineTestEnvironment.UnitHttpBaseUrl);

        await admin.DisposeAsync();
        await admin.DisposeAsync();
    }

    // =========================================================================
    // Model defaults
    // =========================================================================

    [Fact]
    public void TopicMetadata_DefaultValues()
    {
        var meta = new TopicMetadata();
        Assert.Equal("", meta.Name);
        Assert.Equal(0, meta.Partitions);
        Assert.Equal(0, meta.MessageCount);
        Assert.Null(meta.Config);
    }

    [Fact]
    public void QueryResult_DefaultValues()
    {
        var result = new QueryResult();
        Assert.Empty(result.Columns);
        Assert.Empty(result.Rows);
        Assert.Equal(0, result.RowCount);
    }

    [Fact]
    public void ServerInfo_DefaultValues()
    {
        var info = new ServerInfo();
        Assert.Equal("", info.Version);
        Assert.Equal(0, info.Uptime);
    }

    // =========================================================================
    // Mock Handlers
    // =========================================================================

    private class MockHttpHandler : HttpMessageHandler
    {
        private readonly HttpStatusCode _status;
        private readonly string _body;

        public MockHttpHandler(HttpStatusCode status, string body)
        {
            _status = status;
            _body = body;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return Task.FromResult(new HttpResponseMessage(_status)
            {
                Content = new StringContent(_body, Encoding.UTF8, "application/json")
            });
        }
    }

    private class DelegatingMockHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _handler;

        public DelegatingMockHandler(Func<HttpRequestMessage, HttpResponseMessage> handler)
        {
            _handler = handler;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return Task.FromResult(_handler(request));
        }
    }
}
