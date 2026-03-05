using System.Net;
using System.Text;
using System.Text.Json;
using Streamline.Client;
using Xunit;

namespace Streamline.Client.Tests;

public class AdminClientTests
{
    // -- Helpers --

    private static HttpClient MockHttp(HttpStatusCode status, string body)
    {
        var handler = new MockHttpHandler(status, body);
        return new HttpClient(handler) { BaseAddress = new Uri("http://localhost:9094") };
    }

    private static HttpClient MockHttp(Func<HttpRequestMessage, HttpResponseMessage> handler)
    {
        return new HttpClient(new DelegatingMockHandler(handler)) { BaseAddress = new Uri("http://localhost:9094") };
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
        var json = """[{"id":"group-1","state":"Stable","members":["m1"]},{"id":"group-2","state":"Empty","members":[]}]""";
        var http = MockHttp(HttpStatusCode.OK, json);
        await using var admin = new AdminClient(http);

        var groups = await admin.ListConsumerGroupsAsync();

        Assert.Equal(2, groups.Count);
        Assert.Equal("group-1", groups[0].Id);
        Assert.Equal("Stable", groups[0].State);
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
    // Dispose
    // =========================================================================

    [Fact]
    public async Task DisposeAsync_DoesNotThrow()
    {
        var http = MockHttp(HttpStatusCode.OK, "[]");
        var admin = new AdminClient(http);
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
