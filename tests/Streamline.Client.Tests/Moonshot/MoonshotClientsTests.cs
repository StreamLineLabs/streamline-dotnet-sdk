// SPDX-License-Identifier: Apache-2.0
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using Streamline.Client.Moonshot;
using Xunit;

namespace Streamline.Client.Tests.Moonshot;

public class MoonshotClientsTests
{
    private static HttpClient MockHttp(HttpStatusCode status, string body, RequestRecorder? rec = null)
    {
        var handler = new RecordingMockHandler(_ => new HttpResponseMessage(status)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        }, rec);
        return new HttpClient(handler) { BaseAddress = new Uri("http://localhost:9094") };
    }

    private sealed class RequestRecorder
    {
        public HttpRequestMessage? Request { get; set; }
        public string? Body { get; set; }
    }

    private sealed class RecordingMockHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _handler;
        private readonly RequestRecorder? _rec;

        public RecordingMockHandler(Func<HttpRequestMessage, HttpResponseMessage> handler, RequestRecorder? rec)
        {
            _handler = handler;
            _rec = rec;
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (_rec is not null)
            {
                _rec.Request = request;
                if (request.Content is not null)
                {
                    _rec.Body = await request.Content.ReadAsStringAsync(cancellationToken);
                }
            }
            return _handler(request);
        }
    }

    // -- Branches --

    [Fact]
    public async Task Branches_Create_ReturnsView()
    {
        var rec = new RequestRecorder();
        var http = MockHttp(HttpStatusCode.OK,
            """{"id":"orders/exp-a","created_at_ms":1000,"message_count":0}""", rec);
        await using var c = new BranchAdminClient(http);

        var v = await c.CreateAsync("orders", "exp-a");

        Assert.Equal("orders/exp-a", v.Id);
        Assert.Equal(1000, v.CreatedAtMs);
        Assert.Equal(HttpMethod.Post, rec.Request!.Method);
        Assert.Equal("/api/v1/branches", rec.Request.RequestUri!.AbsolutePath);
    }

    [Fact]
    public async Task Branches_Create_RejectsEmpty()
    {
        var http = MockHttp(HttpStatusCode.OK, "{}");
        await using var c = new BranchAdminClient(http);

        await Assert.ThrowsAsync<ArgumentException>(
            () => c.CreateAsync("", "x"));
    }

    [Fact]
    public async Task Branches_Get_404_ThrowsHttpException()
    {
        var http = MockHttp(HttpStatusCode.NotFound, """{"error":"missing"}""");
        await using var c = new BranchAdminClient(http);

        var ex = await Assert.ThrowsAsync<MoonshotHttpException>(
            () => c.GetAsync("orders/missing"));
        Assert.Equal(404, ex.StatusCode);
    }

    [Fact]
    public async Task Branches_List_HandlesItemsWrapper()
    {
        var http = MockHttp(HttpStatusCode.OK,
            """{"items":[{"id":"a","created_at_ms":1,"message_count":0}]}""");
        await using var c = new BranchAdminClient(http);

        var list = await c.ListAsync();

        Assert.Single(list);
        Assert.Equal("a", list[0].Id);
    }

    // -- Contracts --

    [Fact]
    public async Task Contracts_Validate_200_ValidTrue()
    {
        var http = MockHttp(HttpStatusCode.OK, """{"schema_id":7}""");
        await using var c = new ContractsClient(http);

        var r = await c.ValidateAsync(
            new Dictionary<string, object?> { ["name"] = "c" },
            new Dictionary<string, object?> { ["id"] = "x" });

        Assert.True(r.Valid);
        Assert.Equal(7, r.SchemaId);
    }

    [Fact]
    public async Task Contracts_Validate_400_ReturnsFailures()
    {
        var http = MockHttp(HttpStatusCode.BadRequest,
            """{"schema_id":7,"errors":[{"field_path":"id","expected":"string","actual":"int"}]}""");
        await using var c = new ContractsClient(http);

        var r = await c.ValidateAsync(
            new Dictionary<string, object?> { ["name"] = "c" },
            new Dictionary<string, object?> { ["id"] = 1 });

        Assert.False(r.Valid);
        Assert.Single(r.Errors);
        Assert.Equal("id", r.Errors[0].FieldPath);
    }

    [Fact]
    public async Task Contracts_Validate_BytesSentAsValueString()
    {
        var rec = new RequestRecorder();
        var http = MockHttp(HttpStatusCode.OK, "{}", rec);
        await using var c = new ContractsClient(http);

        await c.ValidateAsync(new Dictionary<string, object?> { ["name"] = "c" },
            Encoding.UTF8.GetBytes("hi"));

        var doc = JsonDocument.Parse(rec.Body!);
        Assert.Equal("hi", doc.RootElement.GetProperty("value_string").GetString());
    }

    // -- Attestation --

    [Fact]
    public async Task Attest_Sign_ReturnsEnvelope()
    {
        var http = MockHttp(HttpStatusCode.OK,
            """{"key_id":"broker-0","algorithm":"ed25519","timestamp_ms":1,"payload_sha256":"x","signature_b64":"AAAA","header_name":"streamline-attest","header_value":"v"}""");
        await using var a = new AttestationClient(http);

        var s = await a.SignAsync(new SignParams("t", 0, 1, 1, Value: Encoding.UTF8.GetBytes("hi")));

        Assert.Equal("AAAA", s.SignatureB64);
        Assert.Equal("streamline-attest", s.HeaderName);
    }

    [Fact]
    public async Task Attest_Verify_ReturnsTrue()
    {
        var http = MockHttp(HttpStatusCode.OK, """{"valid":true}""");
        await using var a = new AttestationClient(http);

        var ok = await a.VerifyAsync(new VerifyParams("t", 0, 1, 1, "AAAA",
            Value: Encoding.UTF8.GetBytes("hi")));

        Assert.True(ok);
    }

    [Fact]
    public async Task Attest_Sign_RejectsBothValueForms()
    {
        var http = MockHttp(HttpStatusCode.OK, "{}");
        await using var a = new AttestationClient(http);

        await Assert.ThrowsAsync<ArgumentException>(
            () => a.SignAsync(new SignParams("t", 0, 1, 1,
                Value: new byte[] { 1 }, ValueString: "y")));
    }

    [Fact]
    public async Task Attest_Sign_BytesEncodedAsBase64()
    {
        var rec = new RequestRecorder();
        var http = MockHttp(HttpStatusCode.OK,
            """{"key_id":"k","algorithm":"ed25519","timestamp_ms":1,"payload_sha256":"x","signature_b64":"y","header_name":"streamline-attest","header_value":"v"}""", rec);
        await using var a = new AttestationClient(http);

        await a.SignAsync(new SignParams("t", 0, 1, 1, Value: Encoding.UTF8.GetBytes("hi")));

        var doc = JsonDocument.Parse(rec.Body!);
        Assert.Equal("aGk=", doc.RootElement.GetProperty("value_b64").GetString());
    }

    // -- Search --

    [Fact]
    public async Task Search_ParsesHitsAndTookMs()
    {
        var http = MockHttp(HttpStatusCode.OK,
            """{"hits":[{"partition":1,"offset":5,"score":0.9}],"took_ms":12}""");
        await using var c = new SemanticSearchClient(http);

        var r = await c.SearchAsync("logs", "payment failure", new SearchOptions(K: 5));

        Assert.Equal(12, r.TookMs);
        Assert.Single(r.Hits);
        Assert.Equal(0.9, r.Hits[0].Score, 9);
    }

    [Fact]
    public async Task Search_RejectsEmptyOrLargeK()
    {
        var http = MockHttp(HttpStatusCode.OK, "{}");
        await using var c = new SemanticSearchClient(http);

        await Assert.ThrowsAsync<ArgumentException>(
            () => c.SearchAsync("t", ""));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => c.SearchAsync("t", "q", new SearchOptions(K: 1001)));
    }

    // -- Memory --

    [Fact]
    public async Task Memory_Remember_ReturnsEntries()
    {
        var http = MockHttp(HttpStatusCode.OK,
            """{"written":[{"topic":"a-ep","offset":1},{"topic":"a-sem","offset":2}]}""");
        await using var c = new MemoryClient(http);

        var out_ = await c.RememberAsync(new RememberParams("a", MemoryKind.Fact, "x", Salience: 0.8));

        Assert.Equal(2, out_.Count);
        Assert.Equal(2, out_[1].Offset);
    }

    [Fact]
    public async Task Memory_Procedure_RequiresSkill()
    {
        var http = MockHttp(HttpStatusCode.OK, "{}");
        await using var c = new MemoryClient(http);

        await Assert.ThrowsAsync<ArgumentException>(
            () => c.RememberAsync(new RememberParams("a", MemoryKind.Procedure, "x")));
    }

    [Fact]
    public async Task Memory_Recall_MapsTier()
    {
        var http = MockHttp(HttpStatusCode.OK,
            """{"hits":[{"tier":"semantic","topic":"a","offset":5,"content":"hi","score":0.7}]}""");
        await using var c = new MemoryClient(http);

        var hits = await c.RecallAsync(new RecallParams("a", "q"));

        Assert.Single(hits);
        Assert.Equal("semantic", hits[0].Tier);
        Assert.Equal(0.7, hits[0].Score, 9);
    }
}
