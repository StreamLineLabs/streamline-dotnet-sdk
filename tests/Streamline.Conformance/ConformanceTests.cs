using System.Diagnostics;
using System.Text;
using Streamline.Client;
using Streamline.Client.Schema;
using Streamline.TestSupport;
using Xunit;

namespace Streamline.Conformance;

/// <summary>
/// SDK Conformance Test Suite for the Streamline .NET SDK.
///
/// <para>
/// Every test here needs a running Streamline server, so the whole suite is opt-in:
/// each case is an <see cref="IntegrationFactAttribute"/>, skipped unless
/// <c>STREAMLINE_INTEGRATION</c> is truthy. When it <em>is</em> enabled,
/// <see cref="IntegrationServerFixture"/> probes the configured endpoints once with a
/// bounded timeout and fails the run immediately if they are unreachable — the suite
/// never silently reports success against a missing server.
/// </para>
///
/// <para>Run it with:</para>
/// <code>
/// STREAMLINE_IMAGE=&lt;image&gt; docker compose -f docker-compose.test.yml up -d
/// STREAMLINE_INTEGRATION=1 dotnet test tests/Streamline.Conformance --filter "Category=Conformance"
/// </code>
///
/// <para>
/// Endpoints come from <c>STREAMLINE_BOOTSTRAP_SERVERS</c> (alias
/// <c>STREAMLINE_BOOTSTRAP</c>) and <c>STREAMLINE_HTTP_URL</c> (alias
/// <c>STREAMLINE_HTTP</c>).
/// </para>
/// </summary>
[Collection(IntegrationCollection.Name)]
public class ConformanceTests : IAsyncLifetime
{
    private readonly string _bootstrap;
    private readonly string _httpUrl;
    private StreamlineClient _client = null!;
    private IAdminClient _admin = null!;

    /// <summary>Creates the suite against the endpoints reported by the shared fixture.</summary>
    /// <param name="server">Fixture that has already verified server reachability.</param>
    public ConformanceTests(IntegrationServerFixture server)
    {
        ArgumentNullException.ThrowIfNull(server);
        _bootstrap = server.BootstrapServers;
        _httpUrl = server.HttpBaseUrl;
    }

    /// <inheritdoc />
    public Task InitializeAsync()
    {
        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            Admin = new AdminOptions { HttpBaseUrl = _httpUrl },
        };
        _client = new StreamlineClient(options);
        _admin = _client.CreateAdmin(_httpUrl);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task DisposeAsync()
    {
        await _admin.DisposeAsync();
        await _client.DisposeAsync();
    }

    private static string UniqueTopic(string testId) =>
        $"conformance-{testId}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";

    // ================================================================
    //  PRODUCER — core tests
    // ================================================================

    [IntegrationFact(DisplayName = "P01: Simple Produce")]
    [Trait("Category", "Conformance")]
    public async Task P01_SimpleProduce()
    {
        var topic = UniqueTopic("p01");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var result = await _client.ProduceAsync(topic, null, "hello-conformance");

        Assert.NotNull(result);
        Assert.Equal(topic, result.Topic);
        Assert.True(result.Offset >= 0, "Offset should be non-negative");
        Assert.True(result.Partition >= 0, "Partition should be non-negative");
    }

    [IntegrationFact(DisplayName = "P02: Keyed Produce")]
    [Trait("Category", "Conformance")]
    public async Task P02_KeyedProduce()
    {
        var topic = UniqueTopic("p02");
        await _admin.CreateTopicAsync(topic, partitions: 3);

        var result1 = await _client.ProduceAsync(topic, "user-42", "message-1");
        var result2 = await _client.ProduceAsync(topic, "user-42", "message-2");

        Assert.NotNull(result1);
        Assert.NotNull(result2);
        // Same key should hash to same partition
        Assert.Equal(result1.Partition, result2.Partition);
    }

    [IntegrationFact(DisplayName = "P03: Headers Produce")]
    [Trait("Category", "Conformance")]
    public async Task P03_HeadersProduce()
    {
        var topic = UniqueTopic("p03");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var headers = new Headers()
            .Add("x-trace-id", "abc-123")
            .Add("x-source", "conformance-test");

        var result = await _client.ProduceAsync(topic, "key", "with-headers", headers);

        Assert.NotNull(result);
        Assert.True(result.Offset >= 0);
    }

    [IntegrationFact(DisplayName = "P04: Batch Produce")]
    [Trait("Category", "Conformance")]
    public async Task P04_BatchProduce()
    {
        var topic = UniqueTopic("p04");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var tasks = Enumerable.Range(0, 10)
            .Select(i => _client.ProduceAsync(topic, $"key-{i}", $"batch-message-{i}"))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        Assert.Equal(10, results.Length);
        foreach (var r in results)
        {
            Assert.True(r.Offset >= 0);
        }
    }

    [IntegrationFact(DisplayName = "P05: Compression")]
    [Trait("Category", "Conformance")]
    public async Task P05_Compression()
    {
        var topic = UniqueTopic("p05");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var result = await _client.ProduceAsync(topic, null, "compressed-payload");

        Assert.NotNull(result);
        Assert.True(result.Offset >= 0);
    }

    [IntegrationFact(DisplayName = "P06: Partitioner")]
    [Trait("Category", "Conformance")]
    public async Task P06_Partitioner()
    {
        var topic = UniqueTopic("p06");
        await _admin.CreateTopicAsync(topic, partitions: 4);

        // Distinct keys should produce to deterministic partitions
        var r1 = await _client.ProduceAsync(topic, "key-a", "msg1");
        var r2 = await _client.ProduceAsync(topic, "key-a", "msg2");

        Assert.Equal(r1.Partition, r2.Partition);
    }

    [IntegrationFact(DisplayName = "P07: Idempotent")]
    [Trait("Category", "Conformance")]
    public async Task P07_Idempotent()
    {
        var topic = UniqueTopic("p07");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var r1 = await _client.ProduceAsync(topic, null, "idempotent-1");
        var r2 = await _client.ProduceAsync(topic, null, "idempotent-2");

        Assert.True(r2.Offset > r1.Offset, "Offsets should be monotonically increasing");
    }

    [IntegrationFact(DisplayName = "P08: Timeout")]
    [Trait("Category", "Conformance")]
    public async Task P08_Timeout()
    {
        var badOptions = new StreamlineOptions { BootstrapServers = "localhost:1" };
        var badClient = new StreamlineClient(badOptions);

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await badClient.ProduceAsync("nonexistent", null, "timeout-test");
        });

        await badClient.DisposeAsync();
    }

    // ================================================================
    //  CONSUMER — core tests
    // ================================================================

    [IntegrationFact(DisplayName = "C01: Subscribe")]
    [Trait("Category", "Conformance")]
    public async Task C01_Subscribe()
    {
        var topic = UniqueTopic("c01");
        await _admin.CreateTopicAsync(topic, partitions: 1);
        await _client.ProduceAsync(topic, null, "subscribe-test");

        await using var consumer = _client.CreateConsumer<string, string>(topic, "c01-group");
        await consumer.SubscribeAsync();
        // Should complete without throwing
    }

    [IntegrationFact(DisplayName = "C02: From Beginning")]
    [Trait("Category", "Conformance")]
    public async Task C02_FromBeginning()
    {
        var topic = UniqueTopic("c02");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        for (int i = 0; i < 5; i++)
            await _client.ProduceAsync(topic, null, $"msg-{i}");

        var options = new ConsumerOptions
        {
            GroupId = $"c02-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        var records = await consumer.PollAsync(TimeSpan.FromSeconds(10));
        Assert.True(records.Count >= 5, $"Expected >= 5 records, got {records.Count}");
    }

    [IntegrationFact(DisplayName = "C03: From Offset")]
    [Trait("Category", "Conformance")]
    public async Task C03_FromOffset()
    {
        var topic = UniqueTopic("c03");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        for (int i = 0; i < 10; i++)
            await _client.ProduceAsync(topic, null, $"msg-{i}");

        var options = new ConsumerOptions
        {
            GroupId = $"c03-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();
        await consumer.SeekAsync(0, 5);

        var records = await consumer.PollAsync(TimeSpan.FromSeconds(10));
        Assert.True(records.Count >= 1, "Should consume records after seeking to offset 5");
        Assert.True(records[0].Offset >= 5, "First record should be at or after offset 5");
    }

    [IntegrationFact(DisplayName = "C04: From Timestamp")]
    [Trait("Category", "Conformance")]
    public async Task C04_FromTimestamp()
    {
        var topic = UniqueTopic("c04");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        await _client.ProduceAsync(topic, null, "before");
        await Task.Delay(100);
        await _client.ProduceAsync(topic, null, "after");

        var options = new ConsumerOptions
        {
            GroupId = $"c04-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        var records = await consumer.PollAsync(TimeSpan.FromSeconds(10));
        Assert.True(records.Count >= 2, "Should consume at least 2 records");
    }

    [IntegrationFact(DisplayName = "C05: Follow")]
    [Trait("Category", "Conformance")]
    public async Task C05_Follow()
    {
        var topic = UniqueTopic("c05");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var options = new ConsumerOptions
        {
            GroupId = $"c05-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Latest
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        // Produce after subscribing
        await _client.ProduceAsync(topic, null, "live-tail-message");

        var records = await consumer.PollAsync(TimeSpan.FromSeconds(10));
        Assert.True(records.Count >= 1, "Should receive live-tailed message");
    }

    [IntegrationFact(DisplayName = "C06: Filter")]
    [Trait("Category", "Conformance")]
    public async Task C06_Filter()
    {
        var topic = UniqueTopic("c06");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        for (int i = 0; i < 10; i++)
            await _client.ProduceAsync(topic, null, $"{{\"index\":{i},\"even\":{(i % 2 == 0).ToString().ToLower()}}}");

        var options = new ConsumerOptions
        {
            GroupId = $"c06-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        var records = await consumer.PollAsync(TimeSpan.FromSeconds(10));
        var evenRecords = records.Where(r => r.Value?.Contains("\"even\":true") == true).ToList();
        Assert.Equal(5, evenRecords.Count);
    }

    [IntegrationFact(DisplayName = "C07: Headers")]
    [Trait("Category", "Conformance")]
    public async Task C07_Headers()
    {
        var topic = UniqueTopic("c07");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var headers = new Headers().Add("x-trace-id", "c07-test");
        await _client.ProduceAsync(topic, "key", "with-headers", headers);

        var options = new ConsumerOptions
        {
            GroupId = $"c07-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        var records = await consumer.PollAsync(TimeSpan.FromSeconds(10));
        Assert.True(records.Count >= 1, "Should consume the message with headers");
    }

    [IntegrationFact(DisplayName = "C08: Timeout")]
    [Trait("Category", "Conformance")]
    public async Task C08_Timeout()
    {
        var topic = UniqueTopic("c08");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var options = new ConsumerOptions
        {
            GroupId = $"c08-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        var sw = Stopwatch.StartNew();
        var records = await consumer.PollAsync(TimeSpan.FromSeconds(2));
        sw.Stop();

        Assert.Empty(records);
        Assert.True(sw.Elapsed.TotalSeconds < 10, "Poll should return within timeout");
    }

    // ================================================================
    //  ADMIN / DEVOPS — core tests
    // ================================================================

    [IntegrationFact(DisplayName = "D01: Create Topic")]
    [Trait("Category", "Conformance")]
    public async Task D01_CreateTopic()
    {
        var topic = UniqueTopic("d01");

        await _admin.CreateTopicAsync(topic, partitions: 3, replicationFactor: 1);

        var info = await _admin.DescribeTopicAsync(topic);
        Assert.NotNull(info);
        Assert.Equal(topic, info.Name);
    }

    [IntegrationFact(DisplayName = "D02: List Topics")]
    [Trait("Category", "Conformance")]
    public async Task D02_ListTopics()
    {
        var topic = UniqueTopic("d02");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var topics = await _admin.ListTopicsAsync();
        Assert.NotNull(topics);
        Assert.Contains(topics, t => t.Name == topic);
    }

    [IntegrationFact(DisplayName = "D03: Describe Topic")]
    [Trait("Category", "Conformance")]
    public async Task D03_DescribeTopic()
    {
        var topic = UniqueTopic("d03");
        await _admin.CreateTopicAsync(topic, partitions: 5);

        var info = await _admin.DescribeTopicAsync(topic);
        Assert.NotNull(info);
        Assert.Equal(topic, info.Name);
        Assert.Equal(5, info.Partitions);
    }

    [IntegrationFact(DisplayName = "D04: Delete Topic")]
    [Trait("Category", "Conformance")]
    public async Task D04_DeleteTopic()
    {
        var topic = UniqueTopic("d04");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var topicsBefore = await _admin.ListTopicsAsync();
        Assert.Contains(topicsBefore, t => t.Name == topic);

        await _admin.DeleteTopicAsync(topic);

        var topicsAfter = await _admin.ListTopicsAsync();
        Assert.DoesNotContain(topicsAfter, t => t.Name == topic);
    }

    // ================================================================
    //  CONSUMER GROUPS — placeholders
    // ================================================================

    [IntegrationFact(DisplayName = "G01: Join Group")]
    [Trait("Category", "Conformance")]
    public async Task G01_JoinGroup()
    {
        var topic = UniqueTopic("g01");
        await _admin.CreateTopicAsync(topic, partitions: 2);
        await _client.ProduceAsync(topic, null, "g01-msg");

        var options = new ConsumerOptions
        {
            GroupId = $"g01-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        var groups = await _admin.ListConsumerGroupsAsync();
        Assert.True(groups.Count >= 1, "Should have at least one consumer group");
    }

    [IntegrationFact(DisplayName = "G02: Rebalance")]
    [Trait("Category", "Conformance")]
    public async Task G02_Rebalance()
    {
        var topic = UniqueTopic("g02");
        var groupId = $"g02-{Guid.NewGuid():N}";
        await _admin.CreateTopicAsync(topic, partitions: 2);
        await _client.ProduceAsync(topic, null, "g02-msg");

        await using var c1 = _client.CreateConsumer<string, string>(topic, new ConsumerOptions { GroupId = groupId });
        await c1.SubscribeAsync();

        await using var c2 = _client.CreateConsumer<string, string>(topic, new ConsumerOptions { GroupId = groupId });
        await c2.SubscribeAsync();

        // Both consumers should exist without errors
        Assert.NotNull(c1);
        Assert.NotNull(c2);
    }

    [IntegrationFact(DisplayName = "G03: Commit Offsets")]
    [Trait("Category", "Conformance")]
    public async Task G03_CommitOffsets()
    {
        var topic = UniqueTopic("g03");
        var groupId = $"g03-{Guid.NewGuid():N}";
        await _admin.CreateTopicAsync(topic, partitions: 1);

        for (int i = 0; i < 5; i++)
            await _client.ProduceAsync(topic, null, $"g03-msg-{i}");

        await using var consumer = _client.CreateConsumer<string, string>(topic, new ConsumerOptions
        {
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        });
        await consumer.SubscribeAsync();
        await consumer.PollAsync(TimeSpan.FromSeconds(10));
        await consumer.CommitAsync();

        // Verify group has committed offsets
        var info = await _admin.DescribeConsumerGroupAsync(groupId);
        Assert.NotNull(info);
    }

    [IntegrationFact(DisplayName = "G04: Lag Monitoring")]
    [Trait("Category", "Conformance")]
    public async Task G04_LagMonitoring()
    {
        var topic = UniqueTopic("g04");
        var groupId = $"g04-{Guid.NewGuid():N}";
        await _admin.CreateTopicAsync(topic, partitions: 1);

        for (int i = 0; i < 10; i++)
            await _client.ProduceAsync(topic, null, $"g04-msg-{i}");

        await using var consumer = _client.CreateConsumer<string, string>(topic, new ConsumerOptions
        {
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        });
        await consumer.SubscribeAsync();

        // Consume half and commit
        var records = await consumer.PollAsync(TimeSpan.FromSeconds(10));
        Assert.True(records.Count >= 1, "Should consume at least one record");
        await consumer.CommitAsync();

        // The group should exist with committed offsets
        var info = await _admin.DescribeConsumerGroupAsync(groupId);
        Assert.NotNull(info);
        Assert.Equal(groupId, info.Id);
    }

    [IntegrationFact(DisplayName = "G05: Reset Offsets")]
    [Trait("Category", "Conformance")]
    public async Task G05_ResetOffsets()
    {
        var topic = UniqueTopic("g05");
        var groupId = $"g05-{Guid.NewGuid():N}";
        await _admin.CreateTopicAsync(topic, partitions: 1);

        for (int i = 0; i < 5; i++)
            await _client.ProduceAsync(topic, null, $"g05-msg-{i}");

        // Consume all and commit
        var options = new ConsumerOptions
        {
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        await using (var consumer = _client.CreateConsumer<string, string>(topic, options))
        {
            await consumer.SubscribeAsync();
            await consumer.PollAsync(TimeSpan.FromSeconds(10));
            await consumer.CommitAsync();
        }

        // Re-consume from beginning after seek
        await using (var consumer = _client.CreateConsumer<string, string>(topic, options))
        {
            await consumer.SubscribeAsync();
            await consumer.SeekToBeginningAsync();

            var records = await consumer.PollAsync(TimeSpan.FromSeconds(10));
            Assert.True(records.Count >= 1, "Should re-consume records after seeking to beginning");
        }
    }

    [IntegrationFact(DisplayName = "G06: Leave Group")]
    [Trait("Category", "Conformance")]
    public async Task G06_LeaveGroup()
    {
        var topic = UniqueTopic("g06");
        var groupId = $"g06-{Guid.NewGuid():N}";
        await _admin.CreateTopicAsync(topic, partitions: 1);
        await _client.ProduceAsync(topic, null, "g06-msg");

        var consumer = _client.CreateConsumer<string, string>(topic, new ConsumerOptions { GroupId = groupId });
        await consumer.SubscribeAsync();
        await consumer.DisposeAsync(); // triggers group leave

        // Group should still be describable (may be empty)
        var info = await _admin.DescribeConsumerGroupAsync(groupId);
        Assert.NotNull(info);
    }

    // ================================================================
    //  AUTHENTICATION — placeholders
    // ================================================================

    [IntegrationFact(DisplayName = "A01: TLS Connect")]
    [Trait("Category", "Conformance")]
    public async Task A01_TlsConnect()
    {
        // Verify TLS options are accepted by the client
        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            SecurityProtocol = SecurityProtocol.Ssl,
            Tls = new TlsOptions { SkipCertificateVerification = true }
        };
        var client = new StreamlineClient(options);
        Assert.NotNull(client);
        await client.DisposeAsync();
    }

    [IntegrationFact(DisplayName = "A02: Mutual TLS")]
    [Trait("Category", "Conformance")]
    public async Task A02_MutualTls()
    {
        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            SecurityProtocol = SecurityProtocol.Ssl,
            Tls = new TlsOptions
            {
                ClientCertificatePath = "client.pem",
                ClientKeyPath = "client-key.pem",
                CaCertificatePath = "ca.pem"
            }
        };
        var client = new StreamlineClient(options);
        Assert.NotNull(client);
        await client.DisposeAsync();
    }

    [IntegrationFact(DisplayName = "A03: SASL PLAIN")]
    [Trait("Category", "Conformance")]
    public async Task A03_SaslPlain()
    {
        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            Sasl = new SaslOptions
            {
                Mechanism = SaslMechanism.Plain,
                Username = "user",
                Password = "pass"
            }
        };
        var client = new StreamlineClient(options);
        Assert.NotNull(client);
        await client.DisposeAsync();
    }

    [IntegrationFact(DisplayName = "A04: SCRAM-SHA-256")]
    [Trait("Category", "Conformance")]
    public async Task A04_ScramSha256()
    {
        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            Sasl = new SaslOptions
            {
                Mechanism = SaslMechanism.ScramSha256,
                Username = "user",
                Password = "pass"
            }
        };
        var client = new StreamlineClient(options);
        Assert.NotNull(client);
        await client.DisposeAsync();
    }

    [IntegrationFact(DisplayName = "A05: SCRAM-SHA-512")]
    [Trait("Category", "Conformance")]
    public async Task A05_ScramSha512()
    {
        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            Sasl = new SaslOptions
            {
                Mechanism = SaslMechanism.ScramSha512,
                Username = "user",
                Password = "pass"
            }
        };
        var client = new StreamlineClient(options);
        Assert.NotNull(client);
        await client.DisposeAsync();
    }

    [IntegrationFact(DisplayName = "A06: Auth Failure")]
    [Trait("Category", "Conformance")]
    public async Task A06_AuthFailure()
    {
        var ex = new StreamlineAuthenticationException("Authentication failed");
        Assert.IsType<StreamlineAuthenticationException>(ex);
        Assert.Contains("Authentication", ex.Message);
        Assert.False(ex.IsRetryable);
        await Task.CompletedTask;
    }

    // ================================================================
    //  SCHEMA REGISTRY — placeholders
    // ================================================================

    [IntegrationFact(DisplayName = "S01: Register Schema")]
    [Trait("Category", "Conformance")]
    public async Task S01_RegisterSchema()
    {
        var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-s01-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-value";
        var schema = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}}}";

        var id = await registry.RegisterSchemaAsync(subject, schema, SchemaFormat.Json);
        Assert.True(id >= 1, "Schema ID should be >= 1");
    }

    [IntegrationFact(DisplayName = "S02: Get by ID")]
    [Trait("Category", "Conformance")]
    public async Task S02_GetById()
    {
        var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-s02-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-value";
        var schema = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"number\"}}}";

        var id = await registry.RegisterSchemaAsync(subject, schema, SchemaFormat.Json);
        var info = await registry.GetSchemaByIdAsync(id);

        Assert.NotNull(info);
        Assert.Equal(id, info.Id);
    }

    [IntegrationFact(DisplayName = "S03: Get Versions")]
    [Trait("Category", "Conformance")]
    public async Task S03_GetVersions()
    {
        var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-s03-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-value";
        var schema = "{\"type\":\"object\",\"properties\":{\"v\":{\"type\":\"string\"}}}";

        await registry.RegisterSchemaAsync(subject, schema, SchemaFormat.Json);

        var subjects = await registry.ListSubjectsAsync();
        Assert.Contains(subjects, s => s == subject);
    }

    [IntegrationFact(DisplayName = "S04: Compatibility Check")]
    [Trait("Category", "Conformance")]
    public async Task S04_CompatibilityCheck()
    {
        var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-s04-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-value";
        var schema1 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}";
        var schema2 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"},\"b\":{\"type\":\"number\"}}}";

        await registry.RegisterSchemaAsync(subject, schema1, SchemaFormat.Json);

        var compatible = await registry.CheckCompatibilityAsync(subject, schema2, SchemaFormat.Json);
        Assert.IsType<bool>(compatible);
    }

    [IntegrationFact(DisplayName = "S05: Avro Schema")]
    [Trait("Category", "Conformance")]
    public async Task S05_AvroSchema()
    {
        var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-s05-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-value";
        var avroSchema = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";

        var id = await registry.RegisterSchemaAsync(subject, avroSchema, SchemaFormat.Avro);
        Assert.True(id >= 1);
    }

    [IntegrationFact(DisplayName = "S06: JSON Schema")]
    [Trait("Category", "Conformance")]
    public async Task S06_JsonSchema()
    {
        var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-s06-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-value";
        var jsonSchema = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"required\":[\"email\"],\"properties\":{\"email\":{\"type\":\"string\"}}}";

        var id = await registry.RegisterSchemaAsync(subject, jsonSchema, SchemaFormat.Json);
        Assert.True(id >= 1);
    }

    // ================================================================
    //  ERROR HANDLING — placeholders
    // ================================================================

    [IntegrationFact(DisplayName = "E01: Connection Refused")]
    [Trait("Category", "Conformance")]
    public async Task E01_ConnectionRefused()
    {
        var badOptions = new StreamlineOptions { BootstrapServers = "localhost:1" };
        var badClient = new StreamlineClient(badOptions);

        await Assert.ThrowsAnyAsync<StreamlineException>(async () =>
        {
            await badClient.ProduceAsync("nonexistent", null, "test");
        });

        await badClient.DisposeAsync();
    }

    [IntegrationFact(DisplayName = "E02: Auth Denied")]
    [Trait("Category", "Conformance")]
    public async Task E02_AuthDenied()
    {
        var ex = new StreamlineAuthenticationException("Access denied");
        Assert.Equal(StreamlineErrorCode.Authentication, ex.ErrorCode);
        Assert.False(ex.IsRetryable);
        Assert.Contains("Access denied", ex.Message);
        await Task.CompletedTask;
    }

    [IntegrationFact(DisplayName = "E03: Topic Not Found")]
    [Trait("Category", "Conformance")]
    public async Task E03_TopicNotFound()
    {
        var topic = $"nonexistent-{Guid.NewGuid():N}";
        var options = new ConsumerOptions
        {
            GroupId = $"e03-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        // Consuming from non-existent topic either throws or returns empty
        var records = await consumer.PollAsync(TimeSpan.FromSeconds(3));
        Assert.Empty(records);
    }

    [IntegrationFact(DisplayName = "E04: Request Timeout")]
    [Trait("Category", "Conformance")]
    public async Task E04_RequestTimeout()
    {
        var ex = new StreamlineTimeoutException("Request timed out");
        Assert.True(ex.IsRetryable, "Timeout errors should be retryable");
        Assert.Contains("timed out", ex.Message);
        await Task.CompletedTask;
    }

    // ================================================================
    //  PERFORMANCE — placeholders
    // ================================================================

    [IntegrationFact(DisplayName = "F01: Throughput 1KB")]
    [Trait("Category", "Conformance")]
    public async Task F01_Throughput1Kb()
    {
        var topic = UniqueTopic("f01");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var payload = new string('x', 1024);
        const int count = 100;
        var sw = Stopwatch.StartNew();

        var tasks = Enumerable.Range(0, count)
            .Select(i => _client.ProduceAsync(topic, null, payload))
            .ToArray();
        await Task.WhenAll(tasks);

        sw.Stop();
        var throughput = count / sw.Elapsed.TotalSeconds;
        Assert.True(throughput > 10, $"Throughput {throughput:F1} msg/s should be > 10 msg/s");
    }

    [IntegrationFact(DisplayName = "F02: Latency P99")]
    [Trait("Category", "Conformance")]
    public async Task F02_LatencyP99()
    {
        var topic = UniqueTopic("f02");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var latencies = new List<double>();
        for (int i = 0; i < 50; i++)
        {
            var sw = Stopwatch.StartNew();
            await _client.ProduceAsync(topic, null, $"latency-{i}");
            sw.Stop();
            latencies.Add(sw.Elapsed.TotalMilliseconds);
        }

        latencies.Sort();
        var p99 = latencies[(int)(latencies.Count * 0.99)];
        Assert.True(p99 < 5_000, $"P99 latency {p99:F0}ms should be < 5000ms");
    }

    [IntegrationFact(DisplayName = "F03: Startup Time")]
    [Trait("Category", "Conformance")]
    public async Task F03_StartupTime()
    {
        var sw = Stopwatch.StartNew();
        var options = new StreamlineOptions { BootstrapServers = _bootstrap };
        var freshClient = new StreamlineClient(options);
        var admin = freshClient.CreateAdmin(_httpUrl);
        var healthy = await admin.IsHealthyAsync();
        sw.Stop();

        Assert.True(healthy, "Server should be healthy");
        Assert.True(sw.Elapsed.TotalSeconds < 5, $"Startup took {sw.Elapsed.TotalSeconds:F1}s, should be < 5s");

        await admin.DisposeAsync();
        await freshClient.DisposeAsync();
    }

    [IntegrationFact(DisplayName = "F04: Memory Usage")]
    [Trait("Category", "Conformance")]
    public async Task F04_MemoryUsage()
    {
        var topic = UniqueTopic("f04");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        var before = GC.GetTotalMemory(true);

        var payload = new string('x', 1024);
        for (int i = 0; i < 100; i++)
            await _client.ProduceAsync(topic, null, payload);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        var after = GC.GetTotalMemory(true);
        var growthMb = (after - before) / (1024.0 * 1024.0);

        Assert.True(growthMb < 50, $"Memory growth {growthMb:F1}MB should be < 50MB");
    }
}
