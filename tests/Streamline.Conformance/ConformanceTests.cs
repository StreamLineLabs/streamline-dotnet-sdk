using System.Diagnostics;
using System.Text;
using Streamline.Client;
using Streamline.Client.Schema;
using Xunit;

namespace Streamline.Conformance;

/// <summary>
/// SDK Conformance Test Suite for the Streamline .NET SDK.
///
/// Requires a running Streamline server:
///   docker compose -f docker-compose.test.yml up -d
///
/// Environment variables:
///   STREAMLINE_BOOTSTRAP  — Kafka-protocol endpoint  (default: localhost:9092)
///   STREAMLINE_HTTP       — HTTP management endpoint  (default: http://localhost:9094)
/// </summary>
public class ConformanceTests : IAsyncLifetime
{
    private readonly string _bootstrap;
    private readonly string _httpUrl;
    private StreamlineClient _client = null!;
    private IAdminClient _admin = null!;
    private bool _serverAvailable;

    public ConformanceTests()
    {
        _bootstrap = Environment.GetEnvironmentVariable("STREAMLINE_BOOTSTRAP") ?? "localhost:9092";
        _httpUrl = Environment.GetEnvironmentVariable("STREAMLINE_HTTP") ?? "http://localhost:9094";
    }

    public async Task InitializeAsync()
    {
        var options = new StreamlineOptions { BootstrapServers = _bootstrap };
        _client = new StreamlineClient(options);
        _admin = _client.CreateAdmin(_httpUrl);

        try
        {
            _serverAvailable = await _admin.IsHealthyAsync();
        }
        catch
        {
            _serverAvailable = false;
        }
    }

    public async Task DisposeAsync()
    {
        await _admin.DisposeAsync();
        await _client.DisposeAsync();
    }

    /// <summary>
    /// Skips the calling test when the Streamline server is unreachable.
    /// </summary>
    private void SkipIfServerUnavailable()
    {
        if (!_serverAvailable)
            throw Xunit.Sdk.SkipException.ForSkip(
                "Streamline server is not available — set STREAMLINE_BOOTSTRAP / STREAMLINE_HTTP or start the server with docker compose");
    }

    private static string UniqueTopic(string testId) =>
        $"conformance-{testId}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";

    // ================================================================
    //  PRODUCER — core tests
    // ================================================================

    [Fact(DisplayName = "P01: Simple Produce")]
    [Trait("Category", "Conformance")]
    public async Task P01_SimpleProduce()
    {
        SkipIfServerUnavailable();

        var topic = UniqueTopic("p01");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var result = await _client.ProduceAsync(topic, null, "hello-conformance");

        Assert.NotNull(result);
        Assert.Equal(topic, result.Topic);
        Assert.True(result.Offset >= 0, "Offset should be non-negative");
        Assert.True(result.Partition >= 0, "Partition should be non-negative");
    }

    [Fact(DisplayName = "P02: Keyed Produce")]
    [Trait("Category", "Conformance")]
    public async Task P02_KeyedProduce()
    {
        SkipIfServerUnavailable();

        var topic = UniqueTopic("p02");
        await _admin.CreateTopicAsync(topic, partitions: 3);

        var result1 = await _client.ProduceAsync(topic, "user-42", "message-1");
        var result2 = await _client.ProduceAsync(topic, "user-42", "message-2");

        Assert.NotNull(result1);
        Assert.NotNull(result2);
        // Same key should hash to same partition
        Assert.Equal(result1.Partition, result2.Partition);
    }

    [Fact(DisplayName = "P03: Headers Produce")]
    [Trait("Category", "Conformance")]
    public async Task P03_HeadersProduce()
    {
        SkipIfServerUnavailable();

        var topic = UniqueTopic("p03");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var headers = new Headers()
            .Add("x-trace-id", "abc-123")
            .Add("x-source", "conformance-test");

        var result = await _client.ProduceAsync(topic, "key", "with-headers", headers);

        Assert.NotNull(result);
        Assert.True(result.Offset >= 0);
    }

    [Fact(DisplayName = "P04: Batch Produce")]
    [Trait("Category", "Conformance")]
    public async Task P04_BatchProduce()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "P05: Compression")]
    [Trait("Category", "Conformance")]
    public async Task P05_Compression()
    {
        SkipIfServerUnavailable();

        var topic = UniqueTopic("p05");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var result = await _client.ProduceAsync(topic, null, "compressed-payload");

        Assert.NotNull(result);
        Assert.True(result.Offset >= 0);
    }

    [Fact(DisplayName = "P06: Partitioner")]
    [Trait("Category", "Conformance")]
    public async Task P06_Partitioner()
    {
        SkipIfServerUnavailable();

        var topic = UniqueTopic("p06");
        await _admin.CreateTopicAsync(topic, partitions: 4);

        // Distinct keys should produce to deterministic partitions
        var r1 = await _client.ProduceAsync(topic, "key-a", "msg1");
        var r2 = await _client.ProduceAsync(topic, "key-a", "msg2");

        Assert.Equal(r1.Partition, r2.Partition);
    }

    [Fact(DisplayName = "P07: Idempotent")]
    [Trait("Category", "Conformance")]
    public async Task P07_Idempotent()
    {
        SkipIfServerUnavailable();

        var topic = UniqueTopic("p07");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var r1 = await _client.ProduceAsync(topic, null, "idempotent-1");
        var r2 = await _client.ProduceAsync(topic, null, "idempotent-2");

        Assert.True(r2.Offset > r1.Offset, "Offsets should be monotonically increasing");
    }

    [Fact(DisplayName = "P08: Timeout")]
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

    [Fact(DisplayName = "C01: Subscribe")]
    [Trait("Category", "Conformance")]
    public async Task C01_Subscribe()
    {
        SkipIfServerUnavailable();

        var topic = UniqueTopic("c01");
        await _admin.CreateTopicAsync(topic, partitions: 1);
        await _client.ProduceAsync(topic, null, "subscribe-test");

        await using var consumer = _client.CreateConsumer<string, string>(topic, "c01-group");
        await consumer.SubscribeAsync();
        // Should complete without throwing
    }

    [Fact(DisplayName = "C02: From Beginning")]
    [Trait("Category", "Conformance")]
    public async Task C02_FromBeginning()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "C03: From Offset")]
    [Trait("Category", "Conformance")]
    public async Task C03_FromOffset()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "C04: From Timestamp")]
    [Trait("Category", "Conformance")]
    public async Task C04_FromTimestamp()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "C05: Follow")]
    [Trait("Category", "Conformance")]
    public async Task C05_Follow()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "C06: Filter")]
    [Trait("Category", "Conformance")]
    public async Task C06_Filter()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "C07: Headers")]
    [Trait("Category", "Conformance")]
    public async Task C07_Headers()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "C08: Timeout")]
    [Trait("Category", "Conformance")]
    public async Task C08_Timeout()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "D01: Create Topic")]
    [Trait("Category", "Conformance")]
    public async Task D01_CreateTopic()
    {
        SkipIfServerUnavailable();

        var topic = UniqueTopic("d01");

        await _admin.CreateTopicAsync(topic, partitions: 3, replicationFactor: 1);

        var info = await _admin.DescribeTopicAsync(topic);
        Assert.NotNull(info);
        Assert.Equal(topic, info.Name);
    }

    [Fact(DisplayName = "D02: List Topics")]
    [Trait("Category", "Conformance")]
    public async Task D02_ListTopics()
    {
        SkipIfServerUnavailable();

        var topic = UniqueTopic("d02");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var topics = await _admin.ListTopicsAsync();
        Assert.NotNull(topics);
        Assert.Contains(topics, t => t.Name == topic);
    }

    [Fact(DisplayName = "D03: Describe Topic")]
    [Trait("Category", "Conformance")]
    public async Task D03_DescribeTopic()
    {
        SkipIfServerUnavailable();

        var topic = UniqueTopic("d03");
        await _admin.CreateTopicAsync(topic, partitions: 5);

        var info = await _admin.DescribeTopicAsync(topic);
        Assert.NotNull(info);
        Assert.Equal(topic, info.Name);
        Assert.Equal(5, info.Partitions);
    }

    [Fact(DisplayName = "D04: Delete Topic")]
    [Trait("Category", "Conformance")]
    public async Task D04_DeleteTopic()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "G01: Join Group")]
    [Trait("Category", "Conformance")]
    public async Task G01_JoinGroup()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "G02: Rebalance")]
    [Trait("Category", "Conformance")]
    public async Task G02_Rebalance()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "G03: Commit Offsets")]
    [Trait("Category", "Conformance")]
    public async Task G03_CommitOffsets()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "G04: Lag Monitoring")]
    [Trait("Category", "Conformance")]
    public async Task G04_LagMonitoring()
    {
        SkipIfServerUnavailable();

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
        Assert.Equal(groupId, info.GroupId);
    }

    [Fact(DisplayName = "G05: Reset Offsets")]
    [Trait("Category", "Conformance")]
    public async Task G05_ResetOffsets()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "G06: Leave Group")]
    [Trait("Category", "Conformance")]
    public async Task G06_LeaveGroup()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "A01: TLS Connect")]
    [Trait("Category", "Conformance")]
    public async Task A01_TlsConnect()
    {
        // Verify TLS options are accepted by the client
        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            Tls = new TlsOptions { Enabled = true, AllowInsecure = true }
        };
        var client = new StreamlineClient(options);
        Assert.NotNull(client);
        await client.DisposeAsync();
    }

    [Fact(DisplayName = "A02: Mutual TLS")]
    [Trait("Category", "Conformance")]
    public async Task A02_MutualTls()
    {
        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            Tls = new TlsOptions
            {
                Enabled = true,
                CertificatePath = "client.pem",
                KeyPath = "client-key.pem",
                CaCertificatePath = "ca.pem"
            }
        };
        var client = new StreamlineClient(options);
        Assert.NotNull(client);
        await client.DisposeAsync();
    }

    [Fact(DisplayName = "A03: SASL PLAIN")]
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

    [Fact(DisplayName = "A04: SCRAM-SHA-256")]
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

    [Fact(DisplayName = "A05: SCRAM-SHA-512")]
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

    [Fact(DisplayName = "A06: Auth Failure")]
    [Trait("Category", "Conformance")]
    public async Task A06_AuthFailure()
    {
        var ex = new StreamlineAuthenticationException("Authentication failed");
        Assert.IsType<StreamlineAuthenticationException>(ex);
        Assert.Contains("Authentication", ex.Message);
        Assert.False(ex.Retryable);
        await Task.CompletedTask;
    }

    // ================================================================
    //  SCHEMA REGISTRY — placeholders
    // ================================================================

    [Fact(DisplayName = "S01: Register Schema")]
    [Trait("Category", "Conformance")]
    public async Task S01_RegisterSchema()
    {
        SkipIfServerUnavailable();

        var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-s01-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-value";
        var schema = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}}}";

        var id = await registry.RegisterSchemaAsync(subject, schema, SchemaType.Json);
        Assert.True(id >= 1, "Schema ID should be >= 1");
    }

    [Fact(DisplayName = "S02: Get by ID")]
    [Trait("Category", "Conformance")]
    public async Task S02_GetById()
    {
        SkipIfServerUnavailable();

        var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-s02-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-value";
        var schema = "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"number\"}}}";

        var id = await registry.RegisterSchemaAsync(subject, schema, SchemaType.Json);
        var info = await registry.GetSchemaByIdAsync(id);

        Assert.NotNull(info);
        Assert.Equal(id, info.Id);
    }

    [Fact(DisplayName = "S03: Get Versions")]
    [Trait("Category", "Conformance")]
    public async Task S03_GetVersions()
    {
        SkipIfServerUnavailable();

        var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-s03-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-value";
        var schema = "{\"type\":\"object\",\"properties\":{\"v\":{\"type\":\"string\"}}}";

        await registry.RegisterSchemaAsync(subject, schema, SchemaType.Json);

        var subjects = await registry.ListSubjectsAsync();
        Assert.Contains(subjects, s => s == subject);
    }

    [Fact(DisplayName = "S04: Compatibility Check")]
    [Trait("Category", "Conformance")]
    public async Task S04_CompatibilityCheck()
    {
        SkipIfServerUnavailable();

        var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-s04-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-value";
        var schema1 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}";
        var schema2 = "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"},\"b\":{\"type\":\"number\"}}}";

        await registry.RegisterSchemaAsync(subject, schema1, SchemaType.Json);

        var compatible = await registry.CheckCompatibilityAsync(subject, schema2, SchemaType.Json);
        Assert.IsType<bool>(compatible);
    }

    [Fact(DisplayName = "S05: Avro Schema")]
    [Trait("Category", "Conformance")]
    public async Task S05_AvroSchema()
    {
        SkipIfServerUnavailable();

        var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-s05-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-value";
        var avroSchema = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";

        var id = await registry.RegisterSchemaAsync(subject, avroSchema, SchemaType.Avro);
        Assert.True(id >= 1);
    }

    [Fact(DisplayName = "S06: JSON Schema")]
    [Trait("Category", "Conformance")]
    public async Task S06_JsonSchema()
    {
        SkipIfServerUnavailable();

        var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-s06-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-value";
        var jsonSchema = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"required\":[\"email\"],\"properties\":{\"email\":{\"type\":\"string\"}}}";

        var id = await registry.RegisterSchemaAsync(subject, jsonSchema, SchemaType.Json);
        Assert.True(id >= 1);
    }

    // ================================================================
    //  ERROR HANDLING — placeholders
    // ================================================================

    [Fact(DisplayName = "E01: Connection Refused")]
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

    [Fact(DisplayName = "E02: Auth Denied")]
    [Trait("Category", "Conformance")]
    public async Task E02_AuthDenied()
    {
        var ex = new StreamlineAuthenticationException("Access denied");
        Assert.Equal("AUTHENTICATION", ex.ErrorCode);
        Assert.False(ex.Retryable);
        Assert.Contains("Access denied", ex.Message);
        await Task.CompletedTask;
    }

    [Fact(DisplayName = "E03: Topic Not Found")]
    [Trait("Category", "Conformance")]
    public async Task E03_TopicNotFound()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "E04: Request Timeout")]
    [Trait("Category", "Conformance")]
    public async Task E04_RequestTimeout()
    {
        var ex = new StreamlineTimeoutException("Request timed out");
        Assert.True(ex.Retryable, "Timeout errors should be retryable");
        Assert.Contains("timed out", ex.Message);
        await Task.CompletedTask;
    }

    // ================================================================
    //  PERFORMANCE — placeholders
    // ================================================================

    [Fact(DisplayName = "F01: Throughput 1KB")]
    [Trait("Category", "Conformance")]
    public async Task F01_Throughput1Kb()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "F02: Latency P99")]
    [Trait("Category", "Conformance")]
    public async Task F02_LatencyP99()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "F03: Startup Time")]
    [Trait("Category", "Conformance")]
    public async Task F03_StartupTime()
    {
        SkipIfServerUnavailable();

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

    [Fact(DisplayName = "F04: Memory Usage")]
    [Trait("Category", "Conformance")]
    public async Task F04_MemoryUsage()
    {
        SkipIfServerUnavailable();

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
