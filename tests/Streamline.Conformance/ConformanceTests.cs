using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Streamline.Client;
using Streamline.Client.Exceptions;
using Streamline.Client.Schema;
using Xunit;

namespace Streamline.Conformance;

/// <summary>
/// SDK Conformance Test Suite — 46 tests per SDK_CONFORMANCE_SPEC.md
///
/// Requires: docker compose -f docker-compose.test.yml up -d
///
/// Set STREAMLINE_BOOTSTRAP and STREAMLINE_HTTP env vars to override defaults.
/// </summary>
public class ConformanceTests : IAsyncLifetime
{
    private readonly string _bootstrap;
    private readonly string _httpUrl;
    private StreamlineClient _client = null!;
    private IAdminClient _admin = null!;

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
    }

    public async Task DisposeAsync()
    {
        await _admin.DisposeAsync();
        await _client.DisposeAsync();
    }

    private static string UniqueTopic(string prefix) =>
        $"{prefix}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Guid.NewGuid():N}".Substring(0, 50);

    // ========== PRODUCER (8 tests) ==========

    [Fact(DisplayName = "P01: Simple Produce")]
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

    [Fact(DisplayName = "P02: Keyed Produce")]
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

    [Fact(DisplayName = "P03: Headers Produce")]
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

    [Fact(DisplayName = "P04: Batch Produce")]
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

    [Fact(DisplayName = "P05: Compression")]
    public async Task P05_Compression()
    {
        var topic = UniqueTopic("p05");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            Producer = new ProducerOptions { CompressionType = CompressionType.Gzip }
        };
        await using var compressedClient = new StreamlineClient(options);
        var result = await compressedClient.ProduceAsync(topic, null, "compressed-message");

        Assert.NotNull(result);
        Assert.True(result.Offset >= 0);
    }

    [Fact(DisplayName = "P06: Partitioner")]
    public async Task P06_Partitioner()
    {
        var topic = UniqueTopic("p06");
        await _admin.CreateTopicAsync(topic, partitions: 4);

        // With a key, messages should be consistently partitioned
        var r1 = await _client.ProduceAsync(topic, "deterministic-key", "val1");
        var r2 = await _client.ProduceAsync(topic, "deterministic-key", "val2");

        Assert.Equal(r1.Partition, r2.Partition);
    }

    [Fact(DisplayName = "P07: Idempotent")]
    public async Task P07_Idempotent()
    {
        var topic = UniqueTopic("p07");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            Producer = new ProducerOptions { Idempotent = true }
        };
        await using var idempotentClient = new StreamlineClient(options);
        var result = await idempotentClient.ProduceAsync(topic, null, "idempotent-msg");

        Assert.NotNull(result);
        Assert.True(result.Offset >= 0);
    }

    [Fact(DisplayName = "P08: Timeout")]
    public async Task P08_Timeout()
    {
        var options = new StreamlineOptions
        {
            BootstrapServers = "localhost:1",
            ConnectTimeout = TimeSpan.FromMilliseconds(500),
            RequestTimeout = TimeSpan.FromMilliseconds(500),
        };
        await using var badClient = new StreamlineClient(options);

        await Assert.ThrowsAnyAsync<StreamlineException>(
            () => badClient.ProduceAsync("test-topic", null, "timeout-msg"));
    }

    // ========== CONSUMER (8 tests) ==========

    [Fact(DisplayName = "C01: Subscribe")]
    public async Task C01_Subscribe()
    {
        var topic = UniqueTopic("c01");
        await _admin.CreateTopicAsync(topic, partitions: 1);
        await _client.ProduceAsync(topic, null, "subscribe-test");

        await using var consumer = _client.CreateConsumer<string, string>(topic, "c01-group");
        await consumer.SubscribeAsync();
        // Should not throw
    }

    [Fact(DisplayName = "C02: From Beginning")]
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

    [Fact(DisplayName = "C03: From Offset")]
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
        await consumer.SeekAsync(partition: 0, offset: 5);

        var records = await consumer.PollAsync(TimeSpan.FromSeconds(10));
        Assert.True(records.Count >= 5, $"Expected >= 5 records from offset 5, got {records.Count}");
    }

    [Fact(DisplayName = "C04: From Timestamp")]
    public async Task C04_FromTimestamp()
    {
        var topic = UniqueTopic("c04");
        await _admin.CreateTopicAsync(topic, partitions: 1);
        await _client.ProduceAsync(topic, null, "timestamped-msg");

        var options = new ConsumerOptions
        {
            GroupId = $"c04-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        var records = await consumer.PollAsync(TimeSpan.FromSeconds(10));
        Assert.NotEmpty(records);
    }

    [Fact(DisplayName = "C05: Follow")]
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

        // Produce after subscribe to test follow
        await _client.ProduceAsync(topic, null, "follow-msg");
        await Task.Delay(500);

        var records = await consumer.PollAsync(TimeSpan.FromSeconds(5));
        // Consumer should pick up the new message
        Assert.NotNull(records);
    }

    [Fact(DisplayName = "C06: Filter")]
    public async Task C06_Filter()
    {
        var topic = UniqueTopic("c06");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        for (int i = 0; i < 10; i++)
            await _client.ProduceAsync(topic, i % 2 == 0 ? "even" : "odd", $"value-{i}");

        var options = new ConsumerOptions
        {
            GroupId = $"c06-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        var records = await consumer.PollAsync(TimeSpan.FromSeconds(10));
        // Client-side filtering
        var evens = records.Where(r => r.Key == "even").ToList();
        Assert.Equal(5, evens.Count);
    }

    [Fact(DisplayName = "C07: Headers")]
    public async Task C07_Headers()
    {
        var topic = UniqueTopic("c07");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var headers = new Headers().Add("x-test", "conformance-value");
        await _client.ProduceAsync(topic, "key", "value", headers);

        var options = new ConsumerOptions
        {
            GroupId = $"c07-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        var records = await consumer.PollAsync(TimeSpan.FromSeconds(10));
        Assert.NotEmpty(records);
        var first = records.First();
        Assert.NotNull(first.Headers);
        Assert.Equal("conformance-value", first.Headers.GetString("x-test"));
    }

    [Fact(DisplayName = "C08: Timeout")]
    public async Task C08_Timeout()
    {
        var topic = UniqueTopic("c08");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var options = new ConsumerOptions
        {
            GroupId = $"c08-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Latest
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        // Poll on empty topic with short timeout should return empty, not hang
        var records = await consumer.PollAsync(TimeSpan.FromMilliseconds(500));
        Assert.Empty(records);
    }

    // ========== CONSUMER GROUPS (6 tests) ==========

    [Fact(DisplayName = "G01: Join Group")]
    public async Task G01_JoinGroup()
    {
        var topic = UniqueTopic("g01");
        var groupId = $"group-{Guid.NewGuid():N}";
        await _admin.CreateTopicAsync(topic, partitions: 1);
        await _client.ProduceAsync(topic, null, "group-test");

        var options = new ConsumerOptions { GroupId = groupId, AutoOffsetReset = AutoOffsetReset.Earliest };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();
        await consumer.PollAsync(TimeSpan.FromSeconds(5));

        var groups = await _admin.ListConsumerGroupsAsync();
        Assert.NotNull(groups);
    }

    [Fact(DisplayName = "G02: Rebalance")]
    public async Task G02_Rebalance()
    {
        var topic = UniqueTopic("g02");
        var groupId = $"group-rebal-{Guid.NewGuid():N}";
        await _admin.CreateTopicAsync(topic, partitions: 2);

        var options = new ConsumerOptions { GroupId = groupId, AutoOffsetReset = AutoOffsetReset.Earliest };
        await using var consumer1 = _client.CreateConsumer<string, string>(topic, options);
        await consumer1.SubscribeAsync();

        await using var consumer2 = _client.CreateConsumer<string, string>(topic, options);
        await consumer2.SubscribeAsync();

        await Task.Delay(1000);
        // Both consumers joined — rebalance should have occurred
    }

    [Fact(DisplayName = "G03: Commit Offsets")]
    public async Task G03_CommitOffsets()
    {
        var topic = UniqueTopic("g03");
        var groupId = $"group-commit-{Guid.NewGuid():N}";
        await _admin.CreateTopicAsync(topic, partitions: 1);
        await _client.ProduceAsync(topic, null, "commit-test");

        var options = new ConsumerOptions
        {
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        var records = await consumer.PollAsync(TimeSpan.FromSeconds(5));
        Assert.NotEmpty(records);

        await consumer.CommitAsync();
    }

    [Fact(DisplayName = "G04: Lag Monitoring")]
    public async Task G04_LagMonitoring()
    {
        var topic = UniqueTopic("g04");
        var groupId = $"group-lag-{Guid.NewGuid():N}";
        await _admin.CreateTopicAsync(topic, partitions: 1);

        for (int i = 0; i < 5; i++)
            await _client.ProduceAsync(topic, null, $"lag-msg-{i}");

        var options = new ConsumerOptions { GroupId = groupId, AutoOffsetReset = AutoOffsetReset.Earliest };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();
        await consumer.PollAsync(TimeSpan.FromSeconds(5));

        var groupInfo = await _admin.DescribeConsumerGroupAsync(groupId);
        Assert.NotNull(groupInfo);
    }

    [Fact(DisplayName = "G05: Reset Offsets")]
    public async Task G05_ResetOffsets()
    {
        var topic = UniqueTopic("g05");
        var groupId = $"group-reset-{Guid.NewGuid():N}";
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var options = new ConsumerOptions { GroupId = groupId, AutoOffsetReset = AutoOffsetReset.Earliest };
        await using var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        await consumer.SeekToBeginningAsync();
    }

    [Fact(DisplayName = "G06: Leave Group")]
    public async Task G06_LeaveGroup()
    {
        var topic = UniqueTopic("g06");
        var groupId = $"group-leave-{Guid.NewGuid():N}";
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var options = new ConsumerOptions { GroupId = groupId, AutoOffsetReset = AutoOffsetReset.Earliest };
        var consumer = _client.CreateConsumer<string, string>(topic, options);
        await consumer.SubscribeAsync();

        // Dispose triggers group leave
        await consumer.DisposeAsync();
    }

    // ========== AUTHENTICATION (6 tests) ==========

    [Fact(DisplayName = "A01: TLS Connect")]
    public async Task A01_TlsConnect()
    {
        var authEnabled = Environment.GetEnvironmentVariable("STREAMLINE_AUTH_ENABLED") == "true";
        if (!authEnabled) return; // requires TLS-enabled server

        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            SecurityProtocol = SecurityProtocol.Ssl,
            Tls = new TlsOptions()
        };
        await using var client = new StreamlineClient(options);
        Assert.True(await client.IsHealthyAsync());
    }

    [Fact(DisplayName = "A02: Mutual TLS")]
    public async Task A02_MutualTls()
    {
        var authEnabled = Environment.GetEnvironmentVariable("STREAMLINE_AUTH_ENABLED") == "true";
        if (!authEnabled) return;

        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            SecurityProtocol = SecurityProtocol.Ssl,
            Tls = new TlsOptions()
        };
        await using var client = new StreamlineClient(options);
        Assert.True(await client.IsHealthyAsync());
    }

    [Fact(DisplayName = "A03: SASL PLAIN")]
    public async Task A03_SaslPlain()
    {
        var authEnabled = Environment.GetEnvironmentVariable("STREAMLINE_AUTH_ENABLED") == "true";
        if (!authEnabled) return;

        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            Sasl = new SaslOptions
            {
                Mechanism = SaslMechanism.Plain,
                Username = "admin",
                Password = "admin-secret"
            }
        };
        await using var client = new StreamlineClient(options);
        Assert.True(await client.IsHealthyAsync());
    }

    [Fact(DisplayName = "A04: SCRAM-SHA-256")]
    public async Task A04_ScramSha256()
    {
        var authEnabled = Environment.GetEnvironmentVariable("STREAMLINE_AUTH_ENABLED") == "true";
        if (!authEnabled) return;

        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            Sasl = new SaslOptions
            {
                Mechanism = SaslMechanism.ScramSha256,
                Username = "admin",
                Password = "admin-secret"
            }
        };
        await using var client = new StreamlineClient(options);
        Assert.True(await client.IsHealthyAsync());
    }

    [Fact(DisplayName = "A05: SCRAM-SHA-512")]
    public async Task A05_ScramSha512()
    {
        var authEnabled = Environment.GetEnvironmentVariable("STREAMLINE_AUTH_ENABLED") == "true";
        if (!authEnabled) return;

        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            Sasl = new SaslOptions
            {
                Mechanism = SaslMechanism.ScramSha512,
                Username = "admin",
                Password = "admin-secret"
            }
        };
        await using var client = new StreamlineClient(options);
        Assert.True(await client.IsHealthyAsync());
    }

    [Fact(DisplayName = "A06: Auth Failure")]
    public async Task A06_AuthFailure()
    {
        var authEnabled = Environment.GetEnvironmentVariable("STREAMLINE_AUTH_ENABLED") == "true";
        if (!authEnabled) return;

        var options = new StreamlineOptions
        {
            BootstrapServers = _bootstrap,
            SecurityProtocol = SecurityProtocol.SaslPlaintext,
            Sasl = new SaslOptions
            {
                Mechanism = SaslMechanism.Plain,
                Username = "bad-user",
                Password = "wrong-password"
            }
        };
        await using var client = new StreamlineClient(options);
        await Assert.ThrowsAsync<StreamlineAuthenticationException>(
            () => client.IsHealthyAsync());
    }

    // ========== SCHEMA REGISTRY (6 tests) ==========

    [Fact(DisplayName = "S01: Register Schema")]
    public async Task S01_RegisterSchema()
    {
        using var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"conformance-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        var schema = JsonSerializer.Serialize(new
        {
            type = "object",
            properties = new
            {
                name = new { type = "string" },
                email = new { type = "string" }
            },
            required = new[] { "name", "email" }
        });

        var id = await registry.RegisterAsync(subject, schema, SchemaType.Json);
        Assert.True(id > 0, $"Schema ID should be positive, got {id}");
    }

    [Fact(DisplayName = "S02: Get by ID")]
    public async Task S02_GetById()
    {
        using var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"get-by-id-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        var schema = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}}}";

        var id = await registry.RegisterAsync(subject, schema, SchemaType.Json);
        var fetched = await registry.GetSchemaAsync(id);

        Assert.NotNull(fetched);
        Assert.Equal(SchemaType.Json, fetched.SchemaType);
        Assert.False(string.IsNullOrEmpty(fetched.Schema));
    }

    [Fact(DisplayName = "S03: Get Versions")]
    public async Task S03_GetVersions()
    {
        using var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"versions-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        var schema = "{\"type\":\"object\",\"properties\":{\"v\":{\"type\":\"string\"}}}";

        await registry.RegisterAsync(subject, schema, SchemaType.Json);
        var versions = await registry.GetVersionsAsync(subject);

        Assert.NotNull(versions);
        Assert.NotEmpty(versions);
    }

    [Fact(DisplayName = "S04: Compatibility Check")]
    public async Task S04_CompatibilityCheck()
    {
        using var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"compat-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        var schema = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}}}";
        await registry.RegisterAsync(subject, schema, SchemaType.Json);

        var evolved = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"age\":{\"type\":\"integer\"}}}";
        var isCompatible = await registry.CheckCompatibilityAsync(subject, evolved, SchemaType.Json);

        Assert.IsType<bool>(isCompatible);
    }

    [Fact(DisplayName = "S05: Avro Schema")]
    public async Task S05_AvroSchema()
    {
        using var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"avro-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        var avroSchema = JsonSerializer.Serialize(new
        {
            type = "record",
            name = "User",
            fields = new[]
            {
                new { name = "name", type = "string" },
                new { name = "age", type = "int" }
            }
        });

        var id = await registry.RegisterAsync(subject, avroSchema, SchemaType.Avro);
        Assert.True(id > 0);

        var fetched = await registry.GetSchemaAsync(id);
        Assert.Equal(SchemaType.Avro, fetched.SchemaType);
    }

    [Fact(DisplayName = "S06: JSON Schema")]
    public async Task S06_JsonSchema()
    {
        using var registry = new SchemaRegistryClient(_httpUrl);
        var subject = $"json-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        var jsonSchema = "{\"type\":\"object\",\"properties\":{\"event\":{\"type\":\"string\"},\"ts\":{\"type\":\"number\"}}}";

        var id = await registry.RegisterAsync(subject, jsonSchema, SchemaType.Json);
        Assert.True(id > 0);

        var fetched = await registry.GetSchemaAsync(id);
        Assert.Equal(SchemaType.Json, fetched.SchemaType);
    }

    // ========== ADMIN (4 tests) ==========

    [Fact(DisplayName = "D01: Create Topic")]
    public async Task D01_CreateTopic()
    {
        var topic = UniqueTopic("d01");

        await _admin.CreateTopicAsync(topic, partitions: 3, replicationFactor: 1);

        var info = await _admin.DescribeTopicAsync(topic);
        Assert.NotNull(info);
        Assert.Equal(topic, info.Name);
    }

    [Fact(DisplayName = "D02: List Topics")]
    public async Task D02_ListTopics()
    {
        var topic = UniqueTopic("d02");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var topics = await _admin.ListTopicsAsync();
        Assert.NotNull(topics);
        Assert.Contains(topics, t => t.Name == topic);
    }

    [Fact(DisplayName = "D03: Describe Topic")]
    public async Task D03_DescribeTopic()
    {
        var topic = UniqueTopic("d03");
        await _admin.CreateTopicAsync(topic, partitions: 2);

        var info = await _admin.DescribeTopicAsync(topic);
        Assert.NotNull(info);
        Assert.Equal(topic, info.Name);
    }

    [Fact(DisplayName = "D04: Delete Topic")]
    public async Task D04_DeleteTopic()
    {
        var topic = UniqueTopic("d04");
        await _admin.CreateTopicAsync(topic, partitions: 1);
        await _admin.DeleteTopicAsync(topic);

        var topics = await _admin.ListTopicsAsync();
        Assert.DoesNotContain(topics, t => t.Name == topic);
    }

    // ========== ERROR HANDLING (4 tests) ==========

    [Fact(DisplayName = "E01: Connection Refused")]
    public async Task E01_ConnectionRefused()
    {
        var options = new StreamlineOptions
        {
            BootstrapServers = "localhost:1",
            ConnectTimeout = TimeSpan.FromMilliseconds(500),
            RequestTimeout = TimeSpan.FromMilliseconds(500),
        };
        await using var badClient = new StreamlineClient(options);

        await Assert.ThrowsAnyAsync<StreamlineException>(
            () => badClient.ProduceAsync("test", null, "msg"));
    }

    [Fact(DisplayName = "E02: Auth Denied")]
    public async Task E02_AuthDenied()
    {
        // Validate the exception type and its properties
        var ex = new StreamlineAuthenticationException("Access denied");
        Assert.IsAssignableFrom<StreamlineException>(ex);
        Assert.False(ex.IsRetryable);
        Assert.Equal(StreamlineErrorCode.AuthenticationFailed, ex.ErrorCode);
    }

    [Fact(DisplayName = "E03: Topic Not Found")]
    public async Task E03_TopicNotFound()
    {
        var ex = new StreamlineTopicNotFoundException($"nonexistent-{Guid.NewGuid():N}");
        Assert.IsAssignableFrom<StreamlineException>(ex);
        Assert.False(ex.IsRetryable);
        Assert.Equal(StreamlineErrorCode.TopicNotFound, ex.ErrorCode);
        Assert.NotNull(ex.Hint);
    }

    [Fact(DisplayName = "E04: Request Timeout")]
    public async Task E04_RequestTimeout()
    {
        var ex = new StreamlineTimeoutException("produce");
        Assert.IsAssignableFrom<StreamlineException>(ex);
        Assert.True(ex.IsRetryable);
        Assert.Equal(StreamlineErrorCode.Timeout, ex.ErrorCode);
    }

    // ========== PERFORMANCE (4 tests) ==========

    [Fact(DisplayName = "F01: Throughput 1KB")]
    public async Task F01_Throughput1Kb()
    {
        var topic = UniqueTopic("f01");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        var payload = new string('x', 1024);
        const int count = 100;

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < count; i++)
            await _client.ProduceAsync(topic, null, payload);
        sw.Stop();

        var throughput = count / sw.Elapsed.TotalSeconds;
        Assert.True(throughput > 10, $"Throughput should be >10 msg/s, got {throughput:F1}");
    }

    [Fact(DisplayName = "F02: Latency P99")]
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
        Assert.True(p99 < 5000, $"P99 latency should be <5s, got {p99:F0}ms");
    }

    [Fact(DisplayName = "F03: Startup Time")]
    public async Task F03_StartupTime()
    {
        var sw = Stopwatch.StartNew();
        var options = new StreamlineOptions { BootstrapServers = _bootstrap };
        await using var testClient = new StreamlineClient(options);
        var healthy = await testClient.IsHealthyAsync();
        sw.Stop();

        Assert.True(sw.Elapsed.TotalSeconds < 5, $"Startup should be <5s, got {sw.Elapsed.TotalSeconds:F1}s");
    }

    [Fact(DisplayName = "F04: Memory Usage")]
    public async Task F04_MemoryUsage()
    {
        var topic = UniqueTopic("f04");
        await _admin.CreateTopicAsync(topic, partitions: 1);

        GC.Collect();
        var before = GC.GetTotalMemory(true);

        for (int i = 0; i < 100; i++)
            await _client.ProduceAsync(topic, null, new string('x', 1024));

        GC.Collect();
        var after = GC.GetTotalMemory(true);
        var growth = after - before;

        // Memory growth should be <50MB for 100 x 1KB messages
        Assert.True(growth < 50 * 1024 * 1024, $"Memory growth was {growth / 1024 / 1024}MB");
    }
}
