using System.Text.Json;
using Streamline.Client.Schema;
using Xunit;

namespace Streamline.Client.Tests;

public class SchemaModelTests
{
    // ── SchemaFormat ─────────────────────────────────────────────────

    [Fact]
    public void SchemaFormat_HasExpectedValues()
    {
        Assert.Equal(0, (int)SchemaFormat.Avro);
        Assert.Equal(1, (int)SchemaFormat.Json);
        Assert.Equal(2, (int)SchemaFormat.Protobuf);
    }

    [Fact]
    public void SchemaFormat_SerializesToJsonString()
    {
        var json = JsonSerializer.Serialize(SchemaFormat.Avro);
        Assert.Equal("\"Avro\"", json);
    }

    [Fact]
    public void SchemaFormat_DeserializesFromJsonString()
    {
        var format = JsonSerializer.Deserialize<SchemaFormat>("\"Json\"");
        Assert.Equal(SchemaFormat.Json, format);
    }

    [Fact]
    public void SchemaFormat_RoundTrips()
    {
        foreach (var format in Enum.GetValues<SchemaFormat>())
        {
            var json = JsonSerializer.Serialize(format);
            var deserialized = JsonSerializer.Deserialize<SchemaFormat>(json);
            Assert.Equal(format, deserialized);
        }
    }

    // ── CompatibilityLevel ──────────────────────────────────────────

    [Fact]
    public void CompatibilityLevel_HasAllExpectedValues()
    {
        var values = Enum.GetValues<CompatibilityLevel>();
        Assert.Equal(7, values.Length);
        Assert.Contains(CompatibilityLevel.None, values);
        Assert.Contains(CompatibilityLevel.Backward, values);
        Assert.Contains(CompatibilityLevel.BackwardTransitive, values);
        Assert.Contains(CompatibilityLevel.Forward, values);
        Assert.Contains(CompatibilityLevel.ForwardTransitive, values);
        Assert.Contains(CompatibilityLevel.Full, values);
        Assert.Contains(CompatibilityLevel.FullTransitive, values);
    }

    [Fact]
    public void CompatibilityLevel_SerializesToJsonString()
    {
        var json = JsonSerializer.Serialize(CompatibilityLevel.BackwardTransitive);
        Assert.Equal("\"BackwardTransitive\"", json);
    }

    [Fact]
    public void CompatibilityLevel_RoundTrips()
    {
        foreach (var level in Enum.GetValues<CompatibilityLevel>())
        {
            var json = JsonSerializer.Serialize(level);
            var deserialized = JsonSerializer.Deserialize<CompatibilityLevel>(json);
            Assert.Equal(level, deserialized);
        }
    }

    // ── SchemaReference ─────────────────────────────────────────────

    [Fact]
    public void SchemaReference_ConstructsCorrectly()
    {
        var reference = new SchemaReference("common.proto", "common-value", 1);
        Assert.Equal("common.proto", reference.Name);
        Assert.Equal("common-value", reference.Subject);
        Assert.Equal(1, reference.Version);
    }

    [Fact]
    public void SchemaReference_RecordEquality()
    {
        var a = new SchemaReference("ref", "subj", 1);
        var b = new SchemaReference("ref", "subj", 1);
        Assert.Equal(a, b);
    }

    [Fact]
    public void SchemaReference_JsonRoundTrip()
    {
        var original = new SchemaReference("users.proto", "users-value", 3);
        var json = JsonSerializer.Serialize(original);
        var deserialized = JsonSerializer.Deserialize<SchemaReference>(json)!;

        Assert.Equal(original.Name, deserialized.Name);
        Assert.Equal(original.Subject, deserialized.Subject);
        Assert.Equal(original.Version, deserialized.Version);
    }

    // ── SchemaInfo ──────────────────────────────────────────────────

    [Fact]
    public void SchemaInfo_ConstructsWithRequiredFields()
    {
        var info = new SchemaInfo(42, "events-value", 1, SchemaFormat.Avro, "{\"type\":\"record\"}");
        Assert.Equal(42, info.Id);
        Assert.Equal("events-value", info.Subject);
        Assert.Equal(1, info.Version);
        Assert.Equal(SchemaFormat.Avro, info.Format);
        Assert.Equal("{\"type\":\"record\"}", info.SchemaText);
        Assert.Null(info.References);
    }

    [Fact]
    public void SchemaInfo_ConstructsWithReferences()
    {
        var refs = new List<SchemaReference>
        {
            new("common.proto", "common-value", 1),
            new("types.proto", "types-value", 2),
        };
        var info = new SchemaInfo(1, "orders-value", 3, SchemaFormat.Protobuf, "syntax = \"proto3\";", refs);

        Assert.NotNull(info.References);
        Assert.Equal(2, info.References!.Count);
    }

    [Fact]
    public void SchemaInfo_RecordEquality()
    {
        var a = new SchemaInfo(1, "sub", 1, SchemaFormat.Json, "{}");
        var b = new SchemaInfo(1, "sub", 1, SchemaFormat.Json, "{}");
        Assert.Equal(a, b);
    }

    [Fact]
    public void SchemaInfo_JsonRoundTrip()
    {
        var original = new SchemaInfo(42, "events-value", 2, SchemaFormat.Json,
            "{\"type\":\"object\"}", null);
        var json = JsonSerializer.Serialize(original);
        var deserialized = JsonSerializer.Deserialize<SchemaInfo>(json)!;

        Assert.Equal(original.Id, deserialized.Id);
        Assert.Equal(original.Subject, deserialized.Subject);
        Assert.Equal(original.Version, deserialized.Version);
        Assert.Equal(original.Format, deserialized.Format);
        Assert.Equal(original.SchemaText, deserialized.SchemaText);
    }

    [Fact]
    public void SchemaInfo_JsonPropertyNames_AreCorrect()
    {
        var info = new SchemaInfo(1, "test", 1, SchemaFormat.Avro, "{}");
        var json = JsonSerializer.Serialize(info);

        Assert.Contains("\"id\":", json);
        Assert.Contains("\"subject\":", json);
        Assert.Contains("\"version\":", json);
        Assert.Contains("\"schemaType\":", json);
        Assert.Contains("\"schema\":", json);
    }
}
