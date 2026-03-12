using System.Text.Json.Serialization;

namespace Streamline.Client.Schema;

/// <summary>
/// Schema formats supported by the Streamline Schema Registry.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum SchemaFormat
{
    /// <summary>Apache Avro schema format.</summary>
    Avro,

    /// <summary>JSON Schema format.</summary>
    Json,

    /// <summary>Protocol Buffers schema format.</summary>
    Protobuf,
}
