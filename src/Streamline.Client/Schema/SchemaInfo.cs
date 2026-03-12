using System.Text.Json.Serialization;

namespace Streamline.Client.Schema;

/// <summary>
/// Represents a schema registered in the Streamline Schema Registry.
/// </summary>
/// <param name="Id">The globally unique schema identifier.</param>
/// <param name="Subject">The subject this schema is registered under.</param>
/// <param name="Version">The version number within the subject.</param>
/// <param name="Format">The schema format (Avro, Json, or Protobuf).</param>
/// <param name="SchemaText">The raw schema definition text.</param>
/// <param name="References">Optional references to other schemas (e.g., Protobuf imports).</param>
public record SchemaInfo(
    [property: JsonPropertyName("id")] int Id,
    [property: JsonPropertyName("subject")] string Subject,
    [property: JsonPropertyName("version")] int Version,
    [property: JsonPropertyName("schemaType")] SchemaFormat Format,
    [property: JsonPropertyName("schema")] string SchemaText,
    [property: JsonPropertyName("references")] IReadOnlyList<SchemaReference>? References = null);
