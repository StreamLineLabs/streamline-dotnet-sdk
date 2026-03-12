using System.Text.Json.Serialization;

namespace Streamline.Client.Schema;

/// <summary>
/// A reference to another schema used for schema composition (e.g., Protobuf imports).
/// </summary>
/// <param name="Name">The reference name (e.g., the import path).</param>
/// <param name="Subject">The subject containing the referenced schema.</param>
/// <param name="Version">The version of the referenced schema.</param>
public record SchemaReference(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("subject")] string Subject,
    [property: JsonPropertyName("version")] int Version);
