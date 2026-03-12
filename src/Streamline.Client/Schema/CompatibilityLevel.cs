using System.Text.Json.Serialization;

namespace Streamline.Client.Schema;

/// <summary>
/// Schema compatibility levels for subject evolution rules.
/// </summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum CompatibilityLevel
{
    /// <summary>No compatibility checks are performed.</summary>
    None,

    /// <summary>New schema can read data written by the last schema.</summary>
    Backward,

    /// <summary>New schema can read data written by all previous schemas.</summary>
    BackwardTransitive,

    /// <summary>Last schema can read data written by the new schema.</summary>
    Forward,

    /// <summary>All previous schemas can read data written by the new schema.</summary>
    ForwardTransitive,

    /// <summary>Both backward and forward compatible with the last schema.</summary>
    Full,

    /// <summary>Both backward and forward compatible with all previous schemas.</summary>
    FullTransitive,
}
