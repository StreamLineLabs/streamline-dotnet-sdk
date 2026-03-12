using System.Text;
using System.Text.Json;

namespace Streamline.Client.Schema;

/// <summary>
/// Schema-aware deserializer that reads the schema ID wire-format header and
/// deserializes the payload. The expected wire format is:
/// [magic byte 0x00] [4-byte big-endian schema ID] [serialized payload].
/// </summary>
/// <typeparam name="T">The type to deserialize into.</typeparam>
/// <remarks>
/// This deserializer is compatible with the Confluent Schema Registry wire format.
/// It reads the schema ID from the header and optionally resolves the schema
/// from the registry for validation.
/// </remarks>
public sealed class SchemaDeserializer<T> : IAsyncDisposable
{
    private readonly ISchemaRegistryClient _registryClient;
    private readonly JsonSerializerOptions? _jsonOptions;
    private bool _disposed;

    /// <summary>
    /// Creates a new schema-aware deserializer.
    /// </summary>
    /// <param name="registryClient">The schema registry client for schema resolution.</param>
    /// <param name="jsonOptions">Optional JSON deserialization options. Only used with <see cref="SchemaFormat.Json"/>.</param>
    public SchemaDeserializer(
        ISchemaRegistryClient registryClient,
        JsonSerializerOptions? jsonOptions = null)
    {
        _registryClient = registryClient ?? throw new ArgumentNullException(nameof(registryClient));
        _jsonOptions = jsonOptions;
    }

    /// <summary>
    /// Deserializes a wire-format byte array into an instance of <typeparamref name="T"/>.
    /// Reads the schema ID header and deserializes the remaining payload.
    /// </summary>
    /// <param name="data">The wire-format byte array (header + payload).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="DeserializationResult{T}"/> containing the deserialized value and schema metadata.</returns>
    /// <exception cref="StreamlineSerializationException">
    /// Thrown when the data is null, too short, has an invalid magic byte, or deserialization fails.
    /// </exception>
    public async Task<DeserializationResult<T>> DeserializeAsync(
        byte[] data,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        ValidateWireFormat(data);

        var schemaId = ReadBigEndianInt32(data, 1);
        var payload = new ReadOnlyMemory<byte>(data, SchemaSerializer<T>.HeaderSize, data.Length - SchemaSerializer<T>.HeaderSize);

        // Resolve schema info from registry (uses cache)
        var schemaInfo = await _registryClient.GetSchemaByIdAsync(schemaId, cancellationToken);

        T value;
        try
        {
            value = DeserializePayload(payload.Span);
        }
        catch (Exception ex) when (ex is not StreamlineException)
        {
            throw new StreamlineSerializationException(
                $"Failed to deserialize payload for schema ID {schemaId} into {typeof(T).Name}: {ex.Message}", ex);
        }

        return new DeserializationResult<T>(value, schemaId, schemaInfo);
    }

    /// <summary>
    /// Extracts the schema ID from a wire-format byte array without fully deserializing the payload.
    /// </summary>
    /// <param name="data">The wire-format byte array.</param>
    /// <returns>The schema ID from the header.</returns>
    /// <exception cref="StreamlineSerializationException">Thrown when the data is invalid.</exception>
    public int ReadSchemaId(byte[] data)
    {
        ValidateWireFormat(data);
        return ReadBigEndianInt32(data, 1);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }

    private static void ValidateWireFormat(byte[] data)
    {
        if (data is null || data.Length < SchemaSerializer<T>.HeaderSize)
        {
            throw new StreamlineSerializationException(
                $"Invalid wire format: data must be at least {SchemaSerializer<T>.HeaderSize} bytes, " +
                $"got {data?.Length ?? 0}");
        }

        if (data[0] != SchemaSerializer<T>.MagicByte)
        {
            throw new StreamlineSerializationException(
                $"Invalid wire format: expected magic byte 0x{SchemaSerializer<T>.MagicByte:X2}, " +
                $"got 0x{data[0]:X2}");
        }
    }

    private T DeserializePayload(ReadOnlySpan<byte> payload)
    {
        if (typeof(T) == typeof(byte[]))
            return (T)(object)payload.ToArray();

        if (typeof(T) == typeof(string))
            return (T)(object)Encoding.UTF8.GetString(payload);

        return JsonSerializer.Deserialize<T>(payload, _jsonOptions)
            ?? throw new StreamlineSerializationException(
                $"JSON deserialization returned null for type {typeof(T).Name}");
    }

    internal static int ReadBigEndianInt32(byte[] buffer, int offset)
    {
        return (buffer[offset] << 24)
             | (buffer[offset + 1] << 16)
             | (buffer[offset + 2] << 8)
             | buffer[offset + 3];
    }
}

/// <summary>
/// Result of a schema-aware deserialization, containing both the deserialized value
/// and the associated schema metadata.
/// </summary>
/// <typeparam name="T">The deserialized value type.</typeparam>
/// <param name="Value">The deserialized value.</param>
/// <param name="SchemaId">The schema ID read from the wire-format header.</param>
/// <param name="SchemaInfo">The full schema metadata resolved from the registry.</param>
public record DeserializationResult<T>(
    T Value,
    int SchemaId,
    SchemaInfo SchemaInfo);
