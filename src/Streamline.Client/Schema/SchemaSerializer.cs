using System.Text;
using System.Text.Json;

namespace Streamline.Client.Schema;

/// <summary>
/// Schema-aware serializer that prepends a schema ID wire-format header to the payload.
/// The wire format is: [magic byte 0x00] [4-byte big-endian schema ID] [serialized payload].
/// </summary>
/// <typeparam name="T">The type to serialize.</typeparam>
/// <remarks>
/// This serializer is compatible with Confluent Schema Registry wire format,
/// enabling interoperability with other Kafka ecosystem tools.
/// </remarks>
public sealed class SchemaSerializer<T> : IAsyncDisposable
{
    private readonly ISchemaRegistryClient _registryClient;
    private readonly string _subject;
    private readonly SchemaFormat _format;
    private readonly JsonSerializerOptions? _jsonOptions;
    private int _cachedSchemaId;
    private bool _schemaRegistered;
    private bool _disposed;

    /// <summary>
    /// Magic byte prefix used in the Confluent wire format.
    /// </summary>
    internal const byte MagicByte = 0x00;

    /// <summary>
    /// Length of the wire-format header: 1 byte magic + 4 bytes schema ID.
    /// </summary>
    internal const int HeaderSize = 5;

    /// <summary>
    /// Creates a new schema-aware serializer.
    /// </summary>
    /// <param name="registryClient">The schema registry client for schema ID resolution.</param>
    /// <param name="subject">The subject under which the schema is registered (e.g., "orders-value").</param>
    /// <param name="format">The schema format.</param>
    /// <param name="jsonOptions">Optional JSON serialization options for the payload. Only used with <see cref="SchemaFormat.Json"/>.</param>
    public SchemaSerializer(
        ISchemaRegistryClient registryClient,
        string subject,
        SchemaFormat format,
        JsonSerializerOptions? jsonOptions = null)
    {
        _registryClient = registryClient ?? throw new ArgumentNullException(nameof(registryClient));
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);
        _subject = subject;
        _format = format;
        _jsonOptions = jsonOptions;
    }

    /// <summary>
    /// Serializes the value with a schema ID header prepended.
    /// On first call, registers or retrieves the schema from the registry.
    /// </summary>
    /// <param name="value">The value to serialize.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A byte array containing the wire-format header followed by the serialized payload.</returns>
    /// <exception cref="StreamlineSerializationException">Thrown when serialization fails.</exception>
    public async Task<byte[]> SerializeAsync(T value, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_schemaRegistered)
        {
            await EnsureSchemaRegisteredAsync(cancellationToken);
        }

        byte[] payload;
        try
        {
            payload = SerializePayload(value);
        }
        catch (Exception ex) when (ex is not StreamlineException)
        {
            throw new StreamlineSerializationException(
                $"Failed to serialize value of type {typeof(T).Name}: {ex.Message}", ex);
        }

        var result = new byte[HeaderSize + payload.Length];
        result[0] = MagicByte;
        WriteBigEndianInt32(result, 1, _cachedSchemaId);
        Buffer.BlockCopy(payload, 0, result, HeaderSize, payload.Length);

        return result;
    }

    /// <summary>
    /// Registers the schema and caches the ID for subsequent serializations.
    /// Call this explicitly to fail fast if the schema is invalid.
    /// </summary>
    /// <param name="schemaText">The schema definition text to register.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The registered schema ID.</returns>
    public async Task<int> RegisterSchemaAsync(string schemaText, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(schemaText);

        _cachedSchemaId = await _registryClient.RegisterSchemaAsync(_subject, schemaText, _format, cancellationToken);
        _schemaRegistered = true;
        return _cachedSchemaId;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }

    private async Task EnsureSchemaRegisteredAsync(CancellationToken cancellationToken)
    {
        // Fetch the latest schema for the subject to get the ID
        var latestSchema = await _registryClient.GetLatestSchemaAsync(_subject, cancellationToken);
        _cachedSchemaId = latestSchema.Id;
        _schemaRegistered = true;
    }

    private byte[] SerializePayload(T value)
    {
        if (value is byte[] bytes)
            return bytes;

        if (value is string s)
            return Encoding.UTF8.GetBytes(s);

        return JsonSerializer.SerializeToUtf8Bytes(value, _jsonOptions);
    }

    internal static void WriteBigEndianInt32(byte[] buffer, int offset, int value)
    {
        buffer[offset] = (byte)(value >> 24);
        buffer[offset + 1] = (byte)(value >> 16);
        buffer[offset + 2] = (byte)(value >> 8);
        buffer[offset + 3] = (byte)value;
    }
}
