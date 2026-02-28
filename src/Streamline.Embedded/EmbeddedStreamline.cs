using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace Streamline.Embedded;

/// <summary>
/// Configuration for embedded Streamline instance.
/// </summary>
public class EmbeddedConfig
{
    public string? DataDir { get; set; }
    public bool InMemory { get; set; }
    public int Partitions { get; set; } = 1;
}

/// <summary>
/// Message from an embedded Streamline instance.
/// </summary>
public record EmbeddedMessage(
    string Topic,
    int Partition,
    long Offset,
    byte[]? Key,
    byte[] Value,
    long Timestamp
);

/// <summary>
/// Embedded Streamline instance using P/Invoke to call the native C API.
/// Requires the libstreamline native library to be present.
/// </summary>
public sealed class EmbeddedStreamline : IDisposable
{
    private IntPtr _handle;
    private bool _disposed;

    // P/Invoke declarations
    [DllImport("streamline", CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr streamline_create(string? configJson);

    [DllImport("streamline", CallingConvention = CallingConvention.Cdecl)]
    private static extern void streamline_destroy(IntPtr handle);

    [DllImport("streamline", CallingConvention = CallingConvention.Cdecl)]
    private static extern int streamline_produce(
        IntPtr handle, string topic,
        byte[] value, nuint valueLen,
        byte[]? key, nuint keyLen);

    [DllImport("streamline", CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr streamline_consume(IntPtr handle, string topic, long timeoutMs);

    [DllImport("streamline", CallingConvention = CallingConvention.Cdecl)]
    private static extern void streamline_message_free(IntPtr msg);

    [DllImport("streamline", CallingConvention = CallingConvention.Cdecl)]
    private static extern int streamline_create_topic(IntPtr handle, string topic, int partitions);

    [DllImport("streamline", CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr streamline_last_error();

    [DllImport("streamline", CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr streamline_version();

    public EmbeddedStreamline(EmbeddedConfig? config = null)
    {
        var configJson = config != null ? JsonSerializer.Serialize(config) : null;
        _handle = streamline_create(configJson);
        if (_handle == IntPtr.Zero)
        {
            var error = Marshal.PtrToStringAnsi(streamline_last_error());
            throw new InvalidOperationException($"Failed to create instance: {error}");
        }
    }

    public void Produce(string topic, byte[] value, byte[]? key = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var result = streamline_produce(_handle, topic, value, (nuint)value.Length, key, key != null ? (nuint)key.Length : 0);
        if (result != 0)
            throw new InvalidOperationException($"Produce failed (code {result})");
    }

    public int CreateTopic(string name, int partitions = 1)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return streamline_create_topic(_handle, name, partitions);
    }

    public static string Version()
    {
        return Marshal.PtrToStringAnsi(streamline_version()) ?? "unknown";
    }

    public void Dispose()
    {
        if (!_disposed && _handle != IntPtr.Zero)
        {
            streamline_destroy(_handle);
            _handle = IntPtr.Zero;
            _disposed = true;
        }
        GC.SuppressFinalize(this);
    }

    ~EmbeddedStreamline() => Dispose();
}
