using System.Collections;

namespace Streamline.Client;

/// <summary>
/// Collection of message headers.
/// </summary>
public class Headers : IEnumerable<KeyValuePair<string, byte[]>>
{
    private readonly Dictionary<string, byte[]> _headers = new();

    /// <summary>
    /// Adds a header.
    /// </summary>
    public Headers Add(string key, byte[] value)
    {
        _headers[key] = value;
        return this;
    }

    /// <summary>
    /// Adds a string header.
    /// </summary>
    public Headers Add(string key, string value)
    {
        _headers[key] = System.Text.Encoding.UTF8.GetBytes(value);
        return this;
    }

    /// <summary>
    /// Gets a header value.
    /// </summary>
    public byte[]? Get(string key)
    {
        return _headers.TryGetValue(key, out var value) ? value : null;
    }

    /// <summary>
    /// Gets a header value as string.
    /// </summary>
    public string? GetString(string key)
    {
        var bytes = Get(key);
        return bytes != null ? System.Text.Encoding.UTF8.GetString(bytes) : null;
    }

    /// <summary>
    /// Returns whether headers contain a key.
    /// </summary>
    public bool ContainsKey(string key) => _headers.ContainsKey(key);

    /// <summary>
    /// Returns whether headers are empty.
    /// </summary>
    public bool IsEmpty => _headers.Count == 0;

    /// <inheritdoc />
    public IEnumerator<KeyValuePair<string, byte[]>> GetEnumerator() => _headers.GetEnumerator();

    /// <inheritdoc />
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
