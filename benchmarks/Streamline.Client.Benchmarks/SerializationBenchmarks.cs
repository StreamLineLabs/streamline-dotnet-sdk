using BenchmarkDotNet.Attributes;
using Streamline.Client;

namespace Streamline.Client.Benchmarks;

/// <summary>
/// Benchmarks for Headers serialization and lookup operations.
/// </summary>
[MemoryDiagnoser]
public class SerializationBenchmarks
{
    private Headers _emptyHeaders = null!;
    private Headers _populatedHeaders = null!;
    private byte[] _binaryValue = null!;

    [Params(1, 5, 20)]
    public int HeaderCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _emptyHeaders = new Headers();
        _binaryValue = new byte[128];
        Random.Shared.NextBytes(_binaryValue);

        _populatedHeaders = new Headers();
        for (int i = 0; i < HeaderCount; i++)
        {
            _populatedHeaders.Add($"header-{i}", $"value-{i}");
        }
    }

    [Benchmark]
    public Headers AddStringHeaders()
    {
        var headers = new Headers();
        for (int i = 0; i < HeaderCount; i++)
        {
            headers.Add($"key-{i}", $"value-{i}");
        }
        return headers;
    }

    [Benchmark]
    public Headers AddBinaryHeaders()
    {
        var headers = new Headers();
        for (int i = 0; i < HeaderCount; i++)
        {
            headers.Add($"key-{i}", _binaryValue);
        }
        return headers;
    }

    [Benchmark]
    public string? GetString_Existing()
    {
        return _populatedHeaders.GetString("header-0");
    }

    [Benchmark]
    public string? GetString_Missing()
    {
        return _populatedHeaders.GetString("nonexistent");
    }

    [Benchmark]
    public byte[]? GetBytes_Existing()
    {
        return _populatedHeaders.Get("header-0");
    }

    [Benchmark]
    public bool ContainsKey_Existing()
    {
        return _populatedHeaders.ContainsKey("header-0");
    }

    [Benchmark]
    public bool ContainsKey_Missing()
    {
        return _populatedHeaders.ContainsKey("nonexistent");
    }

    [Benchmark]
    public bool CheckIsEmpty_Empty()
    {
        return _emptyHeaders.IsEmpty;
    }

    [Benchmark]
    public bool CheckIsEmpty_Populated()
    {
        return _populatedHeaders.IsEmpty;
    }

    [Benchmark]
    public int EnumerateHeaders()
    {
        int count = 0;
        foreach (var kvp in _populatedHeaders)
        {
            count++;
        }
        return count;
    }
}
