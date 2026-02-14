using Streamline.Client;
using Xunit;

namespace Streamline.Client.Tests;

public class HeadersTests
{
    [Fact]
    public void NewHeaders_IsEmpty()
    {
        var headers = new Headers();
        Assert.True(headers.IsEmpty);
    }

    [Fact]
    public void Add_ByteArray_CanBeRetrieved()
    {
        var headers = new Headers();
        headers.Add("key", new byte[] { 1, 2, 3 });

        Assert.False(headers.IsEmpty);
        Assert.Equal(new byte[] { 1, 2, 3 }, headers.Get("key"));
    }

    [Fact]
    public void Add_String_CanBeRetrievedAsString()
    {
        var headers = new Headers();
        headers.Add("trace-id", "abc-123");

        Assert.Equal("abc-123", headers.GetString("trace-id"));
    }

    [Fact]
    public void Get_NonExistentKey_ReturnsNull()
    {
        var headers = new Headers();
        Assert.Null(headers.Get("missing"));
        Assert.Null(headers.GetString("missing"));
    }

    [Fact]
    public void ContainsKey_WorksCorrectly()
    {
        var headers = new Headers();
        headers.Add("exists", "value");

        Assert.True(headers.ContainsKey("exists"));
        Assert.False(headers.ContainsKey("missing"));
    }

    [Fact]
    public void Add_ReturnsSelf_ForChaining()
    {
        var headers = new Headers()
            .Add("key1", "value1")
            .Add("key2", "value2");

        Assert.Equal("value1", headers.GetString("key1"));
        Assert.Equal("value2", headers.GetString("key2"));
    }

    [Fact]
    public void Enumerable_IteratesAllHeaders()
    {
        var headers = new Headers()
            .Add("a", "1")
            .Add("b", "2");

        var count = 0;
        foreach (var _ in headers) count++;
        Assert.Equal(2, count);
    }
}
