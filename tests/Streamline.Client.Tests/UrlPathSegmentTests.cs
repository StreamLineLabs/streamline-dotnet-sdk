using Xunit;

namespace Streamline.Client.Tests;

public sealed class UrlPathSegmentTests
{
    [Fact]
    public void Escape_EncodesReservedCharacters()
    {
        Assert.Equal(
            "group%2Fwith%3Freserved%23chars",
            UrlPathSegment.Escape("group/with?reserved#chars", "value"));
    }

    [Theory]
    [InlineData(".")]
    [InlineData("..")]
    [InlineData("")]
    [InlineData(" ")]
    public void Escape_RejectsUnsafeOrEmptySegments(string value)
    {
        Assert.Throws<ArgumentException>(() => UrlPathSegment.Escape(value, "value"));
    }
}
