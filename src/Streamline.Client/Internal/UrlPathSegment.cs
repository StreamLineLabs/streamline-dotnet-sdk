namespace Streamline.Client;

internal static class UrlPathSegment
{
    public static string Escape(string value, string paramName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, paramName);
        if (value is "." or "..")
            throw new ArgumentException("A URL path segment cannot be '.' or '..'.", paramName);

        return Uri.EscapeDataString(value);
    }
}
