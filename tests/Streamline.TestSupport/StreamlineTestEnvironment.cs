using System.Globalization;

namespace Streamline.TestSupport;

/// <summary>
/// Resolves the environment used to select and configure Streamline test runs.
///
/// <para>
/// The default test run is hermetic: no test contacts a broker, an HTTP endpoint,
/// or <c>localhost</c>. Broker-dependent tests are opt-in and only execute when
/// <c>STREAMLINE_INTEGRATION</c> is set to a truthy value.
/// </para>
///
/// <para>Recognised environment variables:</para>
/// <list type="table">
///   <item>
///     <term>STREAMLINE_INTEGRATION</term>
///     <description>Set to <c>1</c>/<c>true</c>/<c>yes</c>/<c>on</c> to enable integration tests.</description>
///   </item>
///   <item>
///     <term>STREAMLINE_BOOTSTRAP_SERVERS</term>
///     <description>Kafka-protocol endpoint. Alias: <c>STREAMLINE_BOOTSTRAP</c>. Default <c>localhost:9092</c>.</description>
///   </item>
///   <item>
///     <term>STREAMLINE_HTTP_URL</term>
///     <description>HTTP management endpoint. Alias: <c>STREAMLINE_HTTP</c>. Default <c>http://localhost:9094</c>.</description>
///   </item>
///   <item>
///     <term>STREAMLINE_IMAGE</term>
///     <description>Container image used by <c>docker-compose.test.yml</c>.</description>
///   </item>
///   <item>
///     <term>STREAMLINE_READY_TIMEOUT_SECONDS</term>
///     <description>Bound on the readiness probe. Default <c>20</c>, clamped to [1, 300].</description>
///   </item>
/// </list>
/// </summary>
public static class StreamlineTestEnvironment
{
    /// <summary>Environment variable that opts a run into integration tests.</summary>
    public const string IntegrationVariable = "STREAMLINE_INTEGRATION";

    /// <summary>Environment variable holding the Kafka-protocol endpoint.</summary>
    public const string BootstrapVariable = "STREAMLINE_BOOTSTRAP_SERVERS";

    /// <summary>Legacy alias for <see cref="BootstrapVariable"/>.</summary>
    public const string BootstrapAliasVariable = "STREAMLINE_BOOTSTRAP";

    /// <summary>Environment variable holding the HTTP management endpoint.</summary>
    public const string HttpUrlVariable = "STREAMLINE_HTTP_URL";

    /// <summary>Legacy alias for <see cref="HttpUrlVariable"/>.</summary>
    public const string HttpUrlAliasVariable = "STREAMLINE_HTTP";

    /// <summary>Environment variable holding the Streamline container image reference.</summary>
    public const string ImageVariable = "STREAMLINE_IMAGE";

    /// <summary>Environment variable bounding the readiness probe, in seconds.</summary>
    public const string ReadyTimeoutVariable = "STREAMLINE_READY_TIMEOUT_SECONDS";

    /// <summary>Environment variable that opts into authentication conformance tests.</summary>
    public const string AuthenticationVariable = "STREAMLINE_AUTH_CONFORMANCE";

    /// <summary>Required authenticated Kafka-protocol endpoint.</summary>
    public const string AuthenticationBootstrapVariable = "STREAMLINE_AUTH_BOOTSTRAP_SERVERS";

    /// <summary>Required pre-created topic used by authentication conformance tests.</summary>
    public const string AuthenticationTopicVariable = "STREAMLINE_AUTH_TOPIC";

    /// <summary>Required authentication protocol: ssl, sasl-plaintext, or sasl-ssl.</summary>
    public const string AuthenticationProtocolVariable = "STREAMLINE_AUTH_SECURITY_PROTOCOL";

    /// <summary>SASL mechanism: plain, scram-sha-256, or scram-sha-512.</summary>
    public const string AuthenticationMechanismVariable = "STREAMLINE_AUTH_SASL_MECHANISM";

    /// <summary>SASL username for the real authentication fixture.</summary>
    public const string AuthenticationUsernameVariable = "STREAMLINE_AUTH_USERNAME";

    /// <summary>SASL password for the real authentication fixture.</summary>
    public const string AuthenticationPasswordVariable = "STREAMLINE_AUTH_PASSWORD";

    /// <summary>Known-invalid password used to verify authentication denial.</summary>
    public const string AuthenticationInvalidPasswordVariable = "STREAMLINE_AUTH_INVALID_PASSWORD";

    /// <summary>Optional CA certificate path for TLS fixtures.</summary>
    public const string AuthenticationCaCertificateVariable = "STREAMLINE_AUTH_CA_CERTIFICATE_PATH";

    /// <summary>Optional client certificate path for mutual TLS fixtures.</summary>
    public const string AuthenticationClientCertificateVariable = "STREAMLINE_AUTH_CLIENT_CERTIFICATE_PATH";

    /// <summary>Optional client key path for mutual TLS fixtures.</summary>
    public const string AuthenticationClientKeyVariable = "STREAMLINE_AUTH_CLIENT_KEY_PATH";

    /// <summary>Default Kafka-protocol endpoint when no override is supplied.</summary>
    public const string DefaultBootstrapServers = "localhost:9092";

    /// <summary>Default HTTP management endpoint when no override is supplied.</summary>
    public const string DefaultHttpBaseUrl = "http://localhost:9094";

    /// <summary>Default container image when <see cref="ImageVariable"/> is unset.</summary>
    public const string DefaultImage = "ghcr.io/streamlinelabs/streamline:latest";

    /// <summary>Default readiness probe bound, in seconds.</summary>
    public const int DefaultReadyTimeoutSeconds = 20;

    /// <summary>
    /// A bootstrap address reserved for hermetic unit tests. It uses the RFC 6761
    /// <c>.invalid</c> top-level domain, which is guaranteed never to resolve, so a
    /// unit test that accidentally opens a connection fails instead of reaching a
    /// developer's local broker.
    /// </summary>
    public const string UnitBootstrapServers = "streamline-unit-tests.invalid:9092";

    /// <summary>
    /// An HTTP base URL reserved for hermetic unit tests. See <see cref="UnitBootstrapServers"/>.
    /// </summary>
    public const string UnitHttpBaseUrl = "http://streamline-unit-tests.invalid:9094";

    /// <summary>
    /// Human-readable instructions shown when integration tests are requested but the
    /// configured endpoints cannot be reached.
    /// </summary>
    public const string SetupHint =
        "Start a server with 'make integration-test', or 'STREAMLINE_IMAGE=<image> docker compose -f docker-compose.test.yml up -d', " +
        "then re-run with STREAMLINE_INTEGRATION=1. Override endpoints with STREAMLINE_BOOTSTRAP_SERVERS and STREAMLINE_HTTP_URL.";

    /// <summary>
    /// Reason reported for tests skipped because integration testing was not requested.
    /// </summary>
    public const string SkipReason =
        "Integration tests are opt-in. Set " + IntegrationVariable + "=1 (and, if needed, " +
        BootstrapVariable + " / " + HttpUrlVariable + ") to run them.";

    /// <summary>
    /// Reason reported when authentication conformance was not explicitly requested.
    /// </summary>
    public const string AuthenticationSkipReason =
        "Authentication conformance requires a real secured broker fixture. Set " +
        IntegrationVariable + "=1 and " + AuthenticationVariable +
        "=1, then provide the STREAMLINE_AUTH_* fixture variables.";

    /// <summary>
    /// Whether broker-dependent tests should execute, based on <see cref="IntegrationVariable"/>.
    /// </summary>
    public static bool IsIntegrationEnabled => IsTruthy(Environment.GetEnvironmentVariable(IntegrationVariable));

    /// <summary>
    /// Whether authentication conformance was explicitly requested.
    /// </summary>
    public static bool IsAuthenticationEnabled =>
        IsTruthy(Environment.GetEnvironmentVariable(AuthenticationVariable));

    /// <summary>
    /// The Kafka-protocol endpoint integration tests should connect to.
    /// </summary>
    public static string BootstrapServers =>
        FirstNonEmpty(
            Environment.GetEnvironmentVariable(BootstrapVariable),
            Environment.GetEnvironmentVariable(BootstrapAliasVariable))
        ?? DefaultBootstrapServers;

    /// <summary>
    /// The HTTP management endpoint integration tests should connect to, without a trailing slash.
    /// </summary>
    public static string HttpBaseUrl =>
        (FirstNonEmpty(
            Environment.GetEnvironmentVariable(HttpUrlVariable),
            Environment.GetEnvironmentVariable(HttpUrlAliasVariable))
        ?? DefaultHttpBaseUrl).TrimEnd('/');

    /// <summary>
    /// The Streamline container image used by the integration compose stack.
    /// </summary>
    public static string Image =>
        FirstNonEmpty(Environment.GetEnvironmentVariable(ImageVariable)) ?? DefaultImage;

    /// <summary>
    /// The bound applied to readiness probes so an unreachable endpoint fails fast
    /// instead of hanging the test run.
    /// </summary>
    public static TimeSpan ReadyTimeout => TimeSpan.FromSeconds(ParseReadyTimeoutSeconds(
        Environment.GetEnvironmentVariable(ReadyTimeoutVariable)));

    /// <summary>
    /// Interprets an environment variable value as a boolean opt-in flag.
    /// </summary>
    /// <param name="value">The raw environment variable value, which may be null.</param>
    /// <returns><see langword="true"/> for <c>1</c>, <c>true</c>, <c>yes</c> or <c>on</c>; otherwise <see langword="false"/>.</returns>
    public static bool IsTruthy(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            _ => false,
        };
    }

    /// <summary>
    /// Parses the readiness timeout, falling back to <see cref="DefaultReadyTimeoutSeconds"/>
    /// and clamping the result to the range [1, 300].
    /// </summary>
    /// <param name="value">The raw environment variable value, which may be null.</param>
    /// <returns>A bounded timeout in seconds.</returns>
    public static int ParseReadyTimeoutSeconds(string? value)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var seconds))
            return DefaultReadyTimeoutSeconds;

        return Math.Clamp(seconds, 1, 300);
    }

    /// <summary>
    /// Splits a bootstrap server list into its first <c>host</c>/<c>port</c> pair.
    /// </summary>
    /// <param name="bootstrapServers">A comma-separated list of <c>host:port</c> pairs.</param>
    /// <returns>The host and port of the first entry; port defaults to 9092 when absent or malformed.</returns>
    public static (string Host, int Port) ParseFirstBroker(string bootstrapServers)
    {
        ArgumentNullException.ThrowIfNull(bootstrapServers);

        var first = bootstrapServers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .FirstOrDefault() ?? DefaultBootstrapServers;

        var separator = first.LastIndexOf(':');
        if (separator <= 0 || separator == first.Length - 1)
            return (first, 9092);

        var host = first[..separator];
        return int.TryParse(first[(separator + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var port)
            ? (host, port)
            : (host, 9092);
    }

    private static string? FirstNonEmpty(params string?[] candidates) =>
        candidates.FirstOrDefault(c => !string.IsNullOrWhiteSpace(c))?.Trim();
}
