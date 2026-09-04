using System.Globalization;
using Xunit;

namespace Streamline.TestSupport;

/// <summary>
/// Authentication protocols supported by the real conformance fixture.
/// </summary>
public enum AuthenticationProtocol
{
    /// <summary>TLS without SASL.</summary>
    Ssl,

    /// <summary>SASL over plaintext transport.</summary>
    SaslPlaintext,

    /// <summary>SASL over TLS.</summary>
    SaslSsl,
}

/// <summary>
/// SASL mechanisms supported by the real conformance fixture.
/// </summary>
public enum AuthenticationMechanism
{
    /// <summary>SASL PLAIN.</summary>
    Plain,

    /// <summary>SCRAM-SHA-256.</summary>
    ScramSha256,

    /// <summary>SCRAM-SHA-512.</summary>
    ScramSha512,
}

/// <summary>
/// Validated configuration for a real secured broker fixture.
/// </summary>
public sealed class AuthenticationFixtureConfiguration
{
    private AuthenticationFixtureConfiguration(
        string bootstrapServers,
        string topic,
        AuthenticationProtocol protocol,
        AuthenticationMechanism? mechanism,
        string? username,
        string? password,
        string invalidPassword,
        string? caCertificatePath,
        string? clientCertificatePath,
        string? clientKeyPath)
    {
        BootstrapServers = bootstrapServers;
        Topic = topic;
        Protocol = protocol;
        Mechanism = mechanism;
        Username = username;
        Password = password;
        InvalidPassword = invalidPassword;
        CaCertificatePath = caCertificatePath;
        ClientCertificatePath = clientCertificatePath;
        ClientKeyPath = clientKeyPath;
    }

    /// <summary>The secured Kafka-protocol endpoint.</summary>
    public string BootstrapServers { get; }

    /// <summary>A pre-created topic the fixture credentials can write to.</summary>
    public string Topic { get; }

    /// <summary>The configured transport/authentication protocol.</summary>
    public AuthenticationProtocol Protocol { get; }

    /// <summary>The configured SASL mechanism, if applicable.</summary>
    public AuthenticationMechanism? Mechanism { get; }

    /// <summary>The valid SASL username, if applicable.</summary>
    public string? Username { get; }

    /// <summary>The valid SASL password, if applicable.</summary>
    public string? Password { get; }

    /// <summary>A password known not to authenticate as <see cref="Username"/>.</summary>
    public string InvalidPassword { get; }

    /// <summary>Optional CA certificate path.</summary>
    public string? CaCertificatePath { get; }

    /// <summary>Optional mutual-TLS client certificate path.</summary>
    public string? ClientCertificatePath { get; }

    /// <summary>Optional mutual-TLS client key path.</summary>
    public string? ClientKeyPath { get; }

    /// <summary>Whether the fixture uses TLS.</summary>
    public bool UsesTls => Protocol is AuthenticationProtocol.Ssl or AuthenticationProtocol.SaslSsl;

    /// <summary>Whether the fixture uses SASL.</summary>
    public bool UsesSasl => Protocol is AuthenticationProtocol.SaslPlaintext or AuthenticationProtocol.SaslSsl;

    /// <summary>Whether the fixture supplies a mutual-TLS client identity.</summary>
    public bool UsesMutualTls => ClientCertificatePath is not null;

    /// <summary>
    /// Loads and validates the authentication fixture from process environment variables.
    /// </summary>
    /// <returns>A validated fixture configuration.</returns>
    public static AuthenticationFixtureConfiguration Load()
    {
        return Load(Environment.GetEnvironmentVariable);
    }

    /// <summary>
    /// Loads and validates the authentication fixture using the supplied value provider.
    /// </summary>
    /// <param name="getValue">Returns a value for an environment variable name.</param>
    /// <returns>A validated fixture configuration.</returns>
    public static AuthenticationFixtureConfiguration Load(Func<string, string?> getValue)
    {
        ArgumentNullException.ThrowIfNull(getValue);

        var bootstrap = Require(getValue, StreamlineTestEnvironment.AuthenticationBootstrapVariable);
        var topic = Require(getValue, StreamlineTestEnvironment.AuthenticationTopicVariable);
        var protocol = ParseProtocol(Require(
            getValue,
            StreamlineTestEnvironment.AuthenticationProtocolVariable));

        AuthenticationMechanism? mechanism = null;
        string? username = null;
        string? password = null;
        var invalidPassword = FirstNonEmpty(
            getValue(StreamlineTestEnvironment.AuthenticationInvalidPasswordVariable))
            ?? "streamline-conformance-known-invalid-password";

        if (protocol is AuthenticationProtocol.SaslPlaintext or AuthenticationProtocol.SaslSsl)
        {
            mechanism = ParseMechanism(Require(
                getValue,
                StreamlineTestEnvironment.AuthenticationMechanismVariable));
            username = Require(getValue, StreamlineTestEnvironment.AuthenticationUsernameVariable);
            password = Require(getValue, StreamlineTestEnvironment.AuthenticationPasswordVariable);
            if (string.Equals(password, invalidPassword, StringComparison.Ordinal))
            {
                throw Invalid(
                    StreamlineTestEnvironment.AuthenticationInvalidPasswordVariable,
                    "must differ from the valid authentication password");
            }
        }

        var caPath = OptionalExistingFile(
            getValue,
            StreamlineTestEnvironment.AuthenticationCaCertificateVariable);
        var clientCertificatePath = OptionalExistingFile(
            getValue,
            StreamlineTestEnvironment.AuthenticationClientCertificateVariable);
        var clientKeyPath = OptionalExistingFile(
            getValue,
            StreamlineTestEnvironment.AuthenticationClientKeyVariable);

        if ((clientCertificatePath is null) != (clientKeyPath is null))
        {
            throw new InvalidOperationException(
                $"{StreamlineTestEnvironment.AuthenticationClientCertificateVariable} and " +
                $"{StreamlineTestEnvironment.AuthenticationClientKeyVariable} must be provided together.");
        }

        if (protocol is AuthenticationProtocol.SaslPlaintext &&
            (caPath is not null || clientCertificatePath is not null))
        {
            throw Invalid(
                StreamlineTestEnvironment.AuthenticationProtocolVariable,
                "must use ssl or sasl-ssl when TLS certificate paths are configured");
        }

        return new AuthenticationFixtureConfiguration(
            bootstrap,
            topic,
            protocol,
            mechanism,
            username,
            password,
            invalidPassword,
            caPath,
            clientCertificatePath,
            clientKeyPath);
    }

    internal static bool TryLoad(out AuthenticationFixtureConfiguration? configuration)
    {
        try
        {
            configuration = Load();
            return true;
        }
        catch (InvalidOperationException)
        {
            configuration = null;
            return false;
        }
    }

    private static AuthenticationProtocol ParseProtocol(string value)
    {
        return Normalize(value) switch
        {
            "ssl" => AuthenticationProtocol.Ssl,
            "saslplaintext" => AuthenticationProtocol.SaslPlaintext,
            "saslssl" => AuthenticationProtocol.SaslSsl,
            _ => throw Invalid(
                StreamlineTestEnvironment.AuthenticationProtocolVariable,
                "must be ssl, sasl-plaintext, or sasl-ssl"),
        };
    }

    private static AuthenticationMechanism ParseMechanism(string value)
    {
        return Normalize(value) switch
        {
            "plain" => AuthenticationMechanism.Plain,
            "scramsha256" => AuthenticationMechanism.ScramSha256,
            "scramsha512" => AuthenticationMechanism.ScramSha512,
            _ => throw Invalid(
                StreamlineTestEnvironment.AuthenticationMechanismVariable,
                "must be plain, scram-sha-256, or scram-sha-512"),
        };
    }

    private static string Require(Func<string, string?> getValue, string variable)
    {
        return FirstNonEmpty(getValue(variable))
            ?? throw Invalid(variable, "is required when authentication conformance is enabled");
    }

    private static string? OptionalExistingFile(Func<string, string?> getValue, string variable)
    {
        var path = FirstNonEmpty(getValue(variable));
        if (path is null)
            return null;

        if (!File.Exists(path))
            throw Invalid(variable, $"references a file that does not exist: {path}");

        return path;
    }

    private static string Normalize(string value)
    {
        return new string(value
            .Where(char.IsLetterOrDigit)
            .Select(char.ToLowerInvariant)
            .ToArray());
    }

    private static string? FirstNonEmpty(params string?[] values)
    {
        return values.FirstOrDefault(value => !string.IsNullOrWhiteSpace(value))?.Trim();
    }

    private static InvalidOperationException Invalid(string variable, string message)
    {
        return new InvalidOperationException(
            $"Authentication fixture variable {variable} {message}. " +
            StreamlineTestEnvironment.AuthenticationSkipReason);
    }
}

/// <summary>
/// Shared fixture for authentication conformance against a real secured broker.
/// </summary>
public sealed class AuthenticationServerFixture : IAsyncLifetime
{
    private AuthenticationFixtureConfiguration? _configuration;

    /// <summary>Whether authentication conformance was explicitly enabled.</summary>
    public bool Enabled =>
        StreamlineTestEnvironment.IsIntegrationEnabled &&
        StreamlineTestEnvironment.IsAuthenticationEnabled;

    /// <summary>The validated fixture configuration.</summary>
    public AuthenticationFixtureConfiguration Configuration =>
        _configuration ?? throw new InvalidOperationException(
            "Authentication fixture configuration is unavailable because the fixture is disabled or invalid.");

    /// <inheritdoc />
    public async Task InitializeAsync()
    {
        if (!Enabled)
            return;

        _configuration = AuthenticationFixtureConfiguration.Load();
        if (!await IntegrationEndpointProbe.IsBrokerReachableAsync(
                _configuration.BootstrapServers,
                StreamlineTestEnvironment.ReadyTimeout).ConfigureAwait(false))
        {
            throw new StreamlineIntegrationUnavailableException(
                $"{StreamlineTestEnvironment.AuthenticationVariable} is enabled but authenticated broker endpoint " +
                $"'{_configuration.BootstrapServers}' did not respond within " +
                $"{StreamlineTestEnvironment.ReadyTimeout.TotalSeconds.ToString("0.#", CultureInfo.InvariantCulture)}s.");
        }
    }

    /// <inheritdoc />
    public Task DisposeAsync() => Task.CompletedTask;
}

/// <summary>
/// Well-known xUnit collection name for authentication conformance.
/// </summary>
public static class AuthenticationIntegrationCollection
{
    /// <summary>The collection name.</summary>
    public const string Name = "streamline-authentication-integration";
}
