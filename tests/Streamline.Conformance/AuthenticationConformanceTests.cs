using Streamline.Client;
using Streamline.TestSupport;
using Xunit;

namespace Streamline.Conformance;

/// <summary>
/// Authentication conformance tests against an explicitly configured secured broker.
/// </summary>
[Collection(AuthenticationIntegrationCollection.Name)]
public sealed class AuthenticationConformanceTests
{
    private readonly AuthenticationFixtureConfiguration _fixture;

    /// <summary>Creates tests from the validated secured broker fixture.</summary>
    /// <param name="server">The fail-closed authentication fixture.</param>
    public AuthenticationConformanceTests(AuthenticationServerFixture server)
    {
        ArgumentNullException.ThrowIfNull(server);
        _fixture = server.Configuration;
    }

    [AuthenticationFact(AuthenticationRequirement.Tls, DisplayName = "A01: TLS Connect")]
    [Trait("Category", "Conformance")]
    public Task A01_TlsConnect() => AssertAuthenticatedProduceAsync();

    [AuthenticationFact(AuthenticationRequirement.MutualTls, DisplayName = "A02: Mutual TLS")]
    [Trait("Category", "Conformance")]
    public Task A02_MutualTls() => AssertAuthenticatedProduceAsync();

    [AuthenticationFact(AuthenticationRequirement.SaslPlain, DisplayName = "A03: SASL PLAIN")]
    [Trait("Category", "Conformance")]
    public Task A03_SaslPlain() => AssertAuthenticatedProduceAsync();

    [AuthenticationFact(AuthenticationRequirement.ScramSha256, DisplayName = "A04: SCRAM-SHA-256")]
    [Trait("Category", "Conformance")]
    public Task A04_ScramSha256() => AssertAuthenticatedProduceAsync();

    [AuthenticationFact(AuthenticationRequirement.ScramSha512, DisplayName = "A05: SCRAM-SHA-512")]
    [Trait("Category", "Conformance")]
    public Task A05_ScramSha512() => AssertAuthenticatedProduceAsync();

    [AuthenticationFact(AuthenticationRequirement.Sasl, DisplayName = "A06: Auth Failure")]
    [Trait("Category", "Conformance")]
    public async Task A06_AuthFailure()
    {
        await using var client = new StreamlineClient(CreateOptions(useInvalidPassword: true));

        var exception = await Assert.ThrowsAsync<StreamlineAuthenticationException>(
            () => client.ProduceAsync(
                _fixture.Topic,
                $"invalid-auth-{Guid.NewGuid():N}",
                "authentication must fail"));

        Assert.Equal(StreamlineErrorCode.Authentication, exception.ErrorCode);
        Assert.False(exception.IsRetryable);
    }

    private async Task AssertAuthenticatedProduceAsync()
    {
        await using var client = new StreamlineClient(CreateOptions(useInvalidPassword: false));

        var metadata = await client.ProduceAsync(
            _fixture.Topic,
            $"auth-{Guid.NewGuid():N}",
            "authentication conformance");

        Assert.Equal(_fixture.Topic, metadata.Topic);
        Assert.True(metadata.Offset >= 0);
    }

    private StreamlineOptions CreateOptions(bool useInvalidPassword)
    {
        return new StreamlineOptions
        {
            BootstrapServers = _fixture.BootstrapServers,
            ConnectTimeout = StreamlineTestEnvironment.ReadyTimeout,
            RequestTimeout = StreamlineTestEnvironment.ReadyTimeout,
            SecurityProtocol = _fixture.Protocol switch
            {
                AuthenticationProtocol.Ssl => SecurityProtocol.Ssl,
                AuthenticationProtocol.SaslPlaintext => SecurityProtocol.SaslPlaintext,
                AuthenticationProtocol.SaslSsl => SecurityProtocol.SaslSsl,
                _ => throw new InvalidOperationException("Unsupported authentication fixture protocol."),
            },
            Tls = _fixture.UsesTls
                ? new TlsOptions
                {
                    CaCertificatePath = _fixture.CaCertificatePath,
                    ClientCertificatePath = _fixture.ClientCertificatePath,
                    ClientKeyPath = _fixture.ClientKeyPath,
                }
                : null,
            Sasl = _fixture.UsesSasl
                ? new SaslOptions
                {
                    Mechanism = _fixture.Mechanism switch
                    {
                        AuthenticationMechanism.Plain => SaslMechanism.Plain,
                        AuthenticationMechanism.ScramSha256 => SaslMechanism.ScramSha256,
                        AuthenticationMechanism.ScramSha512 => SaslMechanism.ScramSha512,
                        _ => throw new InvalidOperationException(
                            "SASL fixture must specify a supported mechanism."),
                    },
                    Username = _fixture.Username,
                    Password = useInvalidPassword ? _fixture.InvalidPassword : _fixture.Password,
                }
                : null,
            Producer = new ProducerOptions
            {
                Retries = 0,
                RetryBackoffMs = 0,
            },
        };
    }
}
