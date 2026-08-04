using Streamline.TestSupport;
using Xunit;

namespace Streamline.Client.Tests;

/// <summary>
/// Regression tests for the environment that decides which tests run.
///
/// <para>
/// These guard the invariant that makes <c>dotnet test</c> reproducible: broker
/// dependent tests are opt-in, and the default run never points at a developer's
/// local <c>localhost</c> broker.
/// </para>
/// </summary>
public class TestSelectionTests
{
    [Theory]
    [InlineData("1")]
    [InlineData("true")]
    [InlineData("TRUE")]
    [InlineData("True")]
    [InlineData("yes")]
    [InlineData("on")]
    [InlineData("  1  ")]
    public void IsTruthy_RecognisesOptInValues(string value)
    {
        Assert.True(StreamlineTestEnvironment.IsTruthy(value));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("0")]
    [InlineData("false")]
    [InlineData("no")]
    [InlineData("off")]
    [InlineData("maybe")]
    public void IsTruthy_TreatsEverythingElseAsOptOut(string? value)
    {
        Assert.False(StreamlineTestEnvironment.IsTruthy(value));
    }

    [Fact]
    public void UnitEndpoints_NeverPointAtLocalhost()
    {
        Assert.DoesNotContain("localhost", StreamlineTestEnvironment.UnitBootstrapServers, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("127.0.0.1", StreamlineTestEnvironment.UnitBootstrapServers, StringComparison.Ordinal);
        Assert.DoesNotContain("localhost", StreamlineTestEnvironment.UnitHttpBaseUrl, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("127.0.0.1", StreamlineTestEnvironment.UnitHttpBaseUrl, StringComparison.Ordinal);
    }

    [Fact]
    public void UnitEndpoints_UseTheReservedInvalidTld()
    {
        // RFC 6761 guarantees ".invalid" never resolves, so an accidental connection
        // attempt from a unit test fails instead of reaching a real broker.
        Assert.Contains(".invalid", StreamlineTestEnvironment.UnitBootstrapServers, StringComparison.Ordinal);
        Assert.Contains(".invalid", StreamlineTestEnvironment.UnitHttpBaseUrl, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null, StreamlineTestEnvironment.DefaultReadyTimeoutSeconds)]
    [InlineData("", StreamlineTestEnvironment.DefaultReadyTimeoutSeconds)]
    [InlineData("not-a-number", StreamlineTestEnvironment.DefaultReadyTimeoutSeconds)]
    [InlineData("45", 45)]
    [InlineData("0", 1)]
    [InlineData("-5", 1)]
    [InlineData("100000", 300)]
    public void ParseReadyTimeoutSeconds_IsAlwaysBounded(string? value, int expected)
    {
        Assert.Equal(expected, StreamlineTestEnvironment.ParseReadyTimeoutSeconds(value));
    }

    [Theory]
    [InlineData("localhost:9092", "localhost", 9092)]
    [InlineData("broker-a:19092,broker-b:19093", "broker-a", 19092)]
    [InlineData(" broker-a:19092 , broker-b:19093 ", "broker-a", 19092)]
    [InlineData("broker-only", "broker-only", 9092)]
    [InlineData("broker:not-a-port", "broker", 9092)]
    public void ParseFirstBroker_SplitsHostAndPort(string bootstrap, string expectedHost, int expectedPort)
    {
        var (host, port) = StreamlineTestEnvironment.ParseFirstBroker(bootstrap);

        Assert.Equal(expectedHost, host);
        Assert.Equal(expectedPort, port);
    }

    [Fact]
    public void ParseFirstBroker_RejectsNull()
    {
        Assert.Throws<ArgumentNullException>(() => StreamlineTestEnvironment.ParseFirstBroker(null!));
    }

    [Fact]
    public void IntegrationFact_SkipsExactlyWhenIntegrationIsDisabled()
    {
        var attribute = new IntegrationFactAttribute();

        if (StreamlineTestEnvironment.IsIntegrationEnabled)
            Assert.Null(attribute.Skip);
        else
            Assert.Equal(StreamlineTestEnvironment.SkipReason, attribute.Skip);
    }

    [Fact]
    public void IntegrationTheory_SkipsExactlyWhenIntegrationIsDisabled()
    {
        var attribute = new IntegrationTheoryAttribute();

        if (StreamlineTestEnvironment.IsIntegrationEnabled)
            Assert.Null(attribute.Skip);
        else
            Assert.Equal(StreamlineTestEnvironment.SkipReason, attribute.Skip);
    }

    [Fact]
    public void IntegrationFact_CarriesTheIntegrationCategoryTrait()
    {
        var traitDiscoverer = typeof(IntegrationFactAttribute)
            .GetCustomAttributes(typeof(Xunit.Sdk.TraitDiscovererAttribute), inherit: false)
            .Cast<Xunit.Sdk.TraitDiscovererAttribute>()
            .SingleOrDefault();

        Assert.NotNull(traitDiscoverer);
        Assert.Equal(TestCategories.Integration, new IntegrationTraitDiscoverer()
            .GetTraits(null!)
            .Single(t => t.Key == TestCategories.TraitName).Value);
    }

    [Fact]
    public async Task EndpointProbe_FailsFastOnAnUnroutableBroker()
    {
        var timeout = TimeSpan.FromMilliseconds(500);
        var started = System.Diagnostics.Stopwatch.StartNew();

        var reachable = await IntegrationEndpointProbe.IsBrokerReachableAsync(
            StreamlineTestEnvironment.UnitBootstrapServers, timeout);

        started.Stop();
        Assert.False(reachable);
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(10),
            $"Probe should be bounded but took {started.Elapsed}");
    }

    [Fact]
    public async Task EndpointProbe_FailsFastOnAnUnroutableHttpEndpoint()
    {
        var timeout = TimeSpan.FromMilliseconds(500);
        var started = System.Diagnostics.Stopwatch.StartNew();

        var reachable = await IntegrationEndpointProbe.IsHttpReachableAsync(
            StreamlineTestEnvironment.UnitHttpBaseUrl, timeout);

        started.Stop();
        Assert.False(reachable);
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(10),
            $"Probe should be bounded but took {started.Elapsed}");
    }

    [Fact]
    public async Task EnsureReachableAsync_ThrowsActionableErrorWhenUnreachable()
    {
        var ex = await Assert.ThrowsAsync<StreamlineIntegrationUnavailableException>(
            () => IntegrationEndpointProbe.EnsureReachableAsync(
                StreamlineTestEnvironment.UnitBootstrapServers,
                StreamlineTestEnvironment.UnitHttpBaseUrl,
                TimeSpan.FromMilliseconds(500)));

        Assert.Contains(StreamlineTestEnvironment.IntegrationVariable, ex.Message, StringComparison.Ordinal);
        Assert.Contains("docker compose", ex.Message, StringComparison.Ordinal);
        Assert.Contains(StreamlineTestEnvironment.BootstrapVariable, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Fixture_DoesNotProbeWhenIntegrationIsDisabled()
    {
        if (StreamlineTestEnvironment.IsIntegrationEnabled)
            return;

        var fixture = new IntegrationServerFixture();

        Assert.False(fixture.Enabled);

        // Completes immediately without opening a socket.
        await fixture.InitializeAsync();
        await fixture.DisposeAsync();
    }

    [Fact]
    public void Fixture_ExposesConfiguredEndpoints()
    {
        var fixture = new IntegrationServerFixture();

        Assert.Equal(StreamlineTestEnvironment.BootstrapServers, fixture.BootstrapServers);
        Assert.Equal(StreamlineTestEnvironment.HttpBaseUrl, fixture.HttpBaseUrl);
        Assert.False(fixture.HttpBaseUrl.EndsWith('/'), "HTTP base URL must not carry a trailing slash");
    }

    [Fact]
    public void Image_DefaultsToTheConfigurableRegistryReference()
    {
        var image = StreamlineTestEnvironment.Image;

        Assert.False(string.IsNullOrWhiteSpace(image));
        Assert.Equal(
            Environment.GetEnvironmentVariable(StreamlineTestEnvironment.ImageVariable) is { Length: > 0 } custom
                ? custom
                : StreamlineTestEnvironment.DefaultImage,
            image);
    }
}
