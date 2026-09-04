using Xunit;

namespace Streamline.TestSupport;

/// <summary>
/// Collection fixture shared by every integration test class.
///
/// <para>
/// When integration testing is disabled the fixture does nothing — it never opens a
/// socket, so the default test run stays hermetic. When
/// <c>STREAMLINE_INTEGRATION</c> is enabled the fixture probes the configured
/// endpoints exactly once with a bounded timeout and throws, failing the run fast,
/// if the server is unreachable.
/// </para>
/// </summary>
public class IntegrationServerFixture : IAsyncLifetime
{
    /// <summary>The Kafka-protocol endpoint integration tests should use.</summary>
    public string BootstrapServers { get; } = StreamlineTestEnvironment.BootstrapServers;

    /// <summary>The HTTP management endpoint integration tests should use.</summary>
    public string HttpBaseUrl { get; } = StreamlineTestEnvironment.HttpBaseUrl;

    /// <summary>Whether the current run opted into integration testing.</summary>
    public bool Enabled { get; } = StreamlineTestEnvironment.IsIntegrationEnabled;

    /// <inheritdoc />
    public async Task InitializeAsync()
    {
        if (!Enabled)
            return;

        await IntegrationEndpointProbe.EnsureReachableAsync(
            BootstrapServers,
            HttpBaseUrl,
            StreamlineTestEnvironment.ReadyTimeout).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task DisposeAsync() => Task.CompletedTask;
}

/// <summary>
/// Well-known name of the xUnit collection that shares
/// <see cref="IntegrationServerFixture"/>.
///
/// <para>
/// xUnit only discovers <c>[CollectionDefinition]</c> types inside the test assembly
/// being run, so each test assembly must declare its own definition bound to this
/// name, for example:
/// </para>
///
/// <code>
/// [CollectionDefinition(IntegrationCollection.Name)]
/// public sealed class IntegrationTestCollection : ICollectionFixture&lt;IntegrationServerFixture&gt; { }
/// </code>
/// </summary>
public static class IntegrationCollection
{
    /// <summary>The xUnit collection name for integration tests.</summary>
    public const string Name = "streamline-integration";
}
