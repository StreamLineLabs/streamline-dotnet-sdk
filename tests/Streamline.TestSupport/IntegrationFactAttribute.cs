using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Streamline.TestSupport;

/// <summary>
/// Trait values applied by this SDK's test attributes.
/// </summary>
public static class TestCategories
{
    /// <summary>The trait name used for test selection.</summary>
    public const string TraitName = "Category";

    /// <summary>Broker/HTTP-dependent tests. Excluded from the default hermetic run.</summary>
    public const string Integration = "Integration";

    /// <summary>Cross-SDK conformance tests. Requires a running Streamline server.</summary>
    public const string Conformance = "Conformance";
}

/// <summary>
/// Marks a test as requiring a reachable Streamline server.
///
/// <para>
/// The test is skipped unless <c>STREAMLINE_INTEGRATION</c> is truthy, so the default
/// <c>dotnet test</c> run stays hermetic. It is also tagged
/// <c>Category=Integration</c> so it can be selected with
/// <c>dotnet test --filter "Category=Integration"</c>.
/// </para>
///
/// <para>
/// When integration testing <em>is</em> enabled, the test does not silently skip:
/// <see cref="IntegrationServerFixture"/> probes the configured endpoints once with a
/// bounded timeout and fails the run if they are unreachable.
/// </para>
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
[TraitDiscoverer("Streamline.TestSupport.IntegrationTraitDiscoverer", "Streamline.TestSupport")]
public sealed class IntegrationFactAttribute : FactAttribute, ITraitAttribute
{
    /// <summary>Creates an opt-in integration fact.</summary>
    public IntegrationFactAttribute()
    {
        if (!StreamlineTestEnvironment.IsIntegrationEnabled)
            Skip = StreamlineTestEnvironment.SkipReason;
    }
}

/// <summary>
/// Data-driven counterpart to <see cref="IntegrationFactAttribute"/>.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
[TraitDiscoverer("Streamline.TestSupport.IntegrationTraitDiscoverer", "Streamline.TestSupport")]
public sealed class IntegrationTheoryAttribute : TheoryAttribute, ITraitAttribute
{
    /// <summary>Creates an opt-in integration theory.</summary>
    public IntegrationTheoryAttribute()
    {
        if (!StreamlineTestEnvironment.IsIntegrationEnabled)
            Skip = StreamlineTestEnvironment.SkipReason;
    }
}

/// <summary>
/// Supplies the <c>Category=Integration</c> trait for <see cref="IntegrationFactAttribute"/>
/// and <see cref="IntegrationTheoryAttribute"/>.
/// </summary>
public sealed class IntegrationTraitDiscoverer : ITraitDiscoverer
{
    /// <inheritdoc />
    public IEnumerable<KeyValuePair<string, string>> GetTraits(IAttributeInfo traitAttribute)
    {
        yield return new KeyValuePair<string, string>(TestCategories.TraitName, TestCategories.Integration);
    }
}
