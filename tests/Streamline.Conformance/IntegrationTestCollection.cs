using Streamline.TestSupport;
using Xunit;

namespace Streamline.Conformance;

/// <summary>
/// Binds <see cref="IntegrationServerFixture"/> to this assembly's conformance tests so
/// the bounded readiness probe runs exactly once per test run.
/// </summary>
[CollectionDefinition(IntegrationCollection.Name)]
public sealed class IntegrationTestCollection : ICollectionFixture<IntegrationServerFixture>
{
}
