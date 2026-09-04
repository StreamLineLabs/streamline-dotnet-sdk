using Streamline.TestSupport;
using Xunit;

namespace Streamline.Client.Tests;

/// <summary>
/// Binds <see cref="IntegrationServerFixture"/> to this assembly's integration tests so
/// the bounded readiness probe runs exactly once per test run.
/// </summary>
[CollectionDefinition(IntegrationCollection.Name)]
public sealed class IntegrationTestCollection : ICollectionFixture<IntegrationServerFixture>
{
}
