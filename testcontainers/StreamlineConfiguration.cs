using Docker.DotNet.Models;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;

namespace Streamline.TestContainers;

/// <summary>
/// Configuration for a <see cref="StreamlineContainer"/>.
/// </summary>
/// <remarks>
/// This class extends the Testcontainers container configuration to support
/// Streamline-specific settings. It is used internally by <see cref="StreamlineBuilder"/>.
/// </remarks>
public sealed class StreamlineConfiguration : ContainerConfiguration
{
    /// <summary>
    /// Initializes a new instance of the <see cref="StreamlineConfiguration"/> class.
    /// </summary>
    public StreamlineConfiguration()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamlineConfiguration"/> class
    /// from a resource configuration.
    /// </summary>
    /// <param name="resourceConfiguration">The resource configuration.</param>
    public StreamlineConfiguration(IResourceConfiguration<CreateContainerParameters> resourceConfiguration)
        : base(resourceConfiguration)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamlineConfiguration"/> class
    /// from a container configuration.
    /// </summary>
    /// <param name="resourceConfiguration">The container configuration.</param>
    public StreamlineConfiguration(IContainerConfiguration resourceConfiguration)
        : base(resourceConfiguration)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="StreamlineConfiguration"/> class
    /// by merging two configurations.
    /// </summary>
    /// <param name="oldValue">The old configuration.</param>
    /// <param name="newValue">The new configuration to merge.</param>
    public StreamlineConfiguration(StreamlineConfiguration oldValue, StreamlineConfiguration newValue)
        : base(oldValue, newValue)
    {
    }
}
