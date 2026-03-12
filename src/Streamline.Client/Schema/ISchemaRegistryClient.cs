namespace Streamline.Client.Schema;

/// <summary>
/// Interface for the Streamline Schema Registry client.
/// Provides methods for registering, retrieving, and managing schemas
/// and their compatibility settings.
/// </summary>
public interface ISchemaRegistryClient : IAsyncDisposable
{
    /// <summary>
    /// Registers a new schema under the specified subject.
    /// If the schema already exists, its existing ID is returned.
    /// </summary>
    /// <param name="subject">The subject to register the schema under (e.g., "orders-value").</param>
    /// <param name="schema">The schema definition text.</param>
    /// <param name="format">The schema format.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The globally unique schema ID.</returns>
    Task<int> RegisterSchemaAsync(
        string subject,
        string schema,
        SchemaFormat format,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves a schema for a specific subject and version.
    /// </summary>
    /// <param name="subject">The subject name.</param>
    /// <param name="version">The schema version number.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The schema information.</returns>
    Task<SchemaInfo> GetSchemaAsync(
        string subject,
        int version,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the latest schema for a subject.
    /// </summary>
    /// <param name="subject">The subject name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The latest schema information.</returns>
    Task<SchemaInfo> GetLatestSchemaAsync(
        string subject,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves a schema by its globally unique ID.
    /// </summary>
    /// <param name="id">The global schema ID.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The schema information.</returns>
    Task<SchemaInfo> GetSchemaByIdAsync(
        int id,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists all registered subjects.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A read-only list of subject names.</returns>
    Task<IReadOnlyList<string>> ListSubjectsAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a subject and all its associated schema versions.
    /// </summary>
    /// <param name="subject">The subject name to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task DeleteSubjectAsync(
        string subject,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks whether a schema is compatible with the existing schemas for a subject.
    /// </summary>
    /// <param name="subject">The subject name to check compatibility against.</param>
    /// <param name="schema">The schema definition text to test.</param>
    /// <param name="format">The schema format.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> if the schema is compatible; otherwise, <c>false</c>.</returns>
    Task<bool> CheckCompatibilityAsync(
        string subject,
        string schema,
        SchemaFormat format,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the compatibility level configured for a subject.
    /// </summary>
    /// <param name="subject">The subject name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The configured compatibility level.</returns>
    Task<CompatibilityLevel> GetCompatibilityLevelAsync(
        string subject,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the compatibility level for a subject.
    /// </summary>
    /// <param name="subject">The subject name.</param>
    /// <param name="level">The compatibility level to set.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SetCompatibilityLevelAsync(
        string subject,
        CompatibilityLevel level,
        CancellationToken cancellationToken = default);
}
