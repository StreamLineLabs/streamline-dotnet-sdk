using System.Text.RegularExpressions;

namespace Streamline.Client;

/// <summary>
/// Validates Kafka topic names against the protocol specification.
/// </summary>
public static partial class TopicNameValidator
{
    /// <summary>Maximum allowed length for a topic name.</summary>
    public const int MaxTopicNameLength = 249;

    [GeneratedRegex(@"^[a-zA-Z0-9._\-]+$")]
    private static partial Regex ValidCharactersRegex();

    /// <summary>
    /// Validates a topic name and throws <see cref="ArgumentException"/> if invalid.
    /// </summary>
    /// <param name="topic">The topic name to validate.</param>
    /// <param name="paramName">The parameter name for the exception.</param>
    /// <exception cref="ArgumentException">Thrown when the topic name is invalid.</exception>
    public static void Validate(string topic, string paramName = "topic")
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("Topic name cannot be null, empty, or whitespace.", paramName);
        }

        if (topic.Length > MaxTopicNameLength)
        {
            throw new ArgumentException(
                $"Topic name cannot exceed {MaxTopicNameLength} characters. Got {topic.Length}.", paramName);
        }

        if (topic is "." or "..")
        {
            throw new ArgumentException(
                $"Topic name cannot be \"{topic}\".", paramName);
        }

        if (!ValidCharactersRegex().IsMatch(topic))
        {
            throw new ArgumentException(
                "Topic name contains invalid characters. Only alphanumeric characters, '.', '_', and '-' are allowed.", paramName);
        }
    }
}
