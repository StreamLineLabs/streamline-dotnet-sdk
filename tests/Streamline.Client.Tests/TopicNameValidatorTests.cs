using Streamline.Client;
using Xunit;

namespace Streamline.Client.Tests;

public class TopicNameValidatorTests
{
    // --- Valid names ---

    [Theory]
    [InlineData("my-topic")]
    [InlineData("my.topic")]
    [InlineData("my_topic")]
    [InlineData("MyTopic123")]
    [InlineData("a")]
    [InlineData("topic-with-dashes")]
    [InlineData("topic.with.dots")]
    [InlineData("topic_with_underscores")]
    [InlineData("MixedCase.with-all_3")]
    public void Validate_ValidTopicNames_DoesNotThrow(string topic)
    {
        TopicNameValidator.Validate(topic);
    }

    // --- Null/empty/whitespace ---

    [Fact]
    public void Validate_NullTopic_ThrowsArgumentException()
    {
        var ex = Assert.Throws<ArgumentException>(() => TopicNameValidator.Validate(null!));
        Assert.Contains("null, empty, or whitespace", ex.Message);
    }

    [Fact]
    public void Validate_EmptyTopic_ThrowsArgumentException()
    {
        var ex = Assert.Throws<ArgumentException>(() => TopicNameValidator.Validate(""));
        Assert.Contains("null, empty, or whitespace", ex.Message);
    }

    [Fact]
    public void Validate_WhitespaceTopic_ThrowsArgumentException()
    {
        var ex = Assert.Throws<ArgumentException>(() => TopicNameValidator.Validate("   "));
        Assert.Contains("null, empty, or whitespace", ex.Message);
    }

    // --- Length ---

    [Fact]
    public void Validate_MaxLengthTopic_DoesNotThrow()
    {
        var topic = new string('a', TopicNameValidator.MaxTopicNameLength);
        TopicNameValidator.Validate(topic);
    }

    [Fact]
    public void Validate_ExceedsMaxLength_ThrowsArgumentException()
    {
        var topic = new string('a', TopicNameValidator.MaxTopicNameLength + 1);
        var ex = Assert.Throws<ArgumentException>(() => TopicNameValidator.Validate(topic));
        Assert.Contains("cannot exceed", ex.Message);
    }

    // --- Reserved names ---

    [Fact]
    public void Validate_SingleDot_ThrowsArgumentException()
    {
        var ex = Assert.Throws<ArgumentException>(() => TopicNameValidator.Validate("."));
        Assert.Contains("\".\"", ex.Message);
    }

    [Fact]
    public void Validate_DoubleDot_ThrowsArgumentException()
    {
        var ex = Assert.Throws<ArgumentException>(() => TopicNameValidator.Validate(".."));
        Assert.Contains("\"..\"", ex.Message);
    }

    // --- Invalid characters ---

    [Theory]
    [InlineData("topic with spaces")]
    [InlineData("topic/slash")]
    [InlineData("topic@at")]
    [InlineData("topic#hash")]
    [InlineData("topic$dollar")]
    [InlineData("topic!bang")]
    [InlineData("topic+plus")]
    [InlineData("topic=equals")]
    public void Validate_InvalidCharacters_ThrowsArgumentException(string topic)
    {
        var ex = Assert.Throws<ArgumentException>(() => TopicNameValidator.Validate(topic));
        Assert.Contains("invalid characters", ex.Message);
    }

    // --- Custom param name ---

    [Fact]
    public void Validate_CustomParamName_IncludedInException()
    {
        var ex = Assert.Throws<ArgumentException>(() => TopicNameValidator.Validate("", "topicName"));
        Assert.Equal("topicName", ex.ParamName);
    }

    // --- Integration: Producer rejects invalid topic ---

    [Fact]
    public async Task Producer_SendAsync_InvalidTopic_ThrowsArgumentException()
    {
        var client = new StreamlineClient("localhost:9092");
        await using var producer = client.CreateProducer<string, string>();

        await Assert.ThrowsAsync<ArgumentException>(
            () => producer.SendAsync("", "key", "value"));
    }

    [Fact]
    public async Task Producer_SendAsync_ReservedDotTopic_ThrowsArgumentException()
    {
        var client = new StreamlineClient("localhost:9092");
        await using var producer = client.CreateProducer<string, string>();

        await Assert.ThrowsAsync<ArgumentException>(
            () => producer.SendAsync(".", "key", "value"));
    }

    [Fact]
    public async Task Producer_SendBatchAsync_InvalidTopic_ThrowsArgumentException()
    {
        var client = new StreamlineClient("localhost:9092");
        await using var producer = client.CreateProducer<string, string>();

        await Assert.ThrowsAsync<ArgumentException>(
            () => producer.SendBatchAsync("topic/invalid", new[] { ((string?)"key", "value") }));
    }

    // --- Integration: Consumer rejects invalid topic ---

    [Fact]
    public void Consumer_Create_InvalidTopic_ThrowsArgumentException()
    {
        var client = new StreamlineClient("localhost:9092");

        Assert.Throws<ArgumentException>(
            () => client.CreateConsumer<string, string>("", "group-id"));
    }

    [Fact]
    public void Consumer_Create_DotDotTopic_ThrowsArgumentException()
    {
        var client = new StreamlineClient("localhost:9092");

        Assert.Throws<ArgumentException>(
            () => client.CreateConsumer<string, string>("..", "group-id"));
    }

    // --- Integration: AdminClient rejects invalid topic ---

    [Fact]
    public async Task AdminClient_CreateTopicAsync_InvalidTopic_ThrowsArgumentException()
    {
        await using var admin = new AdminClient(new HttpClient { BaseAddress = new Uri("http://localhost:9094") });

        await Assert.ThrowsAsync<ArgumentException>(
            () => admin.CreateTopicAsync("topic with spaces"));
    }

    [Fact]
    public async Task AdminClient_DescribeTopicAsync_InvalidTopic_ThrowsArgumentException()
    {
        await using var admin = new AdminClient(new HttpClient { BaseAddress = new Uri("http://localhost:9094") });

        await Assert.ThrowsAsync<ArgumentException>(
            () => admin.DescribeTopicAsync(""));
    }

    [Fact]
    public async Task AdminClient_DeleteTopicAsync_InvalidTopic_ThrowsArgumentException()
    {
        await using var admin = new AdminClient(new HttpClient { BaseAddress = new Uri("http://localhost:9094") });

        await Assert.ThrowsAsync<ArgumentException>(
            () => admin.DeleteTopicAsync("."));
    }
}
