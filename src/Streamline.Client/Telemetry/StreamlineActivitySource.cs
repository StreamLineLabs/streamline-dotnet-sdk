using System.Diagnostics;

namespace Streamline.Client.Telemetry;

/// <summary>
/// Provides OpenTelemetry-compatible distributed tracing for Streamline operations
/// using the standard .NET <see cref="ActivitySource"/> and <see cref="Activity"/> APIs.
///
/// <para>
/// All activities (spans) follow OTel semantic conventions for messaging:
/// <list type="bullet">
///   <item>Activity name: <c>{topic} {operation}</c> (e.g., "orders produce")</item>
///   <item>Tag: <c>messaging.system</c> = "streamline"</item>
///   <item>Tag: <c>messaging.destination.name</c> = topic name</item>
///   <item>Tag: <c>messaging.operation</c> = "produce" | "consume" | "process"</item>
///   <item>Kind: <see cref="ActivityKind.Producer"/> for produce, <see cref="ActivityKind.Consumer"/> for consume</item>
/// </list>
/// </para>
///
/// <para>
/// To enable tracing, register the activity source with your OpenTelemetry configuration:
/// <code>
/// builder.Services.AddOpenTelemetry()
///     .WithTracing(tracing => tracing
///         .AddSource(StreamlineActivitySource.SourceName)
///         .AddOtlpExporter());
/// </code>
/// </para>
///
/// <para>
/// Activities are only created when there is an active listener (e.g., an OTel SDK
/// or DiagnosticListener). When no listener is registered, <see cref="ActivitySource.StartActivity(string, ActivityKind)"/>
/// returns <c>null</c> and there is zero overhead.
/// </para>
/// </summary>
public static class StreamlineActivitySource
{
    /// <summary>
    /// The name of the activity source. Use this when registering with OpenTelemetry.
    /// </summary>
    public const string SourceName = "Streamline.Client";

    /// <summary>
    /// The version of the activity source.
    /// </summary>
    public const string SourceVersion = "0.2.0";

    private static readonly ActivitySource Source = new(SourceName, SourceVersion);

    /// <summary>
    /// Starts an activity for a produce operation.
    /// </summary>
    /// <param name="topic">The destination topic name.</param>
    /// <returns>
    /// An <see cref="Activity"/> if there is an active listener, or <c>null</c> otherwise.
    /// The caller should dispose the activity when the operation completes.
    /// </returns>
    /// <example>
    /// <code>
    /// using var activity = StreamlineActivitySource.StartProduce("orders");
    /// await producer.SendAsync("orders", key, value, headers);
    /// </code>
    /// </example>
    public static Activity? StartProduce(string topic)
    {
        var activity = Source.StartActivity(
            $"{topic} produce",
            ActivityKind.Producer);

        if (activity is not null)
        {
            activity.SetTag("messaging.system", "streamline");
            activity.SetTag("messaging.destination.name", topic);
            activity.SetTag("messaging.operation", "produce");
        }

        return activity;
    }

    /// <summary>
    /// Starts an activity for a consume/poll operation.
    /// </summary>
    /// <param name="topic">The source topic name.</param>
    /// <returns>
    /// An <see cref="Activity"/> if there is an active listener, or <c>null</c> otherwise.
    /// </returns>
    /// <example>
    /// <code>
    /// using var activity = StreamlineActivitySource.StartConsume("events");
    /// var records = await consumer.PollAsync(timeout);
    /// activity?.SetTag("messaging.batch.message_count", records.Count.ToString());
    /// </code>
    /// </example>
    public static Activity? StartConsume(string topic)
    {
        var activity = Source.StartActivity(
            $"{topic} consume",
            ActivityKind.Consumer);

        if (activity is not null)
        {
            activity.SetTag("messaging.system", "streamline");
            activity.SetTag("messaging.destination.name", topic);
            activity.SetTag("messaging.operation", "consume");
        }

        return activity;
    }

    /// <summary>
    /// Starts an activity for processing a single consumed record.
    /// </summary>
    /// <param name="topic">The source topic name.</param>
    /// <param name="partition">The partition number.</param>
    /// <param name="offset">The record offset.</param>
    /// <param name="parentContext">
    /// Optional parent activity context extracted from message headers.
    /// </param>
    /// <returns>
    /// An <see cref="Activity"/> if there is an active listener, or <c>null</c> otherwise.
    /// </returns>
    public static Activity? StartProcess(
        string topic,
        int partition,
        long offset,
        ActivityContext? parentContext = null)
    {
        Activity? activity;

        if (parentContext.HasValue)
        {
            activity = Source.StartActivity(
                $"{topic} process",
                ActivityKind.Consumer,
                parentContext.Value);
        }
        else
        {
            activity = Source.StartActivity(
                $"{topic} process",
                ActivityKind.Consumer);
        }

        if (activity is not null)
        {
            activity.SetTag("messaging.system", "streamline");
            activity.SetTag("messaging.destination.name", topic);
            activity.SetTag("messaging.operation", "process");
            activity.SetTag("messaging.destination.partition.id", partition.ToString());
            activity.SetTag("messaging.message.id", offset.ToString());
        }

        return activity;
    }

    /// <summary>
    /// Injects the current activity context into message headers for propagation.
    /// Uses W3C TraceContext format (traceparent/tracestate).
    /// </summary>
    /// <param name="headers">The message headers to inject context into.</param>
    public static void InjectContext(Headers headers)
    {
        var activity = Activity.Current;
        if (activity is null)
            return;

        var traceParent = activity.Id;
        if (traceParent is not null)
        {
            headers.Add("traceparent", traceParent);
        }

        var traceState = activity.TraceStateString;
        if (!string.IsNullOrEmpty(traceState))
        {
            headers.Add("tracestate", traceState);
        }
    }

    /// <summary>
    /// Extracts an activity context from message headers.
    /// </summary>
    /// <param name="headers">The message headers containing trace context.</param>
    /// <returns>
    /// An <see cref="ActivityContext"/> if traceparent header is present and valid,
    /// or <c>null</c> otherwise.
    /// </returns>
    public static ActivityContext? ExtractContext(Headers headers)
    {
        var traceParent = headers.GetString("traceparent");
        if (string.IsNullOrEmpty(traceParent))
            return null;

        if (ActivityContext.TryParse(traceParent, headers.GetString("tracestate"), out var context))
        {
            return context;
        }

        return null;
    }

    /// <summary>
    /// Records an error on the current activity.
    /// </summary>
    /// <param name="activity">The activity to record the error on.</param>
    /// <param name="exception">The exception that occurred.</param>
    public static void RecordError(Activity? activity, Exception exception)
    {
        if (activity is null)
            return;

        activity.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
        {
            { "exception.type", exception.GetType().FullName ?? exception.GetType().Name },
            { "exception.message", exception.Message },
            { "exception.stacktrace", exception.StackTrace ?? "" },
        }));
    }
}
