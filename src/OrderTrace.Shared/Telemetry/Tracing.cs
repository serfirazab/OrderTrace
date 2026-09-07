using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace OrderTrace.Shared.Telemetry;

// Confluent.Kafka ships no OpenTelemetry auto-instrumentation, so W3C trace context crosses
// the Kafka boundary as plain message headers. The producer injects its current context and
// the consumer extracts it again, making one logical order flow a single trace. Both sides
// talk through the small Inject/Extract helpers below (also exercised by an integration test).
public static class Tracing
{
    // One shared source per whole demo; each service adds it to its tracer provider.
    public const string SourceName = "OrderTrace";

    public static readonly ActivitySource Source = new(SourceName);

    // Deliberately not Propagators.DefaultTextMapPropagator: that global is a no-op until an
    // OpenTelemetry TracerProvider is built (Sdk), so helper stays deterministic everywhere —
    // real services and integration tests alike. Only W3C trace context is carried; no baggage.
    private static readonly TextMapPropagator Propagator = new TraceContextPropagator();

    // Copies the active context (traceparent + tracestate) onto an outgoing message.
    // Header keys are the standard W3C ones, so this works across language/runtime boundaries.
    public static void InjectHeaders(Activity? current, Action<string, string> addHeader)
    {
        if (current is null)
        {
            return; // no active trace to propagate
        }

        var context = new PropagationContext(current.Context, Baggage.Current);
        Propagator.Inject(context, addHeader, (headers, key, value) => headers(key, value));
    }

    // Reads the parent context a producer stored on the message, or null when there is none.
    public static ActivityContext? ExtractContext(Func<string, string?> getHeader)
    {
        var context = Propagator.Extract(
            default,
            getHeader,
            (headers, key) => headers(key) is { } value ? [value] : []);
        return context.ActivityContext != default ? context.ActivityContext : null;
    }
}
