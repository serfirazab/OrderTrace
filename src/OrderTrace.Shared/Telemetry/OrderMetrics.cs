using System.Diagnostics.Metrics;

namespace OrderTrace.Shared.Telemetry;

// Business RED metrics for the order-processing path. The Worker is a consumer with no inbound
// HTTP, so its "rate / errors / duration" come from these instruments (auto ASP.NET/HTTP
// instrumentations cover OrderIngest, FraudCheck and the Worker's outbound fraud call).
public static class OrderMetrics
{
    public const string MeterName = "OrderTrace";

    private static readonly Meter Meter = new(MeterName);

    // How long one order took end-to-end in the worker (attempts included) — the duration that
    // Phase 1 could not attribute. Tagged by outcome so retry storms are visible.
    public static readonly Histogram<double> ProcessDuration = Meter.CreateHistogram<double>(
        name: "order.process.duration",
        unit: "ms",
        description: "Total wall time to finish one order (all attempts included).");

    // Orders persisted after a successful fraud-check.
    public static readonly Counter<long> Processed = Meter.CreateCounter<long>(
        name: "order.process.completed",
        description: "Orders fully processed and persisted.");

    // Orders that exhausted their retries and were skipped as poison.
    public static readonly Counter<long> Failed = Meter.CreateCounter<long>(
        name: "order.process.failed",
        description: "Orders that exhausted retries and were skipped as poison.");

    public static void RecordProcessed(double elapsedMs, int attempts)
        => Record(elapsedMs, attempts, ok: true);

    public static void RecordFailed(double elapsedMs, int attempts)
        => Record(elapsedMs, attempts, ok: false);

    private static void Record(double elapsedMs, int attempts, bool ok)
    {
        ProcessDuration.Record(elapsedMs, new KeyValuePair<string, object?>[]
        {
            new("outcome", ok ? "ok" : "failed"),
            new("attempts", attempts),
        });

        if (ok)
        {
            Processed.Add(1);
        }
        else
        {
            Failed.Add(1);
        }
    }
}
