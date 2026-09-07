using System.Diagnostics;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OrderTrace.FraudCheck.Models;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<ChaosState>();

// Phase 2+3: OpenTelemetry. The worker's instrumented HttpClient sends a W3C traceparent header
// on the fraud-check call; ASP.NET Core instrumentation here extracts it and joins that trace.
// Phase 3: its 503s during a failure-injection spell feed http.server.request.duration RED, and
// logs export with trace_id/span_id so a failing order's Loki lines link to the same waterfall.
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService(builder.Configuration["OTEL_SERVICE_NAME"] ?? "ordertrace-fraudcheck"))
    .WithTracing(t => t
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter())
    .WithMetrics(m => m
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter())
    .WithLogging(logging => logging.AddOtlpExporter(),
        options => options.IncludeFormattedMessage = true);

var app = builder.Build();

app.MapGet("/health", (ChaosState chaos) => Results.Ok(new { status = "ok", service = "FraudCheck", chaos = chaos }));

// Runtime chaos control — changes apply without a restart.
app.MapGet("/_chaos", (ChaosState chaos) => Results.Ok(chaos));
app.MapPost("/_chaos", (ChaosConfig cfg, ChaosState chaos, ILogger<Program> log) =>
{
    chaos.Apply(cfg);
    log.LogWarning("FraudCheck chaos updated LatencyMs={LatencyMs} FailureRate={FailureRate}", cfg.LatencyMs, cfg.FailureRate);
    return Results.Ok(chaos);
});

app.MapPost("/v1/fraudcheck", async (FraudCheckRequest req, ChaosState chaos, ILogger<Program> log) =>
{
    var sw = Stopwatch.StartNew();
    log.LogInformation("FraudCheck start OrderId={OrderId} Amount={Amount}", req.OrderId, req.TotalAmount);

    // Simulated downstream latency (slow dependency — the "why is this order slow?" case).
    if (chaos.LatencyMs > 0)
    {
        await Task.Delay(chaos.LatencyMs);
    }

    // Simulated random failure (the "this order keeps getting retried" case).
    if (Random.Shared.NextDouble() < chaos.FailureRate)
    {
        sw.Stop();
        log.LogError("FraudCheck FAILED OrderId={OrderId} ElapsedMs={ElapsedMs}", req.OrderId, sw.ElapsedMilliseconds);
        return Results.Json(new { message = "Simulated downstream outage" }, statusCode: StatusCodes.Status503ServiceUnavailable);
    }

    // Small normal-case processing jitter so healthy traces are not empty.
    await Task.Delay(Random.Shared.Next(20, 120));

    // Large amounts get flagged for review → declined in this mock.
    var approved = req.TotalAmount < 10_000m;
    var score = Random.Shared.Next(55, 99);

    sw.Stop();
    log.LogInformation("FraudCheck done OrderId={OrderId} Approved={Approved} Score={Score} ElapsedMs={ElapsedMs}",
        req.OrderId, approved, score, sw.ElapsedMilliseconds);

    return Results.Ok(new FraudDecision(req.OrderId, approved, score, DateTimeOffset.UtcNow));
});

app.Run();
