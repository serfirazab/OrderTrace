using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OrderTrace.Shared.Contracts;
using OrderTrace.Shared.Kafka;
using OrderTrace.Shared.Telemetry;

var builder = WebApplication.CreateBuilder(args);

var bootstrap = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:19092";
var serviceName = builder.Configuration["OTEL_SERVICE_NAME"] ?? "ordertrace-orderingest";

builder.Services.AddSingleton<IProducer<string, string>>(_ =>
    new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = bootstrap,
        ClientId = "ordertrace-ingest",
        Acks = Acks.All,
    }).Build());

// Phase 2: OpenTelemetry. The publish handler starts a producer span and Tracing.InjectHeaders
// copies its W3C context onto the Kafka record so the worker can continue the same trace.
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService(serviceName))
    .WithTracing(t => t
        .AddSource(Tracing.SourceName)
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter());

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "ok", service = "OrderIngest" }));

app.MapPost("/orders", async (CreateOrderRequest req, IProducer<string, string> producer, ILogger<Program> log) =>
{
    var evt = new OrderCreated(
        OrderId: Guid.NewGuid(),
        CustomerName: req.CustomerName,
        TotalAmount: req.TotalAmount,
        OccurredAtUtc: DateTimeOffset.UtcNow);

    log.LogInformation("Ingest received OrderId={OrderId} Customer={Customer} Amount={Amount}",
        evt.OrderId, evt.CustomerName, evt.TotalAmount);

    var message = new Message<string, string>
    {
        Key = evt.OrderId.ToString(),
        Value = JsonSerializer.Serialize(evt),
    };

    // Publish happens under its own producer span; the W3C context of that span (which itself
    // hangs under the incoming POST /orders server span) is stored on the message headers.
    using var publish = Tracing.Source.StartActivity("kafka.publish", ActivityKind.Producer);
    publish?.SetTag("messaging.system", "kafka");
    publish?.SetTag("messaging.destination", KafkaTopics.OrderCreated);
    publish?.SetTag("messaging.operation", "publish");

    var headers = new Headers();
    Tracing.InjectHeaders(Activity.Current, (key, value) => headers.Add(key, Encoding.UTF8.GetBytes(value)));
    message.Headers = headers;

    var sw = Stopwatch.StartNew();
    try
    {
        await producer.ProduceAsync(KafkaTopics.OrderCreated, message);
    }
    catch (ProduceException<string, string> ex)
    {
        log.LogError(ex, "Ingest publish FAILED OrderId={OrderId}", evt.OrderId);
        return Results.Json(new { message = "Message broker unavailable" }, statusCode: StatusCodes.Status503ServiceUnavailable);
    }
    sw.Stop();

    log.LogInformation("Ingest published OrderId={OrderId} TraceId={TraceId} SpanId={SpanId} PublishLatencyMs={LatencyMs}",
        evt.OrderId, publish?.TraceId, publish?.SpanId, sw.ElapsedMilliseconds);

    return Results.Accepted($"/orders/{evt.OrderId}", new { id = evt.OrderId, message = "Order accepted for processing" });
});

app.Run();

public sealed record CreateOrderRequest(string CustomerName, decimal TotalAmount);
