using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using OrderTrace.Shared.Contracts;
using OrderTrace.Shared.Kafka;

var builder = WebApplication.CreateBuilder(args);

var bootstrap = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:19092";

// Phase 1 (log-only): a plain producer. No telemetry yet — see Phase 2.
builder.Services.AddSingleton<IProducer<string, string>>(_ =>
    new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = bootstrap,
        ClientId = "ordertrace-ingest",
        Acks = Acks.All,
    }).Build());

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

    log.LogInformation("Ingest published OrderId={OrderId} PublishLatencyMs={LatencyMs}",
        evt.OrderId, sw.ElapsedMilliseconds);

    return Results.Accepted($"/orders/{evt.OrderId}", new { id = evt.OrderId, message = "Order accepted for processing" });
});

app.Run();

public sealed record CreateOrderRequest(string CustomerName, decimal TotalAmount);
