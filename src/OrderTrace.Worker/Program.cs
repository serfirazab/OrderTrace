using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OrderTrace.Shared.Telemetry;
using OrderTrace.Worker.Data;
using OrderTrace.Worker.Options;
using OrderTrace.Worker.Services;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<WorkerSettings>(builder.Configuration.GetSection("Worker"));

builder.Services.AddDbContext<OrderDbContext>(o =>
    o.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

builder.Services.AddHttpClient<FraudCheckClient>(c =>
{
    c.BaseAddress = new Uri(builder.Configuration["FraudCheck:BaseUrl"] ?? "http://localhost:5011");
    c.Timeout = TimeSpan.FromSeconds(15);
});

// Phase 2: OpenTelemetry. OrderConsumerWorker extracts the producer's W3C context from each
// message and starts a consumer span under it; the HTTP fraud-check call and the EF Core
// save are instrumented automatically, so they fall under that same trace.
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService(builder.Configuration["OTEL_SERVICE_NAME"] ?? "ordertrace-worker"))
    .WithTracing(t => t
        .AddSource(Tracing.SourceName)
        .AddHttpClientInstrumentation()
        .AddEntityFrameworkCoreInstrumentation()
        .AddOtlpExporter());

builder.Services.AddHostedService<OrderConsumerWorker>();

var host = builder.Build();

// Phase 1: ensure schema exists on startup so `dotnet run` is all that is needed.
using (var scope = host.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
    await db.Database.EnsureCreatedAsync();
}

await host.RunAsync();
