using Microsoft.EntityFrameworkCore;
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

builder.Services.AddHostedService<OrderConsumerWorker>();

var host = builder.Build();

// Phase 1: ensure schema exists on startup so `dotnet run` is all that is needed.
using (var scope = host.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
    await db.Database.EnsureCreatedAsync();
}

await host.RunAsync();
