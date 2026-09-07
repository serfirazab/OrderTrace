using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using OrderTrace.Shared.Contracts;
using OrderTrace.Worker.Data;
using OrderTrace.Worker.Options;

namespace OrderTrace.Worker.Services;

// Phase 1 (log-only): consume order-created, call fraud-check, persist, commit offset.
// At-least-once: an offset is committed only after the order is persisted. Failures retry
// with a bounded backoff; a message that exhausts retries is treated as poison and skipped.
public sealed class OrderConsumerWorker(
    IServiceScopeFactory scopeFactory,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<WorkerSettings> workerSettings,
    ILogger<OrderConsumerWorker> log) : BackgroundService
{
    private readonly KafkaOptions _kafka = kafkaOptions.Value;
    private readonly WorkerSettings _settings = workerSettings.Value;
    private IConsumer<string, string>? _consumer;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = _kafka.GroupId,
            ClientId = "ordertrace-worker",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
        }).Build();

        _consumer.Subscribe(_kafka.Topic);
        log.LogInformation("Worker subscribed Topic={Topic} Group={Group}", _kafka.Topic, _kafka.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> result;
                try
                {
                    result = _consumer.Consume(stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException ex)
                {
                    log.LogError(ex, "Worker consume error");
                    continue;
                }

                await ProcessAsync(result, stoppingToken);
            }
        }
        finally
        {
            _consumer.Close();
        }
    }

    private async Task ProcessAsync(ConsumeResult<string, string> result, CancellationToken ct)
    {
        var totalStopwatch = Stopwatch.StartNew();

        OrderCreated order;
        try
        {
            order = JsonSerializer.Deserialize<OrderCreated>(result.Message.Value)
                ?? throw new InvalidOperationException("Null payload.");
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Worker UNPARSABLE message Offset={Offset} → poison, skipping", result.Offset.Value);
            Commit(result);
            return;
        }

        using var scope = scopeFactory.CreateScope();
        var fraud = scope.ServiceProvider.GetRequiredService<FraudCheckClient>();
        var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();

        var attempts = 0;
        FraudDecision? decision = null;

        while (attempts < _settings.MaxAttempts)
        {
            attempts++;
            log.LogInformation("Worker attempt OrderId={OrderId} Attempt={Attempt}/{Max}",
                order.OrderId, attempts, _settings.MaxAttempts);

            var callStopwatch = Stopwatch.StartNew();
            try
            {
                decision = await fraud.CheckAsync(order, ct);
                callStopwatch.Stop();
                log.LogInformation("Worker fraudcheck ok OrderId={OrderId} Attempt={Attempt} FraudElapsedMs={ElapsedMs}",
                    order.OrderId, attempts, callStopwatch.ElapsedMilliseconds);
                break;
            }
            catch (Exception ex)
            {
                callStopwatch.Stop();
                log.LogError("Worker fraudcheck FAILED OrderId={OrderId} Attempt={Attempt} FraudElapsedMs={ElapsedMs}",
                    order.OrderId, attempts, callStopwatch.ElapsedMilliseconds);

                if (attempts < _settings.MaxAttempts)
                {
                    await Task.Delay(_settings.BackoffMs, ct);
                }
                else
                {
                    log.LogError(ex, "Worker PERMANENT_FAILURE OrderId={OrderId} AfterAttempts={Attempts}", order.OrderId, attempts);
                }
            }
        }

        totalStopwatch.Stop();

        if (decision is null)
        {
            // Exhausted retries → poison. Skip so the partition is not blocked forever.
            Commit(result);
            return;
        }

        db.OrderEvents.Add(new OrderEvent
        {
            OrderId = order.OrderId,
            CustomerName = order.CustomerName,
            TotalAmount = order.TotalAmount,
            FraudApproved = decision.Approved,
            FraudScore = decision.Score,
            Attempts = attempts,
            TotalLatencyMs = totalStopwatch.ElapsedMilliseconds,
            ProcessedAtUtc = DateTimeOffset.UtcNow,
        });

        await db.SaveChangesAsync(ct);

        log.LogInformation("Worker persisted OrderId={OrderId} Approved={Approved} Attempts={Attempts} TotalLatencyMs={LatencyMs}",
            order.OrderId, decision.Approved, attempts, totalStopwatch.ElapsedMilliseconds);

        Commit(result);
    }

    private void Commit(ConsumeResult<string, string> result)
    {
        try
        {
            _consumer?.Commit(result);
        }
        catch (KafkaException ex)
        {
            log.LogError(ex, "Worker commit failed Offset={Offset}", result.Offset.Value);
        }
    }
}
