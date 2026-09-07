using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using OrderTrace.Shared.Telemetry;
using Testcontainers.Kafka;

namespace OrderTrace.Tests;

// Faz 2'nin ana kanıtı: W3C trace context'inin gerçek bir Kafka broker'ı üzerinden taşınması.
// Producer, aktif Activity'yi message header'larına Inject eder; consumer aynı header'dan
// Extract ettiği parent altında span başlatır — iki taraf da aynı trace_id'yi paylaşmalı.
// Testcontainers KRaft broker'ı, compose'taki cp-kafka:7.9.0 ile aynı imajı kullanır.
public sealed class KafkaTracePropagationTests : IAsyncLifetime
{
    private const string Topic = "order-created";

    private KafkaContainer _kafka = null!;

    // Traces are sampled + recorded in-process so Inject/Extract have a real context to move.
    private static readonly ActivityListener TraceListener = new()
    {
        ShouldListenTo = source => source.Name == Tracing.SourceName,
        Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
    };

    static KafkaTracePropagationTests()
    {
        ActivitySource.AddActivityListener(TraceListener);
    }

    public Task InitializeAsync()
    {
        _kafka = new KafkaBuilder("confluentinc/cp-kafka:7.9.0")
            .WithKRaft()
            .Build();
        return _kafka.StartAsync();
    }

    public Task DisposeAsync() => _kafka.DisposeAsync().AsTask();

    [Fact]
    public async Task ProducerTraceParent_InjectedAsHeader_IsResumedByConsumer()
    {
        var broker = _kafka.GetBootstrapAddress();

        // One partition makes consumed offsets deterministic.
        using (var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker }).Build())
        {
            await admin.CreateTopicsAsync(
            [
                new TopicSpecification { Name = Topic, NumPartitions = 1, ReplicationFactor = 1 },
            ]);
        }

        using var producer = new ProducerBuilder<string, string>(
            new ProducerConfig { BootstrapServers = broker }).Build();
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = broker,
            GroupId = "trace-propagation-test",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();

        // The trace whose identity must survive the async hop.
        using var root = Tracing.Source.StartActivity("test.produce", ActivityKind.Producer);
        Assert.NotNull(root);

        var headers = new Headers();
        Tracing.InjectHeaders(Activity.Current, (key, value) => headers.Add(key, Encoding.UTF8.GetBytes(value)));

        var traceParent = headers.TryGetLastBytes("traceparent", out var raw)
            ? Encoding.UTF8.GetString(raw)
            : null;
        Assert.NotNull(traceParent);
        Assert.StartsWith($"00-{root!.TraceId}-{root.SpanId}-", traceParent);

        await producer.ProduceAsync(Topic, new Message<string, string>
        {
            Key = "order-1",
            Value = "{}",
            Headers = headers,
        });
        producer.Flush(TimeSpan.FromSeconds(5));

        consumer.Assign(new TopicPartitionOffset(Topic, 0, Offset.Beginning));
        var consumed = consumer.Consume(TimeSpan.FromSeconds(20));
        Assert.NotNull(consumed?.Message);
        Assert.NotNull(consumed.Message.Headers);

        // Consumer resumes the exact trace the producer started.
        var parent = Tracing.ExtractContext(name =>
            consumed.Message.Headers.TryGetLastBytes(name, out var bytes)
                ? Encoding.UTF8.GetString(bytes)
                : null);

        Assert.True(parent.HasValue);
        Assert.Equal(root.TraceId, parent!.Value.TraceId);
        Assert.Equal(root.SpanId, parent.Value.SpanId);

        // A span started from the extracted context stays under the same trace_id.
        using var process = Tracing.Source.StartActivity(
            "order-created.process", ActivityKind.Consumer, parent.Value);
        Assert.NotNull(process);
        Assert.Equal(root.TraceId, process!.TraceId);
        Assert.Equal(root.SpanId, process.ParentSpanId);
    }
}
