using OrderTrace.Shared.Kafka;

namespace OrderTrace.Worker.Options;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:19092";
    public string GroupId { get; set; } = "ordertrace-worker";
    public string Topic { get; set; } = KafkaTopics.OrderCreated;
}

public sealed class WorkerSettings
{
    public int MaxAttempts { get; set; } = 3;
    public int BackoffMs { get; set; } = 800;
}
