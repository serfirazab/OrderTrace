namespace OrderTrace.Shared.Contracts;

// Domain event published to Kafka when an order is accepted.
public sealed record OrderCreated(
    Guid OrderId,
    string CustomerName,
    decimal TotalAmount,
    DateTimeOffset OccurredAtUtc);
