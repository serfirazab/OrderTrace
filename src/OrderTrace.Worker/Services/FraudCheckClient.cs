using System.Net.Http.Json;
using System.Text.Json;
using OrderTrace.Shared.Contracts;

namespace OrderTrace.Worker.Services;

// Result of the downstream fraud-check call.
public sealed record FraudDecision(Guid OrderId, bool Approved, int Score, DateTimeOffset CheckedAtUtc);

// HTTP client for the (flaky) external fraud-check dependency.
public sealed class FraudCheckClient(HttpClient http, ILogger<FraudCheckClient> log)
{
    public async Task<FraudDecision> CheckAsync(OrderCreated order, CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(new { order.OrderId, order.CustomerName, order.TotalAmount });
        using var content = new StringContent(payload, System.Text.Encoding.UTF8, "application/json");

        using var response = await http.PostAsync("/v1/fraudcheck", content, ct);
        response.EnsureSuccessStatusCode();

        var decision = await response.Content.ReadFromJsonAsync<FraudDecision>(ct)
            ?? throw new InvalidOperationException("Empty fraud-check response.");

        log.LogInformation("FraudCheckClient received OrderId={OrderId} Approved={Approved}",
            decision.OrderId, decision.Approved);
        return decision;
    }
}
