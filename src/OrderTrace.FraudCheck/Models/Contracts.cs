namespace OrderTrace.FraudCheck.Models;

// HTTP contract for the (mock) external fraud-check dependency.
public sealed record FraudCheckRequest(
    Guid OrderId,
    string CustomerName,
    decimal TotalAmount);

public sealed record FraudDecision(
    Guid OrderId,
    bool Approved,
    int Score,
    DateTimeOffset CheckedAtUtc);

// Runtime-tunable chaos knobs. Changes take effect without a restart.
public sealed record ChaosConfig(int LatencyMs = 0, double FailureRate = 0);

public sealed class ChaosState
{
    private readonly object _gate = new();
    private int _latencyMs;
    private double _failureRate;

    public int LatencyMs { get { lock (_gate) return _latencyMs; } }
    public double FailureRate { get { lock (_gate) return _failureRate; } }

    public void Apply(ChaosConfig cfg)
    {
        lock (_gate)
        {
            _latencyMs = Math.Max(0, cfg.LatencyMs);
            _failureRate = Math.Clamp(cfg.FailureRate, 0, 1);
        }
    }
}
