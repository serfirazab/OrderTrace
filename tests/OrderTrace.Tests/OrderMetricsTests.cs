using System.Diagnostics.Metrics;
using OrderTrace.Shared.Telemetry;

namespace OrderTrace.Tests;

// Phase 3'ün birim kanıtı: retry/poison yolu (Phase 1'in ölçemediği kısım) metrik olarak
// kaydediliyor. Kafka/Postgres gerekmediği için Testcontainers yerine MeterListener ile
// doğrudan doğruluyoruz — gerçek instrument'lar, mock yok. Callback'ler yalnızca test
// süresince dinler; OrderMetrics statik olduğundan başka bir etkileşim yok.
public sealed class OrderMetricsTests
{
    [Fact]
    public void RecordProcessed_And_RecordFailed_EmitDurationSamplesWithTags_AndCounters()
    {
        // OrderMetrics'in statik ctor'unu çalıştırıp instrument'ları oluştur.
        _ = OrderMetrics.Processed;

        var durations = new List<(double Ms, string Outcome, int Attempts)>();
        var completed = 0L;
        var failed = 0L;

        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == OrderMetrics.MeterName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };

        listener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
        {
            durations.Add((value, (string)GetTag(tags, "outcome")!, (int)GetTag(tags, "attempts")!));
        });
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (instrument.Name.EndsWith(".completed", StringComparison.Ordinal))
            {
                completed += value;
            }
            else if (instrument.Name.EndsWith(".failed", StringComparison.Ordinal))
            {
                failed += value;
            }
        });
        listener.Start();

        // İki başarılı sipariş: biri tek denemede, biri retry sonrası (Attempt=2).
        OrderMetrics.RecordProcessed(elapsedMs: 100, attempts: 1);
        OrderMetrics.RecordProcessed(elapsedMs: 1_850, attempts: 2);
        // Bir poison sipariş: retry'ler tükendi (Attempt=3).
        OrderMetrics.RecordFailed(elapsedMs: 2_400, attempts: 3);

        Assert.Equal(2L, completed);
        Assert.Equal(1L, failed);
        Assert.Collection(durations,
            d => { Assert.Equal(100, d.Ms); Assert.Equal("ok", d.Outcome); Assert.Equal(1, d.Attempts); },
            d => { Assert.Equal(1_850, d.Ms); Assert.Equal("ok", d.Outcome); Assert.Equal(2, d.Attempts); },
            d => { Assert.Equal(2_400, d.Ms); Assert.Equal("failed", d.Outcome); Assert.Equal(3, d.Attempts); });
    }

    private static object? GetTag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key)
    {
        foreach (var kv in tags)
        {
            if (kv.Key == key)
            {
                return kv.Value;
            }
        }

        return null;
    }
}
