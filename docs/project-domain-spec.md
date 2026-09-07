# OrderTrace — Project & Domain Specification

> The durable, **committed** counterpart to the local working plan (`docs/plan.md`, which is
> gitignored on purpose). This file is the reviewer-facing "what this project is" reference;
> behavioural coding/git rules live in [`CLAUDE.md`](../CLAUDE.md).

---

## 1. Purpose

An **event-driven order-processing pipeline** that exists to demonstrate **observability**:
the same operational failure that is nearly impossible to root-cause from per-service console
logs (Phase 1) is found in minutes once the system exports **traces, metrics and correlated
logs** over OpenTelemetry (Phases 2–3).

Portfolio formula: *ship a system that works but is painful to debug, then instrument it and
prove the identical failure is found fast.* The non-obvious engineering is carrying the W3C
`traceparent` context across **Kafka**, an async boundary with no automatic instrumentation.

## 2. System context & data flow

```
                       ┌──────────────────────── Grafana UI (grafana/otel-lgtm) ───────────────────────┐
                       │  Tempo (traces) · Prometheus (metrics) · Loki (logs) — single container        │
                       └────────────────────────────────────────▲───────────────────────────────────────┘
                                                                │ OTLP :4317/:4318 — three signals
   POST /orders          Kafka "order-created"                     consume + process                    HTTP
 ┌───────────────┐  pub ┌────────────────────────┐  sub  ┌───────────────────────┐   ┌────────────────────┐
 │  OrderIngest  │─────▶│      Kafka topic       │──────▶│   OrderTrace.Worker   │──▶│   FraudCheck       │
 │  (API :5010)  │      └────────────────────────┘       │   (consumer)          │   │   (mock :5011)    │
 └───────────────┘                                       │          │             │   └────────────────────┘
                                                         │          ▼             │
                                                         │  EF Core / Npgsql      │
                                                         │  order_events          │
                                                         └───────────────────────┘
                                                                PostgreSQL
```

A single accepted order produces a processing path that crosses **three processes and an
HTTP dependency** before it is considered done. Each hop is an opportunity to lose the "why
is this slow / why does it keep failing?" story.

## 3. Runtime topology

| Process | Role | Port | Exit condition for an order |
|---|---|---|---|
| `OrderTrace.OrderIngest` | Web API; Kafka **producer** | `:5010` | `POST /orders` → `202 Accepted` once the record is on the topic |
| `OrderTrace.Worker` | Kafka **consumer** + fraud call + persistence | — | order row written to Postgres, then offset committed |
| `OrderTrace.FraudCheck` | Mock **flaky** HTTP dependency | `:5011` | `POST /v1/fraudcheck` returns a decision, or 503 under chaos |
| Infrastructure | Kafka (KRaft), PostgreSQL, `grafana/otel-lgtm` | via Docker | — |

Worker semantics are **at-least-once**: an offset is committed only after the order is
persisted. A message that exhausts `Worker:MaxAttempts` retries is treated as **poison**,
skipped, and its offset committed so the partition is not blocked forever.

## 4. Contracts

All cross-service message shapes live in `OrderTrace.Shared` (no per-service copies).

### Domain event — Kafka topic `order-created`

```csharp
public sealed record OrderCreated(
    Guid OrderId,
    string CustomerName,
    decimal TotalAmount,
    DateTimeOffset OccurredAtUtc);
```

Topic name constant: `OrderTrace.Shared.Kafka.KafkaTopics.OrderCreated`.

### HTTP contract — FraudCheck

```csharp
public sealed record FraudCheckRequest(Guid OrderId, string CustomerName, decimal TotalAmount);
public sealed record FraudDecision(Guid OrderId, bool Approved, int Score, DateTimeOffset CheckedAtUtc);
```

`Approved` is `false` for large amounts (`>= 10 000`); `Score` is a 55–99 jitter. On chaos
failure the endpoint returns `503` instead of a decision.

### Persistence — `order_events`

```csharp
public sealed class OrderEvent {
    Guid OrderId; string CustomerName; decimal TotalAmount;
    bool FraudApproved; int FraudScore;
    int Attempts;              // how many fraud attempts it took (retries included)
    long TotalLatencyMs;       // wall time across all attempts — Phase 1 could not see this
    DateTimeOffset ProcessedAtUtc;
}
```

The Worker creates the schema on startup (`EnsureCreated`); no migration step is needed.

### Chaos / fault injection

`ChaosConfig(int LatencyMs = 0, double FailureRate = 0)` — off by default, applied at
**runtime** on FraudCheck via `POST /_chaos` (no restart):

| Scenario | Setting | Observable symptom | Reproduced in |
|---|---|---|---|
| Slow dependency | `{"latencyMs":2500,"failureRate":0}` | one order takes ~2.7 s end-to-end | Phase 1 (blind), Phase 2 (traced) |
| Retry storm / poison | `{"latencyMs":0,"failureRate":1}` | each order retried `MaxAttempts=3` with 800 ms backoff, then skipped | Phase 3 (metrics + logs) |

## 5. Observable behaviour contract

All three signals export over **OTLP** to the LGTM stack; the exporter is configured from
environment variables (`OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, and
`OTEL_EXPORTER_OTLP_PROTOCOL`). OTel configuration is intentionally env-driven, so the code
stays close to how it would run in production.

### Traces — the end-to-end story

The W3C `traceparent`/`tracestate` context is propagated **manually** across Kafka
(Confluent.Kafka is not auto-instrumented); HTTP and EF Core hops are auto-instrumented and
fall under the same trace.

| Span (kind) | Where | Parent |
|---|---|---|
| `POST /orders` (server) | OrderIngest | root |
| `kafka.publish` (producer) | OrderIngest | `POST /orders` — context injected onto the message headers |
| `order-created.process` (consumer) | Worker | the producer's extracted context |
| `POST` → `POST /v1/fraudcheck` (client/server) | Worker ↔ FraudCheck | `order-created.process` |
| EF Core / Npgsql span (`db.system=postgresql`) | Worker | `order-created.process` |

Shared helper: `OrderTrace.Shared.Telemetry.Tracing` — `InjectHeaders` / `ExtractContext`,
using an explicit `TraceContextPropagator` (the OTel default propagator is a no-op until an
SDK is built, so the helper stays deterministic in tests).

### Metrics — RED + business

- **HTTP RED** (auto, per service): `http.server.request.duration` on OrderIngest/FraudCheck,
  `http.client.request.duration` on the Worker's fraud call. Prometheus label
  `http_response_status_code` makes error-rate panels trivial.
- **Business RED** (`OrderTrace` meter, Worker only — it is a consumer, so it has no inbound
  HTTP): `OrderTrace.Shared.Telemetry.OrderMetrics`

| Instrument | Semantic name | Prometheus series (unit `ms`) | Tags |
|---|---|---|---|
| Histogram | `order.process.duration` | `order_process_duration_milliseconds_*` | `outcome` (`ok`/`failed`), `attempts` |
| Counter | `order.process.completed` | `order_process_completed_total` | — |
| Counter | `order.process.failed` | `order_process_failed_total` | — |

A retry-storm signature (`attempts="3", outcome="failed"`) is exactly what the blind Phase 1
could not produce.

### Logs — correlated

Structured logs export over OTLP (`WithLogging`, `IncludeFormattedMessage = true`), so every
record carries `trace_id` / `span_id` plus business fields (`OrderId`, `Attempt`, …). In Loki
the resource shows as `{service_name="ordertrace-…"}`. Any log line → its `trace_id` → Tempo
waterfall: the **log→trace jump**.

### Reading the failure stories (evidence)

Both are committed as README screenshots (`docs/screenshots/`):

1. **Slow dependency** — one trace (`…tempo-waterfall.png`) shows a 2.5 s fraud-check child
   span inside `order-created.process`: latency is attributable in one view.
2. **Retry storm** — one trace (`…tempo-retry-waterfall.png`) shows **three HTTP 503** child
   spans under a single `order-created.process` span, matched by Loki error lines sharing
   that `trace_id`, and by `order.process.duration{attempts="3"}` + FraudCheck's 5xx RED.

## 6. Repository layout

```
src/
├── OrderTrace.Shared/        # contracts, topic constants, W3C Tracing + OrderMetrics telemetry
├── OrderTrace.OrderIngest/   # API + Kafka producer (traces · metrics · logs)
├── OrderTrace.Worker/        # consumer + fraud call + Postgres (traces · metrics · logs)
└── OrderTrace.FraudCheck/    # flaky dependency + runtime chaos (traces · metrics · logs)
tests/OrderTrace.Tests/       # Testcontainers Kafka traceprop round-trip + OrderMetrics unit test
infra/docker-compose.yml      # kafka (KRaft) + postgres + grafana/otel-lgtm
docs/                         # this spec (committed); plan.md is gitignored
```

## 7. Conventions & working rules

- Git: single `OrderTrace.sln`; branch model `main` / `develop` / `feature/*`; PRs merge into
  `develop` with **merge commits**; `develop` → `main` via a separate release PR at stable
  points; Conventional Commits (English).
- Code: nullable reference types on, file-scoped namespaces, primary constructors where
  suitable, no `<summary>` XML doc comments, telemetry configured from env vars.
- Tests: flows touching Kafka/Postgres use **Testcontainers** (no mocks); each feature PR
  ships at least one test covering its change.

See [`CLAUDE.md`](../CLAUDE.md) for the authoritative working rules and
[`README.md`](../README.md) for the run-it-yourself walkthroughs.
