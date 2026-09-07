# OrderTrace — Event-Driven Observability Demo

[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4?logo=dotnet)](https://dotnet.microsoft.com/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-KRaft-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-SDK-4B32C3?logo=opentelemetry)](https://opentelemetry.io/)

A portfolio demo of **observability** in an event-driven system. An order travels
`HTTP → Kafka → Worker → (external fraud-check HTTP) → PostgreSQL`, and the question
this project answers is: *when that journey misbehaves, how do you find the root cause?*

> **Purpose:** GitHub portfolio project showcasing OpenTelemetry end-to-end tracing
> across async boundaries (Kafka header propagation), RED metrics, and log→trace
> correlation — visualized in a Grafana **LGTM** stack.

## Status

| Phase | Scope | State |
|---|---|---|
| **1** | Working log-only pipeline + chaos injection (the "blind" baseline) | ✅ implemented |
| **2** | OpenTelemetry distributed tracing + Kafka propagation | ⏳ next |
| **3** | RED metrics + structured logs with trace correlation | 📋 planned |

The project follows the portfolio formula: Phase 1 ships a *working* system that is
painful to debug; a later phase instruments it and proves the same failure is found
in seconds. Git history (issue → branch → PR → merge commit) tells that story.

## Architecture

```
   POST /orders                      order-created (Kafka)
 ┌──────────────┐  publish   ┌────────────────────┐   consume   ┌────────────────────────┐
 │ OrderIngest  │───────────▶│     Kafka topic    │────────────▶│ OrderTrace.Worker     │
 │ (producer)   │            └────────────────────┘             │ (consumer)            │
 └──────────────┘                                              │    │ HTTP              │
                                                               │    ▼                   │
                                                               │  FraudCheck (flaky)   │
                                                               │    │ EF/Npgsql         │
                                                               │    ▼                   │
                                                               │  order_events (PG)    │
                                                               └────────────────────────┘
```

- **OrderIngest** — minimal API (`:5010`): `POST /orders` → publish `OrderCreated` → `202 Accepted`.
- **OrderTrace.Worker** — Kafka consumer: calls **FraudCheck**, persists a per-order
  `order_events` row to PostgreSQL, then commits its offset (at-least-once).
- **FraudCheck** (`:5011`) — mock external dependency. Under chaos it gets slow
  (`latencyMs`) or fails (`failureRate`); knobs change at runtime via `/_chaos`.

## Quick Start (Phase 1)

Prereqs: [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0), Docker Desktop.

```bash
# 1. Infra: Kafka + PostgreSQL (+ LGTM stack from Phase 2)
docker compose -f infra/docker-compose.yml up -d

# 2. Services (three terminals)
dotnet run --project src/OrderTrace.FraudCheck        # :5011
dotnet run --project src/OrderTrace.Worker            # :5011 → worker consumes
dotnet run --project src/OrderTrace.OrderIngest       # :5010
```

> Worker applies its schema on startup (`EnsureCreated`), so no migrations step is needed.

### Smoke test

```bash
# Healthy order (fast path)
curl -s -X POST http://localhost:5010/orders -H 'Content-Type: application/json' \
  -d '{"customerName":"Ada","totalAmount":1250.50}'

# Chaos: make FraudCheck take ~2.5 s → order processing slows down
curl -s -X POST http://localhost:5011/_chaos -H 'Content-Type: application/json' \
  -d '{"latencyMs":2500,"failureRate":0}'

# Reset chaos
curl -s -X POST http://localhost:5011/_chaos -H 'Content-Type: application/json' \
  -d '{"latencyMs":0,"failureRate":0}'
```

Watch the three consoles: each service logs its slice of the story, but no single
log stream can tell you *where* the 2.5 s went — that is exactly the Phase 1 problem,
and Phase 2 (OpenTelemetry) fixes it.

## Repo Layout

```
├── src/
│   ├── OrderTrace.Shared/       # events + topic constants
│   ├── OrderTrace.OrderIngest/  # Kafka producer (API)
│   ├── OrderTrace.Worker/       # consumer + fraud-check + Postgres
│   └── OrderTrace.FraudCheck/   # flaky downstream + runtime chaos
├── infra/docker-compose.yml     # kafka + postgres + grafana/otel-lgtm
├── CLAUDE.md                    # git + code conventions
└── docs/                        # local planning docs (plan.md is gitignored)
```

## License

MIT — Built for portfolio demonstration purposes.
