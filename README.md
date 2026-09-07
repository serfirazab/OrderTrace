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
| **2** | OpenTelemetry distributed tracing + Kafka propagation | ✅ implemented |
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
log stream can tell you *where* the 2.5 s went — that is exactly the Phase 1 problem.

## Phase 2 — Seeing the same order as one trace

Phase 2 instruments the same flow with OpenTelemetry. Each service exports spans over
OTLP to the bundled **LGTM** stack (`infra/docker-compose.yml` already runs it), and the
W3C `traceparent` context is carried manually across Kafka message headers (Confluent.Kafka
has no auto-instrumentation — see `OrderTrace.Shared/Telemetry/Tracing.cs`). The same slow
order now shows as a single waterfall in Grafana/Tempo:

```
ordertrace-orderingest   POST /orders           127 ms   ← root (HTTP server)
 └ kafka.publish         (producer)                      ← traceparent injected on the record
    └ order-created.process           2 725 ms   ← worker resumes the trace from headers
       ├ POST /v1/fraudcheck          2 580 ms   ← joined over HTTP (worker → FraudCheck)
       └ ordertrace (EF Core → PG)       18 ms
```

The 2.6 s is now attributable in seconds: the consumer span is slow because its
fraud-check child span took 2.5 s.

### Run it

Start the services with the OTLP exporter pointed at the LGTM stack:

```bash
# OrderIngest / Worker / FraudCheck (three terminals) — kafka/postgres/LGTM already up
OTEL_SERVICE_NAME=ordertrace-orderingest OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
  dotnet run --project src/OrderTrace.OrderIngest --launch-profile http     # :5010

OTEL_SERVICE_NAME=ordertrace-worker OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
  dotnet run --project src/OrderTrace.Worker

OTEL_SERVICE_NAME=ordertrace-fraudcheck OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
  dotnet run --project src/OrderTrace.FraudCheck --launch-profile http     # :5011
```

Then post an order while FraudCheck is slow, open **http://localhost:3000** (admin/admin),
Explore → **Tempo**, and search by Trace ID:

```bash
curl -s -X POST http://localhost:5011/_chaos -H 'Content-Type: application/json' \
  -d '{"latencyMs":2500,"failureRate":0}'                      # make FraudCheck slow
curl -s -X POST http://localhost:5010/orders -H 'Content-Type: application/json' \
  -d '{"customerName":"Ada","totalAmount":1250.50}'            # order whose trace you want
curl -s -X POST http://localhost:5011/_chaos -H 'Content-Type: application/json' \
  -d '{"latencyMs":0,"failureRate":0}'                         # reset chaos
```

Each service also logs its `TraceId`, so a single id can be matched across the OrderIngest,
Worker and FraudCheck consoles — proof the trace survived the async hop even before opening
Grafana.

## Repo Layout

```
├── src/
│   ├── OrderTrace.Shared/       # events, topic constants, W3C Tracing inject/extract
│   ├── OrderTrace.OrderIngest/  # Kafka producer (API) + OTel
│   ├── OrderTrace.Worker/       # consumer + fraud-check + Postgres + OTel
│   └── OrderTrace.FraudCheck/   # flaky downstream + runtime chaos + OTel
├── tests/
│   └── OrderTrace.Tests/        # Kafka traceparent round-trip (Testcontainers)
├── infra/docker-compose.yml     # kafka + postgres + grafana/otel-lgtm
├── CLAUDE.md                    # git + code conventions
└── docs/                        # local planning docs (plan.md is gitignored)
```

## License

MIT — Built for portfolio demonstration purposes.
