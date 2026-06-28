# PR 9: Real-time OLAP insight — ClickHouse + Grafana (stretch)

**Goal:** Add the enterprise real-time insight layer: Redpanda → ClickHouse (real-time OLAP) → Grafana. ClickHouse ingests the validated topic directly via its Kafka table engine; Grafana shows sub-second, drill-down insight (throughput, event type, top wikis, last 10 events, total).

**Why this shape (supersedes the earlier Prometheus dashboard):** Prometheus/Grafana is an *ops-metrics* layer (counters/rates). Arbitrary group-by "insight" belongs in an OLAP store between the log and the dashboard. So Grafana queries ClickHouse, not a Go-counter shim. (The first PR 9 attempt — Prometheus + a Go dashboard — was closed.)

**Architecture:**
```
validated topic
  → ClickHouse Kafka engine table (consumer group "clickhouse")   ← declarative, no app code
  → materialized view → MergeTree (wsl.events)
  → Grafana (ClickHouse datasource) panels
```

**Tech Stack:** ClickHouse 24.8 + Grafana 11 (+ grafana-clickhouse-datasource). **No Go code** — this is an infra/data-architecture PR (config + SQL only).

## Global Constraints
- Local, free, no cloud/keys (rules Tinybird out for the lab; ClickHouse is what Tinybird runs managed anyway).
- ClickHouse ingests `validated` (clean envelopes), not `raw`.
- It is a 4th independent consumer group on the log — the "many consumers, one log" property again.
- `auto_offset_reset=earliest` (server config) so a fresh ClickHouse reads the backlog.

## Files
- `deploy/clickhouse/init.sql` — Kafka engine table, MergeTree `wsl.events`, materialized view.
- `deploy/clickhouse/config.d/kafka.xml` — librdkafka `auto_offset_reset=earliest`.
- `deploy/grafana/provisioning/datasources/clickhouse.yml` — ClickHouse datasource (uid `clickhouse`).
- `deploy/grafana/provisioning/dashboards/dashboards.yml` + `deploy/grafana/dashboards/wiki-stream-lab.json` — 5 panels.
- `docker-compose.yml` — clickhouse + grafana services.
- `README.md` — insight section.

## Verification (live)
- `docker compose config -q`; bring up clickhouse → init.sql runs clean
- ClickHouse ingests validated backlog: `SELECT count(), group-by type, top wikis` return data
- live growth: produce + validate → ClickHouse count rises with no restart
- Grafana: plugin registered, datasource health OK, dashboard provisioned, query *through Grafana* returns data

## Gotchas hit (and fixed) during build
- `kafka_auto_offset_reset` is **not** a Kafka-engine table setting in CH 24.8 → moved to server config (`config.d/kafka.xml`).
- CH 24.8 image locks the `default` user off the network when no password is set → `CLICKHOUSE_SKIP_USER_SETUP=1` for the local lab.

## Reviewer checklist for Mike
- No consumer code — the Kafka table engine *is* the consumer. Agree that's the cleanest "Redpanda → OLAP" wiring?
- `events` MergeTree `ORDER BY (occurred_at, wiki)` — reasonable for these queries?
- Local-user wide open + anonymous Grafana — fine for local only?
- This PR has **no Go** (infra/SQL). OK for a learning-architecture stretch PR, or would you rather a Go consumer that inserts into ClickHouse (more code, less idiomatic than the Kafka engine)?

## Stop condition
Stretch PR. Remaining PRD stretch after: schema v2 + migration, compacted latest-state topic, OpenTelemetry, SSE live event feed.
