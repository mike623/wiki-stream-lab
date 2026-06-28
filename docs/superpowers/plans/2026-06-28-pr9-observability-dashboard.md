# PR 9: Live Dashboard — Grafana metrics + Go raw-log (stretch)

**Goal:** Visualize the running pipeline in real time — throughput, event-type pie, top-5 wikis, and a total gauge in Grafana; the last 10 raw events on a small Go page.

**Architecture:** `cmd/dashboard` runs on the host, consumes the raw topic, and (a) updates Prometheus counters exposed at `/metrics`, (b) keeps a 10-entry ring buffer of raw events served at `/raw` + a tiny auto-refresh page at `/`. Compose adds Prometheus (scrapes the host dashboard via `host.docker.internal` + Redpanda) and Grafana (auto-provisioned datasource + dashboard). All five requested widgets derive from the **raw** topic (it carries `type` and `wiki` and is the incoming rate).

**Tech Stack:** Go stdlib + `segmentio/kafka-go` + `prometheus/client_golang` (new dep) + Prometheus + Grafana (containers).

## Global Constraints
- Hybrid per Mike: Grafana/Prometheus for metrics (throughput, type pie, top wiki, total); Go for the raw-event log.
- Dashboard runs on host; Prometheus reaches it via `host.docker.internal` (verified reachable on this podman setup).
- Keep label cardinality sane: separate `by_type` and `by_wiki` counters (no type×wiki product).
- Grafana anonymous admin, no login (local only).

## Files
- `cmd/dashboard/main.go` (+`main_test.go`) — consumer + metrics + raw-log ring buffer + HTTP.
- `deploy/prometheus/prometheus.yml` — scrape config.
- `deploy/grafana/provisioning/{datasources,dashboards}/*.yml` — provisioning.
- `deploy/grafana/dashboards/wiki-stream-lab.json` — the dashboard (4 panels).
- `docker-compose.yml` — prometheus + grafana services.
- `README.md` — dashboard section.
- `go.mod`/`go.sum` — prometheus client.

## Verification (live)
- `go build/vet/test ./...`, `gofmt -l .`; `docker compose config -q`
- `docker compose up -d` → prometheus + grafana start
- run dashboard + producer → `/metrics` shows `wsl_events_*`
- Prometheus targets `wiki-stream-lab` + `redpanda` both **up**; `wsl_events_total` queryable; `rate(...)` non-zero
- Grafana `/api/health` ok, datasource + dashboard provisioned
- `/raw` returns last 10 events

## Note on size
Larger than a normal PR because of generated infra config (Grafana dashboard JSON, provisioning YAML, compose services) — analogous to `go.sum`. The reviewable Go is `cmd/dashboard` (~160 lines) + its test.

## Reviewer checklist for Mike
- All 5 widgets come from the raw topic via one consumer — agree that's simpler than instrumenting each stage?
- Separate `by_type` / `by_wiki` counters to avoid cardinality blowup — OK? (`by_wiki` is still ~hundreds of series; fine for a lab, `topk` in the panel.)
- Dashboard on host + `host.docker.internal` scrape vs containerizing the dashboard — kept it on host (lighter). OK?
- Grafana anonymous admin (no auth) — acceptable for local only?

## Stop condition
Stretch PR. After review, remaining PRD stretch: compacted latest-state topic, schema v2 + migration, OpenTelemetry traces.
