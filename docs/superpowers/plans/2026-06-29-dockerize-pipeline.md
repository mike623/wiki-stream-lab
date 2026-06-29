# Dockerize the pipeline — producer/validator/projector as services

**Goal:** Run the whole system with one command. The Go apps become Docker services alongside Redpanda/Console/ClickHouse/Grafana, so `docker compose up -d --build` starts everything.

**Architecture:** One multi-stage `Dockerfile` builds all binaries into a single image (`wiki-stream-lab:local`); each compose service runs a different binary via `command`. A one-shot `topics-init` service creates topics (others wait on `service_completed_successfully`). Apps talk to the broker over the internal listener `redpanda:9092`. The projector keeps SQLite on a named volume.

**Tech Stack:** Docker multi-stage (golang:1.25 build → distroless/static run), `CGO_ENABLED=0` (pure-Go SQLite driver makes this work; distroless static ships CA certs for the producer's HTTPS).

## Key decisions / gotchas (earned during build)
- **`CMD`, not `ENTRYPOINT`** — with `ENTRYPOINT ["/app/producer"]`, compose `command:` becomes *args*, so every service ran the producer. `CMD` is replaced by `command`.
- **`go 1.25` builder** — go.mod requires 1.25.0; the 1.23 builder failed `go mod download`.
- **Internal listener** — services use `KAFKA_BROKERS=redpanda:9092` (not `localhost:19092`, which is the host-facing listener).
- **`topics-init`** one-shot + `depends_on: condition: service_completed_successfully` so apps start only after topics exist.
- **All app services need `build: .`** (sharing `image: wiki-stream-lab:local`) or compose tries to pull a nonexistent image.
- distroless static + `CGO_ENABLED=0`: validates the earlier modernc.org/sqlite (pure Go) choice — no cgo, tiny image, HTTPS works.

## Files
- `Dockerfile` (multi-stage, CMD default).
- `.dockerignore`.
- `docker-compose.yml`: + topics-init, producer, validator, projector services + `sqlite-data` volume.
- `README.md`: "Run the whole thing (one command)" + scale-the-lag-demo.

## Verification (live)
- `docker compose up -d --build` → image builds, topics-init exits 0, 3 apps + infra up
- each service runs the correct binary (logs: producer/validator/projector starting)
- data flows: ClickHouse count grows (35 → 69), SQLite projection populates (`cli db inspect` in the projector container)
- 7 services up

## Reviewer checklist for Mike
- One image, many services via `command` (CMD overridable) — agree that's simpler than per-app Dockerfiles?
- `topics-init` one-shot pattern for ordering — clear?
- Host `go run` still works for dev/the lag demo; Docker is the "run it all" path. Keep both?
- Lag demo now `docker compose up -d --scale projector=3` — nicer than multiple terminals.

## Note
Infra PR (Docker/compose), no Go source changes.
