# PR 3: Redpanda Docker Compose + Topic Setup — Implementation Plan

**Goal:** Stand up a local Redpanda broker + Console via Docker Compose, and create/list the pipeline topics from Go (`cmd/cli`) using the first real dependency, `segmentio/kafka-go`.

**Architecture:** `docker-compose.yml` runs single-node Redpanda (external listener on `localhost:19092`, matching `.env.example`) plus Redpanda Console on `:8080`. A new `internal/kafka` package holds the topic-name constants and `TopicSpec`s and thin admin helpers (`EnsureTopics`, `ListTopics`) over kafka-go. `cmd/cli topics create|list` wires config → helpers.

**Tech Stack:** Go 1.22+, **+ `github.com/segmentio/kafka-go`** (first third-party dep — needed for Kafka admin now and the producer/consumers later). Redpanda + Console images pinned.

## Global Constraints
- Module `github.com/mike623/wiki-stream-lab`, `go 1.22`.
- kafka-go is justified (real Kafka client, used by every later PR); no other deps.
- External broker address `localhost:19092` (from `.env.example` `KAFKA_BROKERS`).
- Partition counts per ADR 0002: `raw`/`validated` = 6 (lag demo needs scale), `dead_letter` = 1 (low volume, ordering-insensitive).
- `EnsureTopics` is idempotent (re-runnable; already-exists is success).
- PR < ~300 reviewable lines (go.sum is generated, not counted).

## Files
- `docker-compose.yml` — Redpanda + Console.
- `internal/kafka/topics.go` — topic constants, `TopicSpec`, `PipelineTopics()`.
- `internal/kafka/admin.go` — `EnsureTopics`, `ListTopics` (alias import `kafkago`).
- `internal/kafka/topics_test.go` — pure test on `PipelineTopics()`.
- `cmd/cli/main.go` — `run([]string) error` dispatch for `topics create|list`.
- `cmd/cli/main_test.go` — arg-parsing/usage errors (no broker needed).
- `README.md` — broker + topics section with verification commands.
- `go.mod` / `go.sum` — add kafka-go.

## Verification (live broker — podman machine running)
- `go build ./...`, `go vet ./...`, `gofmt -l .`, `go test ./...`
- `docker compose up -d` → both services healthy
- `go run ./cmd/cli topics create` → ensures 3 topics
- `go run ./cmd/cli topics list` → shows them
- re-run `topics create` → still succeeds (idempotent)

## Reviewer checklist for Mike
- kafka-go imported with alias `kafkago` because our package is also `kafka` — agree?
- `EnsureTopics` ignores `TopicAlreadyExists` via `errors.Is` — confirm that's the right "idempotent" behavior.
- Partition split 6/6/1 — agree dead_letter doesn't need 6?
- `cmd/cli` is hand-rolled flag dispatch (no cobra) — lean enough?
- go.sum is generated; the reviewable Go is ~200 lines.

## Stop condition
After live verification, stop and wait for review before PR 4 (producer).
