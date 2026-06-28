# Go Implementation Plan & PR Roadmap

Handoff plan for the Claude Code / agent implementer. This repo is built as a sequence of **small, reviewable Go PRs**. Read [`docs/PR_CONSTITUTION.md`](PR_CONSTITUTION.md) before any PR — it is binding.

## Mission

Build `wiki-stream-lab`, a local Kafka learning lab using the Wikimedia Recent Changes SSE firehose, **in Go**, while teaching Go to a tech-lead reviewer (Mike) through the PR stream itself.

Repo: `/Users/mikewong/workspace/wiki-stream-lab`

Read first: 1) `AGENTS.md` 2) `docs/PRD.md` 3) `docs/PR_CONSTITUTION.md` 4) `README.md`.

Do not commit unless Mike explicitly asks.

## Product direction (unchanged from PRD)

Kafka must feel necessary. Core demo:

```text
Wikimedia live SSE -> Kafka durable topic -> multiple consumers -> SQLite projection -> replay/rebuild + lag demo
```

Decisions already made (see `docs/adr/`): Kafka/Redpanda central, partition key `wiki:page_id` (6 partitions), idempotent at-least-once projection. Money-shot for the portfolio demo: **live consumer-lag drain**.

## Target Go layout (grown lazily, not scaffolded up front)

```text
go.mod                       # module github.com/mike623/wiki-stream-lab  (adjust if needed)
docker-compose.yml           # Redpanda + Console (PR 3)
cmd/
  producer/main.go           # SSE -> raw topic        (PR 4)
  validator/main.go          # raw -> validated / DLQ   (PR 5)
  projector/main.go          # validated -> SQLite      (PR 6)
  cli/main.go                # topics, inspect, reset, dlq tail, replay (grows over PRs)
internal/
  event/                     # Wikimedia model, parsing, validation, envelope (PR 2)
  kafka/                     # thin helpers over segmentio/kafka-go         (PR 4+)
  projection/                # SQLite read model + idempotency               (PR 6)
testdata/                    # JSON fixtures for table-driven tests
```

Do **not** create empty packages ahead of need. Each PR adds only the files its concept requires.

## Tooling

- Go 1.22+ (developed on 1.26). Verify: `go version`.
- Kafka client: `segmentio/kafka-go`. SQLite: `modernc.org/sqlite` (pure Go, `CGO_ENABLED=0` friendly) via `database/sql`.
- Broker: Redpanda via `docker compose`. Note: this machine currently runs **podman** as the `docker` shim; the podman machine must be started before compose works (`podman machine start`). `rpk` is not installed — topic ops will be done from Go (`cmd/cli`) or documented `docker compose exec ... rpk` calls.

## PR roadmap

Each PR: one concept, < ~300 changed lines where reasonable, tests, real verification output, **stop for review**.

### PR 0 — Go pivot docs + PR constitution *(this PR)*
Docs only: README, PRD, this plan, `PR_CONSTITUTION.md`, AGENTS/CLAUDE/CONTEXT updated to Go-first. No app code.
**Learning:** how the project will be reviewed and why Go/Kafka.

### PR 1 — Minimal Go module + health entrypoint
`go.mod`; `cmd/wiki-stream-lab/main.go` (or `cmd/api`); env-based config; `slog` logger; graceful `context` cancellation on SIGINT/SIGTERM; one tiny test.
**Learning:** module layout, command entrypoints, `context`, `slog`, test basics.

### PR 2 — Wikimedia SSE event model + parser
Struct for the raw Wikimedia event subset; JSON decode of a fixture; explicit validation errors; table-driven tests.
**Learning:** structs, JSON decoding, pointers vs values, table tests, explicit errors with `%w`.

### PR 3 — Redpanda Docker Compose + topic setup
`docker-compose.yml` (Redpanda + Console); topic creation via `cmd/cli` or documented `rpk`; README verification.
**Learning:** topics/partitions, Redpanda local dev, minimal ops.

### PR 4 — SSE producer -> raw topic
Connect to Wikimedia RecentChanges SSE; produce messages keyed `wiki:page_id`; context cancellation + timeouts; a dry-run/test seam that reads a fixture instead of the network.
**Learning:** HTTP streaming, goroutines only if needed, context-aware I/O, Kafka producer.

### PR 5 — Validator consumer -> validated / DLQ
Consume raw; validate; valid -> `wikimedia.recentchange.validated`; invalid -> `wikimedia.dead_letter` with reason + source partition/offset; tests on validation + routing.
**Learning:** consumers, error boundaries, DLQ, small interfaces (introduced only where there are real callers).

### PR 6 — SQLite projection consumer
Consume validated; idempotently write the page-activity read model; dedupe by event id (`meta.id`) / revision id inside one transaction; SQLite transaction tests.
**Learning:** `database/sql`, transactions, idempotency, at-least-once handling.

### PR 7 — Replay / rebuild demo
Reset SQLite projection; reset consumer offsets (or documented replay path); prove identical rebuild from the Kafka log.
**Learning:** Kafka replay, durable log, read-model rebuilds.

### PR 8 — Backpressure / lag demo
Configurable slow consumer (`SLOW_CONSUMER_MS`); show lag rising then draining as consumers are added to the group; document Redpanda Console / `rpk` verification.
**Learning:** consumer groups, partitions, lag, scaling limits (capped at partition count).

## Handoff report format (every implementation PR)

1. PR title  2. Learning goal  3. Summary of diff  4. Files changed  5. Go idioms used  6. TypeScript habits avoided  7. Commands run + real output  8. What Mike should review carefully  9. Known risks / follow-ups  10. Stop condition: wait for review before next PR.
