# AGENTS.md

This repo is a **Go** Kafka learning project. Optimize for clear learning value, executable milestones, and real verification over breadth. The work ships as small, reviewable PRs — Mike (advanced in TypeScript, learning Go) reviews them like a tech lead. The binding contract for PR shape is [`docs/PR_CONSTITUTION.md`](docs/PR_CONSTITUTION.md).

## Project

Name: wiki-stream-lab
Path: /Users/mikewong/workspace/wiki-stream-lab

Goal: build a local Kafka/Redpanda streaming lab using Wikimedia Recent Changes as a free real-time event source.

## Non-negotiables

- Use a real Kafka-compatible broker locally, preferably Redpanda in Docker Compose.
- Use the live Wikimedia SSE source: https://stream.wikimedia.org/v2/stream/recentchange
- Keep Kafka central. Do not replace the main flow with n8n, cron-only polling, NATS, an in-process Go channel bus, or a basic queue.
- Implementation language is **Go** (1.22+, developed on 1.26), standard library first. No TypeScript/pnpm.
- Make every milestone runnable with documented commands.
- Do not commit unless Mike explicitly asks.
- Do not add secrets. The MVP should require no API keys.
- Do not copy third-party docs or large upstream schemas verbatim; link to them instead.

## Kafka learning outcomes to preserve

The implementation should visibly teach:

1. Producers and topic creation.
2. Message keys and partitioning.
3. Consumer groups.
4. Offset replay.
5. Rebuilding read models from the event log.
6. DLQ routing for bad events.
7. Idempotent projection writes.
8. Consumer lag/backpressure observation.
9. Schema/version changes.

If a task does not exercise one of those outcomes, question whether it belongs in the MVP.

## Proposed Go layout

Single Go module. Grow it lazily — do not scaffold empty packages ahead of need.

- `cmd/producer` — subscribes to SSE and emits Kafka records.
- `cmd/validator` — raw -> validated, malformed -> DLQ.
- `cmd/projector` — consumes validated changes, writes the page-activity projection.
- `cmd/cli` — topics, db inspect/reset, dlq tail, replay commands.
- `internal/event` — Wikimedia model, parsing, validation, internal envelope.
- `internal/kafka` — thin helpers over `segmentio/kafka-go`.
- `internal/projection` — SQLite read model + idempotency helpers.

`testdata/` holds JSON fixtures. See `docs/CC_IMPLEMENTATION_PLAN.md` for which PR introduces each piece.

## Topic naming

Start with:

- `wikimedia.recentchange.raw`
- `wikimedia.recentchange.validated`
- `wikimedia.page_activity.v1`
- `wikimedia.stats.minute.v1`
- `wikimedia.dead_letter`

## Message key guidance

Choose keys intentionally:

- Raw recent-change events: key by `wiki` or `wiki:page_id` depending on the consumer being tested.
- Page projections: key by `wiki:page_id`.
- User/bot analytics: key by `wiki:user` where useful.

Document the choice in code comments or docs because this is a learning repo.

## Verification expectations

Before reporting a milestone/PR complete, run the closest available checks, for example:

- `go build ./...`
- `go test ./...`
- `go vet ./...`
- `gofmt -l .` (should print nothing)
- `docker compose up -d` (Redpanda + Console)
- producer smoke run for 30-60 seconds
- consumer smoke run that proves rows were written to SQLite
- replay command that rebuilds projections from Kafka

Paste the real command output into the PR handoff. If a command cannot run because dependencies or Docker are unavailable, report the exact blocker and leave a clear next command. (Note: `docker` here is a podman shim — start the podman machine first.)

## Code style

- Keep packages small and explicit; short lower-case names; `internal/` for non-public code.
- Model events as Go structs with explicit validation functions (no ORM, no codegen).
- `context.Context` as the first arg for I/O and long-running work; honor cancellation.
- Structured logs via `log/slog`.
- Explicit errors; wrap with `fmt.Errorf("...: %w", err)`. No panics in normal control flow.
- Make shutdown graceful: handle SIGINT/SIGTERM via context cancellation and close Kafka readers/writers.
- Do not swallow errors silently; route malformed events to DLQ with reason and source partition/offset.
- Table-driven tests where they fit.
- Include README commands as soon as they exist.

Full house style and avoid-list: `docs/PR_CONSTITUTION.md`.

## Definition of done for MVP

- One-command local broker startup.
- Producer streams live Wikimedia changes into Kafka.
- At least two independent consumers read from Kafka.
- SQLite projection can be deleted and rebuilt by replaying Kafka history.
- DLQ path is exercised by a test or fixture.
- README explains what Kafka concept each component demonstrates.
