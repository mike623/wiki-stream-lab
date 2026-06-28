# PRD: wiki-stream-lab

## Summary

`wiki-stream-lab` is a personal **Go** Kafka learning lab using Wikimedia Recent Changes as a free, live, public event stream. The project should demonstrate why Kafka is useful beyond workflow automation: durable logs, replayable history, independent consumers, consumer groups, partitioning, projections, dead-letter queues, and backpressure.

It is also a **Go-learning** project with a specific method: the implementation is built as a sequence of small, human-readable pull requests that Mike reviews like a tech lead. Reviewability and Go-teaching value are first-class product requirements, not afterthoughts. The language is Go (standard-library-first), not TypeScript/pnpm.

## Problem

Many beginner Kafka projects use artificial events or simple workflows that could be replaced by n8n, Redis queues, webhooks, or an in-process event emitter. This project should avoid that trap by using a real continuous event source and building features that depend on Kafka semantics.

## Users

Primary user:
- Mike, a senior full-stack engineer (advanced in TypeScript) learning **both Kafka and Go** through a practical personal repo, by reviewing small agent-implemented PRs as a tech lead would.

Secondary users:
- Future agents/Claude Code lanes implementing milestones.
- Interview/demo viewers who want to see Kafka concepts in a runnable project.

## Goals

1. Consume a real public streaming source with no API key.
2. Produce normalized events to Kafka/Redpanda.
3. Build multiple independent consumers over the same stream.
4. Demonstrate offset replay by rebuilding read models from Kafka history.
5. Demonstrate partitioning and ordering decisions.
6. Demonstrate DLQ handling and idempotent writes.
7. Keep the project local, cheap, and easy to run.
8. Teach idiomatic Go through small, reviewable PRs — each one teaches one concept and is reviewable in a single sitting (see `docs/PR_CONSTITUTION.md`).

## Non-goals

- Building a production-grade Wikimedia analytics platform.
- Running a hosted Kafka cluster.
- Creating a large web dashboard before the streaming core works.
- Using LLM summarization in the MVP.
- Replacing Kafka with n8n, Redis Streams, NATS, an in-process Go channel bus, or any app-level event emitter.
- Reintroducing TypeScript/pnpm, or shipping large unreviewable diffs.

## Source data

Use Wikimedia Recent Changes SSE:

https://stream.wikimedia.org/v2/stream/recentchange

The stream emits events for edits, new pages, log actions, categorization changes, bot activity, and other wiki changes.

Important expected fields include, but are not limited to:

- `$schema`
- `meta`
- `id`
- `type`
- `namespace`
- `title`
- `title_url`
- `comment`
- `timestamp`
- `user`
- `bot`
- `wiki`
- `server_name`
- `server_url`
- `minor`
- `patrolled`
- `length`
- `revision`

The app should validate only the subset it needs and preserve raw payloads where useful.

## MVP scope

### M1: Local Kafka foundation

Deliverables:
- `docker-compose.yml` with Redpanda and console/Kafka UI.
- Documented commands to start/stop/reset local infrastructure.
- Topic creation script or startup logic.

Acceptance checks:
- `docker compose up -d` starts broker and console.
- A CLI command can list topics or create expected topics.

### M2: Wikimedia producer

Deliverables:
- SSE client subscribes to recent changes.
- Producer writes raw events to `wikimedia.recentchange.raw`.
- Kafka message key is intentionally chosen and documented.
- Graceful shutdown works.

Acceptance checks:
- Running the producer for 30 seconds writes records to Kafka.
- Console/Kafka UI or a consumer can observe records.

### M3: Schema validator and DLQ

Deliverables:
- Consumer reads raw events.
- Valid subset is transformed into a typed internal event.
- Valid events go to `wikimedia.recentchange.validated`.
- Invalid events go to `wikimedia.dead_letter` with reason, source topic, and raw payload reference/body.

Acceptance checks:
- Unit tests cover valid and invalid fixtures.
- Manual/fixture command can produce a DLQ example.

### M4: SQLite page activity projection

Deliverables:
- Consumer reads validated events.
- Writes latest page state and activity counts to SQLite.
- Writes are idempotent using event ID or source event identity.

Example tables:
- `processed_events(event_id, processed_at)`
- `page_activity(wiki, page_id, title, edit_count, bot_edit_count, last_event_at, last_user)`
- `wiki_stats(wiki, total_events, bot_events, human_events, last_event_at)`

Acceptance checks:
- Running producer + validator + projector creates non-empty SQLite rows.
- Reprocessing the same event does not double-count if idempotency is enabled.

### M5: Replay and rebuild

Deliverables:
- CLI command to clear projections.
- CLI command or documented Kafka consumer offset reset flow to replay from earliest available offset.
- README section explaining offset reset and retention implications.

Acceptance checks:
- Delete SQLite projection.
- Replay from Kafka.
- Projection is rebuilt from Kafka records.

### M6: Backpressure/consumer group demo

Deliverables:
- A deliberately slow consumer mode.
- Commands/docs to observe consumer lag.
- Instructions for running multiple consumers in the same group.

Acceptance checks:
- Lag increases with slow mode.
- Lag decreases when scaling consumers, subject to partition count.

## Stretch scope

- Small dashboard/API for live stats.
- Watchlist alerts for specific pages/projects.
- Bot-vs-human activity charts.
- Compacted topic for latest page state.
- Schema version v2 and migration notes.
- OpenTelemetry traces/metrics.
- ksqlDB or Kafka Streams equivalent experiment.
- Compare Redpanda vs Apache Kafka locally.

## Product experience

The repo should feel like a guided lab. The README should answer:

- What should I run first?
- What Kafka concept am I learning?
- How do I prove the producer is working?
- How do I prove consumers are independent?
- How do I reset/replay?
- How do I observe lag?
- Where is the DLQ and how do I inspect it?

## Technical recommendations

Use:
- **Go 1.22+** (developed on 1.26), standard library first
- **Redpanda** in Docker Compose
- **`segmentio/kafka-go`** Kafka client
- **`modernc.org/sqlite`** (pure-Go, no cgo) via `database/sql`
- **`log/slog`** for structured logging
- `go test` table-driven tests

No web framework, no ORM, no DI framework, no unnecessary generics. See `AGENTS.md` and `docs/PR_CONSTITUTION.md` for the full Go house style and the avoid-list.

Avoid overbuilding the web UI before core streaming behavior is proven.

## Risks

- Wikimedia event shape may vary. Mitigation: validate a subset and preserve raw payloads.
- Kafka local setup can be fiddly. Mitigation: prefer Redpanda for simple single-binary Kafka-compatible local dev.
- Replay can be confused with exactly-once processing. Mitigation: document idempotency and at-least-once semantics clearly.
- Too many packages can slow MVP. Mitigation: scaffold only what is needed first, then split packages if useful.

## Success criteria

The project is successful when Mike can demo:

1. Live Wikimedia edits flowing into Kafka.
2. Multiple consumers independently processing the same stream.
3. A SQLite read model rebuilt by replaying Kafka history.
4. A DLQ containing intentionally malformed test events.
5. Consumer lag/backpressure behavior.
6. Clear docs explaining the Kafka concepts learned.
7. A git history of small Go PRs Mike could review and learn from — each teaching one concept, with tests and real verification output.
