# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status: pipeline + lake implemented (PR 0–11)

The core pipeline (producer → validator → projector), the replay/lag demos, the ClickHouse+Grafana OLAP layer, and the object-storage layer (archiver backup + Parquet lake in RustFS, queryable by DuckDB) are all built and run via Docker Compose. Keep building through the staged PR roadmap in `docs/CC_IMPLEMENTATION_PLAN.md`.

## What this project is

A Go-first local Kafka/Redpanda learning lab. It consumes the live Wikimedia Recent Changes SSE firehose (no API key) and pushes it through a real Kafka pipeline to teach Kafka semantics that an in-process bus or n8n cannot demonstrate: durable logs, partitioning, consumer groups, offset replay, projection rebuild, DLQ, idempotency, and consumer lag.

The learning value is the product. Mike is an advanced TypeScript developer learning Go by reviewing small agent-authored PRs like a tech lead. Every PR must follow `docs/PR_CONSTITUTION.md`.

## Hard constraints

- Kafka stays central. Never replace the main flow with n8n, cron polling, Redis/BullMQ, webhooks, NATS, or an in-process Go channel bus.
- Implementation language is Go, standard-library-first.
- Real broker locally: Redpanda in Docker Compose.
- Real source: `https://stream.wikimedia.org/v2/stream/recentchange`. No mocked firehose in the main path.
- No secrets or API keys.
- Do not commit unless Mike explicitly asks.
- Do not implement the next PR until Mike has reviewed the current one.
- Do not copy upstream Wikimedia schemas/docs verbatim — validate only the subset needed and link out.

## Architecture / data flow

```text
Wikimedia SSE
  -> producer      -> topic: wikimedia.recentchange.raw
  -> validator     -> topic: wikimedia.recentchange.validated  (bad records -> wikimedia.dead_letter)
  -> projector     -> SQLite projection (.data/wiki-stream-lab.sqlite)

validated topic also fans out to independent consumer groups:
  -> ClickHouse (Kafka engine + MV) -> Grafana          real-time OLAP
  -> archiver  (archiver-raw)  -> RustFS: gzipped JSONL  verbatim backup
  -> laker     (lake-parquet)  -> RustFS: Parquet lake    columnar, queried by DuckDB
```

Independent consumers read the same log. The SQLite projection must be deletable and rebuildable by replaying Kafka history, proving the log is the source of truth. The archiver and laker commit Kafka offsets only after the object lands in S3 (at-least-once, idempotent storage).

## Planned Go layout, grown lazily

Do not scaffold empty packages ahead of need.

```text
cmd/producer/main.go
cmd/validator/main.go
cmd/projector/main.go
cmd/archiver/main.go   # validated -> gzipped JSONL backup in S3
cmd/laker/main.go      # validated -> Parquet lake in S3
cmd/cli/main.go
internal/event/
internal/kafka/
internal/projection/
internal/objstore/     # thin S3 client (RustFS) for archiver + laker
testdata/
```

## Topics

`wikimedia.recentchange.raw` · `wikimedia.recentchange.validated` · `wikimedia.page_activity.v1` · `wikimedia.stats.minute.v1` · `wikimedia.dead_letter`

Message keys must be intentional and documented. Default page-oriented key: `wiki:page_id`.

## Go conventions

- Go 1.22+; use the current local stable version.
- Standard library first.
- `context.Context` first argument for I/O and long-running work; honor cancellation.
- `log/slog` for structured logs.
- Explicit errors; wrap with `fmt.Errorf("...: %w", err)`.
- Table-driven tests where useful.
- `segmentio/kafka-go` for Kafka unless a PR documents a better choice.
- `modernc.org/sqlite` via `database/sql` for SQLite unless a PR documents a better choice.
- Small packages and simple names.
- Avoid Gin/Fiber, GORM, DI frameworks, over-abstracted Clean Architecture templates, unnecessary generics, and clever channel choreography before needed.

## Target commands once scaffolded

```bash
go version
go test ./...
go vet ./...
go build ./...
gofmt -l .

docker compose up -d
# then run the Go CLI/producer/validator/projector commands introduced by each PR
```

Config is via env (`.env.example` is the source of truth): `KAFKA_BROKERS`, `WIKIMEDIA_STREAM_URL`, `PRODUCER_MAX_SECONDS`, `PRODUCER_LOG_EVERY`, `SQLITE_PATH`, `SLOW_CONSUMER_MS`, and the S3/object-store vars for archiver + laker (`S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_BUCKET`).

Note: on this machine `docker` may be a podman shim; start the podman machine before compose if needed.

## Verification before reporting a PR done

Run the closest available checks. Paste real command output into the handoff. If something cannot run, report the exact command, exact blocker, and next command. Do not claim success without evidence.

## Handoff format

Use the exact report format from `docs/PR_CONSTITUTION.md`:

1. PR title
2. Learning goal
3. Summary of diff
4. Files changed
5. Go idioms used
6. TypeScript habits avoided
7. Commands run + real output
8. What Mike should review carefully
9. Known risks / follow-ups
10. Stop condition — waiting for review before the next PR.

## Authoritative docs

`docs/PRD.md` (scope), `docs/PR_CONSTITUTION.md` (PR contract), `docs/CC_IMPLEMENTATION_PLAN.md` (PR roadmap), and `AGENTS.md` (agent constraints). When they conflict with this file, update this file to match.
