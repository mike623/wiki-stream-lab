# wiki-stream-lab

A **Go** Kafka learning lab built around a real public event firehose: Wikimedia Recent Changes.

The goal is two things at once:

1. **Learn Kafka** through real streaming patterns that are hard to replace with n8n or a simple event bus: durable append-only logs, partitioning/ordering tradeoffs, consumer groups, offset replay, projection rebuilding, dead-letter handling, and consumer lag/backpressure.
2. **Learn Go** the way a tech lead learns a new language — by *reviewing* small, well-shaped pull requests. Mike is an advanced TypeScript developer becoming effective in Go fast. Every change ships as a PR small enough to review in one sitting and is written to teach one concept. See [`docs/PR_CONSTITUTION.md`](docs/PR_CONSTITUTION.md).

> Not a TypeScript/pnpm project. Implementation is Go, standard-library-first. The earlier TS scaffold direction is superseded.

## Source stream

Wikimedia Recent Changes SSE stream:

https://stream.wikimedia.org/v2/stream/recentchange

Live edits across Wikimedia projects. No API key. High enough volume to make Kafka useful.

## Target architecture

```text
Wikimedia SSE
  -> producer            -> Kafka/Redpanda topic: wikimedia.recentchange.raw
  -> validator           -> wikimedia.recentchange.validated  (bad records -> wikimedia.dead_letter)
  -> projector           -> SQLite page-activity read model (idempotent)
  -> replay / lag demos  -> rebuild read model from the log; observe consumer lag
```

## Stack

- **Go 1.22+** (developed on 1.26), standard library first
- **Redpanda** in Docker Compose (Kafka-compatible broker)
- **[`segmentio/kafka-go`](https://github.com/segmentio/kafka-go)** as the Kafka client
- **[`modernc.org/sqlite`](https://pkg.go.dev/modernc.org/sqlite)** — pure-Go SQLite driver via `database/sql` (no cgo, so `CGO_ENABLED=0` builds just work)
- **`log/slog`** for structured logging
- `go test` with table-driven tests

No web framework, no ORM, no DI framework. See [`AGENTS.md`](AGENTS.md) for the full constraints.

## How this project is built and reviewed

Work lands as a sequence of small PRs (PR 0..8). Each PR teaches one concept, stays well under ~300 changed lines where reasonable, includes tests, and **stops for Mike's review before the next one starts**. The roadmap lives in [`docs/CC_IMPLEMENTATION_PLAN.md`](docs/CC_IMPLEMENTATION_PLAN.md); the rules every PR follows live in [`docs/PR_CONSTITUTION.md`](docs/PR_CONSTITUTION.md).

## Local broker + topics (PR 3 — works now)

```bash
docker compose up -d            # start Redpanda + Console; broker on localhost:19092
go run ./cmd/cli topics create  # create the pipeline topics (idempotent)
go run ./cmd/cli topics list    # list topics on the broker
```

Console UI: http://localhost:8080 · stop with `docker compose down` (add `-v` to wipe the log).

Topics created (partition counts per [ADR 0002](docs/adr/0002-partition-key-wiki-page-id.md)):

| topic | partitions | why |
|---|---|---|
| `wikimedia.recentchange.raw` | 6 | scales the consumer-group lag demo |
| `wikimedia.recentchange.validated` | 6 | same |
| `wikimedia.dead_letter` | 1 | low volume, ordering-insensitive |

> On this machine `docker` is a podman shim — run `podman machine start` first if compose can't connect.

## Other commands

```bash
# build / test
go build ./...
go test ./...
go vet ./...

# run components (later PRs)
go run ./cmd/producer            # PR 4
go run ./cmd/validator           # PR 5
go run ./cmd/projector           # PR 6
```

## Key docs

- Product plan: [`docs/PRD.md`](docs/PRD.md)
- Go implementation plan + PR roadmap: [`docs/CC_IMPLEMENTATION_PLAN.md`](docs/CC_IMPLEMENTATION_PLAN.md)
- PR constitution (how every PR is shaped): [`docs/PR_CONSTITUTION.md`](docs/PR_CONSTITUTION.md)
- Agent instructions / constraints: [`AGENTS.md`](AGENTS.md)
- Domain glossary: [`CONTEXT.md`](CONTEXT.md)
- Architecture decisions: [`docs/adr/`](docs/adr/)
