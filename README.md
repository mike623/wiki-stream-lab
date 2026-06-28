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

# producer (PR 4 — works now): live SSE -> raw topic, keyed wiki:title
go run ./cmd/producer                       # live; stops after PRODUCER_MAX_SECONDS
go run ./cmd/producer -file events.sse      # replay SSE from a file (offline/demo)

# validator (PR 5 — works now): raw -> validated (Envelope), malformed -> dead_letter
go run ./cmd/validator           # consumer group "validator"; Ctrl-C to stop

# projector (PR 6 — works now): validated -> SQLite page-activity, idempotent
go run ./cmd/projector           # consumer group "projector"; Ctrl-C to stop
```

Inspect the read model (PR 7 — `cli db`, no SQL needed):

```bash
go run ./cmd/cli db inspect    # counts, top pages, per-wiki stats
go run ./cmd/cli db reset      # delete the SQLite projection
```

Prove idempotency — replay the validated topic and watch counts NOT change:

```bash
docker compose exec redpanda rpk group seek projector --to start  # rewind offsets
go run ./cmd/projector   # logs applied:0 skipped_duplicates:N; SQLite rows unchanged
```

## Replay / rebuild from the log (PR 7)

The SQLite projection is disposable — the Kafka log is the source of truth. Delete the read model and rebuild it from history:

```bash
go run ./cmd/cli db inspect                         # note the numbers
go run ./cmd/cli db reset                            # 1. delete the projection
docker compose exec redpanda rpk group seek projector --to start   # 2. rewind offsets to earliest
go run ./cmd/projector                               # 3. replay -> rebuilds identically
go run ./cmd/cli db inspect                          # same numbers as before
```

Because the projector is idempotent (dedupe on `event_id`), the rebuild is exact — running it again changes nothing.

**Retention note:** replay only goes back as far as the broker still has data. Redpanda keeps the log per its retention config (effectively unbounded for this single-node dev setup, until you `docker compose down -v`). In production you'd size retention — or use a compacted topic — to control how far back you can rebuild.

Inspect the validator's output:

```bash
docker compose exec redpanda rpk topic consume wikimedia.recentchange.validated -n 1 -o start -f '%v\n'
# {"event_id":"...","event_type":"edit","wiki":"enwiki","title":"...","user":"...","bot":false,"occurred_at":...}
docker compose exec redpanda rpk topic consume wikimedia.dead_letter -n 1 -o start -f '%v\n'
# {"reason":"invalid recentchange event: missing meta.id","source_topic":"...","partition":2,"offset":0,"raw":"..."}
```

Prove the producer worked — consume a couple of raw messages back:

```bash
docker compose exec redpanda rpk topic consume wikimedia.recentchange.raw -n 2 -o start -f '%k => %v\n'
# enwiki:Go (programming language) => {"wiki":"enwiki","title":"Go (programming language)",...}
```

## Backpressure / consumer lag (PR 8)

Consumer **lag** = messages written but not yet processed by a group. Slow the projector to make lag build, then scale the group to drain it.

```bash
go run ./cmd/cli lag                                   # per-partition + total lag (group projector)

docker compose exec redpanda rpk group seek projector --to start   # rewind so the backlog is pending
go run ./cmd/cli lag                                   # total lag == backlog size

SLOW_CONSUMER_MS=500 go run ./cmd/projector            # one slow consumer — Ctrl-C after a bit
go run ./cmd/cli lag                                   # lag only partly drained; it can't keep up
```

Now run **several** projectors in separate terminals (same `projector` group):

```bash
SLOW_CONSUMER_MS=500 go run ./cmd/projector   # terminal 1
SLOW_CONSUMER_MS=500 go run ./cmd/projector   # terminal 2
SLOW_CONSUMER_MS=500 go run ./cmd/projector   # terminal 3
go run ./cmd/cli lag                           # drains faster — work is shared across the group
```

Observed in a run: backlog 20 → one slow consumer left 9 → scaling cleared it to 0.

**The partition ceiling:** the topics have **6 partitions**, and each partition is consumed by at most one member of a group. So adding consumers speeds the drain only **up to 6**; a 7th sits idle. That is the fundamental scaling limit Kafka makes explicit — and why the partition count (and key choice, [ADR 0002](docs/adr/0002-partition-key-wiki-page-id.md)) matters. You can also watch lag live in the Redpanda Console (http://localhost:8080) or via `rpk group describe projector`.

## Real-time insight dashboard (PR 9 — ClickHouse + Grafana)

The enterprise pattern: **Redpanda → ClickHouse (real-time OLAP) → Grafana**. ClickHouse ingests the `validated` topic *directly* via its Kafka table engine — no consumer code — and Grafana queries it for sub-second, drill-down insight (throughput, event type, top wikis, last 10 events, total). This is the analytics layer; arbitrary group-by belongs here, not in ops metrics.

```bash
docker compose up -d            # now also starts ClickHouse + Grafana
go run ./cmd/cli topics create  # if not already
go run ./cmd/producer           # feed the firehose
go run ./cmd/validator          # raw -> validated (ClickHouse reads validated)

open http://localhost:3000      # Grafana, dashboard "wiki-stream-lab (ClickHouse)" (anonymous)
```

How it flows:

```text
validated topic
   │  ClickHouse Kafka engine (consumer group "clickhouse")  ← no app code
   ▼
wsl.kafka_validated ──(materialized view)──► wsl.events (MergeTree)
                                                 │
                                          Grafana (ClickHouse datasource)
```

Query it directly too:

```bash
docker compose exec clickhouse clickhouse-client -q \
  "SELECT event_type, count() c FROM wsl.events GROUP BY event_type ORDER BY c DESC"
docker compose exec clickhouse clickhouse-client -q \
  "SELECT wiki, count() c FROM wsl.events GROUP BY wiki ORDER BY c DESC LIMIT 5"
```

ClickHouse is a **4th independent consumer group** on the log (alongside validator, projector) — the same event stream, read again for a different purpose. All local, no cloud. (ClickHouse is also what Tinybird runs managed — same SQL transfers.)

## Key docs

- Product plan: [`docs/PRD.md`](docs/PRD.md)
- Go implementation plan + PR roadmap: [`docs/CC_IMPLEMENTATION_PLAN.md`](docs/CC_IMPLEMENTATION_PLAN.md)
- PR constitution (how every PR is shaped): [`docs/PR_CONSTITUTION.md`](docs/PR_CONSTITUTION.md)
- Agent instructions / constraints: [`AGENTS.md`](AGENTS.md)
- Domain glossary: [`CONTEXT.md`](CONTEXT.md)
- Architecture decisions: [`docs/adr/`](docs/adr/)
