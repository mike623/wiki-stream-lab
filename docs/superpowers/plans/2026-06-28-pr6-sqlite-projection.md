# PR 6: SQLite Page-Activity Projection — Implementation Plan

**Goal:** Consume `wikimedia.recentchange.validated` and maintain a SQLite read model (page activity + wiki stats), writing idempotently so at-least-once redelivery and full replay both produce the same numbers.

**Architecture:** `internal/projection.Store` wraps `database/sql` + `modernc.org/sqlite` (pure-Go, no cgo). `Apply(ctx, Envelope)` runs one transaction: an `INSERT OR IGNORE` into `processed_events(event_id)` is the dedupe gate (0 rows affected → duplicate → no-op commit, return false); otherwise upsert `page_activity` and `wiki_stats` and commit. `cmd/projector` is a consumer-group loop (`group="projector"`) that decodes the Envelope, calls Apply, and commits the offset after.

**Tech Stack:** Go stdlib + `segmentio/kafka-go` + `modernc.org/sqlite` (second and final core dep).

## Global Constraints
- Idempotent on `event_id` (the Envelope's `EventID = meta.id`).
- Dedupe gate + projection updates share ONE transaction (no double-count window).
- `last_event_at` uses `MAX(...)` so out-of-order delivery never moves it backwards.
- Driver registers as `"sqlite"`; `sql.Open("sqlite", path)`. DB dir created if missing. `.data/` is gitignored.
- PR < ~300 reviewable lines (go.sum generated). Store logic unit-tested; cmd is wiring verified live.

## Files
- `internal/projection/store.go` (+`store_test.go`) — `Open`, `Close`, `Apply`, `GetPage`, schema.
- `cmd/projector/main.go` — consumer loop.
- `README.md` — projector run + inspect + replay-idempotency demo.
- `go.mod`/`go.sum` — add modernc.org/sqlite.

## Tables
- `processed_events(event_id PK, processed_at)` — idempotency ledger.
- `page_activity(wiki, title, edit_count, bot_edit_count, last_event_at, last_user, PK(wiki,title))`.
- `wiki_stats(wiki PK, total_events, bot_events, human_events, last_event_at)`.

## Verification (live)
- `go build/vet/test ./...`, `gofmt -l .`; unit test covers: first apply counts, duplicate event_id is a no-op, new event increments, bot counts, MAX(last_event_at).
- run projector over the validated backlog → `sqlite3` shows populated page_activity + wiki_stats.
- **replay demo:** `rpk group seek projector --to start` then re-run → `applied:0 skipped_duplicates:N`, SQLite counts unchanged.

## Reviewer checklist for Mike
- Dedupe + upserts in one `BeginTx` — confirm the atomicity argument (a crash between dedupe-insert and upsert can't double-count).
- `INSERT OR IGNORE` + `RowsAffected()==0` as the duplicate signal — clear?
- `MAX(last_event_at, excluded.last_event_at)` for out-of-order safety — agree it's worth it?
- modernc.org/sqlite (pure Go) over mattn/go-sqlite3 (cgo) — chosen so `CGO_ENABLED=0` builds work. OK?
- `cmd/projector` has no unit test (wiring identical to the validator pattern); logic lives in the tested `Store`. Acceptable?

## Stop condition
Stop and wait for review before PR 7 (replay/rebuild demo: delete DB, reset offsets, rebuild identically).
