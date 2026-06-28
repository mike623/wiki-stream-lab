# PR 7: Replay / Rebuild Demo — Implementation Plan

**Goal:** Make the "delete the projection, replay the log, rebuild identically" loop a first-class, runnable demo: `cli db inspect`, `cli db reset`, and a documented offset-reset flow, proving the Kafka log is the source of truth.

**Architecture:** Add read methods to `projection.Store` (`Counts`, `TopPages`, `WikiStats`) and extend `cmd/cli` with a `db` command group (`inspect`, `reset`). `db reset` deletes the SQLite file (+ WAL/SHM sidecars); the projector recreates the schema on next start. Offset reset uses the standard `rpk group seek projector --to start` (documented), after which re-running the projector rebuilds from history. README captures the full flow + a retention note.

**Tech Stack:** Go stdlib + existing deps. No new dependencies.

## Global Constraints
- Projection must be deletable and rebuildable purely from the log (PRD M5).
- Rebuild must be exact because the projector dedupes on `event_id` (PR 6).
- `db` commands need no broker; `topics` commands do — keep that split in `cli`.
- PR < ~300 reviewable lines.

## Files
- `internal/projection/store.go` (+`store_test.go`) — add `Counts`, `TopPages`, `WikiStats`, `WikiStat`.
- `cmd/cli/main.go` (+`main_test.go`) — refactor dispatch to `topics|db`; add `db inspect`, `db reset`.
- `README.md` — replay/rebuild section + retention note.

## Verification (live — the whole point)
- `go build/vet/test ./...`, `gofmt -l .`
- `cli db inspect` shows current numbers
- `cli db reset` → `cli db inspect` shows empty
- `rpk group seek projector --to start` → run projector → `cli db inspect` shows the **same** numbers as before (exact rebuild)

## Reviewer checklist for Mike
- `db reset` deletes the file (+ -wal/-shm) rather than truncating tables — agree that's the cleanest "deletable projection"?
- Offset reset via documented `rpk` rather than Go code — acceptable, or do you want a `cli replay` that seeks the group in Go (more code, kafka-go group-offset commit is fiddly)?
- `cli` now loads config for all commands before dispatch; `db` ignores the broker fields. OK?
- Retention caveat documented (replay only as far back as the broker retains).

## Stop condition
Stop and wait for review before PR 8 (backpressure / consumer-lag demo — the money-shot).
