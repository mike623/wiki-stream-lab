# PR 8: Backpressure / Consumer-Lag Demo — Implementation Plan

**Goal (the money-shot):** Make consumer lag observable and show backpressure: a slow projector falls behind, and adding consumers to the group drains the lag — bounded by partition count.

**Architecture:** `config.SlowConsumerMS` adds a per-message delay in the projector (ctx-aware). `internal/kafka.GroupLag` computes per-partition and total lag from committed offsets (`OffsetFetch`) vs high-water marks (`ListOffsets`); the arithmetic is a pure, tested `computeLag`. `cli lag [group] [topic]` prints it (defaults: projector / validated).

**Tech Stack:** Go stdlib + existing `segmentio/kafka-go`. No new deps.

## Global Constraints
- `SLOW_CONSUMER_MS` (default 0 = full speed) drives the slow mode.
- Lag = high-water − committed, per partition, ≥ 0; uncommitted (-1) counts as 0.
- Parallelism within a group is capped at partition count (6) — the teaching point.
- PR < ~300 reviewable lines; `computeLag` unit-tested without a broker.

## Files
- `internal/config/config.go` (+test) — `SlowConsumerMS`.
- `internal/kafka/lag.go` (+`lag_test.go`) — `GroupLag`, `PartitionLag`, pure `computeLag`.
- `cmd/projector/main.go` — slow-mode delay (labeled-break clean shutdown).
- `cmd/cli/main.go` (+test) — `lag` command.
- `README.md` — backpressure section + partition-ceiling explanation.

## Verification (live)
- `go build/vet/test ./...`, `gofmt -l .`
- build a backlog (producer + validator), `rpk group seek projector --to start`
- `cli lag` shows total == backlog, split per partition
- one `SLOW_CONSUMER_MS` projector → lag only partly drains (can't keep up)
- scale / full-speed projector(s) → lag drains to 0

## Observed live
backlog 20 → one slow consumer (500ms) left lag 9 → drained to 0. Per-partition lag visible the whole time. Note: launching several consumers at once triggers group rebalances, so very short windows show rebalance churn — the steady-state behavior (more members → faster drain, capped at 6) is the lesson.

## Reviewer checklist for Mike
- `GroupLag`: committed (`OffsetFetch`) vs high-water (`ListOffsets`); `computeLag` is pure + tested. Reasonable split?
- Slow mode uses `select { <-time.After; <-ctx.Done }` so shutdown is responsive mid-delay (labeled `break loop` to still log the summary).
- Lag command is Go (no rpk needed) — keep, or is rpk enough?
- Partition ceiling (6) documented as the scaling limit.

## Stop condition
This is the last MVP PR. After review, the PR roadmap (PR 0–8) is complete; remaining items are PRD stretch goals (dashboard, compaction, schema v2, OTel).
