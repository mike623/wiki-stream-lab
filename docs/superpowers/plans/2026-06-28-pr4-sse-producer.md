# PR 4: Wikimedia SSE Producer → raw topic — Implementation Plan

**Goal:** Stream the Wikimedia Recent Changes SSE firehose and produce each raw event to `wikimedia.recentchange.raw`, keyed `wiki:title`, with context cancellation, a max-seconds self-stop, and a file seam for offline runs.

**Architecture:** `internal/sse` parses `text/event-stream` over any `io.Reader` (the test/offline seam). `event.PageKey` does a minimal `{wiki,title}` decode so the producer can key events without fully validating them — raw bytes go to the log verbatim; full validation is the validator's job (PR 5). `internal/kafka.NewWriter` wraps a kafka-go `Writer` with the `Hash` balancer so a key maps to a stable partition. `cmd/producer` wires config → source (HTTP or file) → `produce` loop → writer.

**Tech Stack:** Go stdlib (`net/http`, `bufio`) + `segmentio/kafka-go`.

## Global Constraints
- Key = `wiki:title` (ADR 0002 correction). Raw payload stored verbatim (AGENTS.md: preserve raw).
- `context.Context` everywhere; SIGINT/SIGTERM + `PRODUCER_MAX_SECONDS` both cancel the same ctx.
- No new third-party deps beyond kafka-go.
- PR < ~300 reviewable lines; table-driven tests; `produce` testable without network or broker.

## Files
- `internal/sse/sse.go` + `sse_test.go` — SSE `Scan(io.Reader, fn)`.
- `internal/event/parse.go` (+`key_test.go`) — add `PageKey([]byte) (string, error)`.
- `internal/kafka/producer.go` — `NewWriter(broker, topic)`.
- `internal/config/config.go` (+test) — add `ProducerMaxSeconds`, `ProducerLogEvery` (+ `getIntOr`).
- `cmd/producer/main.go` + `main_test.go` — `run`, `openSource`, `produce`.
- `README.md` — producer run + consume-back.

## Verification (live broker + network)
- `go build/vet/test ./...`, `gofmt -l .`
- file replay → consume back via `rpk`, confirm keys + verbatim values + unkeyable-skip
- live run with `PRODUCER_MAX_SECONDS=8` → produces real events, clean self-stop

## Known gotcha (found in live test)
Wikimedia returns **403** without a descriptive `User-Agent` (WMF UA policy) — the producer sets one. Not unit-testable (network path); verified live.

## Reviewer checklist for Mike
- `produce` skips unkeyable events (warn) instead of failing — agree the raw firehose's occasional control payloads shouldn't kill the producer?
- ctx cancellation: signal AND max-seconds both cancel the same ctx; clean stop is not treated as an error (`errors.Is(context.Canceled/DeadlineExceeded)`).
- `Hash` balancer = key→stable partition. Confirm that's the intent.
- Raw bytes stored verbatim (value = original SSE data). Key derived by minimal decode, not full parse.

## Stop condition
Stop and wait for review before PR 5 (validator → validated / DLQ).
