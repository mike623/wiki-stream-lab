# PR 5: Validator Consumer → validated / DLQ — Implementation Plan

**Goal:** Consume `wikimedia.recentchange.raw`, validate each event, and route it: valid → `wikimedia.recentchange.validated` (as a normalized `Envelope`), invalid → `wikimedia.dead_letter` (as a `DeadLetter` with the failure reason and source partition/offset). At-least-once via manual offset commit.

**Architecture:** `internal/kafka.NewReader` gives a consumer-group `Reader` (`GroupID="validator"`). A pure `classify(kafkago.Message) (*event.Envelope, *event.DeadLetter)` decides destination using `event.ParseRaw` — exactly one return is non-nil. `cmd/validator` writes the chosen record and only then commits the offset, so a crash redelivers rather than drops. New domain types `event.Envelope` (+`NewEnvelope`) and `event.DeadLetter` (+`NewDeadLetter`).

**Tech Stack:** Go stdlib + `segmentio/kafka-go`.

## Global Constraints
- Valid → `Envelope` carrying only what PR 6 needs (event_id, event_type, wiki, title, user, bot, occurred_at). EventID = `meta.id` = idempotency key.
- Invalid → `DeadLetter` with reason + source topic/partition/offset + raw payload verbatim.
- At-least-once: commit offset only after the downstream write succeeds.
- Routing distinguished by `errors.Is(err, event.ErrInvalid)` vs decode error — both go to DLQ here, but the model supports the distinction.
- PR < ~300 reviewable lines; `classify` unit-tested without a broker.

## Files
- `internal/event/envelope.go` (+`envelope_test.go`) — `Envelope`/`NewEnvelope`, `DeadLetter`/`NewDeadLetter`.
- `internal/kafka/consumer.go` — `NewReader(brokers, groupID, topic)`.
- `cmd/validator/main.go` (+`main_test.go`) — `run`, `handle`, `classify`.
- `README.md` — validator run + inspect.

## Verification (live)
- `go build/vet/test ./...`, `gofmt -l .`
- run validator against the raw backlog → `rpk` shows `Envelope`s on validated and `DeadLetter`s on dead_letter
- SIGINT → clean "validator stopped" log (no error)
- restart → resumes from committed offset

## Observed live (worth knowing)
At-least-once is real: SIGINT between a downstream write and its offset commit makes the restart reprocess that message → a duplicate on the validated/DLQ topic. **PR 6's idempotent projection is what dedupes this.** Total written can exceed unique raw by the number of in-flight messages at shutdown.

## Reviewer checklist for Mike
- Commit-after-write ordering = at-least-once. Confirm that's the intended semantic (vs at-most-once).
- `context.Canceled` from fetch/write/commit is treated as clean shutdown, not failure — agree.
- Both decode errors and `ErrInvalid` route to DLQ; the `DeadLetter.Reason` records which. OK to treat both the same for now?
- `Envelope` field set — enough for the projection, nothing extra?

## Stop condition
Stop and wait for review before PR 6 (SQLite page-activity projection with idempotency).
