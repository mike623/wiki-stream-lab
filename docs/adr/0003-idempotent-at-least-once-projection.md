# Projection is idempotent over at-least-once delivery

Kafka delivery is at-least-once (a consumer can re-read events after a crash or, deliberately, during replay), so the SQLite projection records each handled `event_id` in a `processed_events` table and skips duplicates inside the same transaction that updates `page_activity`. This is what makes "delete the projection and rebuild it from the log" produce an *identical* result instead of double-counting every edit.

## Consequences

We get correct rebuilds without needing Kafka exactly-once/transactional semantics — we keep at-least-once delivery and push idempotency into the write. The dedupe check and the projection update must commit atomically (one SQLite transaction) or a crash between them reintroduces double-counting.
