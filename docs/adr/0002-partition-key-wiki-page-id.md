# Partition key is `wiki:page_id`

Producer records are keyed by `wiki:page_id` so each page's edits land on a stable partition (preserving per-page edit order, which the Page Activity projection needs) while pages spread evenly across partitions (enabling consumer-group parallelism up to the partition count). Topics start with **6 partitions** so the lag-drain demo has headroom to show scaling.

## Considered options

- **`wiki:page_id` (chosen)** — even load, full parallelism, per-page ordering preserved. Loses global per-wiki ordering, which the per-page counting projection does not need.
- **`wiki`** — preserves global per-wiki ordering but sends all of `enwiki`/`wikidatawiki` to a single partition (severe skew / hot partition), capping that traffic at one consumer no matter how many are added. Kept only as a *teaching* contrast, not the production key.
- **null key (round-robin)** — maximum spread but no ordering at all; a page's edits could be processed out of order, corrupting `last_user`/`last_event_at`.

## Consequences

Kafka guarantees ordering only within a partition. With `wiki:page_id` we accept that two different pages may be processed out of wall-clock order; this is fine because per-page counters are independent. Changing the key later reshuffles the partition assignment of all keys, so it is not a free change once a topic has history.
