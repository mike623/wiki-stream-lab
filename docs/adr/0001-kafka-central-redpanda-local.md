# Kafka stays central; Redpanda runs it locally

The whole point of this repo is to learn Kafka semantics (durable log, partitioning, consumer groups, replay) that a simpler tool cannot teach, so Kafka is the backbone of the main data flow and is **not** substitutable by n8n, Redis, NATS, webhooks, or an in-process Go channel bus — even where one of those would be less code. We run a real Kafka-compatible broker locally via **Redpanda** in Docker Compose (single binary, no ZooKeeper/KRaft fuss) rather than Apache Kafka, to minimize local-dev friction while keeping the protocol identical.

## Consequences

A future reader tempted to "simplify" the pipeline into a queue or direct call should not — that change deletes the learning value, which is the product. Code stays portable to Apache Kafka because Redpanda speaks the Kafka protocol.
