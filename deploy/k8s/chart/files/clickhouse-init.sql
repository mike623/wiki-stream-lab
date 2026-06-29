-- Real-time OLAP layer: ClickHouse ingests the validated topic straight from
-- Redpanda via the Kafka table engine, a materialized view lands each event in
-- a MergeTree, and Grafana queries that for sub-second insight.
--
-- Loaded once on first container start (/docker-entrypoint-initdb.d).
-- ClickHouse becomes its own consumer group ("clickhouse") on the log.

CREATE DATABASE IF NOT EXISTS wsl;

-- 1. Kafka engine table = the consumer. Reads validated envelopes as JSON.
CREATE TABLE IF NOT EXISTS wsl.kafka_validated
(
    event_id    String,
    event_type  String,
    wiki        String,
    title       String,
    user        String,
    bot         Bool,
    occurred_at Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list      = 'redpanda:9092',
    kafka_topic_list       = 'wikimedia.recentchange.validated',
    kafka_group_name       = 'clickhouse',
    kafka_format           = 'JSONEachRow',
    kafka_num_consumers    = 1,
    kafka_skip_broken_messages = 10;

-- 2. Durable destination, columnar + indexed for fast group-by.
CREATE TABLE IF NOT EXISTS wsl.events
(
    event_id    String,
    event_type  LowCardinality(String),
    wiki        LowCardinality(String),
    title       String,
    user        String,
    bot         Bool,
    occurred_at DateTime,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (occurred_at, wiki);

-- 3. Materialized view: copies each row read by the Kafka engine into events,
--    converting the unix timestamp to a DateTime as it goes.
CREATE MATERIALIZED VIEW IF NOT EXISTS wsl.mv_events TO wsl.events AS
SELECT
    event_id,
    event_type,
    wiki,
    title,
    user,
    bot,
    toDateTime(occurred_at) AS occurred_at
FROM wsl.kafka_validated;
