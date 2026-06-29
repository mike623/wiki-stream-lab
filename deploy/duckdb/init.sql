-- Runs on every `docker compose run --rm duckdb` start. Wires DuckDB to the
-- local RustFS store and exposes the Parquet lake as a queryable view.
-- httpfs is downloaded on first run and cached in the duckdb-ext volume (HOME),
-- so later runs work offline.
INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET rustfs (
    TYPE s3,
    KEY_ID 'rustfsadmin',
    SECRET 'rustfsadmin',
    ENDPOINT 'rustfs:9000',  -- compose-network hostname
    URL_STYLE 'path',        -- RustFS/MinIO serve buckets as URL paths
    USE_SSL false,
    REGION 'us-east-1'
);

-- dt= is read back as a typed partition column; engines prune by it.
CREATE OR REPLACE VIEW lake AS
    SELECT * FROM read_parquet(
        's3://wiki-stream-lab/lake/dt=*/**/*.parquet',
        hive_partitioning = true
    );

-- Verbatim raw backup, if you want to inspect it too.
CREATE OR REPLACE VIEW raw AS
    SELECT * FROM read_json_auto(
        's3://wiki-stream-lab/raw/**/*.jsonl.gz'
    );

.mode duckbox
SELECT 'lake ready: try  SELECT wiki, count(*) FROM lake GROUP BY 1 ORDER BY 2 DESC LIMIT 10;' AS hint;
