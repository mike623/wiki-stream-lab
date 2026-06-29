-- Starts the DuckDB web UI server (on the bridged port) and wires the same
-- RustFS views as the CLI container. ui + httpfs download on first run and are
-- cached in the duckdb-ext volume (HOME).
INSTALL ui;
LOAD ui;
-- Must equal the published/browser-facing port: the UI authorizes /localToken
-- by checking the request Referer starts with http://localhost:<ui_local_port>.
SET ui_local_port = 4213;
CALL start_ui_server();

INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET rustfs (
    TYPE s3,
    KEY_ID 'rustfsadmin',
    SECRET 'rustfsadmin',
    ENDPOINT 'rustfs:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);

CREATE OR REPLACE VIEW lake AS
    SELECT * FROM read_parquet(
        's3://wiki-stream-lab/lake/dt=*/**/*.parquet',
        hive_partitioning = true
    );

CREATE OR REPLACE VIEW raw AS
    SELECT * FROM read_json_auto('s3://wiki-stream-lab/raw/**/*.jsonl.gz');
