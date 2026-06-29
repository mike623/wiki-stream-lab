# DuckDB web UI in a container. The UI's HTTP server binds 127.0.0.1 only, so a
# plain port publish can't reach it; socat bridges 0.0.0.0:4213 -> 127.0.0.1:4214.
# The duckdb binary is lifted from the official image; debian adds a shell + socat.
FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates socat \
 && rm -rf /var/lib/apt/lists/*

COPY --from=duckdb/duckdb:1.5.4 /duckdb /usr/local/bin/duckdb
COPY uiinit.sql /uiinit.sql
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 4213
ENTRYPOINT ["/entrypoint.sh"]
