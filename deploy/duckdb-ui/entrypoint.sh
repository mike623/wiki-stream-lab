#!/bin/sh
set -e

# Expose the localhost-only UI server to the container's network interface.
# The UI binds IPv6 [::1]:4214, so connect there; listen on IPv4 0.0.0.0:4213
# for the published host port. Backgrounded (no exec) to stay a live child.
socat TCP4-LISTEN:4213,fork,reuseaddr TCP6:[::1]:4214 &

# Keep a duckdb session alive (tail never closes stdin) so the UI server it
# starts in /uiinit.sql keeps running. Foreground: this is what holds PID 1.
tail -f /dev/null | duckdb -init /uiinit.sql
