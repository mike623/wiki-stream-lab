#!/bin/sh
set -e

# Expose the localhost-only UI server to the container's network interface.
# The UI binds IPv6 [::1]:4213, so connect there; listen on IPv4 0.0.0.0:4213
# for the published host port. Same port both sides on purpose: the UI gates
# /localToken on the request Referer matching http://localhost:<ui_local_port>,
# so the browser-facing port MUST equal ui_local_port or every API call 401s.
# The two listeners don't collide: IPv4 0.0.0.0 and IPv6 [::1] are distinct sockets.
# Backgrounded (no exec) to stay a live child.
socat TCP4-LISTEN:4213,fork,reuseaddr TCP6:[::1]:4213 &

# Keep a duckdb session alive (tail never closes stdin) so the UI server it
# starts in /uiinit.sql keeps running. Foreground: this is what holds PID 1.
tail -f /dev/null | duckdb -init /uiinit.sql
