#!/usr/bin/env sh
# Helm post-renderer: receives rendered manifests on stdin, emits final on stdout.
set -e
dir="$(cd "$(dirname "$0")" && pwd)"
cat > "$dir/all.yaml"
kustomize build "$dir"
rm -f "$dir/all.yaml"
