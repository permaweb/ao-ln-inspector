#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
API_BIN="${API_BIN:-$ROOT_DIR/target/release/ao-ln-inspector}"
PAGER_BIN="${PAGER_BIN:-$ROOT_DIR/target/release/ao-ln-pager}"

if [[ ! -x "$API_BIN" ]]; then
  echo "missing or non-executable API binary: $API_BIN" >&2
  exit 1
fi

if [[ ! -x "$PAGER_BIN" ]]; then
  echo "missing or non-executable pager binary: $PAGER_BIN" >&2
  exit 1
fi

"$API_BIN" &
api_pid=$!

"$PAGER_BIN" &
pager_pid=$!

cleanup() {
  kill "$api_pid" "$pager_pid" 2>/dev/null || true
  wait "$api_pid" 2>/dev/null || true
  wait "$pager_pid" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

wait -n "$api_pid" "$pager_pid"
