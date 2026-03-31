#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ROWS="${1:-100000}"
PORT="${2:-9000}"
WORKERS="${3:-10}"
BENCH_COLS="${4:-10}"
BENCH_BATCH="${5:-100000}"
DEFAULT_BENCH_CLIENT_THREADS="$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 8)"
BENCH_CLIENT_THREADS="${6:-$DEFAULT_BENCH_CLIENT_THREADS}"
BENCH_MODE="${7:-extreme}"

SERVER_BIN="bin/flexql_server"
BENCH_BIN_ORIGINAL="bin/benchmark_flexql"
BENCH_BIN_ORIGINAL2="bin/benchmark_flexql2"
BENCH_BIN_EXTREME="bin/benchmark_flexql_extreme"
DATA_DIR="data/benchmark_data"

BENCH_BIN="$BENCH_BIN_EXTREME"
if [[ "$BENCH_MODE" == "original" ]]; then
  BENCH_BIN="$BENCH_BIN_ORIGINAL2"
elif [[ "$BENCH_MODE" == "original-legacy" ]]; then
  BENCH_BIN="$BENCH_BIN_ORIGINAL"
elif [[ "$BENCH_MODE" == "original2" ]]; then
  BENCH_BIN="$BENCH_BIN_ORIGINAL2"
elif [[ "$BENCH_MODE" == "extreme" ]]; then
  BENCH_BIN="$BENCH_BIN_EXTREME"
else
  echo "Error: invalid benchmark mode '$BENCH_MODE'. Use 'original', 'original2', 'original-legacy', or 'extreme'." >&2
  exit 1
fi

if command -v ss >/dev/null 2>&1; then
  if ss -ltn | grep -q ":${PORT}[[:space:]]"; then
    owner_line="$(ss -ltnp 2>/dev/null | grep ":${PORT}[[:space:]]" | head -n 1 || true)"
    owner_pid=""
    if [[ "$owner_line" =~ pid=([0-9]+) ]]; then
      owner_pid="${BASH_REMATCH[1]}"
    fi

    server_exe="$(readlink -f "$SERVER_BIN" 2>/dev/null || true)"
    owner_exe=""
    if [[ -n "$owner_pid" && -e "/proc/$owner_pid/exe" ]]; then
      owner_exe="$(readlink -f "/proc/$owner_pid/exe" 2>/dev/null || true)"
    fi

    if [[ -n "$server_exe" && -n "$owner_exe" && "$owner_exe" == "$server_exe" ]]; then
      echo "Port ${PORT} is occupied by a stale flexql_server (pid=${owner_pid}); restarting it..."
      kill "$owner_pid" >/dev/null 2>&1 || true
      sleep 0.2
    else
      echo "Error: port ${PORT} is already in use by another process. Stop it and retry." >&2
      ss -ltnp | grep ":${PORT}[[:space:]]" || true
      exit 1
    fi
  fi
fi

# Force Make to check for updates every time you run the script
echo "Checking for code changes and compiling..."
make

# Verify the build actually succeeded in creating the executable files
if [[ ! -x "$SERVER_BIN" ]] || [[ ! -x "$BENCH_BIN_ORIGINAL" ]] || [[ ! -x "$BENCH_BIN_ORIGINAL2" ]] || [[ ! -x "$BENCH_BIN_EXTREME" ]]; then
  echo "Error: Build failed or binaries are missing." >&2
  exit 1
fi

rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

FLEXQL_TRUST_INSERT_VALUES=1 "$SERVER_BIN" "$PORT" "$WORKERS" "$DATA_DIR" > /tmp/flexql_benchmark_server.log 2>&1 &
SERVER_PID=$!

echo "Started flexql_server (pid=$SERVER_PID)"

# Ensure the server is alive and listening before running the benchmark.
for _ in $(seq 1 40); do
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    echo "Error: flexql_server exited during startup." >&2
    tail -n 80 /tmp/flexql_benchmark_server.log || true
    exit 1
  fi
  if command -v ss >/dev/null 2>&1; then
    if ss -ltn | grep -q ":${PORT}[[:space:]]"; then
      break
    fi
  else
    # If socket inspection is unavailable, keep the original startup wait behavior.
    break
  fi
  sleep 0.1
done

cleanup() {
  if kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
    echo "Stopped flexql_server (pid=$SERVER_PID)"
  fi
}
trap cleanup EXIT

sleep 2

if [[ "$BENCH_MODE" == "original" ]]; then
  # Keep original semantics (5 columns) but run via the parallel/bulk backend.
  "$BENCH_BIN" "$ROWS" "5" "$BENCH_BATCH" "$BENCH_CLIENT_THREADS"
elif [[ "$BENCH_MODE" == "original-legacy" ]]; then
  "$BENCH_BIN" "$ROWS"
elif [[ "$BENCH_MODE" == "original2" ]]; then
  "$BENCH_BIN" "$ROWS" "$BENCH_COLS" "$BENCH_BATCH" "$BENCH_CLIENT_THREADS"
else
  "$BENCH_BIN" "$ROWS" "$BENCH_COLS" "$BENCH_BATCH" "$BENCH_CLIENT_THREADS"
fi