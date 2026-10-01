#!/usr/bin/env bash
# Baseline benchmark (issue #10, targets P1/P2): direct-to-broker vs through the
# proxy with recording off and on. Open-loop load from cmd/bench.
#
#   bash bench/baseline/run.sh                     # methodology defaults
#   RATE=0 bash bench/baseline/run.sh              # P1: max throughput (saturate)
#   WARMUP=5s DUR=20s bash bench/baseline/run.sh   # quick smoke
#
# Requires the env from docker-compose.yml to be up, plus Go. Raw latencies and
# summaries land under bench/baseline/<date>/.

set -euo pipefail

RATE="${RATE:-50000}"          # P2 default; 0 = P1 saturate
SIZE="${SIZE:-1024}"           # 1 KB records
WARMUP="${WARMUP:-60s}"        # methodology: 60s warmup
DUR="${DUR:-180s}"             # methodology: 3-minute measured window
TOPIC="${TOPIC:-bench}"
BROKER="${BROKER:-localhost:9092}"
PROXY_LISTEN="${PROXY_LISTEN:-:19092}"
PROXY_ADDR="${PROXY_ADDR:-localhost:19092}"

root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
out="bench/baseline/$(date +%F)"
mkdir -p "$out"

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

command -v docker >/dev/null || { echo "ERROR: docker not found"; exit 1; }
command -v go >/dev/null || { echo "ERROR: go not found"; exit 1; }

say "Ensuring the bench topic exists"
docker compose exec -T kafka kafka-topics.sh --bootstrap-server kafka:29092 \
  --create --if-not-exists --topic "$TOPIC" --partitions 6 --replication-factor 1

say "Building bench + crashpoint"
bin="$(mktemp -d)"
trap 'rm -rf "$bin"; [[ -n "${PROXY_PID:-}" ]] && kill "$PROXY_PID" 2>/dev/null || true' EXIT
go build -o "$bin/bench" ./cmd/bench
go build -o "$bin/crashpoint" ./cmd/crashpoint

run_bench() { # $1 = label, $2 = bootstrap addr
  say "Condition: $1 (rate=$RATE size=$SIZE warmup=$WARMUP dur=$DUR)"
  "$bin/bench" -brokers "$2" -topic "$TOPIC" -rate "$RATE" -size "$SIZE" \
    -warmup "$WARMUP" -duration "$DUR" -out "$out/$1.lat.txt" | tee "$out/$1.summary.txt"
}

start_proxy() { # $1 = record file ("" = off)
  local rec=()
  [[ -n "$1" ]] && rec=(-record "$1")
  "$bin/crashpoint" proxy -listen "$PROXY_LISTEN" -seed "$BROKER" "${rec[@]}" &
  PROXY_PID=$!
  sleep 1
}
stop_proxy() { kill "$PROXY_PID" 2>/dev/null || true; wait "$PROXY_PID" 2>/dev/null || true; PROXY_PID=""; }

run_bench "direct" "$BROKER"

start_proxy ""
run_bench "proxy-record-off" "$PROXY_ADDR"
stop_proxy

start_proxy "$out/frames.jsonl"
run_bench "proxy-record-on" "$PROXY_ADDR"
stop_proxy

say "Done. Raw + summaries under $out/"
echo "Fill the BENCHMARKS.md environment table and the P1/P2 Measured column from these."
