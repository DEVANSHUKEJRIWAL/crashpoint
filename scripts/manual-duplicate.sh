#!/usr/bin/env bash
# Issue #11 — reproduce a duplicate effect by hand.
#
# Runs the no-dedup consumer (bug3) and `kill -9`s it in the window between the
# durable ledger effect and the Kafka offset commit. On restart the offset
# redelivers, bug3 has no dedup, and a SECOND ledger row appears for the same
# event_id. That duplicate is the premise of the whole tool.
#
#   bash scripts/manual-duplicate.sh                 # widened window, proves the mechanism
#   WINDOW=0s bash scripts/manual-duplicate.sh       # the REAL (sub-ms) window — expect it to be hard
#   RESET=1 bash scripts/manual-duplicate.sh         # wipe Kafka+Postgres state first
#
# WINDOW is the consumer's artificial delay between effect and commit (the
# measured window width). WINDOW=0s is the honest "by hand" case; if that cannot
# be hit in MAX_ATTEMPTS, that is the finding — re-read RISKS.md section 5.
#
# Requires: docker compose (env from docker-compose.yml), Go.

set -euo pipefail

EVENTS="${EVENTS:-30}"          # events produced per attempt
KEYS="${KEYS:-10}"
WINDOW="${WINDOW:-3s}"          # consumer WINDOW_DELAY; the window width we measure
MAX_ATTEMPTS="${MAX_ATTEMPTS:-25}"
TOPIC="${TOPIC:-payments.requested}"
GROUP="${GROUP:-payments}"
BROKERS="${BROKERS:-localhost:9092}"
DSN="${DSN:-postgres://crashpoint:crashpoint@localhost:55432/crashpoint}"
DRAIN_SECS="${DRAIN_SECS:-6}"   # time to let the restarted consumer reprocess the redelivered batch

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

say()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
info() { printf '    %s\n' "$*"; }

# psql inside the postgres container (no host psql needed). -tA = bare value.
pq() { docker compose exec -T postgres psql -U postgres -d crashpoint -tAc "$1"; }

dup_count() { pq "SELECT count(*) FROM (SELECT 1 FROM ledger_entries GROUP BY event_id HAVING count(*)>1) d"; }
ledger_count() { pq "SELECT count(*) FROM ledger_entries"; }

# ------------------------------------------------------------------ preflight
command -v docker >/dev/null || { echo "ERROR: docker not found"; exit 1; }
command -v go >/dev/null     || { echo "ERROR: go not found"; exit 1; }

if [[ "${RESET:-0}" == "1" ]]; then
  say "Resetting Kafka + Postgres state"
  docker compose down -v
fi

say "Bringing the environment up"
docker compose up -d kafka kafka-init postgres
# Wait until Postgres answers and the topic exists.
for _ in $(seq 1 30); do pq 'SELECT 1' >/dev/null 2>&1 && break; sleep 1; done
pq 'SELECT 1' >/dev/null || { echo "ERROR: Postgres not reachable at $DSN"; exit 1; }

say "Building workload + consumer binaries"
bin="$(mktemp -d)"
trap 'rm -rf "$bin"; [[ -n "${CPID:-}" ]] && kill -9 "$CPID" 2>/dev/null || true' EXIT
go build -o "$bin/workload" ./cmd/workload
( cd sut && go build -o "$bin/consumer" ./consumer )

run_consumer() { # $1 = WINDOW_DELAY
  KAFKA_BROKERS="$BROKERS" KAFKA_TOPIC="$TOPIC" KAFKA_GROUP="$GROUP" \
  POSTGRES_DSN="$DSN" WINDOW_DELAY="$1" \
    "$bin/consumer" -mode bug3 >/dev/null 2>&1 &
  CPID=$!
}

say "Hunting the window (WINDOW=$WINDOW, up to $MAX_ATTEMPTS attempts)"
start_dups="$(dup_count)"
attempt=0
while (( attempt < MAX_ATTEMPTS )); do
  attempt=$((attempt + 1))

  before="$(ledger_count)"
  "$bin/workload" -brokers "$BROKERS" -topic "$TOPIC" -count "$EVENTS" -keys "$KEYS" \
      -rate 0 -seed "$attempt" -out /dev/null 2>/dev/null

  # Start the no-dedup consumer, then kill it as soon as effects become durable:
  # it is now inside the WINDOW_DELAY sleep, before the offset commit.
  run_consumer "$WINDOW"
  hit=0
  for _ in $(seq 1 100); do
    [[ "$(ledger_count)" -gt "$before" ]] && { hit=1; break; }
    sleep 0.1
  done
  kill -9 "$CPID" 2>/dev/null || true
  wait "$CPID" 2>/dev/null || true
  CPID=""

  if (( hit == 0 )); then
    info "attempt $attempt: consumer wrote nothing before the kill; retrying"
    continue
  fi

  # Restart with no delay to reprocess the redelivered (uncommitted) batch.
  run_consumer "0s"
  sleep "$DRAIN_SECS"
  kill -15 "$CPID" 2>/dev/null || true
  wait "$CPID" 2>/dev/null || true
  CPID=""

  now_dups="$(dup_count)"
  if (( now_dups > start_dups )); then
    say "DUPLICATE REPRODUCED on attempt $attempt (window width = $WINDOW)"
    echo
    echo "Proving SQL:"
    echo "  SELECT event_id, count(*) AS n, array_agg(id ORDER BY id) AS ledger_ids"
    echo "  FROM ledger_entries GROUP BY event_id HAVING count(*) > 1;"
    echo
    pq "SELECT event_id, count(*) AS n, array_agg(id ORDER BY id) AS ledger_ids
        FROM ledger_entries GROUP BY event_id HAVING count(*)>1
        ORDER BY event_id LIMIT 20"
    echo
    info "Attempts: $attempt   Window width: $WINDOW"
    info "Record these in docs/JOURNAL.md (2026-09-30 entry) — this is post #1."
    exit 0
  fi
  info "attempt $attempt: no new duplicate yet"
done

say "No duplicate in $MAX_ATTEMPTS attempts at WINDOW=$WINDOW"
cat <<'MSG'
STOP CONDITION (issue #11): the window could not be hit.
If you were already at WINDOW=0s, re-read RISKS.md section 5 before building
further. Otherwise lower WINDOW toward 0s to measure how hard the real window
is to hit by hand — that difficulty is the motivation for the whole tool.
MSG
exit 1
