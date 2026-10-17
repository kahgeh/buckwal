#!/usr/bin/env bash
# ClickHouse check of one test run's telemetry. Usage: scripts/otel-check.sh RUN_ID
#   Prints `otel check SKIPPED: …` and exits 0 when the observability stack is down;
#   otherwise polls up to 60 s and prints `otel check PASSED: …` (exit 0) or
#   `otel check FAILED: …` (exit 1).
# Reads DURABLE_CH_URL (default http://localhost:8123), DURABLE_CH_USER (default hyperdx_ro)
# and DURABLE_CH_PASSWORD; without it, the password is read from the stack's users SQL file
# at run time. The password only ever reaches curl on stdin (`-K -`), never a command line,
# a file or the output.
set -euo pipefail

RUN_ID="${1:-}"
CH_URL="${DURABLE_CH_URL:-http://localhost:8123}"
CH_USER="${DURABLE_CH_USER:-hyperdx_ro}"
USERS_SQL="$HOME/Dev/p/sre/stack/clickhouse/00_database_users.sql"
SPANS="durable.commit durable.local_write durable.local_fsync durable.ack_wait durable.upload durable.checkpoint_upload durable.restore durable.takeover durable.fence durable.gc"
DEADLINE=60

fail() { echo "otel check FAILED: $*"; exit 1; }

[[ "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]] || fail "usage: otel-check.sh RUN_ID (letters, digits, . _ -)"
[[ "$CH_USER" =~ ^[A-Za-z0-9_]+$ ]] || fail "DURABLE_CH_USER must be letters, digits and _"

command -v nc >/dev/null 2>&1 || fail "nc not found"
if ! curl -sf --max-time 5 "$CH_URL/ping" >/dev/null 2>&1 \
   || ! nc -z -w 3 localhost 4317 >/dev/null 2>&1; then
  echo "otel check SKIPPED: observability stack not running"
  exit 0
fi

# The read-only user's password: the environment's, else the users SQL file's.
password() {
  if [[ -n "${DURABLE_CH_PASSWORD:-}" ]]; then
    printf '%s' "$DURABLE_CH_PASSWORD"
    return
  fi
  [[ -r "$USERS_SQL" ]] || return 1
  sed -n "s/^CREATE USER IF NOT EXISTS $CH_USER IDENTIFIED BY '\([^']*\)'.*/\1/p" "$USERS_SQL" \
    | head -n 1
}
PASSWORD="$(password)" || PASSWORD=""
[[ -n "$PASSWORD" ]] || fail "no password for $CH_USER (set DURABLE_CH_PASSWORD)"

# ch SQL -> the result as TabSeparated, then the HTTP status on its own line; curl's own
# error text on stderr.
ch() {
  local escaped="${PASSWORD//\\/\\\\}"
  escaped="${escaped//\"/\\\"}"
  printf 'user = "%s:%s"\n' "$CH_USER" "$escaped" \
    | curl -sS --max-time 10 -K - --data-binary "$1" "$CH_URL/?default_format=TabSeparated" \
      -w '\n%{http_code}'
}

# query SQL -> sets OUT, or fails with ClickHouse's message, or curl's on a transport error.
query() {
  local r code why
  if ! r="$(ch "$1" 2>&1)"; then
    # Everything but the -w status line: curl's own error text (no body on a transport error).
    why="$(sed '$d' <<<"$r" | tr '\n' ' ' | sed 's/ *$//')"
    fail "query failed: ${why:-curl exited with an error}"
  fi
  code="$(tail -n 1 <<<"$r")"
  OUT="$(sed '$d' <<<"$r")"
  [[ "$code" == 200 ]] || fail "HTTP $code: $(head -c 300 <<<"$OUT" | tr '\n' ' ')"
}

TRACES="FROM otel.otel_traces WHERE ServiceName='turso-durable' AND SpanAttributes['test_run_id']='$RUN_ID'"
LOGS="FROM otel.otel_logs WHERE ServiceName='turso-durable' AND LogAttributes['test_run_id']='$RUN_ID'"

start=$SECONDS
while :; do
  query "SELECT SpanName, count() $TRACES AND Timestamp > now() - INTERVAL 1 DAY GROUP BY SpanName ORDER BY SpanName"
  spans="$OUT"
  query "SELECT count() $LOGS AND toDate(Timestamp) = today()"
  logs="$OUT"
  query "SELECT count() $LOGS AND Timestamp < '2000-01-01'"
  untimed="$OUT"

  missing=""
  for s in $SPANS; do
    awk -F'\t' -v s="$s" '$1 == s && $2 > 0 { found = 1 } END { exit !found }' <<<"$spans" \
      || missing="$missing $s"
  done
  counts="$(awk -F'\t' 'NF == 2 { printf "%s%s=%s", sep, $1, $2; sep = " " }' <<<"$spans")"
  if [[ -z "$missing" && "$logs" -gt 0 && "$untimed" -eq 0 ]]; then
    echo "otel check PASSED: $counts logs=$logs"
    exit 0
  fi
  if (( SECONDS - start >= DEADLINE )); then
    fail "run $RUN_ID after ${DEADLINE}s: missing spans:${missing:- none}; logs=$logs; logs before 2000-01-01=$untimed; spans: ${counts:-none}"
  fi
  sleep 3
done
