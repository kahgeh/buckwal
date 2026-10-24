#!/usr/bin/env bash
# ClickHouse check of one test run's telemetry. Usage: scripts/otel-check.sh RUN_ID
#   Prints `otel check SKIPPED: …` and exits 0 when the observability stack is down;
#   otherwise polls up to 60 s and prints `otel check PASSED: …` (exit 0) or
#   `otel check FAILED: …` (exit 1).
# Reads BUCKWAL_CH_URL (default http://localhost:8123), BUCKWAL_CH_USER (default hyperdx_ro)
# and BUCKWAL_CH_PASSWORD; without it, the password is read at run time from the users SQL
# file that BUCKWAL_CH_USERS_SQL names. With neither set, it prints
# `otel check SKIPPED: no ClickHouse password` and exits 0.
# Also checked (read-only SELECTs): a `buckwal.takeover` span of the run carries
# `buckwal.quiet_ms` (and `buckwal.next_generation_floor`), and the encrypted tamper tests left
# at least one ERROR record `integrity check failed` with no data or code field, and, when
# replica tests ran under the run id (any `buckwal.replica.sync` span), those spans carry
# `buckwal.lag_ms` and `buckwal.trigger`.
# Also checked: the run's spans carry the marking (`deployment.environment.name = test`,
# `host.name`, `service.instance.id` as resource attributes), and at least one span of the run
# has status Error (a failed commit, restore or takeover of the fault tests).
# A WAL `buckwal.commit` of the run carries `buckwal.journal_mode = wal` and ends at or after its
# `buckwal.ack_wait` child.
# The password only ever reaches curl on stdin (`-K -`), never a command line,
# a file or the output.
set -euo pipefail

RUN_ID="${1:-}"
CH_URL="${BUCKWAL_CH_URL:-http://localhost:8123}"
CH_USER="${BUCKWAL_CH_USER:-hyperdx_ro}"
USERS_SQL="${BUCKWAL_CH_USERS_SQL:-}"
SPANS="buckwal.commit buckwal.local_write buckwal.local_fsync buckwal.ack_wait buckwal.upload buckwal.checkpoint_upload buckwal.restore buckwal.takeover buckwal.fence buckwal.gc buckwal.server.request"
DEADLINE=60

fail() { echo "otel check FAILED: $*"; exit 1; }

[[ "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]] || fail "usage: otel-check.sh RUN_ID (letters, digits, . _ -)"
[[ "$CH_USER" =~ ^[A-Za-z0-9_]+$ ]] || fail "BUCKWAL_CH_USER must be letters, digits and _"

if [[ -z "${BUCKWAL_CH_PASSWORD:-}" && -z "$USERS_SQL" ]]; then
  echo "otel check SKIPPED: no ClickHouse password"
  exit 0
fi

command -v nc >/dev/null 2>&1 || fail "nc not found"
if ! curl -sf --max-time 5 "$CH_URL/ping" >/dev/null 2>&1 \
   || ! nc -z -w 3 localhost 4317 >/dev/null 2>&1; then
  echo "otel check SKIPPED: observability stack not running"
  exit 0
fi

# The read-only user's password: the environment's, else the users SQL file's.
password() {
  if [[ -n "${BUCKWAL_CH_PASSWORD:-}" ]]; then
    printf '%s' "$BUCKWAL_CH_PASSWORD"
    return
  fi
  [[ -n "$USERS_SQL" && -r "$USERS_SQL" ]] || return 1
  sed -n "s/^CREATE USER IF NOT EXISTS $CH_USER IDENTIFIED BY '\([^']*\)'.*/\1/p" "$USERS_SQL" \
    | head -n 1
}
PASSWORD="$(password)" || PASSWORD=""
[[ -n "$PASSWORD" ]] || fail "no password for $CH_USER (set BUCKWAL_CH_PASSWORD or BUCKWAL_CH_USERS_SQL)"

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

TRACES="FROM otel.otel_traces WHERE ServiceName='buckwal' AND SpanAttributes['test_run_id']='$RUN_ID'"
LOGS="FROM otel.otel_logs WHERE ServiceName='buckwal' AND LogAttributes['test_run_id']='$RUN_ID'"

start=$SECONDS
while :; do
  query "SELECT SpanName, count() $TRACES AND Timestamp > now() - INTERVAL 1 DAY GROUP BY SpanName ORDER BY SpanName"
  spans="$OUT"
  query "SELECT count() $LOGS AND toDate(Timestamp) = today()"
  logs="$OUT"
  query "SELECT count() $LOGS AND Timestamp < '2000-01-01'"
  untimed="$OUT"

  query "SELECT count() $TRACES AND SpanName = 'buckwal.takeover' AND Timestamp > now() - INTERVAL 1 DAY AND SpanAttributes['buckwal.quiet_ms'] != '' AND SpanAttributes['buckwal.next_generation_floor'] != ''"
  quiet_spans="$OUT"
  query "SELECT count() $LOGS AND toDate(Timestamp) = today() AND Body = 'integrity check failed' AND SeverityText = 'ERROR'"
  integrity="$OUT"
  # An integrity record carries the object key, kind, generation and seq, never bytes.
  query "SELECT count() $LOGS AND toDate(Timestamp) = today() AND Body = 'integrity check failed' AND mapContains(LogAttributes, 'data')"
  integrity_data="$OUT"

  # Replica passes: when the run had any (replica tests ran), at least one must carry both the
  # lag and the trigger (a failed pass has no lag to report).
  query "SELECT count() $TRACES AND SpanName = 'buckwal.replica.sync' AND Timestamp > now() - INTERVAL 1 DAY"
  replica_all="$OUT"
  query "SELECT count() $TRACES AND SpanName = 'buckwal.replica.sync' AND Timestamp > now() - INTERVAL 1 DAY AND SpanAttributes['buckwal.lag_ms'] != '' AND SpanAttributes['buckwal.trigger'] IN ('notification', 'slow_poll', 'start')"
  replica_sync="$OUT"

  # The marking: every span of the run is a test span from a named machine and process.
  query "SELECT count() $TRACES AND Timestamp > now() - INTERVAL 1 DAY AND ResourceAttributes['deployment.environment.name'] = 'test' AND ResourceAttributes['host.name'] != '' AND ResourceAttributes['service.instance.id'] != ''"
  marked="$OUT"
  query "SELECT count() $TRACES AND Timestamp > now() - INTERVAL 1 DAY AND NOT (ResourceAttributes['deployment.environment.name'] = 'test' AND ResourceAttributes['host.name'] != '' AND ResourceAttributes['service.instance.id'] != '')"
  unmarked="$OUT"
  query "SELECT count() $TRACES AND Timestamp > now() - INTERVAL 1 DAY AND StatusCode = 'Error'"
  errors="$OUT"

  # One WAL commit of the run: journal mode `wal`, and its ack wait ends inside it.
  query "SELECT count() FROM otel.otel_traces AS c INNER JOIN otel.otel_traces AS a ON a.TraceId = c.TraceId AND a.ParentSpanId = c.SpanId WHERE c.ServiceName = 'buckwal' AND c.SpanName = 'buckwal.commit' AND c.SpanAttributes['test_run_id'] = '$RUN_ID' AND c.Timestamp > now() - INTERVAL 1 DAY AND c.SpanAttributes['buckwal.journal_mode'] = 'wal' AND a.ServiceName = 'buckwal' AND a.SpanName = 'buckwal.ack_wait' AND a.SpanAttributes['test_run_id'] = '$RUN_ID' AND a.Timestamp > now() - INTERVAL 1 DAY AND toUnixTimestamp64Nano(c.Timestamp) + c.Duration >= toUnixTimestamp64Nano(a.Timestamp) + a.Duration"
  wal_commits="$OUT"

  missing=""
  [[ "$marked" -gt 0 ]] || missing="$missing resource-marking(deployment.environment.name,host.name,service.instance.id)"
  [[ "$unmarked" -eq 0 ]] || missing="$missing every-span-marked(unmarked $unmarked)"
  [[ "$errors" -gt 0 ]] || missing="$missing span-with-status-Error"
  [[ "$wal_commits" -gt 0 ]] || missing="$missing buckwal.commit(buckwal.journal_mode=wal,ends-after-ack_wait)"
  [[ "$replica_all" -eq 0 || "$replica_sync" -gt 0 ]] || missing="$missing buckwal.replica.sync(buckwal.lag_ms,buckwal.trigger)(of $replica_all)"
  for s in $SPANS; do
    awk -F'\t' -v s="$s" '$1 == s && $2 > 0 { found = 1 } END { exit !found }' <<<"$spans" \
      || missing="$missing $s"
  done
  counts="$(awk -F'\t' 'NF == 2 { printf "%s%s=%s", sep, $1, $2; sep = " " }' <<<"$spans")"
  [[ "$quiet_spans" -gt 0 ]] || missing="$missing buckwal.takeover(buckwal.quiet_ms)"
  [[ "$integrity" -gt 0 ]] || missing="$missing log:integrity-check-failed"
  [[ "$integrity_data" -eq 0 ]] || missing="$missing no-data-in-integrity-records(found $integrity_data)"
  if [[ -z "$missing" && "$logs" -gt 0 && "$untimed" -eq 0 ]]; then
    echo "otel check PASSED: $counts logs=$logs takeover_quiet_spans=$quiet_spans integrity_records=$integrity replica_sync_spans=$replica_sync marked_spans=$marked error_spans=$errors"
    exit 0
  fi
  if (( SECONDS - start >= DEADLINE )); then
    fail "run $RUN_ID after ${DEADLINE}s: missing:${missing:- none}; logs=$logs; logs before 2000-01-01=$untimed; spans: ${counts:-none}"
  fi
  sleep 3
done
