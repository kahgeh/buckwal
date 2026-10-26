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
# `buckwal.ack_wait` child. A background commit (`buckwal.batching` starting `background`) has no
# ack wait: it ends with its local fsync, so this check leaves it out.
# Also checked: every `sql` block in README, Telemetry runs in ClickHouse with its
# `!= 'test'` swapped for `= 'test'`. A query that returns no rows passes: the check proves
# that the query runs, not that the run made its data. A failure names the README line of the block.
# Also checked, for the spans of packs (spec packer, Observability): at least one span of the
# run of each of these carries its attributes: `buckwal.generation_switch` (outcome `switched`,
# `buckwal.next_generation`, `buckwal.elapsed_ms`), `buckwal.pack` (outcome `packed`,
# `buckwal.prefix`, `buckwal.pack.generation`, `buckwal.pack.source` `kept` or `downloaded`,
# `buckwal.free_page_share`), `buckwal.pack.gauges` (outcome `ok`, `buckwal.prefix`,
# `buckwal.pack.behind_bytes`, `buckwal.pack.age_seconds`, `buckwal.pack.size_bytes`; and one
# prefix with at least 3 of them, as the alert needs), `buckwal.restore` (`buckwal.prefix`,
# `buckwal.restore.outcome` `restored`, `buckwal.restore.restarts`), `buckwal.prefetch`
# (`buckwal.prefetch.past_half_keep`), `buckwal.vacuum` (outcome `vacuumed`,
# `buckwal.vacuum.pages_before`, `buckwal.free_page_share`) and `buckwal.maintenance`
# (outcome `switched`, `buckwal.wal_bytes`, `buckwal.gate_wait_ms`). The alert queries of the
# spec (ALERT_QUERIES below: "packer not keeping up", "restore gave up", and the free-page
# share warning) must run, with `!= 'test'` swapped for `= 'test'` as for README's.
# The password only ever reaches curl on stdin (`-K -`), never a command line,
# a file or the output.
set -euo pipefail

RUN_ID="${1:-}"
CH_URL="${BUCKWAL_CH_URL:-http://localhost:8123}"
CH_USER="${BUCKWAL_CH_USER:-hyperdx_ro}"
USERS_SQL="${BUCKWAL_CH_USERS_SQL:-}"
SPANS="buckwal.commit buckwal.local_write buckwal.local_fsync buckwal.ack_wait buckwal.upload buckwal.generation_switch buckwal.restore buckwal.prefetch buckwal.takeover buckwal.fence buckwal.gc buckwal.maintenance buckwal.pack buckwal.pack.gauges buckwal.vacuum buckwal.server.request"
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

# Every `sql` block in README, Telemetry, with `!= 'test'` swapped for `= 'test'`, must run.
# Prints the README line and ClickHouse's error of the first block that does not.
readme_queries() {
  local readme line sql="" lineno=0 start=0 count=0 in_section=0 in_block=0 r code body
  local from="!= 'test'" to="= 'test'"
  readme="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/README.md"
  [[ -r "$readme" ]] || fail "README.md not found at $readme"
  while IFS= read -r line; do
    lineno=$((lineno + 1))
    if (( ! in_section )); then
      [[ "$line" == '## Telemetry' ]] && in_section=1
      continue
    fi
    if (( in_block )); then
      if [[ "$line" != '```' ]]; then
        sql+="$line"$'\n'
        continue
      fi
      in_block=0
      count=$((count + 1))
      sql=${sql//"$from"/$to}
      if ! r="$(ch "$sql" 2>&1)"; then
        fail "README.md:$start: query failed: $(tr '\n' ' ' <<<"$r" | head -c 300)"
      fi
      code="$(tail -n 1 <<<"$r")"
      body="$(sed '$d' <<<"$r")"
      [[ "$code" == 200 ]] \
        || fail "README.md:$start: HTTP $code: $(head -c 300 <<<"$body" | tr '\n' ' ')"
    elif [[ "$line" == '```sql' ]]; then
      in_block=1
      start=$lineno
      sql=""
    elif [[ "$line" == '## '* ]]; then
      break
    fi
  done < "$readme"
  (( count > 0 )) || fail "README.md: no sql block in Telemetry"
  README_QUERIES=$count
}
readme_queries

# The alert queries of the spec (packer, Observability), one statement each, separated by a
# line `;;`. Each leaves out test runs, as README's queries do.
ALERT_QUERIES="$(cat <<'SQL'
-- packer not keeping up (warn): per database prefix, over 15 minutes, at least 3 gauges
-- spans, and every one of them with a pack behind by more than half the size of the pack
-- (the ratio skips spans with size_bytes 0: no pack yet), or every one of them with an ended
-- generation that no pack holds for more than 2 hours (this alone fires with no pack)
SELECT SpanAttributes['buckwal.prefix'] AS prefix,
       count() AS spans,
       minIf(toFloat64OrZero(SpanAttributes['buckwal.pack.behind_bytes'])
             / toFloat64OrZero(SpanAttributes['buckwal.pack.size_bytes']),
             toFloat64OrZero(SpanAttributes['buckwal.pack.size_bytes']) > 0) AS behind_share,
       min(toUInt64OrZero(SpanAttributes['buckwal.pack.age_seconds'])) AS age_seconds
FROM otel.otel_traces
WHERE ServiceName = 'buckwal' AND SpanName = 'buckwal.pack.gauges'
  AND SpanAttributes['buckwal.outcome'] = 'ok'
  AND SpanAttributes['buckwal.pack.age_seconds'] != ''
  AND ResourceAttributes['deployment.environment.name'] != 'test'
  AND Timestamp > now() - INTERVAL 15 MINUTE
GROUP BY prefix
HAVING spans >= 3 AND (behind_share > 0.5 OR age_seconds > 7200)
;;
-- restore gave up (warn): restores that gave up in the last 15 minutes, per database
-- prefix and host
SELECT SpanAttributes['buckwal.prefix'] AS prefix,
       ResourceAttributes['host.name'] AS host,
       count() AS gave_up
FROM otel.otel_traces
WHERE ServiceName = 'buckwal' AND SpanName = 'buckwal.restore'
  AND SpanAttributes['buckwal.restore.outcome'] = 'gave_up'
  AND ResourceAttributes['deployment.environment.name'] != 'test'
  AND Timestamp > now() - INTERVAL 15 MINUTE
GROUP BY prefix, host
;;
-- free pages (warn): the newest pack of a database is more than 20% free pages; run
-- `buckwal vacuum`
SELECT SpanAttributes['buckwal.prefix'] AS prefix,
       argMax(toFloat64OrZero(SpanAttributes['buckwal.free_page_share']), Timestamp) AS free_page_share
FROM otel.otel_traces
WHERE ServiceName = 'buckwal' AND SpanName = 'buckwal.pack'
  AND SpanAttributes['buckwal.outcome'] = 'packed'
  AND ResourceAttributes['deployment.environment.name'] != 'test'
  AND Timestamp > now() - INTERVAL 1 DAY
GROUP BY prefix
HAVING free_page_share > 0.2
SQL
)"

# Every statement of ALERT_QUERIES, with `!= 'test'` swapped for `= 'test'`, must run.
alert_queries() {
  local sql="" line n=0 from="!= 'test'" to="= 'test'"
  run_one() {
    [[ -n "${sql//[$' \n']/}" ]] || return 0
    n=$((n + 1))
    query "${sql//"$from"/$to}"
    sql=""
  }
  while IFS= read -r line; do
    if [[ "$line" == ';;' ]]; then
      run_one
    else
      sql+="$line"$'\n'
    fi
  done <<<"$ALERT_QUERIES"
  run_one
  ALERTS=$n
}
alert_queries

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
  query "SELECT count() FROM otel.otel_traces AS c INNER JOIN otel.otel_traces AS a ON a.TraceId = c.TraceId AND a.ParentSpanId = c.SpanId WHERE c.ServiceName = 'buckwal' AND c.SpanName = 'buckwal.commit' AND c.SpanAttributes['test_run_id'] = '$RUN_ID' AND c.Timestamp > now() - INTERVAL 1 DAY AND c.SpanAttributes['buckwal.journal_mode'] = 'wal' AND c.SpanAttributes['buckwal.batching'] NOT LIKE 'background%' AND a.ServiceName = 'buckwal' AND a.SpanName = 'buckwal.ack_wait' AND a.SpanAttributes['test_run_id'] = '$RUN_ID' AND a.Timestamp > now() - INTERVAL 1 DAY AND toUnixTimestamp64Nano(c.Timestamp) + c.Duration >= toUnixTimestamp64Nano(a.Timestamp) + a.Duration"
  wal_commits="$OUT"

  # The spans of packs: one span of the run of each kind with its attributes.
  span_with() { query "SELECT count() $TRACES AND Timestamp > now() - INTERVAL 1 DAY AND SpanName = '$1' AND $2"; }
  span_with buckwal.generation_switch "SpanAttributes['buckwal.outcome'] = 'switched' AND SpanAttributes['buckwal.next_generation'] != '' AND SpanAttributes['buckwal.elapsed_ms'] != ''"
  switch_spans="$OUT"
  span_with buckwal.pack "SpanAttributes['buckwal.outcome'] = 'packed' AND SpanAttributes['buckwal.prefix'] != '' AND SpanAttributes['buckwal.pack.generation'] != '' AND SpanAttributes['buckwal.pack.source'] IN ('kept', 'downloaded') AND SpanAttributes['buckwal.free_page_share'] != ''"
  pack_spans="$OUT"
  span_with buckwal.pack.gauges "SpanAttributes['buckwal.outcome'] = 'ok' AND SpanAttributes['buckwal.prefix'] != '' AND SpanAttributes['buckwal.pack.behind_bytes'] != '' AND SpanAttributes['buckwal.pack.age_seconds'] != '' AND SpanAttributes['buckwal.pack.size_bytes'] != ''"
  gauge_spans="$OUT"
  query "SELECT count() FROM (SELECT SpanAttributes['buckwal.prefix'] AS p, count() AS n $TRACES AND Timestamp > now() - INTERVAL 1 DAY AND SpanName = 'buckwal.pack.gauges' AND SpanAttributes['buckwal.outcome'] = 'ok' GROUP BY p HAVING n >= 3)"
  gauge_prefixes="$OUT"
  span_with buckwal.restore "SpanAttributes['buckwal.prefix'] != '' AND SpanAttributes['buckwal.restore.outcome'] = 'restored' AND SpanAttributes['buckwal.restore.restarts'] != ''"
  restore_spans="$OUT"
  span_with buckwal.prefetch "SpanAttributes['buckwal.prefetch.past_half_keep'] IN ('true', 'false')"
  prefetch_spans="$OUT"
  span_with buckwal.vacuum "SpanAttributes['buckwal.outcome'] = 'vacuumed' AND SpanAttributes['buckwal.vacuum.pages_before'] != '' AND SpanAttributes['buckwal.free_page_share'] != ''"
  vacuum_spans="$OUT"
  span_with buckwal.maintenance "SpanAttributes['buckwal.outcome'] = 'switched' AND SpanAttributes['buckwal.wal_bytes'] != '' AND SpanAttributes['buckwal.gate_wait_ms'] != ''"
  maintenance_spans="$OUT"

  missing=""
  [[ "$switch_spans" -gt 0 ]] || missing="$missing buckwal.generation_switch(outcome=switched,next_generation,elapsed_ms)"
  [[ "$pack_spans" -gt 0 ]] || missing="$missing buckwal.pack(outcome=packed,prefix,pack.generation,pack.source,free_page_share)"
  [[ "$gauge_spans" -gt 0 ]] || missing="$missing buckwal.pack.gauges(outcome=ok,prefix,behind_bytes,age_seconds,size_bytes)"
  [[ "$gauge_prefixes" -gt 0 ]] || missing="$missing buckwal.pack.gauges(3-per-prefix)"
  [[ "$restore_spans" -gt 0 ]] || missing="$missing buckwal.restore(prefix,restore.outcome=restored,restore.restarts)"
  [[ "$prefetch_spans" -gt 0 ]] || missing="$missing buckwal.prefetch(past_half_keep)"
  [[ "$vacuum_spans" -gt 0 ]] || missing="$missing buckwal.vacuum(outcome=vacuumed,pages_before,free_page_share)"
  [[ "$maintenance_spans" -gt 0 ]] || missing="$missing buckwal.maintenance(outcome=switched,wal_bytes,gate_wait_ms)"
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
    echo "otel check PASSED: $counts logs=$logs takeover_quiet_spans=$quiet_spans integrity_records=$integrity replica_sync_spans=$replica_sync readme_queries=$README_QUERIES alert_queries=$ALERTS pack_spans=$pack_spans gauge_spans=$gauge_spans marked_spans=$marked error_spans=$errors"
    exit 0
  fi
  if (( SECONDS - start >= DEADLINE )); then
    fail "run $RUN_ID after ${DEADLINE}s: missing:${missing:- none}; logs=$logs; logs before 2000-01-01=$untimed; spans: ${counts:-none}"
  fi
  sleep 3
done
