#!/usr/bin/env bash
# Usage: scripts/test.sh [unit]
#   (no arg)  fmt, clippy, unit tests, then the RustFS-backed ignored tests in two passes,
#             with telemetry on under one run id:
#               1. plain HTTP, no encryption (the tls_* tests skipped), then a whole-bucket
#                  scan that must find `CREATE TABLE` (the control);
#               2. HTTPS with encryption (TLS material and key made in a temp directory),
#                  every test, a short workload with a marker row, a whole-bucket scan that
#                  must find no plaintext, and the ClickHouse check (scripts/otel-check.sh;
#                  SKIPPED when the observability stack is down).
#             Tests keep their bucket prefixes (BUCKWAL_KEEP_PREFIXES=1) so the scans see
#             everything. Both passes run the `serve` system tests
#             (crates/buckwal-cli/tests/server.rs): plain HTTP without a token in pass 1,
#             HTTPS with a token in pass 2.
#             The passes run side by side, each split in two: four groups, each with its own
#             RustFS container, volume and port (plain-sys 9290, plain-rest 9291, tls-sys
#             9292, tls-rest 9293; scripts/rustfs.sh, BUCKWAL_RUSTFS_*). `sys` is the
#             `system` test binary of buckwal-cli, `rest` is every other ignored test. The
#             script first checks that the four groups name exactly the tests the two passes
#             would. Each group's output goes to its own log, printed when the group ends.
#             The four containers are removed on exit, pass or fail; the default container
#             (buckwal-rustfs) is removed at the start, because plain-sys needs port 9290.
#             Do not run two full runs at once: the ports are shared.
#   unit      fmt, clippy, unit tests only
# BUCKWAL_TEST_SERIAL=1 ./scripts/test.sh runs the two passes one after the other on the
# default container, the way this script ran before the groups: for a failure that might
# come from load. The container is emptied (rustfs.sh reset) before each pass and on exit.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "$(uname)" == "Darwin" && -z "${SDKROOT:-}" ]]; then
  # The newest Command Line Tools SDK cannot be linked by Xcode's linker.
  export SDKROOT="$(xcode-select -p)/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk"
fi
# Reproducible turso_core build (also skips its git call in build.rs).
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-0}"

cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked

[[ "${1:-}" == "unit" ]] && exit 0

TMP="${TMPDIR:-/tmp}"
TMP="${TMP%/}"
WORK=""
WORK0=""
TLS_DIR=""
SAMPLER=""
PASS=""
PEAK_DONE=""

# The peak of the volume samples taken during pass $1, as one line.
peak_line() {
  local mb
  mb="$(awk -v p="$1" '$1 == p && $2 > m { m = $2 } END { print m + 0 }' "$WORK0/disk")"
  echo "peak buckwal-objects: $mb MB ($1)"
}

peak() {
  peak_line "$1"
  PEAK_DONE="$1"
}

# One sample of the volume's size, tagged with the current pass (none while RustFS is
# stopped or paused).
sample() {
  local mb
  mb="$(podman exec buckwal-rustfs du -sm /data 2>/dev/null | awk '{ print $1 }')" || true
  [[ -n "$mb" ]] && echo "$(cat "$WORK0/pass" 2>/dev/null) $mb" >>"$WORK0/disk"
  return 0
}

# Remove a directory this run made with mktemp: never an empty or relative path, nor one
# outside the temp directory.
remove_dir() {
  [[ "$1" == /* && "$1" == "$TMP"/buckwal-* ]] || { echo "test.sh: not removing $1" >&2; return 0; }
  rm -rf "$1" || echo "test.sh: could not remove $1" >&2
}

# One EXIT trap: stop the sampler, remove the workload's directory, empty RustFS's storage
# in plain mode (after otel-check.sh; telemetry is in ClickHouse, not the bucket), remove
# the TLS material, and only then print the running pass's peak. Every step runs even when
# an earlier one fails or stdout/stderr is a closed pipe (`| tee` after Ctrl-C, `| head`):
# no errexit, SIGPIPE ignored, so a failed write is just a failed write. A failed cleanup is
# reported on stderr and the tests' exit code is kept.
cleanup() {
  local rc=$? pending=""
  if [[ "${BUCKWAL_TEST_SERIAL:-}" != 1 ]]; then cleanup_parallel "$rc"; exit "$rc"; fi
  set +e
  trap '' PIPE
  if [[ -n "$SAMPLER" ]]; then
    kill "$SAMPLER" 2>/dev/null
    wait "$SAMPLER" 2>/dev/null
  fi
  if [[ -n "$PASS" && "$PEAK_DONE" != "$PASS" && -n "$WORK0" ]]; then
    pending="$(peak_line "$PASS" 2>/dev/null)"
  fi
  [[ -n "$WORK" ]] && remove_dir "$WORK"
  unset BUCKWAL_TLS_DIR BUCKWAL_ENCRYPTION_KEY_FILE
  scripts/rustfs.sh reset || echo "test.sh: emptying RustFS storage (rustfs.sh reset) failed" >&2
  [[ -n "$TLS_DIR" ]] && remove_dir "$TLS_DIR"
  [[ -n "$WORK0" ]] && remove_dir "$WORK0"
  [[ -n "$pending" ]] && echo "$pending"
  exit "$rc"
}

serial_main() {
  trap cleanup EXIT
  WORK0="$(mktemp -d "$TMP/buckwal-test.XXXXXX")"
  TLS_DIR="$(mktemp -d "$TMP/buckwal-tls.XXXXXX")"
  : >"$WORK0/disk"
  # Every 15 s, the volume's size in MB, tagged with the pass.
  (while sleep 15; do sample; done) &
  SAMPLER=$!

  # Only this run exports; the run id marks every record `deployment.environment.name = test`.
  export BUCKWAL_OTEL=1
  export BUCKWAL_TEST_RUN_ID="buckwal-$(date +%Y%m%d%H%M%S)-$$"
  echo "test run id: $BUCKWAL_TEST_RUN_ID"
  cli() { BUCKWAL_PREFIX="otel/$BUCKWAL_TEST_RUN_ID" cargo run -q --locked -p buckwal-cli -- "$@"; }

  # Pass 1: plain HTTP, no encryption.
  PASS=plain
  echo "$PASS" >"$WORK0/pass"
  scripts/rustfs.sh reset
  eval "$(scripts/rustfs.sh env)"
  BUCKWAL_KEEP_PREFIXES=1 cargo test --workspace --locked -- --ignored --test-threads=1 --skip tls_
  sample
  cli scan --all --expect present
  peak "$PASS"

  # Pass 2: HTTPS with encryption. rustfs.sh (also run by the system tests) serves TLS while
  # BUCKWAL_TLS_DIR is set.
  PASS=https+encryption
  echo "$PASS" >"$WORK0/pass"
  scripts/tls-material.sh "$TLS_DIR/m"
  export BUCKWAL_TLS_DIR="$TLS_DIR/m" BUCKWAL_ENCRYPTION_KEY_FILE="$TLS_DIR/m/encryption.key"
  scripts/rustfs.sh reset
  eval "$(scripts/rustfs.sh env)"
  BUCKWAL_KEEP_PREFIXES=1 cargo test --workspace --locked -- --ignored --test-threads=1

  # A short workload under the run id, so every span kind is exported, with a marker row the
  # scan must not find.
  WORK="$(mktemp -d "$TMP/buckwal-otel.XXXXXX")"
  MARKER="buckwal-marker-$BUCKWAL_TEST_RUN_ID"
  cli bench --dir "$WORK/bench" --commits 50 --batching "window 5ms 8 1MiB" --connections 4
  cli sql --dir "$WORK/bench" "CREATE TABLE marker(t TEXT)"
  cli sql --dir "$WORK/bench" "INSERT INTO marker VALUES ('$MARKER')"
  cli sql --dir "$WORK/bench" "PRAGMA wal_checkpoint(TRUNCATE)"
  cli restore --dir "$WORK/restored"
  cli takeover --dir "$WORK/took"
  cli gc
  sample
  cli scan --all --expect absent --marker "$MARKER"
  peak "$PASS"
  scripts/otel-check.sh "$BUCKWAL_TEST_RUN_ID"
}

# ---------------------------------------------------------------------------------------
# Parallel run: four groups, each with its own RustFS container, volume and port.
# ---------------------------------------------------------------------------------------
GROUPS_ALL=(plain-sys plain-rest tls-sys tls-rest)
group_port() { case "$1" in plain-sys) echo 9290 ;; plain-rest) echo 9291 ;; tls-sys) echo 9292 ;; tls-rest) echo 9293 ;; esac; }
group_name() { echo "buckwal-rustfs-$1"; }
group_volume() { echo "buckwal-objects-$1"; }
# rustfs.sh for group $1: scripts/rustfs.sh "$1" with the group's container, volume and port.
rustfs_of() {
  local g="$1"; shift
  BUCKWAL_RUSTFS_NAME="$(group_name "$g")" BUCKWAL_RUSTFS_VOLUME="$(group_volume "$g")" \
    BUCKWAL_RUSTFS_PORT="$(group_port "$g")" scripts/rustfs.sh "$@"
}

# The cargo commands of a selection, with the test arguments after `--` given in "$@".
#   all: every test target of the workspace (today's pass, as one command)
#   sys: the `system` binary of buckwal-cli
#   rest: every other test target
cargo_sel() {
  local sel="$1"; shift
  case "$sel" in
    all) cargo test --workspace --locked -- "$@" ;;
    sys) cargo test --locked -p buckwal-cli --test system -- "$@" ;;
    rest)
      local rc=0
      cargo test --locked -p buckwal -p buckwal-server -p buckwal-hrana-client -- "$@" || rc=$?
      cargo test --locked -p buckwal-cli --bin buckwal --test replica --test server -- "$@" || rc=$?
      return "$rc"
      ;;
  esac
}

# "<test binary>::<test name>" for every ignored test of a selection, one per line, sorted.
# $1: the selection; the rest: extra libtest arguments (--skip tls_).
list_tests() {
  local sel="$1"; shift
  cargo_sel "$sel" --ignored --list "$@" 2>&1 | awk '
    /^ *Doc-tests / { b = "doc-" $2; next }
    /^ *Running / { b = $0; sub(/^ *Running .*\/deps\//, "", b); sub(/\)$/, "", b); sub(/-[0-9a-f]+$/, "", b); next }
    /: test$/ { print b "::" $0 }' | sort
}

# Fail unless the four groups name, together, the tests the two serial passes name.
check_test_lists() {
  local d="$WORK0/lists"
  mkdir -p "$d"
  { list_tests all --skip tls_; list_tests all; } | sort >"$d/expected"
  { list_tests sys --skip tls_; list_tests rest --skip tls_; list_tests sys; list_tests rest; } | sort >"$d/actual"
  [[ -s "$d/expected" ]] || { echo "test.sh: listing the ignored tests found none" >&2; return 1; }
  if ! diff "$d/expected" "$d/actual" >"$d/diff"; then
    echo "test.sh: the four groups do not name the tests the two passes name:" >&2
    cat "$d/diff" >&2
    return 1
  fi
  echo "test lists: $(wc -l <"$d/expected" | tr -d ' ') tests over the two passes, $(list_tests sys | wc -l | tr -d ' ') + $(list_tests rest | wc -l | tr -d ' ') in a pass's two groups"
}

# One sample of each pass's volume size: the sum over its two containers, in MB.
sample_parallel() {
  local p g mb total
  for p in plain tls; do
    total=0
    for g in "$p-sys" "$p-rest"; do
      mb="$(podman exec "$(group_name "$g")" du -sm /data 2>/dev/null | awk '{ print $1 }')" || true
      # A paused or stopped container does not answer: no sample of this pass this time.
      if [[ -z "$mb" ]]; then total=-1; break; fi
      total=$((total + mb))
    done
    [[ "$total" -ge 0 ]] && echo "$p $total" >>"$WORK0/disk"
  done
  return 0
}

# Start group $1 in the background: its own container, then its tests; its output is its log.
start_group() {
  local g="$1" log="$WORK0/$g.log"
  (
    set -euo pipefail
    export BUCKWAL_RUSTFS_NAME="$(group_name "$g")" BUCKWAL_RUSTFS_VOLUME="$(group_volume "$g")" \
      BUCKWAL_RUSTFS_PORT="$(group_port "$g")"
    case "$g" in
      tls-*) export BUCKWAL_TLS_DIR="$TLS_DIR/m" BUCKWAL_ENCRYPTION_KEY_FILE="$TLS_DIR/m/encryption.key" ;;
    esac
    scripts/rustfs.sh reset
    eval "$(scripts/rustfs.sh env)"
    export BUCKWAL_KEEP_PREFIXES=1
    start=$SECONDS
    case "$g" in
      plain-sys) cargo_sel sys --ignored --test-threads=1 --skip tls_ ;;
      plain-rest) cargo_sel rest --ignored --test-threads=1 --skip tls_ ;;
      tls-sys) cargo_sel sys --ignored --test-threads=1 ;;
      tls-rest) cargo_sel rest --ignored --test-threads=1 ;;
    esac
    echo "group $g: tests finished in $((SECONDS - start)) s"
  ) >"$log" 2>&1 </dev/null &
  GROUP_PIDS+=("$!")
}

cleanup_parallel() {
  local rc="$1" g
  set +e
  trap '' PIPE
  [[ -n "$SAMPLER" ]] && { kill "$SAMPLER" 2>/dev/null; wait "$SAMPLER" 2>/dev/null; }
  # A group still running (Ctrl-C): stop it before its container goes.
  local pid
  # Each group is its own process group (set -m below), so its cargo, test binaries and
  # `buckwal` children all stop with it.
  for pid in ${GROUP_PIDS[@]+"${GROUP_PIDS[@]}"}; do kill -TERM -- "-$pid" 2>/dev/null; done
  wait 2>/dev/null
  for g in "${GROUPS_ALL[@]}"; do
    rustfs_of "$g" down || echo "test.sh: removing $(group_name "$g") failed" >&2
  done
  [[ -n "$WORK" ]] && remove_dir "$WORK"
  [[ -n "$TLS_DIR" ]] && remove_dir "$TLS_DIR"
  [[ -n "$WORK0" ]] && remove_dir "$WORK0"
  echo "full run: $SECONDS s in all (exit $rc)"
}

parallel_main() {
  GROUP_PIDS=()
  trap 'cleanup' EXIT
  WORK0="$(mktemp -d "$TMP/buckwal-test.XXXXXX")"
  TLS_DIR="$(mktemp -d "$TMP/buckwal-tls.XXXXXX")"
  : >"$WORK0/disk"

  export BUCKWAL_OTEL=1
  export BUCKWAL_TEST_RUN_ID="buckwal-$(date +%Y%m%d%H%M%S)-$$"
  echo "test run id: $BUCKWAL_TEST_RUN_ID"
  cli() { BUCKWAL_PREFIX="otel/$BUCKWAL_TEST_RUN_ID" cargo run -q --locked -p buckwal-cli -- "$@"; }

  # Build every test binary once, so the four groups only run them.
  cargo test --workspace --locked --no-run
  check_test_lists

  # plain-sys takes the default container's port.
  scripts/rustfs.sh down
  scripts/tls-material.sh "$TLS_DIR/m"
  (while sleep 15; do sample_parallel; done) &
  SAMPLER=$!

  local g
  set -m
  for g in "${GROUPS_ALL[@]}"; do start_group "$g"; done
  set +m
  local rc=0 i=0 status
  for g in "${GROUPS_ALL[@]}"; do
    status=0
    wait "${GROUP_PIDS[$i]}" || status=$?
    i=$((i + 1))
    local passed
    passed="$(awk '/^test result:/ { n += $4 } END { print n + 0 }' "$WORK0/$g.log")"
    echo "===== group $g: exit $status, $passed tests passed ====="
    sed "s/^/[$g] /" "$WORK0/$g.log"
    [[ "$status" -ne 0 && "$rc" -eq 0 ]] && rc="$status"
  done
  sample_parallel
  [[ "$rc" -ne 0 ]] && exit "$rc"

  # Pass 1's scan: `CREATE TABLE` must be in the bucket of at least one of its groups (a
  # bucket that holds none of the tests that make tables fails its own scan).
  local found=0
  for g in plain-sys plain-rest; do
    (
      export BUCKWAL_RUSTFS_NAME="$(group_name "$g")" BUCKWAL_RUSTFS_PORT="$(group_port "$g")"
      eval "$(scripts/rustfs.sh env)"
      cli scan --all --expect present
    ) && found=1
  done
  [[ "$found" -eq 1 ]] || { echo "test.sh: no plain bucket holds CREATE TABLE" >&2; exit 1; }
  peak_pass plain

  # Pass 2's workload (on tls-rest's container) and its scans over both containers.
  export BUCKWAL_TLS_DIR="$TLS_DIR/m" BUCKWAL_ENCRYPTION_KEY_FILE="$TLS_DIR/m/encryption.key"
  export BUCKWAL_RUSTFS_NAME="$(group_name tls-rest)" BUCKWAL_RUSTFS_VOLUME="$(group_volume tls-rest)" \
    BUCKWAL_RUSTFS_PORT="$(group_port tls-rest)"
  eval "$(scripts/rustfs.sh env)"
  WORK="$(mktemp -d "$TMP/buckwal-otel.XXXXXX")"
  local MARKER="buckwal-marker-$BUCKWAL_TEST_RUN_ID"
  cli bench --dir "$WORK/bench" --commits 50 --batching "window 5ms 8 1MiB" --connections 4
  cli sql --dir "$WORK/bench" "CREATE TABLE marker(t TEXT)"
  cli sql --dir "$WORK/bench" "INSERT INTO marker VALUES ('$MARKER')"
  cli sql --dir "$WORK/bench" "PRAGMA wal_checkpoint(TRUNCATE)"
  cli restore --dir "$WORK/restored"
  cli takeover --dir "$WORK/took"
  cli gc
  sample_parallel
  for g in tls-sys tls-rest; do
    (
      export BUCKWAL_RUSTFS_NAME="$(group_name "$g")" BUCKWAL_RUSTFS_PORT="$(group_port "$g")"
      eval "$(scripts/rustfs.sh env)"
      cli scan --all --expect absent --marker "$MARKER"
    )
  done
  peak_pass tls
  scripts/otel-check.sh "$BUCKWAL_TEST_RUN_ID"
}

# The peak of the sampled sums of pass $1, as one line.
peak_pass() {
  local mb
  mb="$(awk -v p="$1" '$1 == p && $2 > m { m = $2 } END { print m + 0 }' "$WORK0/disk")"
  echo "peak buckwal-objects: $mb MB ($1, its two volumes together)"
}

if [[ "${BUCKWAL_TEST_SERIAL:-}" == 1 ]]; then
  serial_main
else
  parallel_main
fi
