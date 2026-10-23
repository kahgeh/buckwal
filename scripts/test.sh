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
#             everything; RustFS's storage is emptied (scripts/rustfs.sh reset) before each
#             pass and again on exit, pass or fail. The volume's peak size is printed per pass.
#             Both passes run the `serve` system tests (crates/buckwal-cli/tests/server.rs):
#             plain HTTP without a token in pass 1, HTTPS with a token in pass 2.
#   unit      fmt, clippy, unit tests only
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
