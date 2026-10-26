#!/usr/bin/env bash
# Usage: scripts/test.sh [unit]
#   (no arg)  fmt, clippy, unit tests, then the RustFS-backed ignored tests in two passes,
#             with telemetry on under one run id:
#               1. plain HTTP, no encryption (the tls_* tests skipped), then a whole-bucket
#                  scan that must find `CREATE TABLE` (the control);
#               2. HTTPS with encryption (TLS material and key made in a temp directory),
#                  every test, a short workload with a marker row and a `buckwal pack`, a
#                  whole-bucket scan that must find no plaintext (packs included), and the
#                  ClickHouse check (scripts/otel-check.sh; SKIPPED when the observability
#                  stack is down).
#             Tests keep their bucket prefixes (BUCKWAL_KEEP_PREFIXES=1) so the scans see
#             everything. Both passes run the `serve` system tests
#             (crates/buckwal-cli/tests/server.rs): plain HTTP without a token in pass 1,
#             HTTPS with a token in pass 2.
#             The passes run side by side, each split in two: four groups, each with its own
#             RustFS container, volume and port (scripts/rustfs.sh, BUCKWAL_RUSTFS_*). `sys` is the
#             `system` test binary of buckwal-cli, `rest` is every other ignored test. The
#             script first checks that the four groups name exactly the tests the two passes
#             would. Each group's output goes to its own log, printed when the group ends.
#             The four containers are removed on exit, pass or fail.
#             Run slots: a full run first takes a free run slot, a directory that `mkdir`
#             makes under the repository's shared git directory (buckwal-test-slots/<N>, the
#             same for every worktree), with the run's pid and run id in it. Slot N owns the
#             containers buckwal-rustfs-s<N>-<group>, the volumes buckwal-objects-s<N>-<group> and
#             the ports 9300 + 10N to 9303 + 10N (serial mode: buckwal-rustfs-s<N>, port
#             9304 + 10N). BUCKWAL_TEST_SLOTS (default 2) sets how many slots exist; with all
#             taken, a run prints which runs hold them and waits. A slot whose pid is gone
#             is stale: the next run removes that slot's containers and takes it. The default
#             container (buckwal-rustfs) is never touched. SIGINT, SIGTERM and SIGHUP stop the
#             run, clean up and exit 128 + the signal number.
#             Before the tests, the CLI of the build before format 5 (RELEASED below) is
#             built once in a temporary git worktree (--locked --offline) and kept under the
#             shared git directory (buckwal-released/<commit>/buckwal); the upgrade system test
#             makes its format 1 and format 4 buckets with it (BUCKWAL_TEST_RELEASED_BIN).
#   unit      fmt, clippy, unit tests only
# BUCKWAL_TEST_SERIAL=1 ./scripts/test.sh runs the two passes one after the other on the
# run slot's single container, the way this script ran before the groups: for a failure that might
# come from load. The container is emptied (rustfs.sh reset) before each pass and removed on
# exit. Its cargo runs in the foreground, so a SIGTERM takes effect when the running pass
# ends (Ctrl-C reaches cargo too and is immediate).
set -euo pipefail
cd "$(dirname "$0")/.."

# The number of run slots, checked before anything runs.
case "${BUCKWAL_TEST_SLOTS:-2}" in
  [1-9] | [1-9][0-9]) ;;
  *) echo "test.sh: BUCKWAL_TEST_SLOTS must be a number from 1 to 99, not '${BUCKWAL_TEST_SLOTS}'" >&2; exit 2 ;;
esac

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
RUN_ID="buckwal-$(date +%Y%m%d%H%M%S)-$$"
SLOTS="${BUCKWAL_TEST_SLOTS:-2}"
SLOT=""
SLOT_ROOT="$(cd "$(git rev-parse --git-common-dir)" && pwd)/buckwal-test-slots"

# The build before format 5 (packs) that `upgrade_format_1_and_4_buckets` makes its bucket
# with: the CLI at RELEASED, built once with --locked --offline in a temporary git worktree,
# and kept under the shared git directory (buckwal-released/<commit>/buckwal) for later runs
# and worktrees. Exports BUCKWAL_TEST_RELEASED_BIN.
RELEASED=3ac8ff3
RELEASED_TREE=""
released_build() {
  local root bin tree
  root="$(cd "$(git rev-parse --git-common-dir)" && pwd)/buckwal-released/$RELEASED"
  bin="$root/buckwal"
  if [[ ! -x "$bin" ]]; then
    echo "test.sh: building the released CLI at $RELEASED (once; kept in $root)"
    tree="$(mktemp -d "$TMP/buckwal-released.XXXXXX")"
    RELEASED_TREE="$tree"
    git worktree add --quiet --detach "$tree/src" "$RELEASED"
    (cd "$tree/src" && CARGO_TARGET_DIR="$tree/target" cargo build --quiet --locked --offline -p buckwal-cli)
    mkdir -p "$root"
    # A name of this run only: two runs that build at once each move a whole file.
    cp "$tree/target/debug/buckwal" "$bin.new.$$"
    mv "$bin.new.$$" "$bin"
    released_tree_remove
  fi
  export BUCKWAL_TEST_RELEASED_BIN="$bin"
  echo "released CLI ($RELEASED): $bin"
}

# Remove the temporary worktree of `released_build`, if one is left.
released_tree_remove() {
  [[ -n "$RELEASED_TREE" ]] || return 0
  git worktree remove --force "$RELEASED_TREE/src" 2>/dev/null || git worktree prune
  remove_dir "$RELEASED_TREE"
  RELEASED_TREE=""
}

# Remove every container buckwal-rustfs-s<n>[-group] and volume buckwal-objects-s<n>[-group]:
# the names of run slot $1, and no others (the patterns are anchored, so s1 never matches s10).
slot_remove_containers() {
  local n="$1" c v
  for c in $(podman ps -a --format '{{.Names}}' 2>/dev/null | grep -E "^buckwal-rustfs-s${n}(-|\$)" || true); do
    podman rm -f "$c" >/dev/null 2>&1 || echo "test.sh: could not remove container $c" >&2
  done
  for v in $(podman volume ls --format '{{.Name}}' 2>/dev/null | grep -E "^buckwal-objects-s${n}(-|\$)" || true); do
    podman volume rm -f "$v" >/dev/null 2>&1 || echo "test.sh: could not remove volume $v" >&2
  done
}

# Take the first free run slot, waiting while all are taken. A slot whose pid no longer runs
# is stale: it is renamed away (one run wins the rename), its containers are removed, and the
# slot is taken.
take_slot() {
  mkdir -p "$SLOT_ROOT"
  local n d pid ids waited=60
  while :; do
    for ((n = 1; n <= SLOTS; n++)); do
      d="$SLOT_ROOT/$n"
      if ! mkdir "$d" 2>/dev/null; then
        pid="$(cat "$d/pid" 2>/dev/null || true)"
        [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null || continue
        mv "$d" "$SLOT_ROOT/$n.stale.$$" 2>/dev/null || continue
        if [[ "$(cat "$SLOT_ROOT/$n.stale.$$/pid" 2>/dev/null || true)" != "$pid" ]]; then
          # Another run took the slot between the check and the move: give it back.
          mv "$SLOT_ROOT/$n.stale.$$" "$d" 2>/dev/null || true
          continue
        fi
        # The dead run's groups may still run (SIGKILL reaches only the script): stop them
        # before their containers go, or their tests would stop and pause the next run's.
        for g in $(cat "$SLOT_ROOT/$n.stale.$$/groups" 2>/dev/null || true); do
          kill -TERM -- "-$g" 2>/dev/null || true
        done
        echo "test.sh: run slot $n is stale (pid $pid is gone); removing its containers"
        slot_remove_containers "$n"
        rm -rf "$SLOT_ROOT/$n.stale.$$"
        mkdir "$d" 2>/dev/null || continue
      fi
      SLOT="$n"
      echo "$$" >"$d/pid"
      echo "$RUN_ID" >"$d/run"
      echo "run slot $n of $SLOTS"
      return 0
    done
    if [[ "$waited" -ge 60 ]]; then
      ids="$(cat "$SLOT_ROOT"/*/run 2>/dev/null | paste -sd, - | sed 's/,/, /g' || true)"
      echo "test.sh: all $SLOTS run slots are taken (${ids:-no run id yet}); waiting"
      waited=0
    fi
    sleep 5 &
    wait $! || true
    waited=$((waited + 5))
  done
}

# Stop the run when a port of its slot is already in use (before any test starts).
check_ports() {
  local p
  for p in "$@"; do
    if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "test.sh: port $p of run slot $SLOT is in use; free it first" >&2
      exit 1
    fi
  done
}

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
  mb="$(podman exec "$BUCKWAL_RUSTFS_NAME" du -sm /data 2>/dev/null | awk '{ print $1 }')" || true
  [[ -n "$mb" ]] && echo "$(cat "$WORK0/pass" 2>/dev/null) $mb" >>"$WORK0/disk"
  return 0
}

# Remove a directory this run made with mktemp: never an empty or relative path, nor one
# outside the temp directory.
remove_dir() {
  [[ "$1" == /* && "$1" == "$TMP"/buckwal-* ]] || { echo "test.sh: not removing $1" >&2; return 0; }
  rm -rf "$1" || echo "test.sh: could not remove $1" >&2
}

# One EXIT trap: stop the sampler, remove the workload's directory, remove the run slot's
# containers and volumes (after otel-check.sh; telemetry is in ClickHouse, not the bucket),
# remove the TLS material, free the slot, and only then print the running pass's peak. Every step runs even when
# an earlier one fails or stdout/stderr is a closed pipe (`| tee` after Ctrl-C, `| head`):
# no errexit, SIGPIPE ignored, so a failed write is just a failed write. A failed cleanup is
# reported on stderr and the tests' exit code is kept.
cleanup() {
  local rc=$? pending=""
  trap '' INT TERM HUP
  if [[ "${BUCKWAL_TEST_SERIAL:-}" != 1 ]]; then cleanup_parallel "$rc"; exit "$rc"; fi
  set +e
  trap '' PIPE
  released_tree_remove
  if [[ -n "$SAMPLER" ]]; then
    kill "$SAMPLER" 2>/dev/null
    wait "$SAMPLER" 2>/dev/null
  fi
  if [[ -n "$PASS" && "$PEAK_DONE" != "$PASS" && -n "$WORK0" ]]; then
    pending="$(peak_line "$PASS" 2>/dev/null)"
  fi
  [[ -n "$WORK" ]] && remove_dir "$WORK"
  unset BUCKWAL_TLS_DIR BUCKWAL_ENCRYPTION_KEY_FILE
  [[ -n "$SLOT" ]] && slot_remove_containers "$SLOT"
  [[ -n "$TLS_DIR" ]] && remove_dir "$TLS_DIR"
  [[ -n "$WORK0" ]] && remove_dir "$WORK0"
  [[ -n "$pending" ]] && echo "$pending"
  [[ -n "$SLOT" ]] && rm -rf "${SLOT_ROOT:?}/$SLOT"
  echo "full run: $SECONDS s in all (exit $rc)"
  exit "$rc"
}

serial_main() {
  released_build
  export BUCKWAL_RUSTFS_NAME="buckwal-rustfs-s$SLOT" BUCKWAL_RUSTFS_VOLUME="buckwal-objects-s$SLOT" \
    BUCKWAL_RUSTFS_PORT="$((9304 + 10 * SLOT))"
  check_ports "$BUCKWAL_RUSTFS_PORT"
  WORK0="$(mktemp -d "$TMP/buckwal-test.XXXXXX")"
  TLS_DIR="$(mktemp -d "$TMP/buckwal-tls.XXXXXX")"
  : >"$WORK0/disk"
  # Every 15 s, the volume's size in MB, tagged with the pass.
  (while kill -0 $$ 2>/dev/null && sleep 15; do sample; done) &
  SAMPLER=$!

  # Only this run exports; the run id marks every record `deployment.environment.name = test`.
  export BUCKWAL_OTEL=1
  export BUCKWAL_TEST_RUN_ID="$RUN_ID"
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
  # The marker row's generation ended: a pack run puts it into a pack, which the scan reads.
  cli pack --dir "$WORK/packer"
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
# Run slot N's ports are 9300 + 10N to 9303 + 10N (serial mode: 9304 + 10N).
group_port() {
  case "$1" in
    plain-sys) echo $((9300 + 10 * SLOT)) ;; plain-rest) echo $((9301 + 10 * SLOT)) ;;
    tls-sys) echo $((9302 + 10 * SLOT)) ;; tls-rest) echo $((9303 + 10 * SLOT)) ;;
  esac
}
group_name() { echo "buckwal-rustfs-s$SLOT-$1"; }
group_volume() { echo "buckwal-objects-s$SLOT-$1"; }
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
      cargo test --locked -p buckwal-cli --bin buckwal --test replica --test server --test telemetry -- "$@" || rc=$?
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
  released_tree_remove
  [[ -n "$SAMPLER" ]] && { kill "$SAMPLER" 2>/dev/null; wait "$SAMPLER" 2>/dev/null; }
  # A group still running (Ctrl-C): stop it before its container goes.
  local pid
  # Each group is its own process group (set -m below), so its cargo, test binaries and
  # `buckwal` children all stop with it.
  for pid in ${GROUP_PIDS[@]+"${GROUP_PIDS[@]}"}; do kill -TERM -- "-$pid" 2>/dev/null; done
  wait 2>/dev/null
  [[ -n "$SLOT" ]] && slot_remove_containers "$SLOT"
  [[ -n "$WORK" ]] && remove_dir "$WORK"
  [[ -n "$TLS_DIR" ]] && remove_dir "$TLS_DIR"
  [[ -n "$WORK0" ]] && remove_dir "$WORK0"
  [[ -n "$SLOT" ]] && rm -rf "${SLOT_ROOT:?}/$SLOT"
  echo "full run: $SECONDS s in all (exit $rc)"
}

parallel_main() {
  GROUP_PIDS=()
  WORK0="$(mktemp -d "$TMP/buckwal-test.XXXXXX")"
  TLS_DIR="$(mktemp -d "$TMP/buckwal-tls.XXXXXX")"
  : >"$WORK0/disk"

  export BUCKWAL_OTEL=1
  export BUCKWAL_TEST_RUN_ID="$RUN_ID"
  echo "test run id: $BUCKWAL_TEST_RUN_ID"
  cli() { BUCKWAL_PREFIX="otel/$BUCKWAL_TEST_RUN_ID" cargo run -q --locked -p buckwal-cli -- "$@"; }

  # Build every test binary once, so the four groups only run them.
  cargo test --workspace --locked --no-run
  released_build
  check_test_lists

  check_ports "$(group_port plain-sys)" "$(group_port plain-rest)" "$(group_port tls-sys)" "$(group_port tls-rest)"
  scripts/tls-material.sh "$TLS_DIR/m"
  (while kill -0 $$ 2>/dev/null && sleep 15; do sample_parallel; done) &
  SAMPLER=$!

  local g
  set -m
  for g in "${GROUPS_ALL[@]}"; do start_group "$g"; done
  set +m
  echo "${GROUP_PIDS[*]}" >"$SLOT_ROOT/$SLOT/groups"
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
  # The groups have ended: their pids may be used again, so a stale takeover must not kill them.
  rm -f "$SLOT_ROOT/$SLOT/groups"
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
  # The marker row's generation ended: a pack run puts it into a pack, which the scan reads.
  cli pack --dir "$WORK/packer"
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

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
take_slot
if [[ "${BUCKWAL_TEST_SERIAL:-}" == 1 ]]; then
  serial_main
else
  parallel_main
fi
