#!/usr/bin/env bash
# Usage: scripts/test.sh [unit]
#   (no arg)  fmt, clippy, unit tests, then RustFS-backed ignored tests with telemetry on
#             under one run id, a short workload under it, and the ClickHouse check
#             (scripts/otel-check.sh; SKIPPED when the observability stack is down)
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

scripts/rustfs.sh up
eval "$(scripts/rustfs.sh env)"
export DURABLE_OTEL=1
export DURABLE_TEST_RUN_ID="turso-durable-$(date +%Y%m%d%H%M%S)-$$"
echo "test run id: $DURABLE_TEST_RUN_ID"
cargo test --workspace --locked -- --ignored --test-threads=1

# A short workload under the run id, so every span kind is exported, then the check.
WORK="$(mktemp -d "${TMPDIR:-/tmp}/turso-durable-otel.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
cli() { DURABLE_PREFIX="otel/$DURABLE_TEST_RUN_ID" cargo run -q --locked -p turso-durable-cli -- "$@"; }
cli bench --dir "$WORK/bench" --commits 50 --batching "window 5ms 8 1MiB" --connections 4
cli restore --dir "$WORK/restored"
scripts/otel-check.sh "$DURABLE_TEST_RUN_ID"
