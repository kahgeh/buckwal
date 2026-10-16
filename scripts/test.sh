#!/usr/bin/env bash
# Usage: scripts/test.sh [unit]
#   (no arg)  fmt, clippy, unit tests, then RustFS-backed ignored tests
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
cargo test --workspace --locked -- --ignored --test-threads=1
