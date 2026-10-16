# turso-durable

Rust library `turso_durable` (crates/turso_durable) plus CLI `turso-durable` (crates/turso-durable-cli) that makes a tursodb database durable in an S3 bucket.

## Tests

- `./scripts/test.sh unit`: fmt, clippy (-D warnings), unit tests. Fast.
- `./scripts/test.sh`: the above, then starts RustFS and runs `--ignored` tests with `--test-threads=1`.
- `./scripts/test.sh` runs the ignored tests with `DURABLE_OTEL=1` and ends with `scripts/otel-check.sh`, the ClickHouse check of that run's telemetry; it prints `otel check SKIPPED: observability stack not running` and passes when the stack is down.
- Always pass `--locked` to cargo. The script sets `SDKROOT` on macOS (newest CLT SDK does not link) and `SOURCE_DATE_EPOCH`.

## RustFS resource rules

- Only create, stop or remove containers and volumes named `turso-durable-*` (use `scripts/rustfs.sh`).
- Never touch `obs-*` or `tursopoc-*` containers or volumes.
- Never stop or reset the podman machine unless it was down before you started and only `turso-durable-*` containers run.
- The observability stack (`obs-*`): only send it OTLP and run read-only SELECTs as its read-only ClickHouse user. Never print or write its password.

## Dependency audit rule

- Before adding, upgrading or fetching any crate, run the `rust-supply-chain-auditor` subagent and wait for approval.
- Cargo.lock is the audited lock (see README, Dependency audits). Any change to it needs a re-audit.
- turso_core is `default-features = false, features = ["fs", "json", "uuid"]`; never enable "encryption".
- Never build with `--cfg antithesis`. Re-audit before targeting wasm.
