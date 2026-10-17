# turso-durable

Rust library `turso_durable` (crates/turso_durable) plus CLI `turso-durable` (crates/turso-durable-cli) that makes a tursodb database durable in an S3 bucket.

## Tests

- `./scripts/test.sh unit`: fmt, clippy (-D warnings), unit tests. Fast.
- `./scripts/test.sh`: the above, then starts RustFS and runs `--ignored` tests with `--test-threads=1` in two passes: plain HTTP (`--skip tls_`), then HTTPS with encryption (all). About 12 min (11:43).
- Pass 2's TLS material and key are made in a `mktemp -d` dir (`turso-durable-tls.*`) and removed on exit; it exports `TURSO_DURABLE_TLS_DIR` and `DURABLE_ENCRYPTION_KEY_FILE`.
- Each pass ends with `turso-durable scan --all` over every object: pass 1 must find `CREATE TABLE`, pass 2 no plaintext needle (the key is never printed). Tests keep their prefixes (`DURABLE_KEEP_PREFIXES=1`) for it.
- `./scripts/test.sh` runs the ignored tests with `DURABLE_OTEL=1` and ends with `scripts/otel-check.sh`, the ClickHouse check of that run's telemetry; it prints `otel check SKIPPED: observability stack not running` and passes when the stack is down.
- `./scripts/test.sh` empties RustFS (`scripts/rustfs.sh reset`) before each pass and on exit, and prints each pass's `peak turso-durable-objects` (about 684 MB and 708 MB). Outside it, each RustFS test deletes its prefix, and `DURABLE_KEEP_FAILED=1` keeps a failed test's prefix (name on stderr).
- Always pass `--locked` to cargo. The script sets `SDKROOT` on macOS (newest CLT SDK does not link) and `SOURCE_DATE_EPOCH`.

## RustFS resource rules

- Only create, stop or remove containers and volumes named `turso-durable-*` (use `scripts/rustfs.sh`).
- Never touch `obs-*` or `tursopoc-*` containers or volumes.
- Never stop or reset the podman machine unless it was down before you started and only `turso-durable-*` containers run.
- The observability stack (`obs-*`): only send it OTLP and run read-only SELECTs as its read-only ClickHouse user. Never print or write its password.

## Dependency audit rule

- Before adding, upgrading or fetching any crate, run the `rust-supply-chain-auditor` subagent and wait for approval.
- Cargo.lock is the audited lock (see README, Dependency audits). Any change to it needs a re-audit.
- turso_core is `default-features = false, features = ["fs", "json", "uuid", "encryption"]` (audit row 3). Never enable reqwest's `rustls`, `default-tls` or `native-tls` features (aws-lc/openssl); rustls is ring only.
- Never build with `--cfg antithesis`. Re-audit before targeting wasm.
