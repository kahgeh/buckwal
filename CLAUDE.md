# buckwal

Rust library `buckwal` (crates/buckwal) plus CLI `buckwal` (crates/buckwal-cli) that makes a tursodb database durable in an S3 bucket. A database created new in MVCC mode (`--journal-mode mvcc`, `BUCKWAL_JOURNAL_MODE=mvcc`, bucket format v2) is supported with limits (README, MVCC mode). Switching an existing WAL database, Window batching in MVCC and switching back are refused. A second machine can follow the bucket as a read-only replica (`buckwal follow`, `buckwal::open_replica`; replica IO mode, a follower thread woken by bucket event notifications with a slow poll as the safety net; README, Replicas). `buckwal notify-setup` prints the AWS commands for the notification route and runs nothing. Replicas refuse MVCC databases. Architecture: `docs/architecture.md`, decisions: `docs/adr/`. `buckwal serve` (crates/buckwal-server) serves it over tursodb's SQL over HTTP v3 (`serverless/PROTOCOL.md`, turso 0b2bec917); its clients are tursodb's serverless drivers. tursodb's conformance suite and the Go driver passed against it at eb101d9 and again at 10e62fb; the TypeScript and Python drivers also passed their own tests against it, over plain HTTP and over HTTPS with a token (results/serve-clients.txt); every libSQL client was not run (README, Server, Clients run and not run). crates/buckwal-hrana-client is its test-only client (the name is historical).

## Tests

- `./scripts/test.sh unit`: fmt, clippy (-D warnings), unit tests (979 on background-commit, 2026-10-25; 953 on commit-span-truth, 2026-10-24; 946 on error-span-status; 934 on open-skips-prefetch-wait; 933 at 9992398: the server's HTTP and notify tests, the MVCC fencing and limits tests and the replica unit tests included). Fast.
- `./scripts/test.sh`: the above, then starts RustFS and runs `--ignored` tests with `--test-threads=1` in two passes: plain HTTP (`--skip tls_`), then HTTPS with encryption (all). By `--ignored --list` each pass now runs 7 `serve` tests (`mvcc_serve_begin_concurrent` and `serve_background_commit_position_and_open_id` included), 58 system tests (the MVCC ones and the two `background_*` ones included), 21 replica tests (crates/buckwal-cli/tests/replica.rs, `follow` and the posted notifications) and the S3 tests (9, then 13: `s3_list_after_start_after_and_paging` is new). Timing and peaks: 14:34 (874 s) at 9ab3403 on 2026-10-22, before the replica tests; the replica additions are not yet measured. Before the MVCC work: 5 `serve` tests, 38 system tests, about 12 min (baseline at 332b45f).
- The `serve` system tests (crates/buckwal-cli/tests/server.rs, `mvcc_serve_begin_concurrent` included) join both passes: plain HTTP without a token in pass 1, HTTPS with a token in pass 2. `serve` defaults to `127.0.0.1:9295`; tests always use port 0 and read the port from the `serve listening` line.
- Pass 2's TLS material and key are made in a `mktemp -d` dir (`buckwal-tls.*`) and removed on exit; it exports `BUCKWAL_TLS_DIR` and `BUCKWAL_ENCRYPTION_KEY_FILE`.
- Each pass ends with `buckwal scan --all` over every object: pass 1 must find `CREATE TABLE`, pass 2 no plaintext needle (the key is never printed). Tests keep their prefixes (`BUCKWAL_KEEP_PREFIXES=1`) for it.
- `./scripts/test.sh` runs the ignored tests with `BUCKWAL_OTEL=1` and ends with `scripts/otel-check.sh`, the ClickHouse check of that run's telemetry; it prints `otel check SKIPPED: observability stack not running` and passes when the stack is down.
- `./scripts/test.sh` empties RustFS (`scripts/rustfs.sh reset`) before each pass and on exit, and prints each pass's `peak buckwal-objects` (839 MB and 856 MB in the run at 9992398, with the MVCC and replica tests; 722 MB and 764 MB at 9ab3403 before the replica tests; 747 MB and 699 MB in the baseline at 332b45f). Outside it, each RustFS test deletes its prefix, and `BUCKWAL_KEEP_FAILED=1` keeps a failed test's prefix (name on stderr).
- The replica tests post S3-format events to the listener themselves; RustFS delivery is untested. They need no extra container.
- Always pass `--locked` to cargo. The script sets `SDKROOT` on macOS (newest CLT SDK does not link) and `SOURCE_DATE_EPOCH`.

## RustFS resource rules

- Only create, stop or remove containers and volumes named `buckwal-*` (use `scripts/rustfs.sh`).
- Never touch `obs-*` or `tursopoc-*` containers or volumes.
- Never stop or reset the podman machine unless it was down before you started and only `buckwal-*` containers run.
- The observability stack (`obs-*`): only send it OTLP and run read-only SELECTs as its read-only ClickHouse user. Never print or write its password.

## Dependency audit rule

- Before adding, upgrading or fetching any crate, run the `rust-supply-chain-auditor` subagent and wait for approval.
- Cargo.lock is the audited lock (see README, Dependency audits). Any change to it needs a re-audit.
- turso_core is `default-features = false, features = ["fs", "json", "uuid", "encryption", "conn_raw_api"]` (audit row 3; `conn_raw_api` is in-crate and added no crate). The replica follower depends on that non-public API: re-run the probes of `docs/read-replicas-recon.md` on any turso_core upgrade. Never enable reqwest's `rustls`, `default-tls` or `native-tls` features (aws-lc/openssl); rustls is ring only.
- Never build with `--cfg antithesis`. Re-audit before targeting wasm.
