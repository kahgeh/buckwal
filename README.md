# turso-durable

Make a tursodb database durable in an S3 bucket. A Rust library (`turso_durable`) and a CLI (`turso-durable`).

## Use

TODO

## CLI

TODO

## Settings

TODO

## Bucket format

TODO

## S3 features needed

TODO (conditional PUT is required; see `scripts/s3-gate.sh`).

## Limits

TODO

## Test command

- `./scripts/test.sh unit`: fmt, clippy, unit tests.
- `./scripts/test.sh`: the above plus RustFS-backed tests (starts a local RustFS container via `scripts/rustfs.sh`).

## Layout

- `crates/turso_durable`: the library.
- `crates/turso-durable-cli`: the `turso-durable` binary.
- `scripts/`: `test.sh`, `rustfs.sh` (local S3), `s3-gate.sh`.
- `LICENSES/`: third-party licences.

## Provenance

Built on turso_core 0.8.3 (MIT, see `LICENSES/turso-MIT.md`).

## Dependency audits

Always build with `--locked`. Any `Cargo.lock` change needs a re-audit through the rust-supply-chain-auditor.

| # | Date | Scope | Crates and versions | Verdict |
|---|------|-------|---------------------|---------|
| 1 | 2026-10-16 | turso-durable workspace (turso_durable, turso-durable-cli): full resolved `Cargo.lock` (sha256 `ff8bc95a6ecea426e0d43a5db3601c23f1c6d3a0ffc101f83d1d620a5b22539e`) | 261 crates.io packages, no git/path deps (201 compiled on aarch64-apple-darwin). Direct: turso_core =0.8.3 (features `fs`,`json`,`uuid`; vcs 607bff9fd, published by penberg 2026-10-12), turso_parser =0.8.3, reqwest =0.13.2 (`blocking`, no TLS crates in tree), hmac =0.13.0, sha2 =0.11.0, hex =0.4.3, dev tempfile =3.20.0. Notable transitive: aegis 0.9.20 (pure-rust on macOS, so no C build), aristo/aristo-macros 0.4.1 (aretta-ai; macros only validate and pass items through with default features), uuid 1.26.1 + sha1_smol 1.0.1, syn 3.0.6 (via bytemuck_derive 1.12.0), tokio 1.53.1, hyper 1.11.1, crossbeam-epoch 0.9.21. No sqlite/libsql. | APPROVED. cargo-audit (RustSec db 479c888c, 2026-10-16): 0 vulnerabilities, 0 warnings. The earlier known advisories (anyhow 1.0.98, crossbeam-epoch 0.9.18, memmap2, lru, rkyv) are not in this tree. Lock checksums match crates.io; publishers are the expected maintainers or trusted publishing. Build scripts and proc-macros were reviewed: no network, no credential paths, no obfuscation. |

Conditions:
- turso_core 0.8.3 does NOT build with only `["fs"]`. `incremental/dbsp.rs` uses `uuid` without a feature gate, and `vdbe/execute.rs` has a type error in its non-json path. The minimal feature set is `default-features = false, features = ["fs", "json", "uuid"]` (no "encryption"). Verified with `cargo check --offline --locked --workspace --all-targets`. This adds uuid 1.26.1 and sha1_smol 1.0.1, both reviewed and clean.
- Copy the audited `Cargo.lock` with the manifests unchanged, and always build/test with `--locked`. Any lock change (cargo update, new dep or feature) needs a re-audit.
- turso_core's build.rs runs `git rev-parse`/`git show` only to embed a commit hash and timestamp. Set `SOURCE_DATE_EPOCH` for reproducible builds, which also skips the git call.
- Cargo.lock lists cfg-gated crates that normal builds never compile (cfg(antithesis/loom/shuttle): antithesis_sdk 0.3.0, loom, shuttle, generator) plus wasm/windows targets. antithesis_sdk 0.3.0 was not downloaded or reviewed; do not build with `--cfg antithesis`.
- aegis 0.9.20 ships prebuilt wasm static libs (`wasm-libs/*.a`) that are linked only for wasm32. Re-audit before targeting wasm.
