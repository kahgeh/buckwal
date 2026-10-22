# 0002. The MVCC seal runs in the `DurableStorage` wrapper's `truncate`, before the engine cuts the log

- Status: Accepted
- Date: 2026-10-22
- Spec: `mvcc-durability`
- Decision record in the README: D20, D25

## Context

A seal is the end-of-generation upload: seal-start record, pages, manifest, head. In a WAL database it runs when the WAL restarts. An MVCC checkpoint (blocking `TRUNCATE`) runs in this order (`docs/mvcc-recon.md`, Q3): rows as `-wal` frames and a WAL sync, backfill into `app.db` and a DB sync, then the log cut (`storage.truncate`, new salt), the log sync, and last the WAL truncate.

Sealing at the WAL truncate, as WAL mode does, is step 8, after the log is cut. A seal that fails there leaves `-wal` frames and an empty log, and that directory does not open (P5 d k=8: `Corrupt database: WAL has committed frames but logical log header is missing`). Refusing the log's `File::truncate` instead is worse: the engine sets the new salt before the file call, so later acked commits are lost at a crash (P5 b2: 10 rows of 12 expected).

## Decision

The seal runs inside the `truncate` of a `DurableStorage` wrapper (`LogDurableStorage`, given to the engine with `OpenOptions::durable_storage`), and only then does the call delegate to the engine's own `Storage`. A failed seal returns an error before the engine touches the log's salt or offset, so the log and the `-wal` keep their frames and the next checkpoint retries (P8f).

- The `-wal` file never seals in MVCC mode. It ships checkpoint frames as kind 1 records that carry no commit.
- `on_checkpoint_start` adopts a pending seal before any WAL write of the checkpoint, as a backstop for the adopt every commit's sync makes.
- `should_checkpoint` is false: only maintenance (and an explicit checkpoint, which runs as `TRUNCATE`) checkpoints, because an auto-checkpoint's error is swallowed inside a commit (Q6).
- A checkpoint that fails after a successful seal poisons the database. The engine may already hold the new salt while the file keeps the old frames, so writes, checkpoints and seals are refused until the database is reopened, and the open repairs and reconciles. This is wider than the design's "a failed local log truncate": it covers the log truncate, its fsync and the WAL truncate.

Code: `crates/turso_durable/src/io/log_storage.rs` (`truncate`, `on_checkpoint_start`, `on_checkpoint_end`, `poison`), `io/wal_file.rs` (`in_mvcc`), `engine.rs`. Tests: `seal_failure_retry_crash_restore` and `local_truncate_failure_after_seal_poisons_until_reopen` (`crates/turso_durable/src/db/tests/mvcc.rs`).

## Consequences

- A checkpoint stops every transaction, readers included, because the engine's checkpoint lock is held while the seal uploads. A slow upload can fail readers with `Busy` after the 5 s busy timeout (`checkpoint_stops_readers_with_busy_after_timeout_on_slow_seal`).
- A failed seal leaves a whole log and `-wal`, so the generation continues and nothing acked is lost.
- A crash between the log cut and the WAL truncate still leaves `-wal` frames next to an empty log locally. The open-time repair stays (`wal_frames_with_empty_log_is_repaired_and_opens`).
- A poisoned database needs a reopen; there is no way to clear it in place.
- The first commit after each generation change ships the whole local `app.db-log` when the new generation's image must follow the file (whole-log ship).
