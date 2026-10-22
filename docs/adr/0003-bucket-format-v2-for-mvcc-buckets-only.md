# 0003. Bucket format v2 for MVCC buckets only

- Status: Accepted
- Date: 2026-10-22
- Spec: `mvcc-durability`
- Decision record in the README: D21

## Context

An MVCC database has a second file to ship, `app.db-log`, and the bucket must say which kind of database it holds. A build from before this work knows only record kinds 1 and 2 and a head with `format` 1. If it opened an MVCC bucket it would restore, seal or trim it as a WAL bucket and could lose acked commits. No MVCC bucket existed before this work, so nothing needs to be migrated.

## Decision

An MVCC bucket uses format 2; a WAL bucket stays byte-identical format 1.

- A log record of kind 3 holds `app.db-log` extents. Its body is kind 1's, but its ops apply to `app.db-log`. Restore replays kind 1 records into `app.db-wal` and kind 3 records into `app.db-log`, in seq order, under the same writer-run and fence rules.
- The head carries `"format":2` and a trailing `"journal_mode"` (`mvcc` as written by the library). A format 2 head without `wal` or `mvcc` there is `Corrupt`. The local state `app.db-durable` carries `"format":2` for an MVCC directory.
- A kind 3 record in a format 1 bucket is refused (`MVCC: a .db-log record in a WAL bucket` for a writer, `Corrupt` for a restore).
- A v1 build fails to decode a format 2 head (`head: unsupported format 2`), so it never writes to, seals, restores or trims an MVCC bucket.

Code: `crates/buckwal/src/format/head.rs`, `format/record.rs`, `bucket.rs` (`append_log_at`). Tests: `v1_head_bytes_are_unchanged` (`crates/buckwal/src/format/tests.rs`), `restore_wal_bucket_bytes_identical_to_before` (`crates/buckwal/src/db/tests/mvcc/restore.rs`), `wal_bucket_stays_format_1_under_the_same_operations` (`crates/buckwal/tests/mvcc_fencing.rs`), `kind3_record_is_refused_in_a_format1_bucket` (`crates/buckwal/src/bucket/tests.rs`).

## Consequences

- No migration and no mixed buckets: a bucket is one journal mode for its whole life. Opening a WAL bucket as MVCC, or an MVCC bucket as WAL, is refused before anything is written (`wal_bucket_opened_as_mvcc_is_refused`, `mvcc_bucket_opened_as_wal_is_refused`).
- Existing WAL users see no change in their bucket's bytes.
- Every reader of the bucket (restore, verify, GC, trim, scan) must understand format 2 and kind 3.
- The library does not look at the head to choose a journal mode; only the CLI follows the head (one extra GET).
