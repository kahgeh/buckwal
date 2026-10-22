# 0001. The MVCC commit point is the `.db-log` sync, under the confirmed-sync rule

- Status: Accepted
- Date: 2026-10-22
- Spec: `mvcc-durability`
- Decision record in the README: D19

## Context

In a WAL database the commit point is the fsync of `app.db-wal`: the library uploads the bytes written since the last fsync as a log record, and only then lets the fsync happen, so a commit returns `Ok` only when it is in the bucket. An MVCC database writes a commit as one frame to the engine's logical log, `app.db-log`, and syncs that file; the `-wal` carries only checkpoint frames and no commit (`docs/mvcc-recon.md`, Q1, Q2).

The stage-1 probes showed two engine behaviours that a copy of the WAL rule would get wrong. A failed `.db-log` sync does not undo the commit: the engine keeps the frame and chains later frames from it (P3), so dropping the unsent bytes, as the WAL rule does, loses later acked commits. And under group commit one lead writes every queued frame and a follower makes a sync with nothing new (Q6), so a sync with nothing to send must not return `Ok` while anything is unconfirmed. Uploading inside `DurableStorage::on_log_write_complete` was tried and rejected: a failed row came back after reopen in 5 of 24 runs while invisible live (P8d), and it is one PUT per record.

## Decision

The commit point is the `File::sync` of `app.db-log`, through a `LogShippingFile` that records every write as an op. A sync follows the confirmed-sync rule: it returns `Ok` only when every op recorded so far is confirmed in the bucket.

- It settles first: an in-doubt record is settled, and a pending seal whose head is published is adopted, before anything is appended.
- It then uploads the ops not yet confirmed as one kind 3 record, before the local fsync.
- Ops that were never sent are kept, in order, and ship in front of newer ones with the next sync. A sync with nothing new is no early `Ok` while anything is unconfirmed.
- A writer that is fenced fails every sync for good and requeues nothing.

Group commit is the engine's own, so one kind 3 record carries a group, and each engine frame is counted and acked.

Code: `crates/buckwal/src/io/log_file.rs`, `uploader.rs`, `bucket.rs` (`append_log`, `settle_all`). Tests: `sync_ok_only_when_all_ops_confirmed` (`crates/buckwal/tests/mvcc_fencing.rs`), `stats_count_one_commit_per_record_in_group_commit` and `head_stored_reply_lost_next_commit_adopts_and_is_acked` (`crates/buckwal/src/db/tests/mvcc.rs`).

## Consequences

- One upload at a time per database: a slow upload stalls every writer, and while an ordinary commit waits, `BEGIN CONCURRENT` writers fail at once. In MVCC mode every write should use `BEGIN CONCURRENT` (`slow_upload_stalls_writers_and_begin_concurrent_waits`).
- A failed upload keeps the commit in the local database and returns an error: the outcome is unknown, not rolled back. The next sync ships it. It is lost only if the disk is lost, the writer is fenced, or the process stops before anything in its generation is confirmed (`failed_upload_keeps_the_commit_locally_outcome_unknown`).
- Speculative reads: a read inside `BEGIN` or `BEGIN CONCURRENT` can see another connection's commit while its upload is in flight (`speculative_read_inside_begin_sees_unacked_commit`).
- Four promises about batching Off that hold in WAL mode do not hold in MVCC mode, and the README rewrites each where it stands.
- Window batching is refused in MVCC mode, because group commit does the batching.
- No change to turso_core is needed.
