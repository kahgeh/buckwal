# 0004. A new MVCC database starts at generation 2, with the head naming manifest 1

- Status: Accepted
- Date: 2026-10-22
- Spec: `mvcc-durability`
- Decision record in the README: D22

## Context

turso has no open option for MVCC: the database header decides (bytes 18 and 19 are 255). So an MVCC database cannot be made by opening an empty WAL one and switching it through the library's IO hooks, which a crash probe showed is not crash safe (P7). The library builds the empty database itself, and the bucket must then hold an image of it for the first restore.

Every seal moves the head from generation G to G+1 and writes `manifest/G` and the pages of G. The design said a new database starts as "generation 1 as a seal". Built that way, the head would name generation 1 and manifest 1, and the first real seal of generation 1 would write `pages/1/*` and `manifest/1` again, overwriting objects under a live head.

## Decision

A new MVCC database is built in a staging directory (`app.db-creating`) with plain local IO and no wrapper: `PRAGMA journal_mode='mvcc'`, then a `TRUNCATE` checkpoint. Only then does the bucket see anything, and it sees a seal of generation 1: `pages/1/*` and `manifest/1` with plain PUTs first, then the head last, with `If-None-Match`, at generation 2 naming manifest 1 (format 2, epoch 1, `"journal_mode":"mvcc"`). After the head landed, the files move into the database directory, local state last. A crash at any step leaves no head without its manifest and pages, and a staging directory is never opened again: the next open removes it.

Code: `crates/buckwal/src/create.rs`, `bucket.rs` (`create_sealed`), `db.rs` (`create_new_mvcc`). Tests: `create_mvcc_makes_head_manifest_pages_and_local_files` and `create_crash_at_each_step` (`crates/buckwal/src/db/tests.rs`).

## Consequences

- The empty image costs 4 PUTs, and `trim` has nothing to delete in a new bucket.
- The first log records go to `log/2/`, and the first real seal is of generation 2, so it overwrites nothing.
- The first reopen of a created database reports NewGeneration (a header-only local log against an empty image). This is harmless but noisy (`log_only_new_generation_keeps_the_dirty_file`).
- Generation numbers of an MVCC database and a WAL one are not comparable: the first starts at 2.
- A restore of a bucket that was created and never written writes an empty `app.db-log` and opens (`restore_of_created_mvcc_bucket_opens`).
