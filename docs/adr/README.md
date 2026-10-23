# Architecture decision records

Each ADR is in Michael Nygard's form (Title, Status, Context, Decision, Consequences), numbered and named `NNNN-<slug>.md`. An ADR keeps its words as written; where older text uses a word the glossary (`../glossary.md`) says not to use, `../glossary.md` lists it under "Not yet consistent".

D1 to D29 stay in the README, Decisions, with their evidence, and the decisions of the earlier stages (`durable-library` stages 1 to 4 and `durable-server`) are not back-filled here. The first four ADRs record the decisions of `mvcc-durability` that most shape the design, ADR 0005 the decisions of `durable-hardening`, and ADRs 0006 and 0007 the two decisions of `read-replicas` that most shape the design; each names its README section or row.

| # | Title | Status | Date |
|---|---|---|---|
| [0001](0001-mvcc-commit-point-is-the-db-log-sync.md) | The MVCC commit point is the `.db-log` sync, under the confirmed-sync rule | Accepted | 2026-10-22 |
| [0002](0002-mvcc-seal-runs-in-the-durable-storage-truncate.md) | The MVCC seal runs in the `DurableStorage` wrapper's `truncate`, before the engine cuts the log | Accepted | 2026-10-22 |
| [0003](0003-bucket-format-v2-for-mvcc-buckets-only.md) | Bucket format v2 for MVCC buckets only | Accepted | 2026-10-22 |
| [0004](0004-new-mvcc-database-starts-at-generation-2.md) | A new MVCC database starts at generation 2, with the head naming manifest 1 | Accepted | 2026-10-22 |
| [0005](0005-takeover-generation-floor-quiet-and-integrity-codes.md) | Takeover generation floor, takeover quiet from both writers, and integrity codes on encrypted buckets | Accepted | 2026-10-23 |
| [0006](0006-a-third-io-mode-for-replicas.md) | A third IO mode for replicas | Accepted | 2026-10-23 |
| [0007](0007-the-notification-listener-reuses-the-server-crates-listener.md) | The notification listener reuses the server crate's listener | Accepted | 2026-10-23 |
