//! The files buckwal keeps in a database directory, and the error a failed local file
//! operation becomes. Every module names these files through here.
use crate::error::Error;

/// The database file.
pub(crate) const DB_FILE: &str = "app.db";
/// The engine's WAL. The engine itself opens `format!("{db_path}-wal")` (database.rs:1495),
/// which is this name next to [`DB_FILE`].
pub(crate) const WAL_FILE: &str = "app.db-wal";
/// The MVCC engine's logical log (`format!("{db_path}-log")`), next to [`DB_FILE`].
pub(crate) const LOG_FILE: &str = "app.db-log";
/// The local state (`LocalState`, flat JSON).
pub(crate) const STATE_FILE: &str = "app.db-durable";
/// Page numbers checkpointed since the last seal (u32 LE each).
pub(crate) const DIRTY_FILE: &str = "app.db-dirty";
/// The pack a restored copy reads its blocks from, while some are not local yet: the pack
/// pointer's bytes (a u32 LE length, then the bytes), then the pack codes object's bytes.
pub(crate) const PACK_FILE: &str = "app.db-pack";
/// Which blocks of the pack are local: the limit (u64 LE: bytes at or above it are never
/// fetched), then one byte per block (1 = local).
pub(crate) const BLOCKS_FILE: &str = "app.db-blocks";
/// Present while a restore moves its files into the directory (`restore_staged`).
pub(crate) const RESTORING_FILE: &str = "app.db-restoring";
/// A read replica's position file (generation, stage, offset, replica id).
pub(crate) const REPLICA_FILE: &str = "app.db-replica";
/// Names the sibling directory a replica's current copy lives in, once a re-restore has
/// moved it there (`replica::open`). Absent: the copy is in the directory itself.
pub(crate) const REPLICA_AT_FILE: &str = "app.db-replica-at";
/// When a replica's follower last finished a pass (unix milliseconds, text); how far behind a
/// restarted replica is when it must restore again.
pub(crate) const REPLICA_SEEN_FILE: &str = "app.db-replica-seen";
/// The exclusive lock of the process that has the directory open (D4).
pub(crate) const LOCK_FILE: &str = "durable.lock";

/// A local file error: `Io("<what>: <error>")`.
pub(crate) fn io_err(what: &str, e: impl std::fmt::Display) -> Error {
    Error::Io(format!("{what}: {e}"))
}
