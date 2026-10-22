//! The files turso_durable keeps in a database directory, and the error a failed local file
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
/// Present while a restore moves its files into the directory (`restore_staged`).
pub(crate) const RESTORING_FILE: &str = "app.db-restoring";
/// The exclusive lock of the process that has the directory open (D4).
pub(crate) const LOCK_FILE: &str = "durable.lock";

/// A local file error: `Io("<what>: <error>")`.
pub(crate) fn io_err(what: &str, e: impl std::fmt::Display) -> Error {
    Error::Io(format!("{what}: {e}"))
}
