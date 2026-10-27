//! The file record (spec outside-change-detect): what buckwal keeps in the local state about
//! `app.db`, its `-wal` and, in MVCC mode, `app.db-log`, to detect a change made outside
//! buckwal. The helper that keeps it correct and checks it at open is added with its rules.

/// A SHA-256, as raw bytes.
pub(crate) type Sha = [u8; 32];

/// The details of the database files at one moment, with the two flags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileRecord {
    /// The size of `app.db`.
    pub db_size: u64,
    /// The modification time of `app.db`, in ns since the Unix epoch.
    pub db_mtime_ns: u64,
    /// The SHA-256 of the first page of `app.db`, as raw bytes.
    pub db_page1_sha256: Sha,
    /// The size of `app.db-wal`.
    pub wal_size: u64,
    /// The SHA-256 of the 32-byte header of `app.db-wal`.
    pub wal_header_sha256: Sha,
    /// MVCC only: the size of `app.db-log` and the SHA-256 of its header.
    pub log: Option<(u64, Sha)>,
    /// True while a write to `app.db` has started and its fsync has not finished.
    pub db_write_open: bool,
    /// True when a clean close wrote the record.
    pub clean_close: bool,
}
