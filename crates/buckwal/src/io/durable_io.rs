// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! The engine's IO: the platform IO with exactly one file wrapped, `<dir>/app.db-wal`. It
//! keeps the default "no shared-WAL coordination", so the engine coordinates in-process.
//!
//! The MVCC log, `app.db-log`, is never opened here. In an MVCC database the engine does not
//! open it (it is given `LogDurableStorage`, storage/journal_mode.rs:85-100), whose one
//! `LogShippingFile` is the only handle on it: a second wrapper would keep its own ops and
//! break the confirmed-sync rule. So every open of a `.db-log` is refused, and so is any
//! path that resolves to the wrapped log (another spelling, a link).
use super::db_storage::BucketDbStorage;
use super::log_storage::LogDurableStorage;
use super::stash::stash;
use super::wal_file::ShippingFile;
use crate::error::Error;
use crate::uploader::Uploader;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use turso_core::io::FileId;
use turso_core::{
    Clock, Completion, File, MemoryIO, MonotonicInstant, OpenFlags, WallClockInstant, IO,
};

pub(crate) struct DurableIo {
    inner: Arc<dyn IO>,
    /// Exactly the string the engine opens: `format!("{db_path}-wal")`.
    wal_path: String,
    uploader: Arc<Uploader>,
    storage: Arc<BucketDbStorage>,
    /// The `-wal`'s high-water mark (`ShippingFile::high_water`).
    wal_bytes: Arc<AtomicU64>,
    /// A read-only open: the `-wal` refuses every write.
    read_only: bool,
    /// An MVCC database's durable storage, which owns `app.db-log`.
    log: Option<Arc<LogDurableStorage>>,
}

impl DurableIo {
    pub(crate) fn new(
        inner: Arc<dyn IO>,
        wal_path: String,
        uploader: Arc<Uploader>,
        storage: Arc<BucketDbStorage>,
        wal_bytes: Arc<AtomicU64>,
        read_only: bool,
        log: Option<Arc<LogDurableStorage>>,
    ) -> DurableIo {
        DurableIo {
            inner,
            wal_path,
            uploader,
            storage,
            wal_bytes,
            read_only,
            log,
        }
    }

    /// `path` names the wrapped `app.db-log`, however it is spelled.
    fn is_wrapped_log(&self, path: &str) -> bool {
        self.log.as_ref().is_some_and(|log| {
            std::fs::canonicalize(path).is_ok_and(|p| p == log.log_path())
                || path.ends_with(".db-log")
        })
    }
}

impl Clock for DurableIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl IO for DurableIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        // An MVCC database's log: only its wrapper holds it (the module doc).
        if self.is_wrapped_log(path) {
            return Err(stash(Error::Unsupported(
                "MVCC: app.db-log opened outside its wrapper",
            )));
        }
        // A WAL database's: the MVCC log is refused before it exists (D5).
        if path.ends_with(".db-log") {
            return Err(stash(Error::Unsupported("MVCC")));
        }
        let f = self.inner.open_file(path, flags, direct)?;
        if path != self.wal_path {
            return Ok(f);
        }
        let wal = ShippingFile::new(
            f,
            self.inner.clone(),
            self.uploader.clone(),
            self.storage.clone(),
            // In MVCC mode the measure is the log's (`LogShippingFile`); the `-wal` only
            // holds a checkpoint's frames.
            if self.log.is_some() {
                Arc::default()
            } else {
                self.wal_bytes.clone()
            },
            self.read_only,
        )?;
        Ok(Arc::new(if self.log.is_some() {
            wal.in_mvcc()
        } else {
            wal
        }))
    }
    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        // The engine deletes a WAL holding frames next to an empty database file as an orphan
        // (database.rs:2223-2246). Our flows never leave that state, and acknowledged frames
        // must never be deleted silently. That delete passes the engine's own `wal_path`,
        // spelled `format!("{db_path}-wal")` as ours is (database.rs:1495), so this compare
        // catches it.
        if path == self.wal_path {
            return Err(stash(Error::Corrupt(format!(
                "refusing to delete {path}: it may hold acknowledged frames"
            ))));
        }
        self.inner.remove_file(path)
    }
    fn step(&self) -> turso_core::Result<()> {
        self.inner.step()
    }
    fn cancel(&self, c: &[Completion]) -> turso_core::Result<()> {
        self.inner.cancel(c)
    }
    fn drain_completions(&self, c: &[Completion]) -> turso_core::Result<()> {
        self.inner.drain_completions(c)
    }
    fn wait_for_completion(&self, c: Completion) -> turso_core::Result<()> {
        self.inner.wait_for_completion(c)
    }
    fn generate_random_number(&self) -> i64 {
        self.inner.generate_random_number()
    }
    fn fill_bytes(&self, dest: &mut [u8]) {
        self.inner.fill_bytes(dest)
    }
    fn get_memory_io(&self) -> Arc<MemoryIO> {
        self.inner.get_memory_io()
    }
    fn register_fixed_buffer(&self, ptr: NonNull<u8>, len: usize) -> turso_core::Result<u32> {
        self.inner.register_fixed_buffer(ptr, len)
    }
    fn yield_now(&self) {
        self.inner.yield_now()
    }
    fn sleep(&self, duration: Duration) {
        self.inner.sleep(duration)
    }
    fn file_id(&self, path: &str) -> turso_core::Result<FileId> {
        self.inner.file_id(path)
    }
}
