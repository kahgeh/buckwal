//! The MVCC engine's durable storage (stage 2, hybrid C): turso_core's own `Storage`, given to
//! the engine with `OpenOptions::durable_storage`, over `app.db-log` wrapped in a
//! [`LogShippingFile`]. Every `DurableStorage` method delegates to that `Storage`, except:
//!
//! - `truncate` (the checkpoint's log cut, step 6 of docs/mvcc-recon.md Q3) seals first:
//!   pages, manifest and head go up (`BucketDbStorage::seal`, with the page set the WAL seal
//!   uses), and only then does it delegate. A failed seal returns `Err` before the engine
//!   touches the log's salt or offset (P8f), so the log and the WAL keep their frames. It
//!   never returns `Retained` (the TRUNCATE checkpoint asserts `Truncated`).
//! - `on_checkpoint_start` adopts a seal whose head is published but was never confirmed
//!   (`Uploader::settle_log`), before any WAL write of the checkpoint: the backstop of the
//!   adopt every commit's sync makes.
//! - `on_checkpoint_end` poisons the database when the checkpoint failed after its seal (the
//!   log truncate, its fsync or the WAL truncate): the engine may already hold the new salt
//!   while the file keeps the old frames, so every later write is refused, and checkpoints
//!   and seals too, until the database is reopened (the open repairs and reconciles).
//! - `should_checkpoint` is false: maintenance checkpoints, never the engine on its own (an
//!   auto-checkpoint's error is swallowed inside a commit, recon Q6).
//! - `encryption_ctx` is the context built from the same cipher and key as the engine's
//!   `EncryptionOpts`: without it the engine refuses an encrypted MVCC open (P8e).
//!
//! `on_log_write_complete` is the engine's default (a yield): the upload is in the log's
//! `File::sync`, the commit point.
//!
//! No `.db-log` sync can run between the seal's generation change and the delegated log
//! truncate: the checkpoint holds the engine's blocking checkpoint lock, which every
//! transaction holds for reading from its begin (turso_core-0.8.3
//! mvcc/database/mod.rs:6610-6612), so no commit runs inside it. If one ever did, the
//! `LogShippingFile` records that truncate and ships it (its `truncate`), so the new
//! generation's image still follows the file.
use super::db_storage::BucketDbStorage;
use super::log_file::LogShippingFile;
use super::stash::stash;
use super::wal_file::wal_frame_pages;
use crate::error::{Error, Result};
use crate::files::io_err;
use crate::uploader::Uploader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use turso_core::io::FileSyncType;
use turso_core::mvcc::database::{LogRecord, RowVersion};
use turso_core::mvcc::persistent_storage::logical_log::{LogHeader, OnSerializationComplete};
use turso_core::mvcc::persistent_storage::{DurableStorage, LogicalLogTruncateOutcome, Storage};
use turso_core::storage::sqlite3_ondisk::DatabaseHeader;
use turso_core::{CheckpointResult, Completion, EncryptionContext, File, OpenFlags, IO};

/// Tests: a function applied to the local `app.db-log` before it is wrapped (a fault file),
/// and whether to leave the encryption context off the wrapper. Per thread, read by
/// `LogDurableStorage::open` on the thread that opens the database.
#[cfg(test)]
type WrapLog = Arc<dyn Fn(Arc<dyn File>) -> Arc<dyn File>>;
#[cfg(test)]
thread_local! {
    pub(crate) static WRAP_LOG_FILE: std::cell::RefCell<Option<WrapLog>> =
        const { std::cell::RefCell::new(None) };
    pub(crate) static OMIT_LOG_ENCRYPTION: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

pub(crate) struct LogDurableStorage {
    inner: Storage,
    file: Arc<LogShippingFile>,
    db: Arc<BucketDbStorage>,
    uploader: Arc<Uploader>,
    /// The local `-wal`, read for the seal's page set.
    wal_path: PathBuf,
    /// The canonical path of `app.db-log`: `DurableIo` refuses every other open of it.
    log_path: PathBuf,
    /// The checkpoint now running sealed: a failure after this poisons.
    sealed: AtomicBool,
    /// Set once a local step after a seal failed; refuses checkpoints and seals until reopen
    /// (the log file refuses writes with the same error).
    poisoned: Mutex<Option<Error>>,
    read_only: bool,
}

impl std::fmt::Debug for LogDurableStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogDurableStorage")
            .field("log_path", &self.log_path)
            .field("read_only", &self.read_only)
            .finish_non_exhaustive()
    }
}

impl LogDurableStorage {
    /// Open `<dir>/app.db-log` (created empty if missing) with the platform IO,
    /// wrap it, and build the engine's `Storage` over it with `enc` (the database's cipher and
    /// key). The first sync ships the whole local file (`ship_whole_file_next`).
    pub(crate) fn open(
        platform: Arc<dyn IO>,
        log_path: &Path,
        wal_path: &Path,
        uploader: Arc<Uploader>,
        db: Arc<BucketDbStorage>,
        enc: Option<EncryptionContext>,
        read_only: bool,
    ) -> Result<LogDurableStorage> {
        let path = log_path
            .to_str()
            .ok_or_else(|| Error::Config(format!("{} is not UTF-8", log_path.display())))?;
        // A read-only open never creates the log.
        if read_only && !log_path.exists() {
            return Err(Error::Corrupt(format!(
                "{} is missing: restore the database again",
                log_path.display()
            )));
        }
        let flags = if read_only {
            OpenFlags::None
        } else {
            OpenFlags::Create
        };
        let local = platform
            .open_file(path, flags, false)
            .map_err(|e| io_err("open app.db-log", e))?;
        let log_path = std::fs::canonicalize(log_path).map_err(|e| io_err("app.db-log", e))?;
        #[cfg(test)]
        let local = match WRAP_LOG_FILE.with(|w| w.borrow().clone()) {
            Some(wrap) => wrap(local),
            None => local,
        };
        #[cfg(test)]
        let enc = if OMIT_LOG_ENCRYPTION.with(|o| o.get()) {
            None
        } else {
            enc
        };
        let file = Arc::new(
            LogShippingFile::new(local, platform.clone(), uploader.clone(), read_only)
                .map_err(|e| io_err("app.db-log", e))?,
        );
        file.ship_whole_file_next();
        Ok(LogDurableStorage {
            inner: Storage::new(file.clone(), platform, enc),
            file,
            db,
            uploader,
            wal_path: wal_path.to_path_buf(),
            log_path,
            sealed: AtomicBool::new(false),
            poisoned: Mutex::new(None),
            read_only,
        })
    }

    /// The canonical path of the file it wraps.
    pub(crate) fn log_path(&self) -> &Path {
        &self.log_path
    }

    fn refuse_if_poisoned(&self) -> Result<()> {
        match &*crate::db::lock(&self.poisoned) {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// Refuse every later write, checkpoint and seal until reopen.
    fn poison(&self, cause: &str) {
        let e = Error::Io(format!(
            "a checkpoint failed after its seal ({cause}); the local log no longer matches \
             the engine's, so writes are refused: reopen the database"
        ));
        tracing::error!(target: "turso_durable", error = %e, "database poisoned");
        self.file.poison(e.clone());
        crate::db::lock(&self.poisoned).get_or_insert(e);
    }

    /// The seal of the generation the local files hold: the same pages a WAL seal uploads
    /// (written since the last seal, persisted dirty, and every page in a frame of the local
    /// `-wal`), then manifest and head, behind every record already submitted.
    fn seal(&self) -> Result<u64> {
        let wal = match std::fs::read(&self.wal_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(io_err("read -wal for the seal", e)),
        };
        if wal.len() >= 12 {
            self.db
                .note_page_size(u32::from_be_bytes(wal[8..12].try_into().unwrap()));
        }
        self.db.seal(&wal_frame_pages(&wal))
    }
}

impl DurableStorage for LogDurableStorage {
    fn serialize_row_version(
        &self,
        log_record: &mut LogRecord,
        row_version: &RowVersion,
        portable_extension: Option<&[u8]>,
    ) -> turso_core::Result<()> {
        self.inner
            .serialize_row_version(log_record, row_version, portable_extension)
    }

    fn serialize_database_header(
        &self,
        log_record: &mut LogRecord,
        header: &DatabaseHeader,
    ) -> turso_core::Result<()> {
        self.inner.serialize_database_header(log_record, header)
    }

    fn log_tx(
        &self,
        m: LogRecord,
        on_serialization_complete: OnSerializationComplete<'_>,
    ) -> turso_core::Result<(Completion, u64)> {
        self.inner.log_tx(m, on_serialization_complete)
    }

    fn upgrade_header_for_log_tx(&self, m: &LogRecord) -> turso_core::Result<Option<Completion>> {
        self.inner.upgrade_header_for_log_tx(m)
    }

    fn sync(&self, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        self.inner.sync(sync_type)
    }

    fn update_header(&self) -> turso_core::Result<Completion> {
        self.inner.update_header()
    }

    /// Seal, then cut the log. A failed seal returns before the engine changes anything.
    fn truncate(
        &self,
        checkpointed_through_ts: u64,
    ) -> turso_core::Result<(Completion, LogicalLogTruncateOutcome)> {
        if self.read_only {
            // Verify's recovery cut stays local: no seal, nothing shipped.
            return self.inner.truncate(checkpointed_through_ts);
        }
        self.refuse_if_poisoned().map_err(stash)?;
        self.seal().map_err(stash)?;
        self.sealed.store(true, SeqCst);
        let (c, outcome) = self.inner.truncate(checkpointed_through_ts)?;
        if outcome == LogicalLogTruncateOutcome::Retained {
            // Never `Retained` (the TRUNCATE checkpoint asserts `Truncated`, a panic). The log
            // is untouched: the sealed generation's image is the whole local file, which the
            // next sync ships (the generation-change invariant), so nothing is poisoned.
            self.sealed.store(false, SeqCst);
            return Err(stash(Error::Corrupt(
                "the logical log kept frames past the checkpoint boundary".into(),
            )));
        }
        Ok((c, outcome))
    }

    fn reset_to_fresh_header(&self) -> turso_core::Result<Completion> {
        self.inner.reset_to_fresh_header()
    }

    fn get_logical_log_file(&self) -> Arc<dyn File> {
        self.inner.get_logical_log_file()
    }

    fn logical_log_offset(&self) -> u64 {
        self.inner.logical_log_offset()
    }

    fn should_checkpoint(&self) -> bool {
        false
    }

    fn set_checkpoint_threshold(&self, threshold: i64) {
        self.inner.set_checkpoint_threshold(threshold)
    }

    fn checkpoint_threshold(&self) -> i64 {
        self.inner.checkpoint_threshold()
    }

    fn advance_logical_log_offset_after_success(&self, bytes: u64) -> turso_core::Result<()> {
        self.inner.advance_logical_log_offset_after_success(bytes)
    }

    fn discard_pending_log_write(&self) -> turso_core::Result<()> {
        self.inner.discard_pending_log_write()
    }

    fn restore_logical_log_state_after_recovery(&self, offset: u64, running_crc: u32) {
        self.inner
            .restore_logical_log_state_after_recovery(offset, running_crc)
    }

    fn set_header(&self, header: LogHeader) {
        self.inner.set_header(header)
    }

    /// Before any WAL write of the checkpoint: refuse it when poisoned, and adopt a seal
    /// whose head is published (the seal's flush then runs after the adopt).
    fn on_checkpoint_start(&self) -> turso_core::Result<()> {
        self.sealed.store(false, SeqCst);
        if self.read_only {
            return Ok(());
        }
        self.refuse_if_poisoned().map_err(stash)?;
        self.uploader.settle_log().map_err(stash)?;
        Ok(())
    }

    /// A checkpoint that failed after its seal poisons the database (module doc).
    fn on_checkpoint_end(
        &self,
        result: turso_core::Result<&CheckpointResult>,
    ) -> turso_core::Result<()> {
        if self.sealed.swap(false, SeqCst) {
            if let Err(e) = result {
                self.poison(&e.to_string());
            }
        }
        Ok(())
    }

    fn encryption_ctx(&self) -> Option<EncryptionContext> {
        self.inner.encryption_ctx()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The engine's own auto-checkpoint never runs: whatever threshold is set, the wrapper
    /// says no.
    #[test]
    fn should_checkpoint_is_always_false() {
        let d = tempfile::tempdir().unwrap();
        let store = Arc::new(crate::store::MemStore::new());
        let (e, _c) = crate::engine::tests::writer(store, d.path());
        let path = d.path().join("app.db-log");
        std::fs::write(&path, b"").unwrap();
        let s = LogDurableStorage::open(
            Arc::new(turso_core::PlatformIO::new().unwrap()),
            &path,
            &d.path().join("app.db-wal"),
            e.uploader.clone(),
            e.storage.clone(),
            None,
            false,
        )
        .unwrap();
        for t in [-1, 0, 1, i64::MAX] {
            s.set_checkpoint_threshold(t);
            assert!(!s.should_checkpoint(), "threshold {t}");
        }
    }
}
