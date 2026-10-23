//! [`takeover`]: restore the database into an empty directory and take it over there, fencing
//! the writer that held it. The same mechanism as the first writer [`crate::open`] of a
//! restored directory (`db::attach_existing`), without opening the engine.
use crate::config::Config;
use crate::db::{attach_existing, DirGuard};
use crate::encryption::{self, check_page_one};
use crate::error::{Error, Result};
use crate::files::{io_err, LOCK_FILE};
use crate::format::LocalState;
use crate::restore::{remove_restored, restore_staged, RestoreReport};
use crate::store::ObjectStore;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// What a [`takeover`] did.
#[derive(Debug, Clone)]
pub struct TakeoverReport {
    /// The generation taken over (the head's).
    pub generation: u64,
    /// The head's epoch now: one above the previous writer's.
    pub epoch: u64,
    /// The seq of the fence record in `log/<generation>`.
    pub fence_seq: u64,
    /// The new writer id the head names.
    pub writer_id: String,
    /// The last restore (a takeover restores again when the generation moved meanwhile).
    pub restore: RestoreReport,
    /// The takeover quiet: how long after the takeover the new writer's seal of this
    /// generation waits (`lease + request_timeout`), because some earlier writer's last record
    /// before the fence is a seal-start record (that writer may be several takeovers back),
    /// or a record before the fence could not be read, so that writer's page and manifest
    /// PUTs may still be in flight. Zero otherwise. It is persisted in the directory with its
    /// length, so the [`crate::open`] that follows waits that full length again from the open
    /// (on the monotonic clock, whatever the wall clock did meanwhile); a checkpoint or close
    /// in the quiet waits for its end, and maintenance does not start meanwhile.
    pub quiet: Duration,
    pub elapsed: Duration,
}

/// How many times a takeover restores when the old writer keeps sealing in between.
const ATTEMPTS: u32 = 3;

/// Take the database `config` names over into `config.dir`, which must be missing or empty
/// (`Config` otherwise, before any request is sent; a `durable.lock` alone counts as empty, so
/// a takeover that failed "restore again" can be retried into the same directory): restore it there, then write the head
/// with a new writer id at the next epoch and append a fence record. A writer still running
/// elsewhere is fenced for good: its next commit or seal fails `Fenced`. Every commit it
/// acked before the fence is in the directory.
///
/// When the old writer sealed between the restore and the head write ("restore again"), the
/// restored files are removed and the restore runs again, at most 3 times in all. No engine
/// opens: [`crate::open`] of `config.dir` continues as the new writer. A takeover that fails
/// after its head write (a crash, or `Timeout` when the old writer kept appending) resumes at
/// that open.
///
/// The restored page 1 must fit the configuration (D16): with `encryption_key_file` set, the
/// key and cipher; without it, a plaintext database. Otherwise the takeover fails with
/// `Encryption` before its head write: nothing is written to the bucket, the restored files
/// are removed, and the old writer keeps running.
pub fn takeover(config: Config) -> Result<TakeoverReport> {
    let store = config.s3_store()?;
    takeover_with_store(config, store)
}

#[doc(hidden)]
pub fn takeover_with_store(config: Config, store: Arc<dyn ObjectStore>) -> Result<TakeoverReport> {
    let start = Instant::now();
    config.validate()?;
    let encryption = encryption::load(&config)?;
    // The lock file: a failed takeover leaves it (`DirGuard` never deletes it); the lock
    // itself still excludes a live holder.
    refuse_entries(&config.dir, &[LOCK_FILE])?;
    let guard = DirGuard::acquire(&config.dir)?;
    let dir = guard.dir().to_path_buf();
    // Checked again under the lock: nothing else may have appeared.
    refuse_entries(&dir, &[LOCK_FILE])?;
    let cfg = config.engine(false);
    let keys = config.keys_for(encryption.as_deref());
    let mut attempt = 1;
    loop {
        let staged = restore_staged(store.clone(), &keys, &dir, &cfg)?;
        let restore = staged.report;
        let state = LocalState::read(&dir)?
            .ok_or_else(|| Error::Corrupt("the restore wrote no local state".into()))?;
        // Before the claim, with or without a key. Page 1 as the restore saw it, or the newer
        // local one: no GET.
        let page1 = encryption::file_page_one(&dir)?.or(staged.page1);
        if let Err(e) = check_page_one(page1.as_deref(), encryption.as_deref()) {
            remove_restored(&dir)?;
            return Err(e);
        }
        match attach_existing(&dir, &store, &keys, &cfg, state) {
            Ok(a) => {
                let fence = a
                    .fence
                    .ok_or_else(|| Error::Corrupt("the takeover appended no fence".into()))?;
                return Ok(TakeoverReport {
                    generation: a.bucket.head().generation,
                    epoch: a.bucket.head().epoch,
                    fence_seq: fence.pos.seq,
                    writer_id: a.state.writer_id,
                    restore,
                    quiet: a.quiet,
                    elapsed: start.elapsed(),
                });
            }
            Err(Error::Fenced(m)) if m == crate::bucket::RESTORE_AGAIN => {
                // Nothing was written to the bucket: the directory is emptied again.
                remove_restored(&dir)?;
                if attempt == ATTEMPTS {
                    return Err(Error::Fenced(m));
                }
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

/// `Config` when `dir` holds an entry other than `allowed`. A missing `dir` is empty.
fn refuse_entries(dir: &Path, allowed: &[&str]) -> Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io_err(&format!("list {}", dir.display()), e)),
    };
    for e in entries {
        let e = e.map_err(|e| io_err(&format!("list {}", dir.display()), e))?;
        let name = e.file_name();
        if !allowed.iter().any(|a| name == *a) {
            return Err(Error::Config(format!(
                "{} is not empty ({}): a takeover restores into a missing or empty directory; \
                 a directory holding an interrupted takeover continues it at its next open",
                dir.display(),
                name.to_string_lossy()
            )));
        }
    }
    Ok(())
}
