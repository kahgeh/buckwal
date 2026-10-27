//! The file record (spec outside-change-detect): what buckwal keeps in the local state about
//! `app.db`, its `-wal` and, in MVCC mode, `app.db-log`, to detect a change made outside
//! buckwal.
//!
//! Two rules keep the `app.db` part correct, and every writer of `app.db` follows them
//! through [`DbFiles`]:
//! 1. Before the first write to `app.db` after a record, [`DbFiles::begin_write`] sets
//!    `db_write_open` on and `clean_close` off, and fsyncs the local state.
//! 2. After the fsync of `app.db`, [`DbFiles::end_write`] writes the new `app.db` part and sets
//!    `db_write_open` off.
//!
//! A clean close writes the whole record ([`DbFiles::write_clean_close`]); [`mark_in_use`] turns
//! the mark off at open. [`check_at_open`]
//! compares the files with the record before reconcile.
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE, LOG_FILE, WAL_FILE};
use crate::format::LocalState;
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex, Weak};
use std::time::UNIX_EPOCH;

/// A SHA-256, as raw bytes.
pub(crate) type Sha = [u8; 32];

/// The bytes of `app.db` that stand for its first page: one page at the default page size.
/// The page size is not known here (an encrypted header cannot be read), and these bytes hold
/// the whole header, with the change counter.
const PAGE1_BYTES: usize = 4096;
/// The header of a `-wal`, and of an `app.db-log`.
const HEADER_BYTES: usize = 32;

/// The refusal of an open that finds a changed file.
const CHANGED: &str =
    "app.db was changed outside buckwal since buckwal last used it; restore into a new directory";

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

fn sha(bytes: &[u8]) -> Sha {
    Sha256::digest(bytes).into()
}

/// Up to `n` bytes from the start of `path`; none when the file is missing.
fn head_bytes(path: &Path, n: usize) -> Result<Vec<u8>> {
    let f = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_err("open for the file record", e)),
    };
    let mut buf = Vec::with_capacity(n);
    f.take(n as u64)
        .read_to_end(&mut buf)
        .map_err(|e| io_err("read for the file record", e))?;
    Ok(buf)
}

/// The size of `path`, and its modification time in ns; `(0, 0)` when it is missing.
fn size_and_time(path: &Path) -> Result<(u64, u64)> {
    match fs::metadata(path) {
        Ok(m) => {
            let ns = m
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map_or(0, |d| d.as_nanos() as u64);
            Ok((m.len(), ns))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok((0, 0)),
        Err(e) => Err(io_err("stat for the file record", e)),
    }
}

/// The `app.db` part: size, modification time, and the SHA-256 of the first page.
fn db_part(dir: &Path) -> Result<(u64, u64, Sha)> {
    let path = dir.join(DB_FILE);
    let (size, ns) = size_and_time(&path)?;
    Ok((size, ns, sha(&head_bytes(&path, PAGE1_BYTES)?)))
}

/// A `-wal` or `-log` part: size and the SHA-256 of the header.
fn header_part(dir: &Path, name: &str) -> Result<(u64, Sha)> {
    let path = dir.join(name);
    let (size, _) = size_and_time(&path)?;
    Ok((size, sha(&head_bytes(&path, HEADER_BYTES)?)))
}

/// The whole record now, with the given flags.
fn capture(dir: &Path, mvcc: bool, db_write_open: bool, clean_close: bool) -> Result<FileRecord> {
    let (db_size, db_mtime_ns, db_page1_sha256) = db_part(dir)?;
    let (wal_size, wal_header_sha256) = header_part(dir, WAL_FILE)?;
    let log = if mvcc {
        Some(header_part(dir, LOG_FILE)?)
    } else {
        None
    };
    Ok(FileRecord {
        db_size,
        db_mtime_ns,
        db_page1_sha256,
        wal_size,
        wal_header_sha256,
        log,
        db_write_open,
        clean_close,
    })
}

fn changed(file: &str, detail: &str) -> Error {
    tracing::error!(target: "buckwal", file, detail, "files changed outside buckwal");
    Error::Corrupt(CHANGED.into())
}

/// Compare the files in `dir` with the file record of `state`, before reconcile. Writes
/// nothing. `Corrupt` (and one ERROR log) when a file differs. What it compares depends on the
/// flags (spec, "What buckwal checks at open"):
/// - no record: nothing;
/// - `db_write_open` on: nothing (the write was unfinished, so `app.db` cannot be compared);
/// - `clean_close` off, `db_write_open` off (a crash): the `app.db` part only;
/// - `clean_close` on: the `app.db` part, the `-wal` part and (MVCC) the `-log` part.
pub(crate) fn check_at_open(dir: &Path, state: &LocalState) -> Result<()> {
    let Some(rec) = &state.files else {
        return Ok(());
    };
    if rec.db_write_open {
        return Ok(());
    }
    let (size, ns, page1) = db_part(dir)?;
    if size != rec.db_size {
        return Err(changed(DB_FILE, "size"));
    }
    if ns != rec.db_mtime_ns {
        return Err(changed(DB_FILE, "time"));
    }
    if page1 != rec.db_page1_sha256 {
        return Err(changed(DB_FILE, "page1"));
    }
    if !rec.clean_close {
        return Ok(());
    }
    let (wal_size, wal_header) = header_part(dir, WAL_FILE)?;
    if wal_size != rec.wal_size {
        return Err(changed(WAL_FILE, "size"));
    }
    if wal_header != rec.wal_header_sha256 {
        return Err(changed(WAL_FILE, "wal_header"));
    }
    if let Some((log_size, log_header)) = &rec.log {
        let (size, header) = header_part(dir, LOG_FILE)?;
        if size != *log_size {
            return Err(changed(LOG_FILE, "size"));
        }
        if header != *log_header {
            return Err(changed(LOG_FILE, "wal_header"));
        }
    }
    Ok(())
}

/// Tests only: a hook per directory, called once `begin_write` has set `db_write_open` on and
/// fsynced the state, before the write it announces. A test copies the directory in it, which
/// is a crash in the middle of a write to `app.db`.
#[cfg(test)]
pub(crate) type WriteHook = Arc<dyn Fn() + Send + Sync>;
#[cfg(test)]
pub(crate) static AFTER_BEGIN: Mutex<Vec<(PathBuf, WriteHook)>> = Mutex::new(Vec::new());

/// The open `DbFiles` of each directory in this process. A directory has one open at a time (the
/// directory lock), and the storage of its engine finds its helper here: the storage is built
/// in `engine.rs`, which an open reaches with no place for one more argument. A replica or a
/// test that opens an engine alone registers nothing, and its storage keeps no record.
static REGISTRY: Mutex<Vec<(PathBuf, Weak<DbFiles>)>> = Mutex::new(Vec::new());

/// Make `files` the helper of its directory, and forget the helpers that are gone.
pub(crate) fn register(files: &Arc<DbFiles>) {
    let mut r = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    r.retain(|(d, w)| *d != files.dir && w.strong_count() > 0);
    r.push((files.dir.clone(), Arc::downgrade(files)));
}

/// The open helper of `dir`, if one was registered and is still open.
pub(crate) fn lookup(dir: &Path) -> Option<Arc<DbFiles>> {
    let r = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
    r.iter()
        .find(|(d, _)| d == dir)
        .and_then(|(_, w)| w.upgrade())
}

/// Turn `clean_close` off in `state` and fsync it, when it holds a record that a clean close
/// wrote. A writer open and a read-only open call it after the check and before reconcile,
/// which can write the `-wal`.
pub(crate) fn mark_in_use(dir: &Path, state: &mut LocalState) -> Result<()> {
    match &mut state.files {
        Some(rec) if rec.clean_close => {
            rec.clean_close = false;
            state.write_atomic(dir)
        }
        _ => Ok(()),
    }
}

/// Rule 1 on `state`: `db_write_open` on, `clean_close` off. With no record yet (a directory
/// from before the record, or a new one) it starts one whose `db_write_open` is on, so nothing
/// is compared until the first rule 2.
fn open_write_in(state: &mut LocalState, mvcc: bool) {
    let rec = state.files.get_or_insert_with(|| FileRecord {
        db_size: 0,
        db_mtime_ns: 0,
        db_page1_sha256: sha(&[]),
        wal_size: 0,
        wal_header_sha256: sha(&[]),
        log: mvcc.then(|| (0, sha(&[]))),
        db_write_open: true,
        clean_close: false,
    });
    rec.db_write_open = true;
    rec.clean_close = false;
}

/// Rule 2 on `state`: the new `app.db` part, `db_write_open` off.
fn close_write_in(dir: &Path, state: &mut LocalState) -> Result<()> {
    let (size, ns, page1) = db_part(dir)?;
    if let Some(rec) = &mut state.files {
        rec.db_size = size;
        rec.db_mtime_ns = ns;
        rec.db_page1_sha256 = page1;
        rec.db_write_open = false;
    }
    Ok(())
}

/// Rule 1 for a writer that has no [`DbFiles`] yet (the seed of a never-sealed database,
/// during reconcile): set it in `state` and fsync the local state, before the write.
pub(crate) fn begin_offline_write(dir: &Path, state: &mut LocalState, mvcc: bool) -> Result<()> {
    open_write_in(state, mvcc);
    state.write_atomic(dir)
}

/// Rule 2 for that writer, after the fsync of `app.db`.
pub(crate) fn end_offline_write(dir: &Path, state: &mut LocalState) -> Result<()> {
    close_write_in(dir, state)?;
    state.write_atomic(dir)
}

/// What the helper keeps between calls, under one lock.
struct Inner {
    /// The local state as last written. Every write of `app.db-durable` after the open goes
    /// through [`DbFiles`], so the `files` part of it is never lost.
    state: LocalState,
    /// True from `begin_write` until an `end_write` that no write overtook.
    write_open: bool,
}

/// The one writer of `app.db-durable` once the engine is open, and the keeper of the file
/// record. Lock order: a caller's own lock (the dirty mutex) first, this one after.
pub(crate) struct DbFiles {
    dir: PathBuf,
    mvcc: bool,
    inner: Mutex<Inner>,
    /// Counts the writes to `app.db` that started; `end_write` keeps `db_write_open` on when a
    /// write started after the fsync it follows did.
    started: AtomicU64,
    /// The writes that started and have not ended (their [`WriteGuard`] is alive). An fsync
    /// that starts while one is in flight does not cover it.
    inflight: AtomicUsize,
}

/// A write to `app.db` that has started. Dropped when the write call has returned.
pub(crate) struct WriteGuard<'a>(&'a DbFiles);

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, SeqCst);
    }
}

impl DbFiles {
    /// `state` is the local state the open ended with (after the check and the reconcile).
    pub(crate) fn new(dir: &Path, mvcc: bool, state: LocalState) -> DbFiles {
        DbFiles {
            dir: dir.to_path_buf(),
            mvcc,
            inner: Mutex::new(Inner {
                state,
                write_open: false,
            }),
            started: AtomicU64::new(0),
            inflight: AtomicUsize::new(0),
        }
    }

    fn write(&self, inner: &mut Inner) -> Result<()> {
        inner.state.write_atomic(&self.dir)
    }

    /// Write `state` (the caller's copy of the other fields) with the current file record.
    /// Every writer of `app.db-durable` after the open calls this and no other write.
    pub(crate) fn write_state(&self, state: &LocalState) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let files = inner.state.files.take();
        inner.state = LocalState {
            files,
            ..state.clone()
        };
        self.write(&mut inner)
    }

    /// Rule 1. Call before every write to `app.db`. Only the first call after a record does
    /// work: it sets `db_write_open` on and `clean_close` off and fsyncs the local state (when
    /// a record exists on disk).
    pub(crate) fn begin_write(&self) -> Result<WriteGuard<'_>> {
        // Order matters (with `sync_token` and `end_write`): `inflight` first, then `started`.
        // `sync_token` reads `started` first and `inflight` second. A write that is not yet
        // counted in `started` when the token reads it has either raised `inflight` before the
        // token's second read (the token is None) or raises both after it, so `end_write` then
        // sees `started` changed or `inflight` above 0. With the order the other way round, a
        // write could land between the two reads of the token with both counters unchanged.
        self.inflight.fetch_add(1, SeqCst);
        self.started.fetch_add(1, SeqCst);
        let guard = WriteGuard(self);
        let mut inner = self.inner.lock().unwrap();
        if inner.write_open {
            return Ok(guard);
        }
        // With no record on disk, an open checks nothing whatever happens, so the first rule 1
        // needs no fsync of its own: the record starts at the first rule 2. (A state write in
        // between persists the flag, which is safe.)
        let first = inner.state.files.is_none();
        open_write_in(&mut inner.state, self.mvcc);
        if !first {
            self.write(&mut inner)?;
        }
        inner.write_open = true;
        #[cfg(test)]
        {
            let hook = AFTER_BEGIN
                .lock()
                .unwrap()
                .iter()
                .find(|(d, _)| *d == self.dir)
                .map(|(_, h)| h.clone());
            if let Some(h) = hook {
                h();
            }
        }
        Ok(guard)
    }

    /// The token for `end_write`. Call it before the fsync of `app.db` starts.
    /// `None` while a write is in flight: that fsync does not cover it, so no `end_write`
    /// may follow it.
    pub(crate) fn sync_token(&self) -> Option<u64> {
        // `started` first, then `inflight` (see `begin_write`).
        let started = self.started.load(SeqCst);
        if self.inflight.load(SeqCst) > 0 {
            return None;
        }
        Some(started)
    }

    /// Rule 2. Call after the fsync of `app.db` that started at `token`. Writes the new
    /// `app.db` part and sets `db_write_open` off, unless a write came after the fsync started
    /// (then the flag stays on, and the next fsync ends it).
    pub(crate) fn end_write(&self, token: Option<u64>) -> Result<()> {
        let Some(token) = token else {
            return Ok(());
        };
        let mut inner = self.inner.lock().unwrap();
        if !inner.write_open || self.started.load(SeqCst) != token || self.inflight.load(SeqCst) > 0
        {
            return Ok(());
        }
        close_write_in(&self.dir, &mut inner.state)?;
        // Without the fsync of the directory: a lost rename leaves the record of rule 1, which
        // says a write is open, so the next open checks nothing (safe, only weaker).
        inner.state.write_atomic_lazy(&self.dir)?;
        inner.write_open = false;
        Ok(())
    }

    /// The whole record, with `clean_close` on. Call it once, at the end of a close, after
    /// the engine, the uploader and the packer have stopped.
    pub(crate) fn write_clean_close(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.state.files = Some(capture(&self.dir, self.mvcc, false, true)?);
        self.write(&mut inner)?;
        inner.write_open = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
