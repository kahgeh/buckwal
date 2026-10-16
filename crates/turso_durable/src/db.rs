//! [`open`] and [`Database`]: the directory locks (D4), the order of an open, and shutdown.
use crate::bucket::{self, Bucket};
use crate::config::{Config, EngineConfig};
use crate::conn::Connection;
use crate::engine::{engine_error, open_engine, Engine};
use crate::error::{Error, Result};
use crate::format::{decode_record, Keys, LocalState, LogRecord};
use crate::reconcile::{reconcile, Reconciled};
use crate::restore::{clear_interrupted_restore, restore_staged};
use crate::store::{with_retry, ObjectStore, StatsSnap};
use crate::trim::spawn_trimmer;
use crate::uploader::Uploader;
use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{mpsc, Arc, LazyLock, Mutex, MutexGuard};
use std::thread::JoinHandle;
use turso_core::{File, OpenFlags, PlatformIO, IO};

const DB_FILE: &str = "app.db";
const LOCK_FILE: &str = "durable.lock";

/// The canonical directories this process holds (D4). `durable.lock`'s `fcntl` lock is per
/// process and is released when any handle to it in the process closes, so a second open in
/// this process must be refused before it opens any file.
static OPEN_DIRS: LazyLock<Mutex<HashSet<PathBuf>>> = LazyLock::new(Default::default);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn io_err(what: &str, e: impl std::fmt::Display) -> Error {
    Error::Io(format!("{what}: {e}"))
}

/// A directory this process holds: its entry in the process registry, and the exclusive lock
/// on `<dir>/durable.lock`. Dropping it unlocks and leaves the registry.
pub(crate) struct DirGuard {
    dir: PathBuf,
    lock: Option<Arc<dyn File>>,
}

impl DirGuard {
    /// Create `dir`, enter it in the registry (`AlreadyOpen`), then lock `durable.lock`
    /// (`Locked`).
    pub(crate) fn acquire(dir: &Path) -> Result<DirGuard> {
        fs::create_dir_all(dir).map_err(|e| io_err(&format!("create {}", dir.display()), e))?;
        let canon =
            fs::canonicalize(dir).map_err(|e| io_err(&format!("resolve {}", dir.display()), e))?;
        if !lock(&OPEN_DIRS).insert(canon.clone()) {
            return Err(Error::AlreadyOpen(format!(
                "{} is already open in this process",
                canon.display()
            )));
        }
        // From here on, dropping `guard` leaves the registry.
        let mut guard = DirGuard {
            dir: canon,
            lock: None,
        };
        let path = guard.dir.join(LOCK_FILE);
        let path = path
            .to_str()
            .ok_or_else(|| Error::Config(format!("{} is not UTF-8", path.display())))?;
        let io = PlatformIO::new().map_err(|e| io_err("platform IO", e))?;
        // NoLock, then an explicit lock: LIMBO_DISABLE_FILE_LOCK cannot skip it.
        let file = io
            .open_file(path, OpenFlags::Create | OpenFlags::NoLock, false)
            .map_err(|e| io_err(&format!("open {path}"), e))?;
        file.lock_file(true).map_err(|_| {
            Error::Locked(format!(
                "{} is open in another process",
                guard.dir.display()
            ))
        })?;
        guard.lock = Some(file);
        Ok(guard)
    }

    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }
}

impl Drop for DirGuard {
    fn drop(&mut self) {
        if let Some(f) = self.lock.take() {
            let _ = f.unlock_file();
        }
        lock(&OPEN_DIRS).remove(&self.dir);
    }
}

/// What an open database holds until it is closed.
struct Open {
    engine: Engine,
    trimmer: Option<JoinHandle<()>>,
    /// Dropped last: the lock outlives every file of the engine.
    guard: DirGuard,
}

/// Shared by every [`Database`] handle and [`Connection`] of one open.
pub(crate) struct Shared {
    pub(crate) read_only: bool,
    store: Arc<dyn ObjectStore>,
    uploader: Arc<Uploader>,
    /// None once closed.
    open: Mutex<Option<Open>>,
    connections: AtomicUsize,
    /// What the open's reconcile found (tests).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) reconciled: Option<Reconciled>,
}

impl Shared {
    pub(crate) fn connection_closed(&self) {
        self.connections.fetch_sub(1, SeqCst);
    }

    /// Drain the uploader, stop the trimmer, drop the engine (and with it the engine's
    /// registry entry), unlock `durable.lock` and leave the process registry.
    fn shutdown(&self) {
        let open = lock(&self.open).take();
        self.finish(open);
    }

    fn finish(&self, open: Option<Open>) {
        let Some(open) = open else {
            return;
        };
        let Open {
            engine,
            trimmer,
            guard,
        } = open;
        drop(engine);
        // Runs every queued job; the uploader thread's end drops the last trim sender.
        self.uploader.shutdown();
        if let Some(t) = trimmer {
            let _ = t.join();
        }
        drop(guard);
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// An open database. Clones share it; it closes when the last handle and the last
/// [`Connection`] are gone, or at [`Database::close`].
#[derive(Clone)]
pub struct Database {
    pub(crate) shared: Arc<Shared>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("generation", &self.generation())
            .field("read_only", &self.shared.read_only)
            .finish_non_exhaustive()
    }
}

/// Open the database `config` names, as its one writer: restore it into an empty directory
/// when the bucket already holds it, create it when it does not, or continue the directory's
/// own database.
pub fn open(config: Config) -> Result<Database> {
    let store = config.s3_store()?;
    open_with_store(config, store)
}

#[doc(hidden)]
pub fn open_with_store(config: Config, store: Arc<dyn ObjectStore>) -> Result<Database> {
    open_impl(config, store, false)
}

/// Open for reading only (verification): nothing is ever written to the bucket. The
/// directory is not claimed, no trimmer runs, every statement that writes is refused, and
/// close never checkpoints.
#[doc(hidden)]
pub fn open_read_only(config: Config) -> Result<Database> {
    let store = config.s3_store()?;
    open_read_only_with_store(config, store)
}

#[doc(hidden)]
pub fn open_read_only_with_store(config: Config, store: Arc<dyn ObjectStore>) -> Result<Database> {
    open_impl(config, store, true)
}

fn open_impl(config: Config, store: Arc<dyn ObjectStore>, read_only: bool) -> Result<Database> {
    config.validate()?;
    let guard = DirGuard::acquire(&config.dir)?;
    let dir = guard.dir().to_path_buf();
    let cfg = config.engine(read_only);
    let keys = config.keys();
    clear_interrupted_restore(&dir)?;
    refuse_mvcc_file(&dir.join(DB_FILE))?;

    let (bucket, state, reconciled) = match LocalState::read(&dir)? {
        Some(state) => attach_existing(&dir, &store, &keys, &cfg, state)?,
        None if dir.join(DB_FILE).exists() => {
            return Err(Error::Config(format!(
                "{} holds an app.db not created by turso_durable",
                dir.display()
            )))
        }
        None if head_exists(&*store, &keys, &cfg)? => {
            restore_staged(store.clone(), &keys, &dir, &cfg)?;
            let state = LocalState::read(&dir)?
                .ok_or_else(|| Error::Corrupt("the restore wrote no local state".into()))?;
            attach_existing(&dir, &store, &keys, &cfg, state)?
        }
        None if read_only => {
            return Err(Error::NotFound(format!(
                "no database in the bucket at prefix {:?}",
                config.prefix
            )))
        }
        None => {
            let (bucket, state) = create_new(&dir, &store, &keys, &cfg)?;
            (bucket, state, None)
        }
    };

    let (trim_tx, trim_rx) = if read_only {
        (None, None)
    } else {
        let (tx, rx) = mpsc::channel();
        (Some(tx), Some(rx))
    };
    let engine = open_engine(&dir, bucket, &state, &cfg, trim_tx)?;
    // open_engine sent the head at open as the trimmer's first snapshot.
    let trimmer = trim_rx.map(|rx| {
        spawn_trimmer(
            store.clone(),
            keys.clone(),
            config.trim_grace,
            rx,
            cfg.events.clone(),
        )
    });
    let uploader = engine.uploader.clone();
    Ok(Database {
        shared: Arc::new(Shared {
            read_only,
            store,
            uploader,
            open: Mutex::new(Some(Open {
                engine,
                trimmer,
                guard,
            })),
            connections: AtomicUsize::new(0),
            reconciled,
        }),
    })
}

/// Bytes 18/19 of an existing `app.db`, read before the engine opens it: 255 is MVCC (D5).
fn refuse_mvcc_file(path: &Path) -> Result<()> {
    let mut f = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io_err("open app.db", e)),
    };
    let mut header = Vec::with_capacity(20);
    f.by_ref()
        .take(20)
        .read_to_end(&mut header)
        .map_err(|e| io_err("read app.db header", e))?;
    if header.len() == 20 && (header[18] == 255 || header[19] == 255) {
        return Err(Error::Unsupported("MVCC"));
    }
    Ok(())
}

fn head_exists(store: &dyn ObjectStore, keys: &Keys, cfg: &EngineConfig) -> Result<bool> {
    let key = keys.head();
    Ok(with_retry(&cfg.retry, &key, |_| store.get(&key))?.is_some())
}

/// 16 random bytes from the platform, in hex.
fn new_writer_id() -> Result<String> {
    let mut id = [0u8; 16];
    PlatformIO::new()
        .map_err(|e| io_err("platform IO", e))?
        .fill_bytes(&mut id);
    Ok(hex::encode(id))
}

/// A new database: a new writer id, the head at generation 1, then the local state.
fn create_new(
    dir: &Path,
    store: &Arc<dyn ObjectStore>,
    keys: &Keys,
    cfg: &EngineConfig,
) -> Result<(Bucket, LocalState)> {
    let writer_id = new_writer_id()?;
    let bucket = Bucket::create(
        store.clone(),
        keys.clone(),
        cfg.retry.clone(),
        cfg.upload_concurrency,
        writer_id.clone(),
    )?;
    let state = LocalState {
        format: 1,
        writer_id,
        generation: 1,
        complete: true,
        claimed: true,
        head_etag: String::new(),
    };
    state.write_atomic(dir)?;
    Ok((bucket, state))
}

/// The directory's own database: a writer claims a restored copy first, then attaches and
/// reconciles against the current log (a record acked by the old writer since the restore
/// comes in as `Behind`). A read-only open attaches as the directory's writer id, unclaimed.
///
/// A writer that just claimed, or whose generation's last record is another writer's (a
/// claim whose record never landed), appends one empty record at its next seq before the
/// engine opens: a still-running old writer's next append collides with it and is fenced
/// for good, so it can never ack into a generation this writer will seal (`bucket::claim`).
/// If the old writer got there first, this append is `Fenced` and the open fails.
fn attach_existing(
    dir: &Path,
    store: &Arc<dyn ObjectStore>,
    keys: &Keys,
    cfg: &EngineConfig,
    mut state: LocalState,
) -> Result<(Bucket, LocalState, Option<Reconciled>)> {
    let claiming = !state.claimed && !cfg.read_only;
    if claiming {
        let writer_id = new_writer_id()?;
        let etag = bucket::claim(&**store, keys, &cfg.retry, &state.head_etag, &writer_id)?;
        state.writer_id = writer_id;
        state.claimed = true;
        state.head_etag = etag;
        state.write_atomic(dir)?;
    }
    let mut bucket = Bucket::attach(
        store.clone(),
        keys.clone(),
        cfg.retry.clone(),
        cfg.upload_concurrency,
        &state.writer_id,
    )?;
    let reconciled = reconcile(dir, &mut bucket, &mut state, cfg)?;
    if !cfg.read_only && (claiming || last_record_is_foreign(&bucket, &state.writer_id, cfg)?) {
        bucket.append_log(vec![])?;
    }
    Ok((bucket, state, Some(reconciled)))
}

/// The last record of the head's generation was written by a writer other than `writer_id`.
fn last_record_is_foreign(bucket: &Bucket, writer_id: &str, cfg: &EngineConfig) -> Result<bool> {
    let seq = bucket.next_seq();
    if seq <= 1 {
        return Ok(false);
    }
    let store = bucket.store();
    let key = bucket.keys().log(bucket.head().generation, seq - 1);
    let Some(obj) = with_retry(&cfg.retry, &key, |_| store.get(&key))? else {
        return Ok(false);
    };
    Ok(match decode_record(&obj.body)? {
        LogRecord::Extents { writer_id: w, .. } | LogRecord::Fence { writer_id: w } => {
            w != writer_id
        }
    })
}

impl Database {
    /// A new connection; open one per thread.
    pub fn connect(&self) -> Result<Connection> {
        let open = lock(&self.shared.open);
        let Some(open) = open.as_ref() else {
            return Err(Error::Config("the database is closed".into()));
        };
        crate::io::stash::clear();
        let core = open.engine.db.connect().map_err(engine_error)?;
        self.shared.connections.fetch_add(1, SeqCst);
        let io: Arc<dyn IO> = open.engine.io.clone();
        Ok(Connection::new(self.shared.clone(), core, io))
    }

    /// The generation the bucket's head is at, as this writer last saw it.
    pub fn generation(&self) -> u64 {
        self.shared.uploader.generation()
    }

    /// Requests and bytes sent to the bucket so far.
    pub fn bucket_stats(&self) -> StatsSnap {
        self.shared.store.stats()
    }

    /// Close the database: drain the uploader, stop the trimmer, release the directory.
    /// Every [`Connection`] must be closed first (`Config` otherwise). Other `Database`
    /// handles see a closed database afterwards.
    pub fn close(self) -> Result<()> {
        // Checked under the lock `connect` holds, so no connection appears in between.
        let mut guard = lock(&self.shared.open);
        let n = self.shared.connections.load(SeqCst);
        if n > 0 {
            return Err(Error::Config(format!(
                "{n} connection(s) still open; close them first"
            )));
        }
        let open = guard.take();
        drop(guard);
        self.shared.finish(open);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
