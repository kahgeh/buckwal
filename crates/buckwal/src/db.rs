//! [`open`] and [`Database`]: the directory locks (D4), the order of an open, and shutdown.
use crate::batching::Batching;
use crate::bucket::{self, Bucket, FenceOutcome, Timeouts};
use crate::config::{Config, EngineConfig, Event, EventHook, JournalMode};
use crate::conn::Connection;
use crate::create::{self, clear_interrupted_create};
use crate::encryption::{self, check_page_one};
use crate::engine::{engine_error, open_engine, Engine, EngineDb};
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE, LOCK_FILE, RESTORING_FILE};
use crate::format::{Head, Keys, LocalState, LogRecord, FORMAT, FORMAT_V2};
use crate::maintenance::{Gate, Maintenance, MaintenanceConfig};
use crate::pack::thread::PackerThreads;
use crate::reconcile::{reconcile, Reconciled};
use crate::restore::{clear_interrupted_restore, remove_restored, restore_staged, sealed_page_one};
use crate::stats::{StatsCell, UploadStats};
use crate::store::{with_retry, ObjectStore, StatsSnap};
use crate::trim::spawn_trimmer;
use crate::uploader::Uploader;
use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{mpsc, Arc, LazyLock, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};
use tracing::field::Empty;
use turso_core::{File, OpenFlags, PlatformIO, IO};

/// The canonical directories this process holds (D4). `durable.lock`'s `fcntl` lock is per
/// process and is released when any handle to it in the process closes, so a second open in
/// this process must be refused before it opens any file.
static OPEN_DIRS: LazyLock<Mutex<HashSet<PathBuf>>> = LazyLock::new(Default::default);

pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
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
    /// The packer thread and the pack gauges (a writer of a format 5 bucket). Stopped first
    /// and joined before the lock drops: a pack run works in this directory.
    packer: Option<PackerThreads>,
    /// Dropped last: the lock outlives every file of the engine.
    guard: Option<DirGuard>,
}

/// Shared by every [`Database`] handle and [`Connection`] of one open.
pub(crate) struct Shared {
    pub(crate) read_only: bool,
    store: Arc<dyn ObjectStore>,
    pub(crate) uploader: Arc<Uploader>,
    /// Every public `Connection` call's engine work passes it; maintenance closes it.
    pub(crate) gate: Arc<Gate>,
    /// The maintenance thread (a writer open with maintenance on). Stopped in `finish`
    /// before the engine drops.
    maintenance: Option<Maintenance>,
    /// The engine's `-wal` high-water mark (`Engine::wal_bytes`).
    wal_bytes: Arc<AtomicU64>,
    /// None once closed.
    open: Mutex<Option<Open>>,
    connections: AtomicUsize,
    /// The setting a connection without its own commits with ([`Database::set_batching`]).
    batching: Mutex<Batching>,
    /// The database's journal mode (its head's, which `open` made equal to the config's).
    pub(crate) journal_mode: JournalMode,
    /// The uploader's counters; connections record their acks in them.
    pub(crate) stats: Arc<StatsCell>,
    events: Option<EventHook>,
    /// What the open's reconcile found (tests).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) reconciled: Option<Reconciled>,
    /// A replica's directory lock, shared by the `Replica` and by every copy's `Shared`, so it
    /// is held until the last connection of any copy is gone (`Open::guard` is the writer's).
    #[allow(dead_code)]
    dir_guard: Option<Arc<DirGuard>>,
}

impl Shared {
    pub(crate) fn connection_closed(&self) {
        self.connections.fetch_sub(1, SeqCst);
    }

    pub(crate) fn batching(&self) -> Batching {
        *lock(&self.batching)
    }

    pub(crate) fn emit(&self, e: Event) {
        if let Some(h) = &self.events {
            h(&e);
        }
    }

    /// The current generation's `-wal` bytes: what the checkpoints of `maintenance` measure.
    pub(crate) fn maintenance_wal_bytes(&self) -> u64 {
        self.wal_bytes.load(SeqCst)
    }

    /// The current generation's log objects (formats 1 to 4 measure them too) and `-wal`
    /// bytes.
    pub(crate) fn maintenance_measures(&self) -> (u64, u64) {
        (
            self.uploader.generation_log_objects(),
            self.maintenance_wal_bytes(),
        )
    }

    /// After a `Connection` call: wake the maintenance thread if a checkpoint is due (atomic
    /// loads only, unless one is).
    pub(crate) fn poke_maintenance(&self) {
        if let Some(m) = &self.maintenance {
            let (log_objects, wal_bytes) = self.maintenance_measures();
            m.poke(log_objects, wal_bytes);
        }
    }

    /// Whether the packer thread and the pack gauges run (tests).
    #[cfg(test)]
    pub(crate) fn packer_threads(&self) -> (bool, bool) {
        lock(&self.open)
            .as_ref()
            .and_then(|o| o.packer.as_ref())
            .map_or((false, false), PackerThreads::running)
    }

    #[cfg(test)]
    pub(crate) fn maintenance(&self) -> Option<&Maintenance> {
        self.maintenance.as_ref()
    }

    /// The engine, while the database is open (maintenance connects to it).
    pub(crate) fn engine_db(&self) -> Option<EngineDb> {
        lock(&self.open).as_ref().map(|o| o.engine.db.clone())
    }

    /// Ask the prefetch thread to stop, without waiting for it (the engine's drop joins it).
    pub(crate) fn stop_prefetch(&self) {
        if let Some(o) = lock(&self.open).as_ref() {
            o.engine.stop_prefetch();
        }
    }

    /// Stop maintenance, drop the engine (and with it the engine's registry entry), drain the
    /// uploader, stop the trimmer, unlock `durable.lock` and leave the process registry.
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
            packer,
            guard,
        } = open;
        // Now, so that a pack run in progress stops while the rest shuts down.
        if let Some(p) = &packer {
            p.signal_stop();
        }
        // Before the engine drops: a run in progress ends first (it holds its own engine
        // connection), and none starts after this.
        if let Some(m) = &self.maintenance {
            m.stop();
        }
        drop(engine);
        // Runs every queued job; the uploader thread's end drops the last trim sender.
        self.uploader.shutdown();
        if let Some(t) = trimmer {
            let _ = t.join();
        }
        if let Some(p) = packer {
            p.join();
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
///
/// Opening an empty directory whose bucket prefix holds a database, or a directory restored
/// by [`crate::restore`] and never opened, takes the database over as [`crate::takeover`]
/// does: this process claims it under a new writer id at the next epoch and appends a fence
/// record, and a writer still running elsewhere is fenced for good (its next commit fails
/// `Fenced`). A takeover interrupted by a crash resumes at the next open. [`is_claimed`]
/// tells the cases apart beforehand.
pub fn open(config: Config) -> Result<Database> {
    let store = config.s3_store()?;
    open_with_store(config, store)
}

/// Whether `dir` holds a database a writer already owns, so a writer [`open`] continues it.
/// False for a missing or empty directory, one restored and never opened as a writer, one
/// whose takeover was interrupted (its claim id is set), or one an interrupted restore left:
/// there `open` would restore, claim or create the database, taking it over from any writer
/// still running. It reads only local files.
pub fn is_claimed(dir: &Path) -> Result<bool> {
    if dir.join(RESTORING_FILE).exists() {
        return Ok(false);
    }
    Ok(LocalState::read(dir)?.is_some_and(|s| s.claimed && s.claim_id.is_empty()))
}

#[doc(hidden)]
pub fn open_with_store(config: Config, store: Arc<dyn ObjectStore>) -> Result<Database> {
    open_impl(config, store, false)
}

/// Open for reading only (verification): nothing is ever written to the bucket. The
/// directory is not claimed, no GC runs, every statement that writes is refused, and
/// close never checkpoints. An MVCC database's own recovery at open may still write the
/// local files (backfill into `app.db`, the log header, a cut of `-wal`); those writes
/// stay local, are never uploaded and never seal. A read-only open never creates
/// `app.db-log`. A WAL database's files are written less: reconcile still replaces a `-wal`
/// that is behind the bucket's image (and records `complete=false` in `app.db-durable`)
/// before the engine opens, and a local file ahead of the bucket is refused, not uploaded.
#[doc(hidden)]
pub fn open_read_only(config: Config) -> Result<Database> {
    let store = config.s3_store()?;
    open_read_only_with_store(config, store)
}

#[doc(hidden)]
pub fn open_read_only_with_store(config: Config, store: Arc<dyn ObjectStore>) -> Result<Database> {
    open_impl(config, store, true)
}

/// How many times a read-only open of an empty directory restores when the head moves during
/// its restore (`Error::Stale`).
const READ_ONLY_RESTORES: u32 = 3;

fn open_impl(config: Config, store: Arc<dyn ObjectStore>, read_only: bool) -> Result<Database> {
    config.validate()?;
    let encryption = encryption::load(&config)?;
    let guard = DirGuard::acquire(&config.dir)?;
    let dir = guard.dir().to_path_buf();
    let mut cfg = config.engine(if read_only {
        crate::io::IoMode::ReadOnly
    } else {
        crate::io::IoMode::Writer
    });
    let keys = config.keys_for(encryption.as_deref());
    cfg.encryption = encryption;
    clear_interrupted_restore(&dir)?;
    clear_interrupted_create(&dir)?;
    if !read_only {
        // A `BUCKWAL_PACK_DIR` that cannot work fails the open before any bucket request.
        config.prepare_pack_dir()?;
        crate::pack::run::sweep_work_dirs(&config);
    }
    let local = LocalState::read(&dir)?;
    // The recorded mode first: an MVCC directory opened as WAL is "switching back", not just
    // an MVCC file. Before the claim, the epoch raise or any other write to the bucket.
    if let Some(state) = &local {
        refuse_journal_mode_mismatch(state_mode(state), cfg.journal_mode)?;
    }
    refuse_mvcc_file(&dir.join(DB_FILE), cfg.journal_mode)?;

    let (bucket, state, reconciled) = match local {
        Some(state) => {
            refuse_encryption_mismatch(&dir, &store, &keys, &cfg, None)?;
            attached(attach_existing(&dir, &store, &keys, &cfg, state)?)
        }
        None if dir.join(DB_FILE).exists() => {
            return Err(Error::Config(format!(
                "{} holds an app.db not created by buckwal",
                dir.display()
            )))
        }
        None if head_exists(&*store, &keys, &cfg)? => {
            let mut attempt = 1;
            loop {
                let staged = restore_staged(store.clone(), &keys, &dir, &cfg)?;
                let state = LocalState::read(&dir)?
                    .ok_or_else(|| Error::Corrupt("the restore wrote no local state".into()))?;
                refuse_encryption_mismatch(&dir, &store, &keys, &cfg, staged.page1)?;
                match attach_existing(&dir, &store, &keys, &cfg, state) {
                    // A read-only open writes nothing to the bucket: when the writer switched
                    // the generation during this open's own restore, the copy is removed and
                    // restored again.
                    Err(Error::Stale(m)) if read_only && attempt < READ_ONLY_RESTORES => {
                        tracing::info!(target: "buckwal", reason = %m, "read-only open: the head moved during the restore; restoring again");
                        remove_restored(&dir)?;
                        attempt += 1;
                    }
                    r => break attached(r?),
                }
            }
        }
        None if read_only => {
            return Err(Error::NotFound(format!(
                "no database in the bucket at prefix {:?}",
                config.prefix
            )))
        }
        None if cfg.journal_mode == JournalMode::Mvcc => {
            // Created, then opened by `open_engine` in MVCC mode like any other.
            let (bucket, state) = create_new_mvcc(&dir, &store, &keys, &cfg)?;
            (bucket, state, None)
        }
        None => {
            let (bucket, state) = create_new(&dir, &store, &keys, &cfg)?;
            (bucket, state, None)
        }
    };

    // GC on the writer's own thread (never a read-only open's, never on a commit's path).
    let (trim_tx, trim_rx) = if read_only || crate::trim::no_writer_gc() {
        (None, None)
    } else {
        let (tx, rx) = mpsc::channel();
        (Some(tx), Some(rx))
    };
    // Known before the engine opens: a restored or attached copy that is incomplete (pages
    // still to prefetch) or has a non-empty `app.db` is not fresh, so `allocate_page_one`
    // has nothing to do and would only wait on the prefetch's page-cache lock. A crashed
    // fresh create (complete, empty `app.db`) still gets page 1.
    let known_not_fresh = reconciled.is_some()
        && (!state.complete || fs::metadata(dir.join(DB_FILE)).is_ok_and(|m| m.len() > 0));
    let engine = open_engine(&dir, bucket, &state, &cfg, trim_tx)?;
    // open_engine sent the open's wake: the trimmer's pass at start.
    let trimmer = trim_rx.map(|rx| {
        let storage = engine.storage.clone();
        spawn_trimmer(
            store.clone(),
            keys.clone(),
            crate::gc::GcAges::new(cfg.gc_min_age(), config.pack_orphan_grace)
                .with_pack_keep(config.pack_keep),
            rx,
            cfg.events.clone(),
            Arc::new(move || storage.reading_pack()),
        )
    });
    // Packs only in a format 5 bucket (formats 1 to 4 and MVCC formats 2 and 4 seal), and only
    // on a writer. The packer thread only with `config.pack` (`BUCKWAL_PACK`); the gauges
    // always. Neither does anything before its first interval has passed.
    let packer = (!read_only && engine.storage.packed()).then(|| {
        PackerThreads::spawn(
            &config,
            store.clone(),
            keys.clone(),
            engine.uploader.bucket_counters(),
        )
    });
    let uploader = engine.uploader.clone();
    let wal_bytes = engine.wal_bytes.clone();
    // PASSIVE checkpoints only where a WAL restart uploads no page (format 5, WAL mode); the
    // fixed log-object trigger only where no pack bounds a restore (formats 1 to 4).
    let maintenance = MaintenanceConfig::from_config(&config, engine.storage.packed());
    tracing::info!(
        target: "buckwal",
        generation = uploader.generation(),
        writer_id = %state.writer_id,
        read_only,
        dir = %dir.display(),
        "database opened"
    );
    let db = Database {
        shared: Arc::new_cyclic(|weak| Shared {
            read_only,
            store,
            stats: uploader.stats(),
            uploader,
            gate: Arc::default(),
            // It wakes only on a call's poke, so never before this Arc exists.
            maintenance: (!read_only && maintenance.enabled())
                .then(|| Maintenance::spawn(weak.clone(), maintenance)),
            wal_bytes,
            open: Mutex::new(Some(Open {
                engine,
                trimmer,
                packer,
                guard: Some(guard),
            })),
            connections: AtomicUsize::new(0),
            batching: Mutex::new(config.batching),
            journal_mode: config.journal_mode,
            events: cfg.events.clone(),
            reconciled,
            dir_guard: None,
        }),
    };
    if !read_only && !known_not_fresh {
        allocate_page_one(&db)?;
    }
    Ok(db)
}

/// A replica's database: the engine opened in `IoMode::Replica`, read-only for every
/// connection handed out, with no maintenance, trimmer or writer attach.
pub(crate) fn replica_database(
    store: Arc<dyn ObjectStore>,
    engine: Engine,
    cfg: &EngineConfig,
    guard: Arc<DirGuard>,
) -> Database {
    let uploader = engine.uploader.clone();
    let wal_bytes = engine.wal_bytes.clone();
    Database {
        shared: Arc::new(Shared {
            read_only: true,
            store,
            stats: uploader.stats(),
            uploader,
            gate: Arc::default(),
            maintenance: None,
            wal_bytes,
            open: Mutex::new(Some(Open {
                engine,
                trimmer: None,
                packer: None,
                guard: None,
            })),
            connections: AtomicUsize::new(0),
            batching: Mutex::new(Batching::Off),
            journal_mode: JournalMode::Wal,
            events: cfg.events.clone(),
            reconciled: None,
            dir_guard: Some(guard),
        }),
    }
}

/// Allocate page 1 of a fresh database (`PRAGMA page_count` is 0) on a connection of its own,
/// alone, before the open returns. turso_core 0.8.3 checks, then locks, in
/// `maybe_allocate_page1`, so the first writes to a fresh database on several connections at
/// once can allocate it twice and panic (pager.rs:3290-3297, 5644-5648). `BEGIN IMMEDIATE`
/// allocates it (`begin_write_tx`) and `ROLLBACK` commits nothing, so no ack waits on any
/// batching. A database that is not fresh is only read: the check is one `PRAGMA page_count`
/// and starts no seal of its own (its automatic WAL actions are off, below). The connection
/// is dropped before this returns, and is the only one then, so its close would be the last
/// and checkpoint the WAL, which commits a log record: its automatic WAL actions are
/// disabled, and the open commits nothing for it. The open skips it for a database it knows
/// is not fresh (restored or attached, with pages to fetch or an `app.db` not empty): the
/// prefetch holds the page-cache lock across each page GET, so the read would wait for about
/// the whole prefetch. Only a writer's open calls it: a read-only open refuses `BEGIN
/// IMMEDIATE`, and every other write, so it has nothing to race.
fn allocate_page_one(db: &Database) -> Result<()> {
    let conn = db.connect_with(|core| core.wal_auto_actions_disable())?;
    let rows = conn.query("PRAGMA page_count", &[])?;
    if rows
        .first()
        .and_then(|r| r.first())
        .and_then(crate::Value::as_int)
        != Some(0)
    {
        return Ok(());
    }
    match conn.execute("BEGIN IMMEDIATE", &[]) {
        Err(Error::Unsupported(_)) => return Ok(()),
        r => r?,
    };
    conn.execute("ROLLBACK", &[])?;
    Ok(())
}

/// What `open_impl` keeps of an `Attached`.
fn attached(a: Attached) -> (Bucket, LocalState, Option<Reconciled>) {
    (a.bucket, a.state, Some(a.reconciled))
}

/// Bytes 18/19 of an existing `app.db`, read before the engine opens it: 255 is MVCC (D5).
/// With no head to compare to, an MVCC file is refused when this open is a WAL one. An MVCC
/// open of one is left to the engine open.
fn refuse_mvcc_file(path: &Path, config_mode: JournalMode) -> Result<()> {
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
    if header.len() == 20
        && is_mvcc_header([header[18], header[19]])
        && config_mode == JournalMode::Wal
    {
        return Err(Error::Unsupported("MVCC"));
    }
    Ok(())
}

/// Bytes 18/19 of page 1: 255 in either is the MVCC header (D5).
pub(crate) fn is_mvcc_header(bytes: [u8; 2]) -> bool {
    bytes[0] == 255 || bytes[1] == 255
}

/// The journal mode a local state records: format 2 is an MVCC database.
fn state_mode(state: &LocalState) -> JournalMode {
    if state.format == FORMAT_V2 {
        JournalMode::Mvcc
    } else {
        JournalMode::Wal
    }
}

/// An existing database is never switched between modes in either direction.
fn refuse_journal_mode_mismatch(stored: JournalMode, config: JournalMode) -> Result<()> {
    match (stored, config) {
        (JournalMode::Wal, JournalMode::Mvcc) => Err(Error::Unsupported(
            "MVCC: switching an existing WAL database",
        )),
        (JournalMode::Mvcc, JournalMode::Wal) => {
            Err(Error::Unsupported("MVCC: switching back to WAL"))
        }
        _ => Ok(()),
    }
}

/// The bucket's head (and page 1 bytes 18/19, when the caller holds page 1) against the mode
/// this open asks for. The head's mode decides: a WAL bucket opened as MVCC and an MVCC bucket
/// opened as WAL are both refused. A WAL bucket whose page 1 carries the MVCC header is
/// refused too (`Unsupported("MVCC")`). Nothing is written before this runs.
pub(crate) fn refuse_mode_mismatch(
    head: &Head,
    header_bytes_18_19: Option<[u8; 2]>,
    config_mode: JournalMode,
) -> Result<()> {
    refuse_journal_mode_mismatch(head.journal_mode, config_mode)?;
    match header_bytes_18_19 {
        Some(h) if is_mvcc_header(h) && head.journal_mode == JournalMode::Wal => {
            Err(Error::Unsupported("MVCC"))
        }
        _ => Ok(()),
    }
}

/// Page 1 against the encryption setting (`encryption::check_page_one`) before a writer open
/// writes anything to the bucket: its claim, epoch raise or fence would otherwise land before
/// `open_engine`'s check refuses the open. Page 1 comes from the local files, else `restored`
/// (the page this open's own restore saw: no extra GET). A writer open of a directory restored
/// earlier that holds none (the bucket's manifest has it) GETs the sealed page 1
/// (`sealed_page_one`). A read-only open writes nothing to the bucket and leaves that case to
/// `open_engine`.
fn refuse_encryption_mismatch(
    dir: &Path,
    store: &Arc<dyn ObjectStore>,
    keys: &Keys,
    cfg: &EngineConfig,
    restored: Option<Vec<u8>>,
) -> Result<()> {
    let page1 = match encryption::file_page_one(dir)?.or(restored) {
        Some(p) => Some(p),
        None if cfg.read_only => None,
        None => sealed_page_one(&**store, keys, &cfg.retry)?,
    };
    check_page_one(page1.as_deref(), cfg.encryption.as_deref())
}

fn head_exists(store: &dyn ObjectStore, keys: &Keys, cfg: &EngineConfig) -> Result<bool> {
    let key = keys.head();
    Ok(with_retry(&cfg.retry, &key, |_| store.get(&key))?.is_some())
}

/// 16 random bytes from the platform, in hex.
pub(crate) fn new_writer_id() -> Result<String> {
    let mut id = [0u8; 16];
    PlatformIO::new()
        .map_err(|e| io_err("platform IO", e))?
        .fill_bytes(&mut id);
    Ok(hex::encode(id))
}

/// A new database: a new writer id, the head at generation 1 and epoch 1, then the local
/// state.
fn create_new(
    dir: &Path,
    store: &Arc<dyn ObjectStore>,
    keys: &Keys,
    cfg: &EngineConfig,
) -> Result<(Bucket, LocalState)> {
    let writer_id = new_writer_id()?;
    let mut bucket = Bucket::create_with(
        store.clone(),
        keys.clone(),
        cfg.retry.clone(),
        cfg.upload_concurrency,
        writer_id.clone(),
        timeouts_of(cfg),
    )?;
    bucket.set_lease(cfg.lease);
    let state = LocalState {
        format: FORMAT,
        writer_id,
        generation: 1,
        complete: true,
        claimed: true,
        head_etag: String::new(),
        epoch: bucket.head().epoch,
        claim_id: String::new(),
        seal_not_before_ms: 0,
        seal_quiet_ms: 0,
        next_generation_floor: 0,
    };
    state.write_atomic(dir)?;
    Ok((bucket, state))
}

/// A new MVCC database (`create`): the empty image built in `<dir>/app.db-creating`, the
/// bucket created with it as its first pack (`Bucket::create_packed_mvcc_with`: the pack, its
/// codes, the pointer, then the head, last), then the local state (format 2, at the head's
/// generation, claimed, complete) written into the staging directory and the files moved in
/// (`create::move_created`). Nothing goes to the bucket before the image is whole; a failure or
/// crash before the head leaves only the staging directory (and an orphan pack, and maybe a
/// pointer no head names), which the next open removes and a retry replaces. A crash after
/// the head leaves a bucket the next open restores. (Tests of the seal formats create a format
/// 2 or 4 bucket as a seal instead, `Bucket::create_sealed_with`.)
fn create_new_mvcc(
    dir: &Path,
    store: &Arc<dyn ObjectStore>,
    keys: &Keys,
    cfg: &EngineConfig,
) -> Result<(Bucket, LocalState)> {
    let staging = create::staging_dir(dir);
    let image = create::build_mvcc_image(&staging, cfg.encryption.as_deref())?;
    let writer_id = new_writer_id()?;
    let mut bucket = match create_sealed_mvcc(store, keys, cfg, &writer_id, &image)? {
        Some(b) => b,
        None => Bucket::create_packed_mvcc_with(
            store.clone(),
            keys.clone(),
            cfg.retry.clone(),
            cfg.upload_concurrency,
            writer_id.clone(),
            &staging.join(DB_FILE),
            image.page_size,
            timeouts_of(cfg),
        )?,
    };
    bucket.set_lease(cfg.lease);
    let state = LocalState {
        format: FORMAT_V2,
        writer_id,
        generation: bucket.head().generation,
        complete: true,
        claimed: true,
        head_etag: String::new(),
        epoch: bucket.head().epoch,
        claim_id: String::new(),
        seal_not_before_ms: 0,
        seal_quiet_ms: 0,
        next_generation_floor: 0,
    };
    state.write_atomic(&staging)?;
    create::move_created(&staging, dir)?;
    Ok((bucket, state))
}

/// Tests of the seal formats (`LegacySeal`, `LegacyFormat3`): the bucket of a new MVCC
/// database created as a seal of generation 1 (format 2, 4 or 3), as before format 5. `None`
/// otherwise: a new MVCC database is a format 5 one.
#[cfg(test)]
fn create_sealed_mvcc(
    store: &Arc<dyn ObjectStore>,
    keys: &Keys,
    cfg: &EngineConfig,
    writer_id: &str,
    image: &create::Image,
) -> Result<Option<Bucket>> {
    if keys.new_mvcc_packed() {
        return Ok(None);
    }
    Bucket::create_sealed_with(
        store.clone(),
        keys.clone(),
        cfg.retry.clone(),
        cfg.upload_concurrency,
        writer_id.to_string(),
        image.clone(),
        timeouts_of(cfg),
    )
    .map(Some)
}

#[cfg(not(test))]
fn create_sealed_mvcc(
    _: &Arc<dyn ObjectStore>,
    _: &Keys,
    _: &EngineConfig,
    _: &str,
    _: &create::Image,
) -> Result<Option<Bucket>> {
    Ok(None)
}

/// What `attach_existing` hands on.
pub(crate) struct Attached {
    pub bucket: Bucket,
    pub state: LocalState,
    pub reconciled: Reconciled,
    /// The fence record this open appended: a takeover's, or the one a writer open appends
    /// when its generation's last record is another writer's.
    pub fence: Option<FenceOutcome>,
    /// The takeover quiet this open's fence set (`ZERO` for none, and without a fence).
    pub quiet: Duration,
}

/// The directory's own database, in this order:
///
/// 1. A writer open of a restored copy (unclaimed) takes it over first (`claim`): the head
///    names a new writer id at the next epoch.
/// 2. Attach as the directory's writer id, with the directory's epoch as the least it
///    accepts in the head. A read-only open attaches as the directory's writer id,
///    unclaimed, and never writes anything to the bucket.
/// 3. A writer open that did not take over raises the head's epoch (`Bucket::raise_epoch`),
///    then records the new epoch in the local state (after the raise, so the local epoch
///    never exceeds the head's). A head PUT that an earlier process of this writer still had
///    in flight then gets a 412 instead of landing on a generation this process appends to.
///    If such a PUT of a seal landed between the attach and the raise, the open attaches
///    again, at the sealed generation, and raises that head.
/// 4. A takeover, or a writer open whose generation's last record is another writer's (a
///    takeover whose fence never landed), appends a fence record (`Bucket::append_fence`):
///    a still-running old writer's next append collides with it and is fenced for good, so
///    it can never ack into a generation this writer will seal. Records the old writer
///    landed first are stepped over.
/// 5. The takeover quiet (`set_quiet`): a fence before which another writer's last record is a
///    seal-start record (`FenceOutcome::seal_may_be_in_flight`) keeps this writer from sealing
///    the generation for `lease + request_timeout`, persisted as `seal_not_before_ms` (its
///    end) and `seal_quiet_ms` (its length); without a fence, a quiet still pending for the
///    generation is waited again in full from this open, its persisted length on the
///    monotonic clock.
/// 6. Reconcile, after the fence: whatever the old writer acked before the fence (after the
///    restore) comes in as `Behind`.
pub(crate) fn attach_existing(
    dir: &Path,
    store: &Arc<dyn ObjectStore>,
    keys: &Keys,
    cfg: &EngineConfig,
    mut state: LocalState,
) -> Result<Attached> {
    let generation = state.generation;
    let (mut bucket, fence) = if !state.claimed && !cfg.read_only {
        let (bucket, fence) = claim(dir, store, keys, cfg, &mut state)?;
        (bucket, Some(fence))
    } else {
        let mut bucket = attach(store, keys, cfg, &state)?;
        let mut fence = None;
        if !cfg.read_only {
            state.epoch = raise_at_open(store, keys, cfg, &state, &mut bucket)?;
            state.write_atomic(dir)?;
            if last_record_is_foreign(&bucket, &state.writer_id, cfg)? {
                set_provisional_quiet(cfg, None, &mut state);
                state.write_atomic(dir)?;
                fence = Some(resume_takeover(cfg, &state.writer_id, &mut bucket)?);
            }
        }
        (bucket, fence)
    };
    let quiet = if cfg.read_only {
        Duration::ZERO
    } else {
        set_quiet(
            dir,
            cfg,
            &mut state,
            &mut bucket,
            fence.as_ref(),
            generation,
        )?
    };
    let reconciled = reconcile(dir, &mut bucket, &mut state, cfg)?;
    Ok(Attached {
        bucket,
        state,
        reconciled,
        fence,
        quiet,
    })
}

/// This writer's own timeouts, written into every head it writes.
fn timeouts_of(cfg: &EngineConfig) -> Timeouts {
    Timeouts {
        lease: cfg.lease,
        request_timeout: cfg.request_timeout,
    }
}

/// This writer's own takeover quiet: how long a writer whose fence may follow another writer's
/// seal still in flight must not seal that generation (`Bucket::set_seal_not_before`), when
/// nothing is known of the replaced writer's timeouts.
fn quiet_of(cfg: &EngineConfig) -> Duration {
    timeouts_of(cfg).sum()
}

/// The takeover quiet persisted before a fence is appended, in case the open crashes between
/// the fence and `set_quiet`: the next open then finds its own fence as the last record (no
/// fence again) and waits the persisted length (`set_quiet`). `set_quiet` replaces it.
///
/// It is the larger of both writers' `lease + request_timeout`: the replaced head's
/// (`replaced`, from the takeover's head write) and this writer's own. Where the replaced head
/// is not known (a resumed claim, a takeover whose fence never landed) a pending quiet already
/// persisted counts too, so a restart does not shorten it.
fn set_provisional_quiet(cfg: &EngineConfig, replaced: Option<&Head>, state: &mut LocalState) {
    let pending = if state.seal_not_before_ms != 0 {
        Duration::from_millis(state.seal_quiet_ms)
    } else {
        Duration::ZERO
    };
    let quiet = timeouts_of(cfg).quiet_for(replaced).max(pending);
    state.seal_not_before_ms = bucket::unix_ms(SystemTime::now() + quiet);
    state.seal_quiet_ms = duration_ms(quiet);
}

/// The persisted takeover quiet length (`seal_quiet_ms`), this writer's own sum when absent.
fn pending_quiet(cfg: &EngineConfig, state: &LocalState) -> Duration {
    match state.seal_quiet_ms {
        0 => quiet_of(cfg),
        ms => Duration::from_millis(ms),
    }
}

/// `d` in whole ms, at least 1 for a nonzero `d`, so a pending quiet's length is never read
/// back as absent (0).
fn duration_ms(d: Duration) -> u64 {
    let ms = u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    if ms == 0 && !d.is_zero() {
        1
    } else {
        ms
    }
}

/// Step 5 of `attach_existing`. With a `fence` from this open: when another writer may still
/// have a seal of the generation in flight (`FenceOutcome::seal_may_be_in_flight`), the quiet
/// is `lease + request_timeout` from now, persisted as `state.seal_not_before_ms` (unix ms,
/// its end) and `state.seal_quiet_ms` (its length), and set on the bucket as an `Instant`;
/// otherwise 0 is persisted for both (replacing the provisional quiet). Returns the quiet set.
///
/// Without a fence: a quiet pending for `generation` (the local state's at open), when the
/// head is still at it (`seal_not_before_ms` not 0, so no seal of the generation cleared it),
/// is waited again in full from this open: its persisted length (`seal_quiet_ms`; this
/// process's `lease + request_timeout` when the key is absent), on the monotonic clock. The
/// wall-clock deadline is never read back as a remainder: a wall clock stepped forward since
/// it was written would shorten the quiet (to nothing), and one stepped back would stretch
/// it; a reopen with a shorter lease cannot cut it either. The new end and the length are
/// persisted. One of an older generation is dropped (0). The seal of the generation writes 0
/// to both keys (`BucketDbStorage::seal`).
fn set_quiet(
    dir: &Path,
    cfg: &EngineConfig,
    state: &mut LocalState,
    bucket: &mut Bucket,
    fence: Option<&FenceOutcome>,
    generation: u64,
) -> Result<Duration> {
    let Some(fence) = fence else {
        if state.seal_not_before_ms == 0 || bucket.head().generation != generation {
            state.seal_not_before_ms = 0;
            state.seal_quiet_ms = 0;
            return Ok(Duration::ZERO);
        }
        let length = pending_quiet(cfg, state);
        let until = Instant::now() + length;
        state.seal_not_before_ms = bucket::unix_ms(SystemTime::now() + length);
        state.seal_quiet_ms = duration_ms(length);
        state.write_atomic(dir)?;
        bucket.set_seal_not_before(generation, until);
        tracing::info!(
            target: "buckwal",
            generation,
            ms = state.seal_quiet_ms,
            "takeover quiet: pending at open, waited again in full"
        );
        return Ok(Duration::ZERO);
    };
    let quiet = if fence.seal_may_be_in_flight {
        // The provisional quiet persisted before the fence: both writers' larger sum.
        pending_quiet(cfg, state)
    } else {
        Duration::ZERO
    };
    let (until, wall) = (Instant::now() + quiet, SystemTime::now() + quiet);
    state.seal_not_before_ms = if quiet.is_zero() {
        0
    } else {
        bucket::unix_ms(wall)
    };
    state.seal_quiet_ms = duration_ms(quiet);
    state.write_atomic(dir)?;
    if !quiet.is_zero() {
        bucket.set_seal_not_before(fence.pos.generation, until);
        tracing::info!(
            target: "buckwal",
            generation = fence.pos.generation,
            ms = quiet.as_millis() as u64,
            "takeover quiet: the old writer's seal may still be uploading"
        );
    }
    Ok(quiet)
}

/// `Bucket::attach` as `state`'s writer id, with its epoch as the least accepted, and the
/// configured lease (before any record goes out).
fn attach(
    store: &Arc<dyn ObjectStore>,
    keys: &Keys,
    cfg: &EngineConfig,
    state: &LocalState,
) -> Result<Bucket> {
    let mut bucket = Bucket::attach(
        store.clone(),
        keys.clone(),
        cfg.retry.clone(),
        cfg.upload_concurrency,
        &state.writer_id,
        state.epoch,
    )?;
    bucket.set_lease(cfg.lease);
    bucket.set_request_timeout(cfg.request_timeout);
    Ok(bucket)
}

/// The open-time epoch raise. When the raise finds this writer's own sealed head instead (a
/// crashed seal's late head PUT landed after the attach read the head), attach again, once,
/// and raise that head. Returns the new epoch.
fn raise_at_open(
    store: &Arc<dyn ObjectStore>,
    keys: &Keys,
    cfg: &EngineConfig,
    state: &LocalState,
    bucket: &mut Bucket,
) -> Result<u64> {
    if let Some(epoch) = bucket.raise_epoch()? {
        return Ok(epoch);
    }
    tracing::info!(
        target: "buckwal",
        // The head the landed seal published: the stale head's seal target.
        generation = bucket.seal_target(),
        "a seal of an earlier process landed during the open; attaching again"
    );
    *bucket = attach(store, keys, cfg, state)?;
    bucket
        .raise_epoch()?
        .ok_or_else(|| Error::Fenced("the head moved again during the open; open again".into()))
}

/// A takeover of a restored copy, in a `buckwal.takeover` span (`buckwal.generation`,
/// `buckwal.epoch`, `buckwal.seq` (the fence's), `buckwal.retries`, `s3.status`,
/// `buckwal.outcome` (`ok`, `integrity` for a failed integrity check, else `error`), and the
/// takeover quiet and the floor: `buckwal.quiet_ms` (the quiet this takeover sets, 0 when the
/// old writer had no seal in flight), `buckwal.quiet_old_ms` (the replaced head's
/// `lease_ms + request_timeout_ms`, absent when it had none), `buckwal.quiet_own_ms` (this
/// writer's sum) and `buckwal.next_generation_floor` (the floor the takeover head carries)):
///
/// 1. The claim id: `state.claim_id`, an interrupted claim's, or a new id persisted as
///    `claim_id` before anything is sent, so a crash after the head write resumes under the
///    same id (`take_over_counted` returns a head that already names it).
/// 2. The head write (`bucket::take_over_counted`), on the restored generation; then the
///    claimed state `{writer_id: claim_id, claimed: true, head_etag, epoch, claim_id: ""}`.
/// 3. Attach, and the fence record (`Bucket::append_fence`).
///
/// `buckwal.epoch` is recorded as soon as the head write has landed, so a takeover that fails
/// after it (a fence `Timeout`) still reports the epoch it took.
///
/// It emits `Event::TookOver` and logs the takeover.
fn claim(
    dir: &Path,
    store: &Arc<dyn ObjectStore>,
    keys: &Keys,
    cfg: &EngineConfig,
    state: &mut LocalState,
) -> Result<(Bucket, FenceOutcome)> {
    let span = takeover_span(state.generation);
    let retries = AtomicU64::new(0);
    let r = span.in_scope(|| claim_in_span(dir, store, keys, cfg, state, &retries, &span));
    span.record("buckwal.retries", retries.load(SeqCst));
    if let Ok((_, fence)) = &r {
        // What `set_quiet` will set: the provisional quiet when the old writer's seal may
        // still be uploading, else none.
        let quiet = if fence.seal_may_be_in_flight {
            state.seal_quiet_ms
        } else {
            0
        };
        span.record("buckwal.quiet_ms", quiet);
    }
    let ended = r.as_ref().map(|(bucket, fence)| (bucket, fence));
    took_over(&span, cfg, &state.writer_id, ended);
    r
}

/// A `buckwal.takeover` span for `generation`, its other fields filled in as the takeover
/// goes.
fn takeover_span(generation: u64) -> tracing::Span {
    tracing::info_span!(
        target: "buckwal",
        "buckwal.takeover",
        buckwal.generation = generation,
        buckwal.epoch = Empty,
        buckwal.seq = Empty,
        buckwal.retries = Empty,
        s3.status = Empty,
        buckwal.outcome = Empty,
        buckwal.quiet_ms = Empty,
        buckwal.quiet_old_ms = Empty,
        buckwal.quiet_own_ms = Empty,
        buckwal.next_generation_floor = Empty,
    )
}

/// The end of a takeover in `span`: its outcome, and on success the fence's seq, the "took
/// over" log line and `Event::TookOver`.
fn took_over(
    span: &tracing::Span,
    cfg: &EngineConfig,
    writer_id: &str,
    ended: std::result::Result<(&Bucket, &FenceOutcome), &Error>,
) {
    let (bucket, fence) = match ended {
        Ok(ok) => ok,
        Err(e) => {
            span.record("buckwal.outcome", e.outcome());
            return;
        }
    };
    let (generation, epoch) = (bucket.head().generation, bucket.head().epoch);
    span.record("buckwal.seq", fence.pos.seq);
    span.record("buckwal.outcome", "ok");
    span.in_scope(|| {
        tracing::info!(
            target: "buckwal",
            generation,
            epoch,
            fence_seq = fence.pos.seq,
            writer_id,
            "took over"
        )
    });
    if let Some(h) = &cfg.events {
        h(&Event::TookOver {
            generation,
            epoch,
            fence_seq: fence.pos.seq,
        });
    }
}

/// The fence of a takeover resumed through `open`: the head already names this writer (its
/// claim's head write landed, and this open raised the epoch again), but the generation's
/// last record is still another writer's, because the claim's fence never landed. The fence
/// completes the takeover, so it runs in a `buckwal.takeover` span like `claim`'s (the head's
/// generation and epoch, the fence's seq, the open's retries so far) and emits
/// `Event::TookOver`.
fn resume_takeover(
    cfg: &EngineConfig,
    writer_id: &str,
    bucket: &mut Bucket,
) -> Result<FenceOutcome> {
    let span = takeover_span(bucket.head().generation);
    span.record("buckwal.epoch", bucket.head().epoch);
    span.record("buckwal.quiet_own_ms", duration_ms(timeouts_of(cfg).sum()));
    if let Some(floor) = bucket.head().next_generation_floor {
        span.record("buckwal.next_generation_floor", floor);
    }
    let r = span.in_scope(|| bucket.append_fence());
    span.record("buckwal.retries", bucket.counters().retries.load(SeqCst));
    took_over(&span, cfg, writer_id, r.as_ref().map(|f| (&*bucket, f)));
    r
}

fn claim_in_span(
    dir: &Path,
    store: &Arc<dyn ObjectStore>,
    keys: &Keys,
    cfg: &EngineConfig,
    state: &mut LocalState,
    retries: &AtomicU64,
    span: &tracing::Span,
) -> Result<(Bucket, FenceOutcome)> {
    // Known from the configuration: a takeover that fails before its head write has it too.
    span.record("buckwal.quiet_own_ms", duration_ms(timeouts_of(cfg).sum()));
    if state.claim_id.is_empty() {
        state.claim_id = new_writer_id()?;
        state.write_atomic(dir)?;
    }
    let (head, etag, replaced) = bucket::take_over_counted(
        &**store,
        keys,
        &cfg.retry,
        state.generation,
        &state.claim_id,
        timeouts_of(cfg),
        retries,
    )?;
    span.record("buckwal.epoch", head.epoch);
    if let Some(old) = replaced
        .as_ref()
        .and_then(|h| Some((h.lease_ms?, h.request_timeout_ms?)))
    {
        span.record("buckwal.quiet_old_ms", old.0.saturating_add(old.1));
    }
    if let Some(floor) = head.next_generation_floor {
        span.record("buckwal.next_generation_floor", floor);
    }
    #[cfg(test)]
    if FAIL_AFTER_CLAIM_HEAD.with(|f| f.get()) {
        return Err(Error::Io(
            "injected: crash after the claim's head write".into(),
        ));
    }
    state.writer_id = std::mem::take(&mut state.claim_id);
    state.claimed = true;
    state.head_etag = etag;
    state.epoch = head.epoch;
    // Kept locally too: a restart before the first seal keeps it, and reconcile tells the
    // skip seal's head from a stale directory by it.
    state.next_generation_floor = head.next_generation_floor.unwrap_or(0);
    set_provisional_quiet(cfg, replaced.as_ref(), state);
    state.write_atomic(dir)?;
    let mut bucket = attach(store, keys, cfg, state)?;
    let fence = bucket.append_fence();
    retries.fetch_add(bucket.counters().retries.load(SeqCst), SeqCst);
    #[cfg(test)]
    if fence.is_ok() && FAIL_AFTER_CLAIM_FENCE.with(|f| f.get()) {
        return Err(Error::Io("injected: crash after the claim's fence".into()));
    }
    Ok((bucket, fence?))
}

#[cfg(test)]
thread_local! {
    /// Tests: `attach_existing` fails with `Io` right after the claim's head write, before
    /// the claimed local state is written (a crash mid-claim). Per thread, so parallel tests
    /// do not see it.
    pub(crate) static FAIL_AFTER_CLAIM_HEAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Tests: `attach_existing` fails with `Io` right after the claim's fence landed, before
    /// the takeover quiet is persisted (a crash mid-claim).
    pub(crate) static FAIL_AFTER_CLAIM_FENCE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The last record of the head's generation (an Extents or a fence record) was written by a
/// writer other than `writer_id`: a takeover by `writer_id` whose fence never landed.
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
    let gen_seq = (bucket.head().generation, seq - 1);
    Ok(
        match bucket
            .keys()
            .decode_record(&obj.body, gen_seq.0, gen_seq.1)?
        {
            LogRecord::Extents { writer_id: w, .. } | LogRecord::Fence { writer_id: w, .. } => {
                w != writer_id
            }
        },
    )
}

impl Database {
    #[cfg(test)]
    pub(crate) fn engine_io(&self) -> Arc<crate::io::DurableIo> {
        lock(&self.shared.open).as_ref().unwrap().engine.io.clone()
    }

    /// A new connection; open one per thread.
    pub fn connect(&self) -> Result<Connection> {
        self.connect_with(|_| {})
    }

    /// `connect`, with `setup` run on the engine connection before it is wrapped.
    fn connect_with(&self, setup: impl FnOnce(&turso_core::Connection)) -> Result<Connection> {
        let open = lock(&self.shared.open);
        let Some(open) = open.as_ref() else {
            return Err(Error::Config("the database is closed".into()));
        };
        crate::io::stash::clear();
        let core = open.engine.db.connect().map_err(engine_error)?;
        setup(&core);
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

    /// How many commits each log PUT carried and how long acks waited, since open.
    pub fn upload_stats(&self) -> UploadStats {
        let (commits_behind, oldest_behind) = self.shared.uploader.behind();
        UploadStats {
            commits_behind,
            oldest_behind,
            ..self.shared.stats.snapshot()
        }
    }

    /// The highest commit number whose log record is in the bucket, in this open. Every lower
    /// commit is in the bucket too. 0 before the first. Commit numbers are
    /// [`Connection::last_commit`]'s; they restart at 1 with every open.
    pub fn durable_commit(&self) -> u64 {
        self.shared.uploader.durable_commit()
    }

    /// Block until [`Database::durable_commit`] reaches `commit`. `Timeout` after `timeout`,
    /// `UploadFailed` while the bucket fails, `Fenced` once another writer took over.
    pub fn wait_durable(&self, commit: u64, timeout: Duration) -> Result<()> {
        self.shared.uploader.wait_durable(commit, timeout)
    }

    /// Commit with `b` from the next commit of every connection without a setting of its
    /// own ([`Connection::set_batching`]); a commit already waiting keeps the setting it was
    /// made with. Starts as [`Config::batching`]. `Config` when `b` is invalid.
    pub fn set_batching(&self, b: Batching) -> Result<()> {
        b.validate()?;
        b.check_mode(self.shared.journal_mode)?;
        *lock(&self.shared.batching) = b;
        Ok(())
    }

    /// The database's setting ([`Database::set_batching`]).
    pub fn batching(&self) -> Batching {
        self.shared.batching()
    }

    /// Close the database: drain the uploader, stop the trimmer, release the directory.
    /// Every [`Connection`] must be closed first (`Config` otherwise). Other `Database`
    /// handles see a closed database afterwards.
    ///
    /// Right after a takeover of a generation whose old writer was sealing it
    /// ([`crate::TakeoverReport::quiet`]), the close-time checkpoint's seal first waits out
    /// that quiet (`lease + request_timeout` after the takeover, or its full persisted length
    /// after an open that found it pending), so close can take that long.
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
