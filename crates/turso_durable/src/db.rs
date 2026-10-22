//! [`open`] and [`Database`]: the directory locks (D4), the order of an open, and shutdown.
use crate::batching::Batching;
use crate::bucket::{self, Bucket, FenceOutcome};
use crate::config::{Config, EngineConfig, Event, EventHook, JournalMode};
use crate::conn::Connection;
use crate::encryption::{self, check_page_one};
use crate::engine::{engine_error, open_engine, Engine, EngineDb};
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE, LOCK_FILE, RESTORING_FILE};
use crate::format::{decode_record, Head, Keys, LocalState, LogRecord, FORMAT, FORMAT_V2};
use crate::maintenance::{Gate, Maintenance, MaintenanceConfig};
use crate::reconcile::{reconcile, Reconciled};
use crate::restore::{clear_interrupted_restore, restore_staged, sealed_page_one};
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
    /// Dropped last: the lock outlives every file of the engine.
    guard: DirGuard,
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

    /// The current generation's log objects and `-wal` bytes: what maintenance measures.
    pub(crate) fn maintenance_measures(&self) -> (u64, u64) {
        (
            self.uploader.generation_log_objects(),
            self.wal_bytes.load(SeqCst),
        )
    }

    /// After a `Connection` call: wake maintenance if the generation is due (atomic loads
    /// only, unless it is).
    pub(crate) fn poke_maintenance(&self) {
        if let Some(m) = &self.maintenance {
            let (log_objects, wal_bytes) = self.maintenance_measures();
            m.poke(log_objects, wal_bytes);
        }
    }

    #[cfg(test)]
    pub(crate) fn maintenance(&self) -> Option<&Maintenance> {
        self.maintenance.as_ref()
    }

    /// The engine, while the database is open (maintenance connects to it).
    pub(crate) fn engine_db(&self) -> Option<EngineDb> {
        lock(&self.open).as_ref().map(|o| o.engine.db.clone())
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
            guard,
        } = open;
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
    let encryption = encryption::load(&config)?;
    let guard = DirGuard::acquire(&config.dir)?;
    let dir = guard.dir().to_path_buf();
    let mut cfg = config.engine(read_only);
    cfg.encryption = encryption;
    let keys = config.keys();
    clear_interrupted_restore(&dir)?;
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
                "{} holds an app.db not created by turso_durable",
                dir.display()
            )))
        }
        None if head_exists(&*store, &keys, &cfg)? => {
            let staged = restore_staged(store.clone(), &keys, &dir, &cfg)?;
            let state = LocalState::read(&dir)?
                .ok_or_else(|| Error::Corrupt("the restore wrote no local state".into()))?;
            refuse_encryption_mismatch(&dir, &store, &keys, &cfg, staged.page1)?;
            attached(attach_existing(&dir, &store, &keys, &cfg, state)?)
        }
        None if read_only => {
            return Err(Error::NotFound(format!(
                "no database in the bucket at prefix {:?}",
                config.prefix
            )))
        }
        None if cfg.journal_mode == JournalMode::Mvcc => {
            // A new MVCC database is created once the MVCC engine is wired in; until then it
            // must not become a WAL bucket.
            return Err(Error::Unsupported("MVCC"));
        }
        None => {
            let (bucket, state) = create_new(&dir, &store, &keys, &cfg)?;
            (bucket, state, None)
        }
    };

    // GC on the writer's own thread (never a read-only open's, never on a commit's path).
    let (trim_tx, trim_rx) = if read_only || !config.gc_after_seal {
        (None, None)
    } else {
        let (tx, rx) = mpsc::channel();
        (Some(tx), Some(rx))
    };
    let engine = open_engine(&dir, bucket, &state, &cfg, trim_tx)?;
    // open_engine sent the open's wake: the trimmer's pass at start.
    let trimmer = trim_rx.map(|rx| {
        spawn_trimmer(
            store.clone(),
            keys.clone(),
            cfg.gc_min_age(),
            rx,
            cfg.events.clone(),
        )
    });
    let uploader = engine.uploader.clone();
    let wal_bytes = engine.wal_bytes.clone();
    let maintenance = MaintenanceConfig::from_config(&config);
    tracing::info!(
        target: "turso_durable",
        generation = uploader.generation(),
        writer_id = %state.writer_id,
        read_only,
        dir = %dir.display(),
        "database opened"
    );
    Ok(Database {
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
                guard,
            })),
            connections: AtomicUsize::new(0),
            batching: Mutex::new(config.batching),
            journal_mode: config.journal_mode,
            events: cfg.events.clone(),
            reconciled,
        }),
    })
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
fn new_writer_id() -> Result<String> {
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
    let mut bucket = Bucket::create(
        store.clone(),
        keys.clone(),
        cfg.retry.clone(),
        cfg.upload_concurrency,
        writer_id.clone(),
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
    };
    state.write_atomic(dir)?;
    Ok((bucket, state))
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
                set_provisional_quiet(cfg, &mut state);
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

/// The takeover quiet: how long a writer whose fence may follow another writer's seal still in
/// flight must not seal that generation (`Bucket::set_seal_not_before`).
fn quiet_of(cfg: &EngineConfig) -> Duration {
    cfg.lease.saturating_add(cfg.request_timeout)
}

/// The takeover quiet persisted before a fence is appended, in case the open crashes between
/// the fence and `set_quiet`: the next open then finds its own fence as the last record (no
/// fence again) and waits the persisted length (`set_quiet`). `set_quiet` replaces it.
fn set_provisional_quiet(cfg: &EngineConfig, state: &mut LocalState) {
    let quiet = quiet_of(cfg);
    state.seal_not_before_ms = bucket::unix_ms(SystemTime::now() + quiet);
    state.seal_quiet_ms = duration_ms(quiet);
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
        let length = match state.seal_quiet_ms {
            0 => quiet_of(cfg),
            ms => Duration::from_millis(ms),
        };
        let until = Instant::now() + length;
        state.seal_not_before_ms = bucket::unix_ms(SystemTime::now() + length);
        state.seal_quiet_ms = duration_ms(length);
        state.write_atomic(dir)?;
        bucket.set_seal_not_before(generation, until);
        tracing::info!(
            target: "turso_durable",
            generation,
            ms = state.seal_quiet_ms,
            "takeover quiet: pending at open, waited again in full"
        );
        return Ok(Duration::ZERO);
    };
    let quiet = if fence.seal_may_be_in_flight {
        quiet_of(cfg)
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
            target: "turso_durable",
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
        target: "turso_durable",
        generation = bucket.head().generation + 1,
        "a seal of an earlier process landed during the open; attaching again"
    );
    *bucket = attach(store, keys, cfg, state)?;
    bucket
        .raise_epoch()?
        .ok_or_else(|| Error::Fenced("the head moved again during the open; open again".into()))
}

/// A takeover of a restored copy, in a `durable.takeover` span (`durable.generation`,
/// `durable.epoch`, `durable.seq` (the fence's), `durable.retries`, `s3.status`,
/// `durable.outcome`):
///
/// 1. The claim id: `state.claim_id`, an interrupted claim's, or a new id persisted as
///    `claim_id` before anything is sent, so a crash after the head write resumes under the
///    same id (`take_over_counted` returns a head that already names it).
/// 2. The head write (`bucket::take_over_counted`), on the restored generation; then the
///    claimed state `{writer_id: claim_id, claimed: true, head_etag, epoch, claim_id: ""}`.
/// 3. Attach, and the fence record (`Bucket::append_fence`).
///
/// `durable.epoch` is recorded as soon as the head write has landed, so a takeover that fails
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
    span.record("durable.retries", retries.load(SeqCst));
    let ended = r.as_ref().map(|(bucket, fence)| (bucket, fence));
    took_over(&span, cfg, &state.writer_id, ended);
    r
}

/// A `durable.takeover` span for `generation`, its other fields filled in as the takeover
/// goes.
fn takeover_span(generation: u64) -> tracing::Span {
    tracing::info_span!(
        target: "turso_durable",
        "durable.takeover",
        durable.generation = generation,
        durable.epoch = Empty,
        durable.seq = Empty,
        durable.retries = Empty,
        s3.status = Empty,
        durable.outcome = Empty,
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
    let Ok((bucket, fence)) = ended else {
        span.record("durable.outcome", "error");
        return;
    };
    let (generation, epoch) = (bucket.head().generation, bucket.head().epoch);
    span.record("durable.seq", fence.pos.seq);
    span.record("durable.outcome", "ok");
    span.in_scope(|| {
        tracing::info!(
            target: "turso_durable",
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
/// completes the takeover, so it runs in a `durable.takeover` span like `claim`'s (the head's
/// generation and epoch, the fence's seq, the open's retries so far) and emits
/// `Event::TookOver`.
fn resume_takeover(
    cfg: &EngineConfig,
    writer_id: &str,
    bucket: &mut Bucket,
) -> Result<FenceOutcome> {
    let span = takeover_span(bucket.head().generation);
    span.record("durable.epoch", bucket.head().epoch);
    let r = span.in_scope(|| bucket.append_fence());
    span.record("durable.retries", bucket.counters().retries.load(SeqCst));
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
    if state.claim_id.is_empty() {
        state.claim_id = new_writer_id()?;
        state.write_atomic(dir)?;
    }
    let (head, etag) = bucket::take_over_counted(
        &**store,
        keys,
        &cfg.retry,
        state.generation,
        &state.claim_id,
        retries,
    )?;
    span.record("durable.epoch", head.epoch);
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
    set_provisional_quiet(cfg, state);
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
    Ok(match decode_record(&obj.body)? {
        LogRecord::Extents { writer_id: w, .. } | LogRecord::Fence { writer_id: w, .. } => {
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

    /// How many commits each log PUT carried and how long acks waited, since open.
    pub fn upload_stats(&self) -> UploadStats {
        self.shared.stats.snapshot()
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
