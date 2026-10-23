//! [`open_replica`] and [`Replica`]: restore (or resume from the position file), open the
//! engine in the replica IO mode, and run the follower thread.
//!
//! A replica never goes through the writer's attach or reconcile: it opens its own local files
//! and follows the log through the position file. Nothing it does writes to the bucket.
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use crate::bucket::Bucket;
use crate::config::EngineConfig;
use crate::config::{Config, JournalMode};
use crate::db::{new_writer_id, replica_database, Database, DirGuard};
use crate::encryption::Encryption;
use crate::encryption::{self, check_page_one, file_page_one};
use crate::engine::{engine_error, open_engine};
use crate::error::{Error, Result};
use crate::files::{
    io_err, DB_FILE, LOCK_FILE, LOG_FILE, REPLICA_AT_FILE, REPLICA_FILE, STATE_FILE, WAL_FILE,
};
use crate::format::Head;
use crate::format::{Keys, LocalState, Manifest};
use crate::io::IoMode;
use crate::reconcile::replace_file;
use crate::replica::{
    apply::{Applier, PageDecrypt},
    connection::ReadConnection,
    position::{Position, Stage},
    stats::ReplicaStats,
    sync::{reason, Follower, Outcome, PassResult, Reason, StopReason, Trigger},
    waker::{ReplicaWaker, WakeState},
};
use crate::restore::fsync_dir;
use crate::restore::{clear_interrupted_restore, remove_restored, restore_staged};
use crate::store::{with_retry, ObjectStore, RetryPolicy};

/// Open the read replica `config` names. See [`Replica`].
pub fn open_replica(config: Config) -> Result<Replica> {
    let store = config.s3_store()?;
    open_replica_with_store(config, store)
}

/// An open read replica: the follower thread, and read-only connections over the directory.
/// Dropping it stops the follower and releases the directory.
///
/// # Where the data lives, and the lock
///
/// `config.dir` holds `durable.lock` for the replica's whole life; the process lock never
/// moves. The database files (`app.db`, `-wal`, the local state, the position file) are in
/// `config.dir` after a restore at open. A re-restore builds the new copy in a fresh sibling
/// directory (`.<name>.data-<pid>-<nanos>`, same parent, same filesystem), opens an engine on
/// it, and only then writes `app.db-replica-at` in `config.dir` (atomically) naming it. That
/// write is the durable switch: a reopen follows the pointer, and removes whatever the
/// previous copy or an interrupted re-restore left. The `Database` that `connect` uses is
/// replaced under a mutex; a connection opened before keeps the old engine alive (on its
/// unlinked files) until it is closed.
pub struct Replica {
    current: Arc<Mutex<Database>>,
    wake: Arc<WakeState>,
    stats: Arc<Mutex<ReplicaStats>>,
    thread: Option<JoinHandle<()>>,
    /// Dropped last: the lock outlives every file of every copy.
    _guard: DirGuard,
}

impl std::fmt::Debug for Replica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Replica {
    /// A new read-only connection; open one per thread. Works after the follower has stopped,
    /// on the last state it applied. After a re-restore it is on the new copy; connections
    /// opened before keep reading the copy they were opened on (an open transaction may get
    /// an error).
    pub fn connect(&self) -> Result<ReadConnection> {
        let db = lock(&self.current).clone();
        db.connect().map(ReadConnection::new)
    }

    /// Progress and cost so far; readable after the follower has stopped.
    pub fn stats(&self) -> ReplicaStats {
        let mut s = lock(&self.stats).clone();
        s.notifications_received = self.wake.received();
        s
    }

    /// For the code that receives bucket notifications: call `wake` on each.
    pub fn waker(&self) -> ReplicaWaker {
        ReplicaWaker::new(self.wake.clone())
    }

    /// Stop the follower and release the directory. Every [`ReadConnection`] must be closed
    /// first (`Config` otherwise).
    pub fn close(mut self) -> Result<()> {
        self.stop_follower();
        let db = lock(&self.current).clone();
        db.close()
    }

    #[cfg(test)]
    fn thread_finished(&self) -> bool {
        self.thread.as_ref().is_none_or(|t| t.is_finished())
    }

    fn stop_follower(&mut self) {
        self.wake.stop();
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        self.stop_follower();
    }
}

/// What opening or re-opening a copy needs besides the directory.
struct Env {
    store: Arc<dyn ObjectStore>,
    keys: Keys,
    cfg: EngineConfig,
    encryption: Option<Arc<Encryption>>,
    /// A seal's checkpoint may stay Busy for two slow-poll intervals.
    busy_budget: Duration,
}

impl Env {
    /// Restore the bucket into `data` (empty), check page one against the key, write the
    /// position file and return the local state.
    fn restore_copy(&self, data: &Path, replica_id: String) -> Result<LocalState> {
        let staged = restore_staged(self.store.clone(), &self.keys, data, &self.cfg)?;
        let page1 = file_page_one(data)?.or(staged.page1);
        if let Err(e) = check_page_one(page1.as_deref(), self.encryption.as_deref()) {
            let _ = remove_restored(data);
            return Err(e);
        }
        let state = LocalState::read(data)?
            .ok_or_else(|| Error::Corrupt("the restore wrote no local state".into()))?;
        Position {
            generation: state.generation,
            stage: Stage::Applying,
            offset: 0,
            new_offset: 0,
            replica_id,
        }
        .write(data)?;
        Ok(state)
    }

    /// The engine over `data` in the replica IO mode, as a `Database` with no directory
    /// guard, and the follower over its internal apply connection.
    fn open_copy(
        &self,
        data: &Path,
        state: &LocalState,
        head: Head,
        head_etag: String,
    ) -> Result<(Database, Follower)> {
        let manifest =
            manifest_before(&*self.store, &self.keys, &self.cfg.retry, state.generation)?;
        let page_size = manifest
            .as_ref()
            .map(|m| m.page_size as usize)
            .or_else(|| local_page_size(data))
            .unwrap_or(4096);
        let bucket = Bucket::for_replica(
            self.store.clone(),
            self.keys.clone(),
            self.cfg.retry.clone(),
            self.cfg.upload_concurrency,
            head,
            head_etag,
            manifest,
        );
        let engine = open_engine(data, bucket, state, &self.cfg, None)?;
        let db = replica_database(self.store.clone(), engine, &self.cfg);
        // The follower's own connection: never handed out, and it never checkpoints by itself.
        let core = db
            .shared
            .engine_db()
            .ok_or_else(|| Error::Config("the database is closed".into()))?
            .connect()
            .map_err(engine_error)?;
        core.wal_auto_actions_disable();
        let decrypt = match &self.encryption {
            Some(enc) => Some(PageDecrypt::from_encryption(enc, page_size)?),
            None => None,
        };
        let follower = Follower::resume(
            self.store.clone(),
            self.keys.clone(),
            self.cfg.retry.clone(),
            Applier::new(core, decrypt),
            page_size,
            data.to_path_buf(),
            self.busy_budget,
        )?;
        Ok((db, follower))
    }

    /// The head, read afresh; an MVCC head is refused.
    fn read_head(&self) -> Result<(Head, String)> {
        let key = self.keys.head();
        let obj = with_retry(&self.cfg.retry, &key, |_| self.store.get(&key))?
            .ok_or_else(|| Error::NotFound(key.clone()))?;
        let head = self.keys.decode_head_open(&obj.body)?;
        if head.journal_mode == JournalMode::Mvcc {
            return Err(Error::Unsupported("replica: MVCC"));
        }
        Ok((head, obj.etag))
    }
}

#[doc(hidden)]
pub fn open_replica_with_store(config: Config, store: Arc<dyn ObjectStore>) -> Result<Replica> {
    config.validate()?;
    config.validate_replica()?;
    let encryption = encryption::load(&config)?;
    // The lock is taken first, as the writer's open does (it creates the directory and
    // `durable.lock`, nothing else). Every refusal below happens before any other file is
    // touched.
    let guard = DirGuard::acquire(&config.dir)?;
    let dir = guard.dir().to_path_buf();
    let keys = config.keys_for(encryption.as_deref());
    let mut cfg = config.engine(IoMode::Replica);
    cfg.encryption = encryption.clone();
    // A replica follows WAL databases only; an MVCC head is refused below.
    cfg.journal_mode = JournalMode::Wal;
    let env = Env {
        store: store.clone(),
        keys: keys.clone(),
        cfg,
        encryption,
        // A seal's checkpoint may stay Busy for two slow-poll intervals.
        busy_budget: config.replica_poll_interval * 2,
    };

    // The head first: an MVCC bucket or a wrong key is refused before anything is fetched.
    let (head, head_etag) = env.read_head()?;

    clear_interrupted_restore(&dir)?;
    let mut data = settle_layout(&dir)?;
    let state = match resumable_state(&data)? {
        Some(state) => state,
        None => {
            // No usable position file or database: restore afresh into the directory itself.
            // Only a directory that is empty or holds a previous replica's files is cleared
            // for it.
            clear_replica_dir(&dir)?;
            data = dir.clone();
            env.restore_copy(&data, new_writer_id()?)?
        }
    };

    let (db, follower) = env.open_copy(&data, &state, head, head_etag)?;
    let stats = Arc::new(Mutex::new(ReplicaStats {
        generation: follower.generation(),
        seq: follower.last_seq(),
        ..ReplicaStats::default()
    }));
    let wake = Arc::new(WakeState::default());
    let current = Arc::new(Mutex::new(db));
    let runner = Runner {
        follower,
        env,
        dir,
        data,
        current: current.clone(),
        stats: stats.clone(),
        wake: wake.clone(),
        interval: config.replica_poll_interval,
        caught_up: Instant::now(),
    };
    // The thread reports to the subscriber of the thread that opened the replica.
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    let thread = std::thread::Builder::new()
        .name("buckwal-replica".into())
        .spawn(move || tracing::dispatcher::with_default(&dispatch, || runner.run()))
        .map_err(|e| io_err("spawn replica thread", e))?;
    Ok(Replica {
        current,
        wake,
        stats,
        thread: Some(thread),
        _guard: guard,
    })
}

/// The local state of a directory that can resume: a readable position file, a local state of
/// a replica and a database file. A missing or corrupt position file, or no database, is
/// `None` (restore afresh). A writer's directory is an error: it is never cleared.
fn resumable_state(dir: &Path) -> Result<Option<LocalState>> {
    let position = Position::read(dir);
    let state = LocalState::read(dir);
    if let Ok(Some(s)) = &state {
        refuse_writer_dir(dir, s)?;
    }
    match (position, state) {
        (Ok(Some(_)), Ok(Some(s))) if dir.join(DB_FILE).exists() => Ok(Some(s)),
        _ => Ok(None),
    }
}

fn refuse_writer_dir(dir: &Path, s: &LocalState) -> Result<()> {
    if s.claimed {
        return Err(Error::Config(format!(
            "{} holds a writer's database; a replica needs a directory of its own",
            dir.display()
        )));
    }
    Ok(())
}

/// Whether `name` is a file a replica directory holds.
fn is_replica_file(name: &str) -> bool {
    name == DB_FILE || name.starts_with("app.db-") || name == STATE_FILE || name == LOCK_FILE
}

// ---- Where the current copy lives ---------------------------------------------------------

/// `<parent>/.<name>.data-`: the name prefix of `dir`'s copy directories.
fn copy_prefix(dir: &Path) -> Option<String> {
    dir.file_name()
        .map(|n| format!(".{}.data-", n.to_string_lossy()))
}

/// A new, empty copy directory beside `dir`.
fn new_copy_dir(dir: &Path) -> Result<PathBuf> {
    let (Some(parent), Some(prefix)) = (dir.parent(), copy_prefix(dir)) else {
        return Err(Error::Config(format!(
            "cannot restore beside {}: it has no parent directory",
            dir.display()
        )));
    };
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let copy = parent.join(format!("{prefix}{}-{nanos}", std::process::id()));
    fs::create_dir(&copy).map_err(|e| io_err(&format!("create {}", copy.display()), e))?;
    Ok(copy)
}

/// The copy directory `dir`'s pointer file names, if the pointer is valid and the directory
/// exists.
fn pointed_copy(dir: &Path) -> Option<PathBuf> {
    let name = fs::read_to_string(dir.join(REPLICA_AT_FILE)).ok()?;
    let name = name.trim();
    let prefix = copy_prefix(dir)?;
    if !name.starts_with(&prefix) || name.contains(['/', '\\']) {
        return None;
    }
    let copy = dir.parent()?.join(name);
    copy.is_dir().then_some(copy)
}

fn write_pointer(dir: &Path, copy: &Path) -> Result<()> {
    let name = copy
        .file_name()
        .ok_or_else(|| Error::Config("a copy directory without a name".into()))?
        .to_string_lossy();
    replace_file(dir, REPLICA_AT_FILE, name.as_bytes())
}

/// Remove a copy directory and the staging directories a restore into it left.
fn remove_copy(copy: &Path) {
    let _ = clear_interrupted_restore(copy);
    let _ = fs::remove_dir_all(copy);
}

/// Remove every copy directory beside `dir` except `keep`, and the staging directories of
/// restores into them: what an interrupted re-restore left. Called with `dir` held.
fn sweep_copies(dir: &Path, keep: Option<&Path>) -> Result<()> {
    let (Some(parent), Some(prefix)) = (dir.parent(), copy_prefix(dir)) else {
        return Ok(());
    };
    let staged_prefix = format!(".{prefix}");
    let entries = fs::read_dir(parent).map_err(|e| io_err("list parent dir", e))?;
    for e in entries {
        let e = e.map_err(|e| io_err("list parent dir", e))?;
        let name = e.file_name().to_string_lossy().into_owned();
        if !(name.starts_with(&prefix) || name.starts_with(&staged_prefix)) {
            continue;
        }
        if keep.is_some_and(|k| k == e.path()) {
            continue;
        }
        fs::remove_dir_all(e.path()).map_err(|e| io_err("remove replica copy", e))?;
    }
    Ok(())
}

/// Work out where the current copy is: the pointed copy if the pointer is valid (and then
/// the previous copy's files in `dir` are leftovers of a re-restore, removed), else `dir`
/// itself. Copies nobody points to are removed.
fn settle_layout(dir: &Path) -> Result<PathBuf> {
    let pointed = pointed_copy(dir);
    sweep_copies(dir, pointed.as_deref())?;
    match pointed {
        Some(copy) => {
            remove_own_files(dir, false, None)?;
            Ok(copy)
        }
        None => {
            // A pointer to a copy that is gone: the files in `dir` (if any) are what is left.
            match fs::remove_file(dir.join(REPLICA_AT_FILE)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_err("remove replica pointer", e)),
            }
            Ok(dir.to_path_buf())
        }
    }
}

/// The files of a replica directory in the order they are removed (not the lock): the
/// position file first (without it the rest is no longer resumable), the local state LAST, so
/// a directory with a database file always has its state and is never refused as a stranger's;
/// the pointer just before the state.
fn removal_order(dir: &Path, with_pointer: bool) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for e in fs::read_dir(dir).map_err(|e| io_err("list replica dir", e))? {
        let e = e.map_err(|e| io_err("list replica dir", e))?;
        let name = e.file_name().to_string_lossy().into_owned();
        if name == LOCK_FILE || (name == REPLICA_AT_FILE && !with_pointer) {
            continue;
        }
        let rank = match name.as_str() {
            REPLICA_FILE => 0,
            REPLICA_AT_FILE => 2,
            STATE_FILE => 3,
            _ => 1,
        };
        files.push((rank, name, e.path()));
    }
    files.sort();
    Ok(files.into_iter().map(|(_, _, p)| p).collect())
}

/// Remove the replica files of `dir` in `removal_order`, then fsync it. `limit` stops after
/// that many removals (tests: a crash).
fn remove_own_files(dir: &Path, with_pointer: bool, limit: Option<usize>) -> Result<()> {
    let state = dir.join(STATE_FILE);
    for (i, f) in removal_order(dir, with_pointer)?.into_iter().enumerate() {
        if limit.is_some_and(|l| i >= l) {
            return Ok(());
        }
        if f == state {
            // Everything else is gone for good before the state is.
            fsync_dir(dir)?;
        }
        fs::remove_file(&f).map_err(|e| io_err(&format!("remove {}", f.display()), e))?;
    }
    fsync_dir(dir)
}

#[cfg(test)]
fn replica_file_count(dir: &Path) -> usize {
    removal_order(dir, true).unwrap().len()
}

/// Make `dir` ready for a restore: refuse a writer's directory and anything that is not a
/// replica's file, then remove the previous replica's files (not the lock): the pointed copy
/// directory, then the files in `dir`, position file first and local state last.
fn clear_replica_dir(dir: &Path) -> Result<()> {
    clear_replica_dir_n(dir, None)
}

fn clear_replica_dir_n(dir: &Path, limit: Option<usize>) -> Result<()> {
    let copy = pointed_copy(dir);
    for d in std::iter::once(dir).chain(copy.as_deref()) {
        match LocalState::read(d) {
            Ok(Some(s)) => refuse_writer_dir(d, &s)?,
            Ok(None) => {
                // Without a local state the files are not known to be a replica's: they may
                // be a writer's database, whose `-wal` or `-log` holds commits only it has.
                for f in [DB_FILE, WAL_FILE, LOG_FILE] {
                    if d.join(f).exists() {
                        return Err(Error::Config(format!(
                            "{} holds {f} without a replica's local state, so it is not known \
                             to be a replica's and is left alone. If it is a replica's directory \
                             left by a crash, delete {f} (and any other app.db* file) in it, or \
                             the directory, and open again. If it is a writer's, give the \
                             replica a directory of its own.",
                            d.display()
                        )));
                    }
                }
            }
            Err(e) => {
                return Err(Error::Config(format!(
                    "{} holds a local state that cannot be read ({e}); it is not cleared for a \
                     replica. Delete {} (and the other app.db* files in it) to restore afresh, \
                     or give the replica a directory of its own.",
                    d.display(),
                    d.join(STATE_FILE).display()
                )))
            }
        }
    }
    for e in fs::read_dir(dir).map_err(|e| io_err("list replica dir", e))? {
        let e = e.map_err(|e| io_err("list replica dir", e))?;
        let name = e.file_name().to_string_lossy().into_owned();
        if !is_replica_file(&name) {
            return Err(Error::Config(format!(
                "{} holds {name}, which is not a replica's file; a replica needs an empty \
                 directory or its own",
                dir.display()
            )));
        }
    }
    if let Some(copy) = copy {
        remove_copy(&copy);
    }
    remove_own_files(dir, true, limit)
}

/// The manifest the replica's lazy page fetches use: the newest one below the generation its
/// local state was restored at (what the head named then), or none if the database was never
/// sealed. Later seals' pages arrive through the log and the local checkpoints.
fn manifest_before(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    generation: u64,
) -> Result<Option<Manifest>> {
    let root = keys.manifest_root();
    let listed = with_retry(retry, &root, |_| store.list(&root))?;
    let Some(g) = listed
        .iter()
        .filter_map(|k| keys.parse_manifest(k))
        .filter(|g| *g < generation)
        .max()
    else {
        return Ok(None);
    };
    let key = keys.manifest(g);
    let obj = with_retry(retry, &key, |_| store.get(&key))?
        .ok_or_else(|| Error::NotFound(key.clone()))?;
    keys.decode_manifest(&obj.body, g).map(Some)
}

/// The page size of the local files: `app.db`'s header, else the `-wal`'s.
fn local_page_size(dir: &Path) -> Option<usize> {
    let db = fs::read(dir.join(DB_FILE)).ok().filter(|b| b.len() >= 18);
    let from_db = db.and_then(|b| match u16::from_be_bytes([b[16], b[17]]) {
        0 => None,
        1 => Some(65536),
        n => Some(n as usize),
    });
    from_db.or_else(|| {
        let wal = fs::read(dir.join(WAL_FILE)).ok()?;
        let b = wal.get(8..12)?;
        let n = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize;
        (n != 0).then_some(n)
    })
}

/// What to do after a pass.
enum Next {
    /// The follower is done.
    Stop,
    /// Wait for a wake-up or the slow poll.
    Wait,
    /// Run another pass at once (after a re-restore).
    Now,
}

/// The follower thread's state.
struct Runner {
    follower: Follower,
    env: Env,
    /// The replica directory: holds the lock and the pointer.
    dir: PathBuf,
    /// Where the current copy's files are: `dir` or a sibling copy directory.
    data: PathBuf,
    /// The `Database` `Replica::connect` uses; replaced by a re-restore.
    current: Arc<Mutex<Database>>,
    stats: Arc<Mutex<ReplicaStats>>,
    wake: Arc<WakeState>,
    interval: Duration,
    /// When a pass last succeeded (start of the thread before any): how long the replica has
    /// been behind when it must restore again.
    caught_up: Instant,
}

impl Runner {
    fn run(mut self) {
        let mut trigger = Trigger::Start;
        let mut checked_first_poll = false;
        loop {
            match self.one_pass(trigger) {
                Next::Stop => return,
                Next::Now => {
                    trigger = Trigger::Start;
                    continue;
                }
                Next::Wait => {}
            }
            let deadline = Instant::now() + self.interval;
            let Some(next) = self.wake.wait(deadline) else {
                return;
            };
            if next == Trigger::SlowPoll && !checked_first_poll {
                checked_first_poll = true;
                if self.wake.received() == 0 {
                    tracing::warn!(
                        target: "buckwal",
                        "no notification received yet, running on the slow poll only"
                    );
                }
            }
            trigger = next;
        }
    }

    /// Run one pass and fold it into the stats.
    fn one_pass(&mut self, trigger: Trigger) -> Next {
        {
            let mut s = lock(&self.stats);
            match trigger {
                Trigger::Notification => s.passes_notification += 1,
                Trigger::SlowPoll => s.passes_slow_poll += 1,
                Trigger::Start => {}
            }
        }
        let r = self.follower.pass(trigger);
        let ok = matches!(
            r.outcome,
            Outcome::Applied | Outcome::Nothing | Outcome::CheckpointDeferred
        );
        let lag = if ok { self.measure_lag(&r) } else { None };
        {
            let mut s = lock(&self.stats);
            s.generation = self.follower.generation();
            s.seq = self.follower.last_seq();
            s.objects_fetched += r.objects;
            s.fenced_records_skipped += r.skipped;
            if r.outcome == Outcome::CheckpointDeferred {
                s.deferred_checkpoints += 1;
            }
            if ok {
                s.last_pass_at = Some(SystemTime::now());
                if lag.is_some() {
                    s.lag_ms = lag;
                }
                if trigger == Trigger::Notification && !r.found_new {
                    s.notification_passes_nothing_new += 1;
                }
            }
        }
        if ok {
            self.caught_up = Instant::now();
        }
        self.handle(r)
    }

    /// The lag after a pass: 0 when nothing new came in, else now minus the newest applied
    /// log object's `Last-Modified` by the bucket's clock (one HEAD of that object).
    fn measure_lag(&self, r: &PassResult) -> Option<u64> {
        if !r.found_new {
            return Some(0);
        }
        let seq = self.follower.last_seq();
        if seq == 0 {
            return None;
        }
        let key = self.env.keys.log(self.follower.generation(), seq);
        let st = with_retry(&self.env.cfg.retry, &key, |_| self.env.store.stat(&key)).ok()??;
        Some(st.age.as_millis() as u64)
    }

    fn handle(&mut self, r: PassResult) -> Next {
        if let Some(why) = r.outcome.re_restore() {
            return self.re_restore(why);
        }
        match r.outcome {
            Outcome::Applied
            | Outcome::Nothing
            | Outcome::CheckpointDeferred
            | Outcome::GenerationMoved { .. } => Next::Wait,
            Outcome::Failed(m) => {
                tracing::warn!(target: "buckwal", error = %m, "replica: pass failed, trying again later");
                Next::Wait
            }
            Outcome::Stopped(why) => {
                let key = match &why {
                    StopReason::Integrity(k) | StopReason::Corrupt(k) => Some(k.as_str()),
                    _ => None,
                };
                self.stop(why.as_str(), key, r.detail);
                Next::Stop
            }
            Outcome::ReRestore(_) => unreachable!("handled by re_restore"),
        }
    }

    /// The replica fell behind (a log object it needs is gone, or the log and the local files
    /// disagree): log it once, restore into a fresh copy beside the current one, swap to it
    /// and remove the old one. A failed attempt leaves the current copy as it was and is
    /// tried again at the next pass.
    fn re_restore(&mut self, why: Reason) -> Next {
        let reason = match why {
            Reason::Conflict => reason::CONFLICT,
            Reason::Trimmed | Reason::Gap => reason::TRIMMED,
        };
        let (generation, seq) = (self.follower.generation(), self.follower.last_seq());
        let behind = self.caught_up.elapsed();
        tracing::warn!(
            target: "buckwal",
            replica_id = self.follower.replica_id(),
            generation,
            seq,
            behind_ms = behind.as_millis() as u64,
            reason,
            "replica fell behind, restoring again"
        );
        if let Err(e) = self.restore_again() {
            tracing::warn!(target: "buckwal", error = %e, "replica: restoring again failed, trying again later");
            return Next::Wait;
        }
        let mut s = lock(&self.stats);
        s.re_restores += 1;
        s.last_re_restore_at = Some((
            SystemTime::now(),
            format!(
                "log/{generation}/{seq} {reason}: replica was {} behind",
                human(behind)
            ),
        ));
        s.generation = self.follower.generation();
        s.seq = self.follower.last_seq();
        // Measured afresh by the next pass.
        s.lag_ms = None;
        drop(s);
        self.caught_up = Instant::now();
        Next::Now
    }

    fn restore_again(&mut self) -> Result<()> {
        let copy = new_copy_dir(&self.dir)?;
        match self.build_copy(&copy) {
            Err(e) => {
                remove_copy(&copy);
                Err(e)
            }
            Ok((db, follower)) => {
                // The durable switch: from here a reopen follows the new copy.
                if let Err(e) = write_pointer(&self.dir, &copy) {
                    drop((db, follower));
                    remove_copy(&copy);
                    return Err(e);
                }
                let old = std::mem::replace(&mut self.data, copy);
                let old_db = std::mem::replace(&mut *lock(&self.current), db);
                // The old apply connection goes first, then the handle; readers still on the
                // old copy keep its engine (and its unlinked files) until they close.
                self.follower = follower;
                drop(old_db);
                self.remove_old_copy(&old);
                Ok(())
            }
        }
    }

    fn build_copy(&self, copy: &Path) -> Result<(Database, Follower)> {
        let (head, etag) = self.env.read_head()?;
        let state = self
            .env
            .restore_copy(copy, self.follower.replica_id().to_string())?;
        self.env.open_copy(copy, &state, head, etag)
    }

    fn remove_old_copy(&self, old: &Path) {
        if old == self.dir {
            if let Err(e) = remove_own_files(&self.dir, false, None) {
                tracing::warn!(target: "buckwal", error = %e, "replica: could not remove the old copy");
            }
        } else {
            remove_copy(old);
        }
    }

    fn stop(&self, reason: &str, key: Option<&str>, detail: Option<String>) {
        lock(&self.stats).stopped = Some(reason.to_string());
        tracing::error!(
            target: "buckwal",
            generation = self.follower.generation(),
            seq = self.follower.last_seq(),
            reason,
            key,
            detail = detail.as_deref(),
            "replica stopped"
        );
    }
}

/// `14m`, `2h5m`, `37s`, `120ms`.
fn human(d: Duration) -> String {
    let s = d.as_secs();
    match s {
        0 => format!("{}ms", d.as_millis()),
        1..=59 => format!("{s}s"),
        60..=3599 => format!("{}m", s / 60),
        _ => format!("{}h{}m", s / 3600, s % 3600 / 60),
    }
}

#[cfg(test)]
mod tests;
