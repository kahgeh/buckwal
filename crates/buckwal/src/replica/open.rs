//! [`open_replica`] and [`Replica`]: restore (or resume from the position file), open the
//! engine in the replica IO mode, and run the follower thread.
//!
//! A replica never goes through the writer's attach or reconcile: it opens its own local files
//! and follows the log through the position file. Nothing it does writes to the bucket.
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use crate::bucket::Bucket;
use crate::config::{Config, JournalMode};
use crate::db::{new_writer_id, replica_database, Database, DirGuard};
use crate::encryption::{self, check_page_one, file_page_one};
use crate::engine::{engine_error, open_engine};
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE, LOCK_FILE, LOG_FILE, STATE_FILE, WAL_FILE};
use crate::format::{Keys, LocalState, Manifest};
use crate::io::IoMode;
use crate::replica::{
    apply::{Applier, PageDecrypt},
    connection::ReadConnection,
    position::{Position, Stage},
    stats::ReplicaStats,
    sync::{reason, Follower, Outcome, PassResult, Reason, StopReason, Trigger},
    waker::{ReplicaWaker, WakeState},
};
use crate::restore::{clear_interrupted_restore, remove_restored, restore_staged};
use crate::store::{with_retry, ObjectStore, RetryPolicy};

/// Open the read replica `config` names. See [`Replica`].
pub fn open_replica(config: Config) -> Result<Replica> {
    let store = config.s3_store()?;
    open_replica_with_store(config, store)
}

/// An open read replica: the follower thread, and read-only connections over the directory.
/// Dropping it stops the follower and releases the directory.
pub struct Replica {
    db: Database,
    wake: Arc<WakeState>,
    stats: Arc<Mutex<ReplicaStats>>,
    thread: Option<JoinHandle<()>>,
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
    /// on the last state it applied.
    pub fn connect(&self) -> Result<ReadConnection> {
        self.db.connect().map(ReadConnection::new)
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
        let db = self.db.clone();
        drop(self);
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

#[doc(hidden)]
pub fn open_replica_with_store(config: Config, store: Arc<dyn ObjectStore>) -> Result<Replica> {
    config.validate()?;
    config.validate_replica()?;
    let encryption = encryption::load(&config)?;
    let guard = DirGuard::acquire(&config.dir)?;
    let dir = guard.dir().to_path_buf();
    let keys = config.keys_for(encryption.as_deref());
    let mut cfg = config.engine(IoMode::Replica);
    cfg.encryption = encryption.clone();
    // A replica follows WAL databases only; an MVCC head is refused below.
    cfg.journal_mode = JournalMode::Wal;
    // The lock is taken first, as the writer's open does (it creates the directory and
    // `durable.lock`, nothing else). Every refusal below happens before any other file is
    // touched.

    // The head first: an MVCC bucket or a wrong key is refused before anything is fetched.
    let head_key = keys.head();
    let head_obj = with_retry(&cfg.retry, &head_key, |_| store.get(&head_key))?
        .ok_or_else(|| Error::NotFound(head_key.clone()))?;
    let head = keys.decode_head_open(&head_obj.body)?;
    if head.journal_mode == JournalMode::Mvcc {
        return Err(Error::Unsupported("replica: MVCC"));
    }

    clear_interrupted_restore(&dir)?;
    let state = match resumable_state(&dir)? {
        Some(state) => state,
        None => {
            // No usable position file or database: restore afresh. Only a directory that is
            // empty or holds a previous replica's files is cleared for it.
            clear_replica_dir(&dir)?;
            let staged = restore_staged(store.clone(), &keys, &dir, &cfg)?;
            let page1 = file_page_one(&dir)?.or(staged.page1);
            if let Err(e) = check_page_one(page1.as_deref(), encryption.as_deref()) {
                let _ = remove_restored(&dir);
                return Err(e);
            }
            let state = LocalState::read(&dir)?
                .ok_or_else(|| Error::Corrupt("the restore wrote no local state".into()))?;
            Position {
                generation: state.generation,
                stage: Stage::Applying,
                offset: 0,
                new_offset: 0,
                replica_id: new_writer_id()?,
            }
            .write(&dir)?;
            state
        }
    };

    let manifest = manifest_before(&*store, &keys, &cfg.retry, state.generation)?;
    let page_size = manifest
        .as_ref()
        .map(|m| m.page_size as usize)
        .or_else(|| local_page_size(&dir))
        .unwrap_or(4096);
    let bucket = Bucket::for_replica(
        store.clone(),
        keys.clone(),
        cfg.retry.clone(),
        cfg.upload_concurrency,
        head,
        head_obj.etag,
        manifest,
    );
    let engine = open_engine(&dir, bucket, &state, &cfg, None)?;
    let db = replica_database(store.clone(), engine, guard, &cfg);

    // The follower's own connection: never handed out, and it never checkpoints by itself.
    let core = db
        .shared
        .engine_db()
        .ok_or_else(|| Error::Config("the database is closed".into()))?
        .connect()
        .map_err(engine_error)?;
    core.wal_auto_actions_disable();
    let decrypt = match &encryption {
        Some(enc) => Some(PageDecrypt::from_encryption(enc, page_size)?),
        None => None,
    };
    let follower = Follower::resume(
        store.clone(),
        keys.clone(),
        cfg.retry.clone(),
        Applier::new(core, decrypt),
        page_size,
        dir,
        // A seal's checkpoint may stay Busy for two slow-poll intervals.
        config.replica_poll_interval * 2,
    )?;

    let stats = Arc::new(Mutex::new(ReplicaStats {
        generation: follower.generation(),
        seq: follower.last_seq(),
        ..ReplicaStats::default()
    }));
    let wake = Arc::new(WakeState::default());
    let runner = Runner {
        follower,
        store,
        keys,
        retry: cfg.retry.clone(),
        stats: stats.clone(),
        wake: wake.clone(),
        interval: config.replica_poll_interval,
    };
    // The thread reports to the subscriber of the thread that opened the replica.
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    let thread = std::thread::Builder::new()
        .name("buckwal-replica".into())
        .spawn(move || tracing::dispatcher::with_default(&dispatch, || runner.run()))
        .map_err(|e| io_err("spawn replica thread", e))?;
    Ok(Replica {
        db,
        wake,
        stats,
        thread: Some(thread),
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

/// Make `dir` ready for a restore: refuse a writer's directory and anything that is not a
/// replica's file, then remove the previous replica's files (not the lock).
fn clear_replica_dir(dir: &Path) -> Result<()> {
    match LocalState::read(dir) {
        Ok(Some(s)) => refuse_writer_dir(dir, &s)?,
        Ok(None) => {
            // Without a local state the files are not known to be a replica's: they may be a
            // writer's database, whose `-wal` or `-log` holds commits only it has.
            for f in [DB_FILE, WAL_FILE, LOG_FILE] {
                if dir.join(f).exists() {
                    return Err(Error::Config(format!(
                        "{} holds {f} without a replica's local state; it is not cleared for \
                         a replica",
                        dir.display()
                    )));
                }
            }
        }
        Err(e) => {
            return Err(Error::Config(format!(
                "{} holds a local state that cannot be read ({e}); it is not cleared for a \
                 replica",
                dir.display()
            )))
        }
    }
    let mut files = Vec::new();
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
        if name != LOCK_FILE {
            files.push(e.path());
        }
    }
    for f in files {
        fs::remove_file(&f).map_err(|e| io_err(&format!("remove {}", f.display()), e))?;
    }
    Ok(())
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

/// The follower thread's state.
struct Runner {
    follower: Follower,
    store: Arc<dyn ObjectStore>,
    keys: Keys,
    retry: RetryPolicy,
    stats: Arc<Mutex<ReplicaStats>>,
    wake: Arc<WakeState>,
    interval: Duration,
}

impl Runner {
    fn run(mut self) {
        let mut trigger = Trigger::Start;
        let mut checked_first_poll = false;
        loop {
            if !self.one_pass(trigger) {
                return;
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

    /// Run one pass and fold it into the stats. False when the follower must stop.
    fn one_pass(&mut self, trigger: Trigger) -> bool {
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
        let key = self.keys.log(self.follower.generation(), seq);
        let st = with_retry(&self.retry, &key, |_| self.store.stat(&key)).ok()??;
        Some(st.age.as_millis() as u64)
    }

    fn handle(&self, r: PassResult) -> bool {
        // Task 9b restores again for these. Until then the follower stops.
        if let Some(why) = r.outcome.re_restore() {
            let name = match why {
                Reason::Trimmed => "trimmed",
                Reason::Conflict => reason::CONFLICT,
                Reason::Gap => "gap",
            };
            self.stop(name, None, r.detail);
            return false;
        }
        match r.outcome {
            Outcome::Applied
            | Outcome::Nothing
            | Outcome::CheckpointDeferred
            | Outcome::GenerationMoved { .. } => true,
            Outcome::Failed(m) => {
                tracing::warn!(target: "buckwal", error = %m, "replica: pass failed, trying again later");
                true
            }
            Outcome::Stopped(why) => {
                let key = match &why {
                    StopReason::Integrity(k) | StopReason::Corrupt(k) => Some(k.as_str()),
                    _ => None,
                };
                self.stop(why.as_str(), key, r.detail);
                false
            }
            Outcome::ReRestore(_) => unreachable!("handled by re_restore"),
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

#[cfg(test)]
mod tests;
