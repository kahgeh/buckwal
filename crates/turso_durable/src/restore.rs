// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! Restore onto an empty directory: rebuild the current generation's `-wal` from its log and
//! leave every database page in the bucket, to be fetched on first read.

use crate::config::{Config, EngineConfig};
use crate::db::DirGuard;
use crate::error::{Error, Result};
use crate::format::{apply_ops, decode_record, Head, Keys, LocalState, LogRecord, Manifest};
use crate::store::{with_retry, ObjectStore, RetryPolicy};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct RestoreReport {
    /// The generation restored (the head's).
    pub generation: u64,
    /// Log objects replayed into the `-wal`.
    pub log_objects: u64,
    pub gets: u64,
    pub lists: u64,
    /// Bytes downloaded.
    pub bytes: u64,
    pub elapsed: Duration,
}

fn io_err(what: &str, e: std::io::Error) -> Error {
    Error::Io(format!("{what}: {e}"))
}

fn get(store: &dyn ObjectStore, retry: &RetryPolicy, key: &str) -> Result<Vec<u8>> {
    with_retry(retry, key, |_| store.get(key))?
        .map(|o| o.body)
        .ok_or_else(|| Error::NotFound(key.to_string()))
}

/// Every log record of generation `gen`, in order. The seqs must run 1, 2, 3, … with no gap.
pub(crate) fn fetch_generation(
    store: &dyn ObjectStore,
    keys: &Keys,
    gen: u64,
    concurrency: usize,
    retry: &RetryPolicy,
) -> Result<Vec<LogRecord>> {
    let prefix = keys.log_prefix(gen);
    let mut seqs: Vec<u64> = with_retry(retry, &prefix, |_| store.list(&prefix))?
        .iter()
        .filter_map(|k| keys.parse_log(k))
        .filter(|(g, _)| *g == gen)
        .map(|(_, s)| s)
        .collect();
    seqs.sort_unstable();
    for (i, &s) in seqs.iter().enumerate() {
        let want = i as u64 + 1;
        if s != want {
            return Err(Error::Corrupt(format!(
                "gap in log/{gen}: expected seq {want}, found {s}"
            )));
        }
    }
    let slots: Vec<Mutex<Option<LogRecord>>> = seqs.iter().map(|_| Mutex::new(None)).collect();
    let next = AtomicUsize::new(0);
    let failed: Mutex<Option<Error>> = Mutex::new(None);
    std::thread::scope(|s| {
        for _ in 0..concurrency.max(1).min(seqs.len()) {
            s.spawn(|| loop {
                if failed.lock().unwrap().is_some() {
                    return;
                }
                let i = next.fetch_add(1, Relaxed);
                let Some(&seq) = seqs.get(i) else {
                    return;
                };
                let r = get(store, retry, &keys.log(gen, seq)).and_then(|b| decode_record(&b));
                match r {
                    Ok(rec) => *slots[i].lock().unwrap() = Some(rec),
                    Err(e) => {
                        failed.lock().unwrap().get_or_insert(e);
                        return;
                    }
                }
            });
        }
    });
    if let Some(e) = failed.into_inner().unwrap() {
        return Err(e);
    }
    Ok(slots
        .into_iter()
        .map(|m| m.into_inner().unwrap().expect("every slot was fetched"))
        .collect())
}

/// MVCC lives in header bytes 18/19 of page 1 (D5).
fn refuse_mvcc(page1: &[u8]) -> Result<()> {
    if page1.len() < 20 {
        return Err(Error::Corrupt("page 1 is shorter than its header".into()));
    }
    if page1[18] == 255 || page1[19] == 255 {
        return Err(Error::Unsupported("MVCC"));
    }
    Ok(())
}

/// The first whole page-1 frame of a `-wal` image (layout as in `wal_frame_pages`).
fn wal_page_one(wal: &[u8]) -> Option<&[u8]> {
    let ps = u32::from_be_bytes(wal.get(8..12)?.try_into().ok()?) as usize;
    if ps == 0 {
        return None;
    }
    wal.get(32..)?
        .chunks_exact(24 + ps)
        .find(|f| f[0..4] == 1u32.to_be_bytes())
        .map(|f| &f[24..])
}

/// The `-wal` image `B` of the head's generation: every record of `log/<generation>`
/// replayed in order, and the number of records.
///
/// Which writers' records a generation may hold: runs of records by other writer ids (one run
/// per writer), then only records of `head.writer_id`. A claim (`bucket::claim`) hands a restored copy's head
/// to a new writer id inside the same generation, so the records before it are the old
/// writer's. Each writer's next seq is its own counter and the first `put_if_absent`
/// collision fences the loser for good, so bucket fencing can only produce "the old writer's
/// records, then the claimer's" (a claim of a claimed copy adds one more run). A record of
/// another writer after a record of `head.writer_id`, a writer whose records resume after
/// another writer's, or any fence record means two writers interleaved (the replay would be
/// inconsistent): `Fenced`.
pub(crate) fn generation_image(
    store: &dyn ObjectStore,
    keys: &Keys,
    head: &Head,
    cfg: &EngineConfig,
) -> Result<(Vec<u8>, u64)> {
    let records = fetch_generation(
        store,
        keys,
        head.generation,
        cfg.upload_concurrency,
        &cfg.retry,
    )?;
    let fenced = |writer_id: &str| {
        Error::Fenced(format!(
            "log/{} holds a record of writer {writer_id:?} after one of {:?}",
            head.generation, head.writer_id
        ))
    };
    let mut wal = Vec::new();
    // Writers whose run of records has ended; each writer's records are one run.
    let mut ended: Vec<&str> = Vec::new();
    let mut current: Option<&str> = None;
    for rec in &records {
        match rec {
            LogRecord::Extents { writer_id, syncs } => {
                if current != Some(writer_id.as_str()) {
                    if let Some(prev) = current {
                        if prev == head.writer_id {
                            return Err(fenced(writer_id));
                        }
                        ended.push(prev);
                    }
                    if ended.contains(&writer_id.as_str()) {
                        return Err(Error::Fenced(format!(
                            "log/{} holds records of writer {writer_id:?} on both sides of \
                             another writer's",
                            head.generation
                        )));
                    }
                    current = Some(writer_id);
                }
                for ops in syncs {
                    apply_ops(&mut wal, ops)?;
                }
            }
            LogRecord::Fence { writer_id } => {
                return Err(Error::Fenced(format!(
                    "log/{} holds a fence record of writer {writer_id:?}",
                    head.generation
                )));
            }
        }
    }
    Ok((wal, records.len() as u64))
}

/// The database file to put next to the `-wal` image `wal` while the bucket holds no page
/// (never sealed). The engine reads an empty database file as a new database and deletes a
/// WAL holding frames next to it as an orphan (database.rs:738-760, 2223-2246). Any page-1
/// frame bootstraps it (page size, reserved space); page 1 itself is read from the WAL, which
/// holds the current version. Empty when `wal` has no page-1 frame.
pub(crate) fn page_one_seed(wal: &[u8]) -> Result<Vec<u8>> {
    match wal_page_one(wal) {
        Some(p) => {
            refuse_mvcc(p)?;
            Ok(p.to_vec())
        }
        None => Ok(Vec::new()),
    }
}

/// Write `bytes` to `path` and fsync it.
pub(crate) fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = fs::File::create(path).map_err(|e| io_err("create", e))?;
    f.write_all(bytes)
        .and_then(|_| f.sync_all())
        .map_err(|e| io_err("write", e))
}

/// Restore the bucket's current state into `dir`, which must hold no database yet. It
/// writes nothing to the bucket.
pub(crate) fn restore_into(
    store: Arc<dyn ObjectStore>,
    keys: &Keys,
    dir: &Path,
    cfg: &EngineConfig,
) -> Result<RestoreReport> {
    let start = Instant::now();
    let before = store.stats();
    for f in ["app.db", "app.db-wal"] {
        if dir.join(f).exists() {
            return Err(Error::Config(format!(
                "{} already holds a database ({f})",
                dir.display()
            )));
        }
    }
    let retry = &cfg.retry;
    let head_key = keys.head();
    let head_obj = with_retry(retry, &head_key, |_| store.get(&head_key))?
        .ok_or_else(|| Error::NotFound(head_key.clone()))?;
    let head = Head::decode(&head_obj.body)?;
    if let Some(g) = head.manifest {
        let manifest = Manifest::decode(&get(&*store, retry, &keys.manifest(g))?)?;
        let pg = manifest
            .pages
            .get(&1)
            .ok_or_else(|| Error::Corrupt(format!("manifest/{g} has no entry for page 1")))?;
        refuse_mvcc(&get(&*store, retry, &keys.page(*pg, 1))?)?;
    }
    let (wal, log_objects) = generation_image(&*store, keys, &head, cfg)?;
    // Never sealed: the bucket holds no page, so the database file is seeded (`page_one_seed`).
    let db = match head.manifest {
        Some(_) => Vec::new(),
        None => page_one_seed(&wal)?,
    };
    fs::create_dir_all(dir).map_err(|e| io_err("create dir", e))?;
    write_synced(&dir.join("app.db-wal"), &wal)?;
    write_synced(&dir.join("app.db"), &db)?;
    fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io_err("fsync dir", e))?;
    LocalState {
        format: 1,
        writer_id: head.writer_id.clone(),
        generation: head.generation,
        complete: false,
        // Nothing is written to the bucket: the first writer open claims the head (with
        // this ETag) under a new writer id.
        claimed: false,
        head_etag: head_obj.etag,
    }
    .write_atomic(dir)?;
    let delta = store.stats().since(&before);
    Ok(RestoreReport {
        generation: head.generation,
        log_objects,
        gets: delta.gets,
        lists: delta.lists,
        bytes: delta.bytes_down,
        elapsed: start.elapsed(),
    })
}

/// Restore the bucket `config` names into `dir`, which must hold no database yet. Nothing is
/// written to the bucket; the first [`crate::open`] of `dir` claims the database.
pub fn restore(config: &Config, dir: &Path) -> Result<RestoreReport> {
    let store = config.s3_store()?;
    restore_with_store(config, store, dir)
}

#[doc(hidden)]
pub fn restore_with_store(
    config: &Config,
    store: Arc<dyn ObjectStore>,
    dir: &Path,
) -> Result<RestoreReport> {
    config.validate()?;
    let guard = DirGuard::acquire(dir)?;
    restore_staged(store, &config.keys(), guard.dir(), &config.engine(false))
}

/// Present while a restore moves its files into the directory; see `restore_staged`.
const RESTORING: &str = "app.db-restoring";
/// Moved in this order: the local state last, so a directory with a state file is whole.
const RESTORED: [&str; 3] = ["app.db-wal", "app.db", "app.db-durable"];

/// Restore into `dir` (held by the caller: `DirGuard`) without ever leaving it half
/// restored: the files are built in a staging directory next to it (same filesystem), then
/// renamed in while `app.db-restoring` marks the move. A failure before the move leaves `dir`
/// as it was; a crash during it leaves the marker, and `clear_interrupted_restore` removes
/// the partial files at the next open or restore.
pub(crate) fn restore_staged(
    store: Arc<dyn ObjectStore>,
    keys: &Keys,
    dir: &Path,
    cfg: &EngineConfig,
) -> Result<RestoreReport> {
    clear_interrupted_restore(dir)?;
    for f in ["app.db", "app.db-wal", "app.db-durable"] {
        if dir.join(f).exists() {
            return Err(Error::Config(format!(
                "{} already holds a database ({f})",
                dir.display()
            )));
        }
    }
    let staging = staging_dir(dir)?;
    let r = restore_into(store, keys, &staging, cfg)
        .and_then(|report| move_restored(&staging, dir).map(|_| report));
    let _ = fs::remove_dir_all(&staging);
    r
}

/// The name prefix of `dir`'s staging directories: `.<name>.restore-`.
fn staging_prefix(dir: &Path) -> Option<String> {
    dir.file_name()
        .map(|n| format!(".{}.restore-", n.to_string_lossy()))
}

fn sweep_staging_dirs(dir: &Path) -> Result<()> {
    let (Some(parent), Some(prefix)) = (dir.parent(), staging_prefix(dir)) else {
        return Ok(());
    };
    let entries = fs::read_dir(parent).map_err(|e| io_err("list parent dir", e))?;
    for e in entries {
        let e = e.map_err(|e| io_err("list parent dir", e))?;
        if e.file_name().to_string_lossy().starts_with(&prefix) {
            fs::remove_dir_all(e.path()).map_err(|e| io_err("remove staging dir", e))?;
        }
    }
    Ok(())
}

/// `<parent>/.<name>.restore-<pid>-<nanos>`.
fn staging_dir(dir: &Path) -> Result<PathBuf> {
    let (Some(parent), Some(prefix)) = (dir.parent(), staging_prefix(dir)) else {
        return Err(Error::Config(format!(
            "cannot restore into {}: it has no parent directory",
            dir.display()
        )));
    };
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    Ok(parent.join(format!("{prefix}{}-{nanos}", std::process::id())))
}

fn fsync_dir(dir: &Path) -> Result<()> {
    fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io_err("fsync dir", e))
}

fn move_restored(staging: &Path, dir: &Path) -> Result<()> {
    write_synced(&dir.join(RESTORING), b"")?;
    fsync_dir(dir)?;
    for f in RESTORED {
        let from = staging.join(f);
        if from.exists() {
            fs::rename(&from, dir.join(f)).map_err(|e| io_err("move restored file", e))?;
        }
    }
    fsync_dir(dir)?;
    fs::remove_file(dir.join(RESTORING)).map_err(|e| io_err("remove restore marker", e))?;
    fsync_dir(dir)
}

/// Remove what an interrupted restore of `dir` left: staging directories next to it
/// (`.<name>.restore-*`, a crash during `restore_into`), and, when `app.db-restoring` is
/// present (a crash while moving the files in), the files it moved, so the directory is
/// empty again. Called with the directory held, so no restore of it is running.
pub(crate) fn clear_interrupted_restore(dir: &Path) -> Result<()> {
    sweep_staging_dirs(dir)?;
    if !dir.join(RESTORING).exists() {
        return Ok(());
    }
    for f in RESTORED.iter().chain(&["app.db-dirty"]) {
        match fs::remove_file(dir.join(f)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err("remove partly restored file", e)),
        }
    }
    fsync_dir(dir)?;
    fs::remove_file(dir.join(RESTORING)).map_err(|e| io_err("remove restore marker", e))?;
    fsync_dir(dir)
}
