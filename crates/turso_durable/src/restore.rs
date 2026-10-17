// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! Restore onto an empty directory: rebuild the current generation's `-wal` from its log and
//! leave every database page in the bucket, to be fetched on first read.

use crate::config::{Config, EngineConfig};
use crate::db::DirGuard;
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE, DIRTY_FILE, RESTORING_FILE, STATE_FILE, WAL_FILE};
use crate::format::{
    apply_ops, decode_record, Head, Keys, LocalState, LogRecord, Manifest, FORMAT,
};
use crate::store::{with_retry, ObjectStore, RetryPolicy, StoreError};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::field::Empty;

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

/// `with_retry` that adds its extra attempts (attempt numbers above 0) to `retries`.
fn counted<T>(
    retries: &AtomicU64,
    retry: &RetryPolicy,
    what: &str,
    mut op: impl FnMut() -> std::result::Result<T, StoreError>,
) -> Result<T> {
    with_retry(retry, what, |attempt| {
        if attempt > 0 {
            retries.fetch_add(1, Relaxed);
        }
        op()
    })
}

fn get(
    store: &dyn ObjectStore,
    retry: &RetryPolicy,
    retries: &AtomicU64,
    key: &str,
) -> Result<Vec<u8>> {
    counted(retries, retry, key, || store.get(key))?
        .map(|o| o.body)
        .ok_or_else(|| Error::NotFound(key.to_string()))
}

/// Every log record of generation `gen`, in order. The seqs must run 1, 2, 3, … with no gap.
/// Extra attempts of its requests are added to `retries`.
pub(crate) fn fetch_generation(
    store: &dyn ObjectStore,
    keys: &Keys,
    gen: u64,
    concurrency: usize,
    retry: &RetryPolicy,
    retries: &AtomicU64,
) -> Result<Vec<LogRecord>> {
    let prefix = keys.log_prefix(gen);
    let mut seqs: Vec<u64> = counted(retries, retry, &prefix, || store.list(&prefix))?
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
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    std::thread::scope(|s| {
        for _ in 0..concurrency.max(1).min(seqs.len()) {
            s.spawn(|| {
                tracing::dispatcher::with_default(&dispatch, || loop {
                    if failed.lock().unwrap().is_some() {
                        return;
                    }
                    let i = next.fetch_add(1, Relaxed);
                    let Some(&seq) = seqs.get(i) else {
                        return;
                    };
                    let r = get(store, retry, retries, &keys.log(gen, seq))
                        .and_then(|b| decode_record(&b));
                    match r {
                        Ok(rec) => *slots[i].lock().unwrap() = Some(rec),
                        Err(e) => {
                            failed.lock().unwrap().get_or_insert(e);
                            return;
                        }
                    }
                })
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
pub(crate) fn wal_page_one(wal: &[u8]) -> Option<&[u8]> {
    let ps = u32::from_be_bytes(wal.get(8..12)?.try_into().ok()?) as usize;
    if ps == 0 {
        return None;
    }
    wal.get(32..)?
        .chunks_exact(24 + ps)
        .find(|f| f[0..4] == 1u32.to_be_bytes())
        .map(|f| &f[24..])
}

/// Page 1 as `manifest/<g>` maps it.
fn manifest_page_one(
    store: &dyn ObjectStore,
    keys: &Keys,
    g: u64,
    retry: &RetryPolicy,
    retries: &AtomicU64,
) -> Result<Vec<u8>> {
    let manifest = Manifest::decode(&get(store, retry, retries, &keys.manifest(g))?)?;
    let pg = manifest
        .pages
        .get(&1)
        .ok_or_else(|| Error::Corrupt(format!("manifest/{g} has no entry for page 1")))?;
    get(store, retry, retries, &keys.page(*pg, 1))
}

/// The bucket's sealed page 1: the head, its manifest, then the page (3 GETs). None while the
/// bucket holds no manifest (never sealed: page 1 is in the log).
pub(crate) fn sealed_page_one(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
) -> Result<Option<Vec<u8>>> {
    let retries = AtomicU64::new(0);
    let head = Head::decode(&get(store, retry, &retries, &keys.head())?)?;
    head.manifest
        .map(|g| manifest_page_one(store, keys, g, retry, &retries))
        .transpose()
}

/// The `-wal` image `B` of the head's generation: every record of `log/<generation>`
/// replayed in order, and the number of records (which is also the highest seq: the seqs run
/// 1, 2, 3, …).
///
/// Which writers' records a generation may hold: runs of records by other writer ids (one run
/// per writer), then only records of `head.writer_id`. A takeover
/// (`bucket::take_over_counted`) hands a restored copy's head to a new writer id inside the
/// same generation, so the records before it are the old writer's. Each writer's next seq is
/// its own counter and the first `put_if_absent` collision fences the loser for good, so
/// bucket fencing can only produce "the old writer's records, then the new writer's" (a
/// takeover of a taken-over copy adds one more run). A record of another writer after a
/// record of `head.writer_id`, or a writer whose records resume after another writer's, means
/// two writers interleaved (the replay would be inconsistent): `Fenced`. Logs written before
/// stage 3 hold no fence record and follow only this rule.
/// A fence record `F(w, k)` (every takeover appends one) ends that rule: after it every
/// Extents record must be by `w`, and a later fence needs an epoch above `k`, or `k` from the
/// same writer (a resumed claim).
pub(crate) fn generation_image(
    store: &dyn ObjectStore,
    keys: &Keys,
    head: &Head,
    cfg: &EngineConfig,
) -> Result<(Vec<u8>, u64)> {
    generation_image_counted(store, keys, head, cfg, &AtomicU64::new(0))
}

/// [`generation_image`], adding the extra attempts of its requests to `retries`.
fn generation_image_counted(
    store: &dyn ObjectStore,
    keys: &Keys,
    head: &Head,
    cfg: &EngineConfig,
    retries: &AtomicU64,
) -> Result<(Vec<u8>, u64)> {
    let records = fetch_generation(
        store,
        keys,
        head.generation,
        cfg.upload_concurrency,
        &cfg.retry,
        retries,
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
    // The latest fence (writer, epoch), once the log holds one.
    let mut fence: Option<(&str, u64)> = None;
    for (i, rec) in records.iter().enumerate() {
        let seq = i + 1;
        match rec {
            LogRecord::Extents { writer_id, syncs } => {
                if let Some((fencer, epoch)) = fence {
                    if writer_id != fencer {
                        return Err(Error::Fenced(format!(
                            "log/{} seq {seq}: a record of writer {writer_id:?} after the \
                             fence of writer {fencer:?} (epoch {epoch})",
                            head.generation
                        )));
                    }
                } else if current != Some(writer_id.as_str()) {
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
            LogRecord::Fence { writer_id, epoch } => {
                if let Some((prev_writer, prev_epoch)) = fence {
                    let rises =
                        *epoch > prev_epoch || (*epoch == prev_epoch && writer_id == prev_writer);
                    if !rises {
                        return Err(Error::Fenced(format!(
                            "log/{} seq {seq}: fence of writer {writer_id:?} (epoch {epoch}) \
                             does not follow the fence of writer {prev_writer:?} (epoch \
                             {prev_epoch})",
                            head.generation
                        )));
                    }
                }
                fence = Some((writer_id, *epoch));
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
/// writes nothing to the bucket. Runs in a `durable.restore` span, and logs the finished
/// restore. The span's `durable.seq` is the highest log seq replayed (0 for an empty log), and
/// `durable.retries` the extra attempts of the restore's requests.
pub(crate) fn restore_into(
    store: Arc<dyn ObjectStore>,
    keys: &Keys,
    dir: &Path,
    cfg: &EngineConfig,
) -> Result<RestoreReport> {
    let span = tracing::info_span!(
        target: "turso_durable",
        "durable.restore",
        durable.generation = Empty,
        durable.log_objects = Empty,
        durable.gets = Empty,
        durable.bytes = Empty,
        durable.seq = Empty,
        durable.retries = Empty,
        s3.status = Empty,
    );
    let _in = span.enter();
    let retries = AtomicU64::new(0);
    let r = restore_files(store, keys, dir, cfg, &retries);
    span.record("durable.retries", retries.load(Relaxed));
    let r = r?;
    span.record("durable.generation", r.generation);
    span.record("durable.log_objects", r.log_objects);
    // The seqs replayed run 1..=log_objects.
    span.record("durable.seq", r.log_objects);
    span.record("durable.gets", r.gets);
    span.record("durable.bytes", r.bytes);
    tracing::info!(
        target: "turso_durable",
        generation = r.generation,
        log_objects = r.log_objects,
        gets = r.gets,
        bytes = r.bytes,
        ms = r.elapsed.as_millis() as u64,
        "restore finished"
    );
    Ok(r)
}

fn restore_files(
    store: Arc<dyn ObjectStore>,
    keys: &Keys,
    dir: &Path,
    cfg: &EngineConfig,
    retries: &AtomicU64,
) -> Result<RestoreReport> {
    let start = Instant::now();
    let before = store.stats();
    for f in [DB_FILE, WAL_FILE] {
        if dir.join(f).exists() {
            return Err(Error::Config(format!(
                "{} already holds a database ({f})",
                dir.display()
            )));
        }
    }
    let retry = &cfg.retry;
    let head_key = keys.head();
    let head_obj = counted(retries, retry, &head_key, || store.get(&head_key))?
        .ok_or_else(|| Error::NotFound(head_key.clone()))?;
    let head = Head::decode(&head_obj.body)?;
    if let Some(g) = head.manifest {
        refuse_mvcc(&manifest_page_one(&*store, keys, g, retry, retries)?)?;
    }
    let (wal, log_objects) = generation_image_counted(&*store, keys, &head, cfg, retries)?;
    // Never sealed: the bucket holds no page, so the database file is seeded (`page_one_seed`).
    let db = match head.manifest {
        Some(_) => Vec::new(),
        None => page_one_seed(&wal)?,
    };
    fs::create_dir_all(dir).map_err(|e| io_err("create dir", e))?;
    write_synced(&dir.join(WAL_FILE), &wal)?;
    write_synced(&dir.join(DB_FILE), &db)?;
    fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io_err("fsync dir", e))?;
    LocalState {
        format: FORMAT,
        writer_id: head.writer_id.clone(),
        generation: head.generation,
        complete: false,
        // Nothing is written to the bucket: the first writer open claims the head (with
        // this ETag) under a new writer id.
        claimed: false,
        head_etag: head_obj.etag,
        epoch: head.epoch,
        claim_id: String::new(),
        seal_not_before_ms: 0,
        seal_quiet_ms: 0,
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
/// written to the bucket; the first writer [`crate::open`] of `dir` takes the database over
/// (as [`crate::takeover`] does).
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

/// Moved in this order: the local state last, so a directory with a state file is whole.
const RESTORED: [&str; 3] = [WAL_FILE, DB_FILE, STATE_FILE];

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
    for f in [DB_FILE, WAL_FILE, STATE_FILE] {
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
    write_synced(&dir.join(RESTORING_FILE), b"")?;
    fsync_dir(dir)?;
    for f in RESTORED {
        let from = staging.join(f);
        if from.exists() {
            fs::rename(&from, dir.join(f)).map_err(|e| io_err("move restored file", e))?;
        }
    }
    fsync_dir(dir)?;
    fs::remove_file(dir.join(RESTORING_FILE)).map_err(|e| io_err("remove restore marker", e))?;
    fsync_dir(dir)
}

/// Remove what an interrupted restore of `dir` left: staging directories next to it
/// (`.<name>.restore-*`, a crash during `restore_into`), and, when `app.db-restoring` is
/// present (a crash while moving the files in), the files it moved, so the directory is
/// empty again. Called with the directory held, so no restore of it is running.
pub(crate) fn clear_interrupted_restore(dir: &Path) -> Result<()> {
    sweep_staging_dirs(dir)?;
    if !dir.join(RESTORING_FILE).exists() {
        return Ok(());
    }
    remove_restored(dir)?;
    fs::remove_file(dir.join(RESTORING_FILE)).map_err(|e| io_err("remove restore marker", e))?;
    fsync_dir(dir)
}

/// Remove the files a restore moves into `dir` and the dirty file, those that exist, then
/// fsync `dir`: the directory holds no database afterwards. Called with the directory held.
pub(crate) fn remove_restored(dir: &Path) -> Result<()> {
    for f in RESTORED.iter().chain(&[DIRTY_FILE]) {
        match fs::remove_file(dir.join(f)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err("remove restored file", e)),
        }
    }
    fsync_dir(dir)
}
