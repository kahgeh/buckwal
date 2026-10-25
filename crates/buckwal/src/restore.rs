// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! Restore onto an empty directory: rebuild the current generation's `-wal` from its log and
//! leave every database page in the bucket, to be fetched on first read.

use crate::config::{Config, EngineConfig, JournalMode};
use crate::db::{refuse_mode_mismatch, DirGuard};
use crate::encryption::Encryption;
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE, DIRTY_FILE, LOG_FILE, RESTORING_FILE, STATE_FILE, WAL_FILE};
use crate::format::{
    apply_ops, Head, Keys, LocalState, LogFile, LogRecord, FORMAT, FORMAT_V2, FORMAT_V3, FORMAT_V4,
};
use crate::store::{with_retry, ObjectStore, RetryPolicy, StoreError};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::field::Empty;
use turso_core::io::FileSyncType;
use turso_core::mvcc::persistent_storage::{DurableStorage, Storage};
use turso_core::{
    Completion, DatabaseOpts, EncryptionOpts, OpenFlags, OpenOptions, PlatformIO, SqliteDialect, IO,
};

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
                        .and_then(|b| keys.decode_record(&b, gen, seq));
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

/// Page 1 (header bytes 18/19, D5) against the head's journal mode and the one this
/// restore asks for (`refuse_mode_mismatch`).
fn refuse_mvcc(page1: &[u8], head: &Head, mode: JournalMode) -> Result<()> {
    if page1.len() < 20 {
        return Err(Error::Corrupt("page 1 is shorter than its header".into()));
    }
    refuse_mode_mismatch(head, Some([page1[18], page1[19]]), mode)
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
    let manifest = keys.decode_manifest(&get(store, retry, retries, &keys.manifest(g))?, g)?;
    let pg = manifest
        .pages
        .get(&1)
        .ok_or_else(|| Error::Corrupt(format!("manifest/{g} has no entry for page 1")))?;
    let obj = get(store, retry, retries, &keys.page(*pg, 1))?;
    keys.open_page(1, *pg, &obj).map(<[u8]>::to_vec)
}

/// The bucket's sealed page 1: the head, its manifest, then the page (3 GETs). None while the
/// bucket holds no manifest (never sealed: page 1 is in the log).
pub(crate) fn sealed_page_one(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
) -> Result<Option<Vec<u8>>> {
    let retries = AtomicU64::new(0);
    let head = keys.decode_head_open(&get(store, retry, &retries, &keys.head())?)?;
    head.manifest
        .map(|g| manifest_page_one(store, keys, g, retry, &retries))
        .transpose()
}

/// The two images of the head's generation, and the number of records they were made from
/// (which is also the highest seq: the seqs run 1, 2, 3, …).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Images {
    /// The `-wal` image `B`: the kind-1 records replayed in order.
    pub wal: Vec<u8>,
    /// The `app.db-log` image: the kind-3 records replayed in order (empty in a WAL bucket).
    pub log: Vec<u8>,
    pub records: u64,
}

/// The images of the head's generation: every record of `log/<generation>` replayed in
/// order, kind 1 into the `-wal` image and kind 3 into the `app.db-log` image.
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
) -> Result<Images> {
    generation_image_counted(store, keys, head, cfg, &AtomicU64::new(0))
}

/// [`generation_image`], adding the extra attempts of its requests to `retries`.
fn generation_image_counted(
    store: &dyn ObjectStore,
    keys: &Keys,
    head: &Head,
    cfg: &EngineConfig,
    retries: &AtomicU64,
) -> Result<Images> {
    let records = fetch_generation(
        store,
        keys,
        head.generation,
        cfg.upload_concurrency,
        &cfg.retry,
        retries,
    )?;
    replay(&records, head)
}

/// The images of `records`, generation `head.generation`'s log from seq 1 in order, under the
/// rules [`generation_image`] names. A pack run replays an ended generation with it, with
/// `head` the current head at that generation, so a pack holds exactly the records a restore
/// applies.
pub(crate) fn replay(records: &[LogRecord], head: &Head) -> Result<Images> {
    let fenced = |writer_id: &str| {
        Error::Fenced(format!(
            "log/{} holds a record of writer {writer_id:?} after one of {:?}",
            head.generation, head.writer_id
        ))
    };
    let mut wal = Vec::new();
    let mut log = Vec::new();
    // Writers whose run of records has ended; each writer's records are one run.
    let mut ended: Vec<&str> = Vec::new();
    let mut current: Option<&str> = None;
    // The latest fence (writer, epoch), once the log holds one.
    let mut fence: Option<(&str, u64)> = None;
    for (i, rec) in records.iter().enumerate() {
        let seq = i + 1;
        match rec {
            LogRecord::Extents {
                file,
                writer_id,
                syncs,
                ..
            } => {
                // Kind 3 exists only in an MVCC bucket (format 2, or 3 when encrypted).
                if *file == LogFile::Log && head.journal_mode != JournalMode::Mvcc {
                    return Err(Error::Corrupt(format!(
                        "log/{} seq {seq}: a .db-log record in a format {} bucket",
                        head.generation, head.format
                    )));
                }
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
                let image = match file {
                    LogFile::Wal => &mut wal,
                    LogFile::Log => &mut log,
                };
                for ops in syncs {
                    apply_ops(image, ops)?;
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
    Ok(Images {
        wal,
        log,
        records: records.len() as u64,
    })
}

/// The database file to put next to the `-wal` image `wal` while the bucket holds no page
/// (never sealed). The engine reads an empty database file as a new database and deletes a
/// WAL holding frames next to it as an orphan (database.rs:738-760, 2223-2246). Any page-1
/// frame bootstraps it (page size, reserved space); page 1 itself is read from the WAL, which
/// holds the current version. Empty when `wal` has no page-1 frame.
pub(crate) fn page_one_seed(wal: &[u8], head: &Head, mode: JournalMode) -> Result<Vec<u8>> {
    match wal_page_one(wal) {
        Some(p) => {
            refuse_mvcc(p, head, mode)?;
            Ok(p.to_vec())
        }
        None => Ok(Vec::new()),
    }
}

/// What a database file's header says, read through the engine (an encrypted file's header is
/// ciphertext).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DbFacts {
    pub page_size: u32,
    pub page_count: u64,
    /// Pages on the free list.
    pub free_pages: u64,
}

fn local_engine(what: &str, e: turso_core::LimboError) -> Error {
    Error::Engine(format!("{what}: {e}"))
}

/// Open `<dir>/app.db` (and its `-wal`) with the engine over plain local files, as
/// `create::build_mvcc_image` does: with `enc`, the database option, the cipher and key at
/// open, and the key on the connection. Nothing goes to a bucket.
fn open_local(
    dir: &Path,
    enc: Option<&Encryption>,
) -> Result<(Arc<turso_core::Database>, Arc<turso_core::Connection>)> {
    let db_path = dir.join(DB_FILE);
    let path = db_path
        .to_str()
        .ok_or_else(|| Error::Config(format!("{} is not UTF-8", db_path.display())))?;
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().map_err(|e| io_err("platform IO", e))?);
    let mut opts = OpenOptions::new(Arc::new(SqliteDialect)).flags(OpenFlags::Create);
    if let Some(enc) = enc {
        opts = opts
            .db_opts(DatabaseOpts::new().with_encryption(true))
            .encryption(EncryptionOpts {
                cipher: enc.cipher.to_string(),
                hexkey: hex::encode(enc.key.as_slice()),
            });
    }
    let db = turso_core::Database::open(io, path, opts)
        .map_err(|e| local_engine("open the local database", e))?;
    let conn = db
        .connect_with_encryption(enc.map(|e| e.key.clone()))
        .map_err(|e| local_engine("connect to the local database", e))?;
    Ok((db, conn))
}

fn pragma_int(conn: &Arc<turso_core::Connection>, sql: &str) -> Result<i64> {
    let rows = conn
        .query(sql)
        .map_err(|e| local_engine(sql, e))?
        .ok_or_else(|| Error::Engine(format!("{sql} returned no statement")))?
        .run_collect_rows()
        .map_err(|e| local_engine(sql, e))?;
    rows.first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_int())
        .ok_or_else(|| Error::Engine(format!("{sql} returned no integer")))
}

/// The header facts of the database file `<dir>/app.db`.
pub(crate) fn db_facts(dir: &Path, enc: Option<&Encryption>) -> Result<DbFacts> {
    let (db, conn) = open_local(dir, enc)?;
    let n = |sql: &str| -> Result<u64> {
        u64::try_from(pragma_int(&conn, sql)?)
            .map_err(|_| Error::Engine(format!("{sql} returned a negative value")))
    };
    let facts = DbFacts {
        page_size: u32::try_from(n("PRAGMA page_size")?)
            .map_err(|_| Error::Engine("PRAGMA page_size is too large".into()))?,
        page_count: n("PRAGMA page_count")?,
        free_pages: n("PRAGMA freelist_count")?,
    };
    conn.close()
        .map_err(|e| local_engine("close the local database", e))?;
    drop(db);
    Ok(facts)
}

/// Apply one ended generation of a WAL database, its `-wal` image `wal`, to the database file
/// `<dir>/app.db` with a local checkpoint of the engine: write the image as `<dir>/app.db-wal`,
/// open the engine over the two local files (`open_local`), run a TRUNCATE checkpoint and
/// close. Afterwards `app.db` holds every commit of the generation and the `-wal` is empty. A
/// missing or empty `app.db` (no pack yet) is seeded with the image's page 1
/// (`page_one_seed`, as a restore of a bucket with no page does). An image with no whole frame
/// changes nothing. `head` is the head at that generation (its journal mode is checked
/// against page 1).
pub(crate) fn apply_generation(
    dir: &Path,
    wal: &[u8],
    head: &Head,
    enc: Option<&Encryption>,
) -> Result<()> {
    let wal_path = dir.join(WAL_FILE);
    let db_path = dir.join(DB_FILE);
    if !wal_image_holds_frames(wal) {
        return Ok(());
    }
    if file_len(&db_path)? == 0 {
        let seed = page_one_seed(wal, head, JournalMode::Wal)?;
        if seed.is_empty() {
            return Err(Error::Corrupt(format!(
                "log/{}: the first generation of a database without a pack has no page 1",
                head.generation
            )));
        }
        write_synced(&db_path, &seed)?;
    }
    write_synced(&wal_path, wal)?;
    {
        let (db, conn) = open_local(dir, enc)?;
        conn.checkpoint(turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .map_err(|e| local_engine("local checkpoint", e))?;
        conn.close()
            .map_err(|e| local_engine("close the local database", e))?;
        drop(db);
    }
    let left = file_len(&wal_path)?;
    if left != 0 {
        return Err(Error::Engine(format!(
            "the local TRUNCATE checkpoint left a {left}-byte app.db-wal"
        )));
    }
    Ok(())
}

/// Whether a `-wal` image holds at least one whole frame (`wal_holds_frames` on bytes).
fn wal_image_holds_frames(wal: &[u8]) -> bool {
    let Some(ps) = wal.get(8..12) else {
        return false;
    };
    let ps = u64::from(u32::from_be_bytes(ps.try_into().unwrap()));
    ps > 0 && wal.len() as u64 >= WAL_HEADER_LEN + WAL_FRAME_HEADER_LEN + ps
}

/// The engine's logical-log header size (`LOG_HDR_SIZE`).
const LOG_HEADER_LEN: u64 = 56;

/// The `-wal` header (32 bytes) and the frame header (24 bytes) of the SQLite WAL format.
const WAL_HEADER_LEN: u64 = 32;
const WAL_FRAME_HEADER_LEN: u64 = 24;

fn file_len(path: &Path) -> Result<u64> {
    match fs::metadata(path) {
        Ok(m) => Ok(m.len()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(io_err("stat", e)),
    }
}

/// Whether `<dir>/app.db-wal` holds at least one whole frame.
fn wal_holds_frames(wal: &Path) -> Result<bool> {
    let len = file_len(wal)?;
    if len < WAL_HEADER_LEN {
        return Ok(false);
    }
    let mut header = [0u8; 12];
    fs::File::open(wal)
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut header))
        .map_err(|e| io_err("read -wal header", e))?;
    let page_size = u64::from(u32::from_be_bytes(header[8..12].try_into().unwrap()));
    Ok(page_size > 0 && len >= WAL_HEADER_LEN + WAL_FRAME_HEADER_LEN + page_size)
}

/// The engine refuses to open WAL frames next to a log under 56 bytes ("WAL has committed
/// frames but logical log header is missing", docs/mvcc-recon.md Q4). When `<dir>` is in that
/// state, write the log a fresh header with the engine's own `reset_to_fresh_header` (a
/// standalone `Storage`, pinned by `reset_to_fresh_header_works_standalone`), and fsync it.
/// The header is never encrypted, so no key is needed. Returns whether it wrote. Called with
/// the directory held and the engine not open: after a restore, and before every MVCC open
/// (a crash between a seal's log truncate and its `-wal` truncate leaves this state).
pub(crate) fn repair_wal_without_log(dir: &Path) -> Result<bool> {
    let log_path = dir.join(LOG_FILE);
    if file_len(&log_path)? >= LOG_HEADER_LEN || !wal_holds_frames(&dir.join(WAL_FILE))? {
        return Ok(false);
    }
    let path = log_path
        .to_str()
        .ok_or_else(|| Error::Config(format!("{} is not UTF-8", log_path.display())))?;
    let engine = |what: &str, e: turso_core::LimboError| io_err(what, e);
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().map_err(|e| io_err("platform IO", e))?);
    let file = io
        .open_file(path, OpenFlags::Create, false)
        .map_err(|e| engine("open app.db-log", e))?;
    let storage = Storage::new(file.clone(), io.clone(), None);
    let c = storage
        .reset_to_fresh_header()
        .map_err(|e| engine("write the app.db-log header", e))?;
    io.wait_for_completion(c)
        .map_err(|e| engine("write the app.db-log header", e))?;
    let c = file
        .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
        .map_err(|e| engine("fsync app.db-log", e))?;
    io.wait_for_completion(c)
        .map_err(|e| engine("fsync app.db-log", e))?;
    fsync_dir(dir)?;
    Ok(true)
}

/// Write `bytes` to `path` and fsync it.
pub(crate) fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = fs::File::create(path).map_err(|e| io_err("create", e))?;
    f.write_all(bytes)
        .and_then(|_| f.sync_all())
        .map_err(|e| io_err("write", e))
}

/// [`restore_into_seen`] without the page (tests; the library restores through
/// [`restore_staged`]).
#[cfg(test)]
pub(crate) fn restore_into(
    store: Arc<dyn ObjectStore>,
    keys: &Keys,
    dir: &Path,
    cfg: &EngineConfig,
) -> Result<RestoreReport> {
    restore_into_seen(store, keys, dir, cfg).map(|(report, _)| report)
}

/// The `format` of the local state a restore of `head` writes: the head's, except that an
/// encrypted bucket's format 3 says nothing of the mode, which the local state's format does
/// (1 WAL, 2 MVCC; whether the database is encrypted is the configuration's, checked
/// against page 1).
pub(crate) fn local_format(head: &Head) -> u32 {
    match (head.format, head.journal_mode) {
        (FORMAT_V3 | FORMAT_V4, JournalMode::Mvcc) => FORMAT_V2,
        (FORMAT_V3 | FORMAT_V4, JournalMode::Wal) => FORMAT,
        (f, _) => f,
    }
}

/// Restore the bucket's current state into `dir`, which must hold no database yet. It
/// writes nothing to the bucket. Runs in a `buckwal.restore` span, and logs the finished
/// restore. The span's `buckwal.seq` is the highest log seq replayed (0 for an empty log), and
/// `buckwal.retries` the extra attempts of the restore's requests, and `buckwal.outcome`
/// `ok`, `integrity` (a failed integrity check) or `error`. Also returns page 1 as the
/// restore saw it: the sealed page 1 it fetched for the MVCC check, or the WAL's page-1 frame
/// it seeded `app.db` with; None when it saw neither.
fn restore_into_seen(
    store: Arc<dyn ObjectStore>,
    keys: &Keys,
    dir: &Path,
    cfg: &EngineConfig,
) -> Result<(RestoreReport, Option<Vec<u8>>)> {
    let span = tracing::info_span!(
        target: "buckwal",
        "buckwal.restore",
        buckwal.generation = Empty,
        buckwal.log_objects = Empty,
        buckwal.gets = Empty,
        buckwal.bytes = Empty,
        buckwal.seq = Empty,
        buckwal.retries = Empty,
        s3.status = Empty,
        buckwal.outcome = Empty,
    );
    let _in = span.enter();
    let retries = AtomicU64::new(0);
    let r = restore_files(store, keys, dir, cfg, &retries);
    span.record("buckwal.retries", retries.load(Relaxed));
    let (r, page1) = match r {
        Ok(r) => r,
        Err(e) => {
            span.record("buckwal.outcome", e.outcome());
            return Err(e);
        }
    };
    span.record("buckwal.outcome", "ok");
    span.record("buckwal.generation", r.generation);
    span.record("buckwal.log_objects", r.log_objects);
    // The seqs replayed run 1..=log_objects.
    span.record("buckwal.seq", r.log_objects);
    span.record("buckwal.gets", r.gets);
    span.record("buckwal.bytes", r.bytes);
    tracing::info!(
        target: "buckwal",
        generation = r.generation,
        log_objects = r.log_objects,
        gets = r.gets,
        bytes = r.bytes,
        ms = r.elapsed.as_millis() as u64,
        "restore finished"
    );
    Ok((r, page1))
}

fn restore_files(
    store: Arc<dyn ObjectStore>,
    keys: &Keys,
    dir: &Path,
    cfg: &EngineConfig,
    retries: &AtomicU64,
) -> Result<(RestoreReport, Option<Vec<u8>>)> {
    let start = Instant::now();
    let before = store.stats();
    for f in [DB_FILE, WAL_FILE, LOG_FILE] {
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
    let head = keys.decode_head_open(&head_obj.body)?;
    // The mode is checked on the head alone first: nothing is fetched for a mismatch.
    refuse_mode_mismatch(&head, None, cfg.journal_mode)?;
    let sealed_page1 = match head.manifest {
        Some(g) => {
            let p = manifest_page_one(&*store, keys, g, retry, retries)?;
            refuse_mvcc(&p, &head, cfg.journal_mode)?;
            Some(p)
        }
        None => None,
    };
    let Images {
        wal,
        log,
        records: log_objects,
    } = generation_image_counted(&*store, keys, &head, cfg, retries)?;
    // Never sealed: the bucket holds no page, so the database file is seeded (`page_one_seed`).
    let db = match head.manifest {
        Some(_) => Vec::new(),
        None => page_one_seed(&wal, &head, cfg.journal_mode)?,
    };
    fs::create_dir_all(dir).map_err(|e| io_err("create dir", e))?;
    write_synced(&dir.join(WAL_FILE), &wal)?;
    // Only an MVCC database has an `app.db-log`; a WAL restore writes exactly what it did.
    if head.journal_mode == JournalMode::Mvcc {
        write_synced(&dir.join(LOG_FILE), &log)?;
        repair_wal_without_log(dir)?;
    }
    write_synced(&dir.join(DB_FILE), &db)?;
    fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io_err("fsync dir", e))?;
    LocalState {
        format: local_format(&head),
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
        next_generation_floor: 0,
    }
    .write_atomic(dir)?;
    let delta = store.stats().since(&before);
    let report = RestoreReport {
        generation: head.generation,
        log_objects,
        gets: delta.gets,
        lists: delta.lists,
        bytes: delta.bytes_down,
        elapsed: start.elapsed(),
    };
    let page1 = sealed_page1.or_else(|| (!db.is_empty()).then_some(db));
    Ok((report, page1))
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
    restore_staged(
        store,
        &config.keys()?,
        guard.dir(),
        &config.engine(crate::io::IoMode::Writer),
    )
    .map(|staged| staged.report)
}

/// What [`restore_staged`] hands on.
pub(crate) struct Staged {
    pub report: RestoreReport,
    /// Page 1 as the restore saw it (`restore_into_seen`), for the encryption check before a
    /// claim: no extra GET.
    pub page1: Option<Vec<u8>>,
}

/// Moved in this order: the local state last, so a directory with a state file is whole.
const RESTORED: [&str; 4] = [WAL_FILE, LOG_FILE, DB_FILE, STATE_FILE];

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
) -> Result<Staged> {
    clear_interrupted_restore(dir)?;
    for f in [DB_FILE, WAL_FILE, LOG_FILE, STATE_FILE] {
        if dir.join(f).exists() {
            return Err(Error::Config(format!(
                "{} already holds a database ({f})",
                dir.display()
            )));
        }
    }
    let staging = staging_dir(dir)?;
    let r = restore_into_seen(store, keys, &staging, cfg)
        .and_then(|(report, page1)| move_restored(&staging, dir).map(|_| Staged { report, page1 }));
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

pub(crate) fn fsync_dir(dir: &Path) -> Result<()> {
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
/// (`.<name>.restore-*`, a crash during `restore_into_seen`), and, when `app.db-restoring` is
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
