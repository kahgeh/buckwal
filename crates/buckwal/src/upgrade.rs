//! [`upgrade`]: convert a bucket of format 1 to 4 to format 5 in place (spec Choice D (a)).
//!
//! The order of the steps:
//!
//! 1. A takeover, as `buckwal takeover` makes it, in a staging directory next to the target
//!    directory: the head names the upgrade's writer id at the next epoch, and the fence record
//!    ends the log of the head's generation G. Any old writer still running is fenced for good.
//!    The head stays at its old format from here until step 5, so nothing else can write a
//!    pointer: a pointer found later is a leftover of an earlier upgrade of this bucket.
//! 2. The first pack, built locally from the bucket's objects of the old format: every page that
//!    the head's manifest names (as stored, so in an encrypted bucket the engine's ciphertext),
//!    then the whole log of G applied with a local checkpoint, as a restore applies it (the
//!    records after the fence included; the upgrade appends none).
//! 3. The pack and its codes (`pack::first::upload_first`, checked as a pack run checks them).
//! 4. The pointer: `If-None-Match`, or, over a leftover pointer, `If-Match` on the ETag read.
//!    Before it, a GET of the head must find the head of step 1 still in place.
//! 5. The head at format 5, last, with `If-Match` on the ETag of step 1. Then the pointer is
//!    written again and checked (as an MVCC create does after its head, spec rule 2).
//! 6. The target directory gets the restored copy of the new head, as a restore makes it: the
//!    pack as `app.db`, and a local state that the first writer open claims.
//!
//! The pack generation and the head's generation are both T, the generation that a generation
//! switch of the taken-over head moves to: `max(G + 1, the head's floor)`. The pack holds G
//! whole, and every generation between G and T has no log (skipped generations). So the chain
//! of generations from the first pack ends at once at the head's generation: a restore and a
//! pack run read no log of the old format.
//!
//! A crash or a failure before step 5 leaves a bucket of the old format that works as before.
//! It holds a pack (and maybe a pointer) that older code does not read. The next upgrade starts
//! over: its takeover moves the floor, so its pack has a higher pack generation, and it replaces
//! the leftover pointer. GC deletes the pack of the failed run later. A format 1 to 3 bucket
//! gets a new database id at each run; a format 4 bucket keeps its own.
use crate::config::{Config, EngineConfig, JournalMode};
use crate::db::DirGuard;
use crate::encryption;
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE, LOCK_FILE, LOG_FILE, WAL_FILE};
use crate::format::{Head, Keys, LocalState, FORMAT_V4, FORMAT_V5};
use crate::pack::first::{self, FirstPack};
use crate::restore::{
    apply_images_over, db_facts, fetch_generation, fsync_dir, local_format, move_restored,
    rename_or_copy, repair_wal_without_log, replay, write_synced,
};
use crate::store::{with_retry, CondPut, ObjectStore};
use std::fs;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// What an [`upgrade`] did.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct UpgradeReport {
    /// The head's format before the upgrade (1 to 4).
    pub from_format: u32,
    /// The head's generation before the upgrade: the generation the takeover's fence record
    /// ended, which the first pack holds whole.
    pub from_generation: u64,
    /// The head's generation now, and the pack generation of the first pack.
    pub generation: u64,
    /// The head's epoch (the takeover's).
    pub epoch: u64,
    /// The seq of the takeover's fence record in `log/<from_generation>`.
    pub fence_seq: u64,
    /// The database id the format 5 head carries (new for a format 1 to 3 bucket).
    pub database_id: String,
    /// The first pack (`pack/<generation>/<run id>`, with the prefix).
    pub pack_key: String,
    /// The first pack's size in bytes.
    pub bytes: u64,
    /// A pointer of an earlier upgrade run was found and replaced.
    pub replaced_pointer: bool,
    pub elapsed: Duration,
}

/// Convert the bucket `config` names from format 1 to 4 to format 5 in place (spec Choice D
/// (a)). Run it on the machine that will be the writer: `dir` must be missing or empty, and
/// afterwards holds the restored copy, which the first writer [`crate::open`] of `dir` claims.
///
/// It takes the database over first, as [`crate::takeover`] does, so a writer still running
/// elsewhere is fenced for good. A bucket that is already of format 5 is `Config`, and nothing
/// is written. A failure before the head write leaves a bucket of the old format that works as
/// before; running the command again starts over. A failure after the head write (only the
/// local copy in `dir` can fail then) leaves the bucket upgraded: a restore, or an open of an
/// empty directory, continues it. Builds before format 5 refuse the bucket afterwards. The
/// upgrade does not vacuum the database.
pub fn upgrade(config: &Config, dir: &Path) -> Result<UpgradeReport> {
    let store = config.s3_store()?;
    upgrade_with_store(config, store, dir)
}

#[doc(hidden)]
pub fn upgrade_with_store(
    config: &Config,
    store: Arc<dyn ObjectStore>,
    dir: &Path,
) -> Result<UpgradeReport> {
    let start = Instant::now();
    config.validate()?;
    let encryption = encryption::load(config)?;
    refuse_entries(dir)?;
    let guard = DirGuard::acquire(dir)?;
    let dir = guard.dir().to_path_buf();
    refuse_entries(&dir)?;
    sweep_staging_dirs(&dir)?;
    let mut cfg = config.engine(crate::io::IoMode::Writer);
    cfg.encryption = encryption.clone();
    let keys = config.keys_for(encryption.as_deref());
    // Nothing is written to a bucket of format 5, or to no bucket at all.
    let head_key = keys.head();
    let Some(obj) = with_retry(&cfg.retry, &head_key, |_| store.get(&head_key))? else {
        return Err(Error::NotFound(format!(
            "{head_key}: no database at this prefix to upgrade"
        )));
    };
    let before = keys.decode_head_open(&obj.body)?;
    if before.format == FORMAT_V5 {
        return Err(Error::Config(
            "the bucket is already of format 5: there is nothing to upgrade. To continue the \
             database on this machine, restore it, or open an empty directory as the writer \
             (sql --claim)"
                .into(),
        ));
    }
    check_free_disk(&*store, &keys, &cfg, &before, &dir)?;
    let staging = staging_dir(&dir)?;
    let r = run(config, &cfg, &store, &keys, &dir, &staging, start);
    let _ = fs::remove_dir_all(&staging);
    r
}

#[allow(clippy::too_many_arguments)]
fn run(
    config: &Config,
    cfg: &EngineConfig,
    store: &Arc<dyn ObjectStore>,
    keys: &Keys,
    dir: &Path,
    staging: &Path,
    start: Instant,
) -> Result<UpgradeReport> {
    let enc = cfg.encryption.as_deref();
    let retry = &cfg.retry;
    // Step 1: the takeover, in a directory of its own (its copy is not used after).
    let mut tcfg = config.clone();
    tcfg.dir = staging.join("takeover");
    let taken = crate::takeover::takeover_with_store(tcfg, store.clone())?;
    let head_key = keys.head();
    let obj = with_retry(retry, &head_key, |_| store.get(&head_key))?
        .ok_or_else(|| Error::NotFound(head_key.clone()))?;
    let head = keys.decode_head(&obj.body)?;
    if head.writer_id != taken.writer_id || head.epoch != taken.epoch {
        return Err(changed());
    }
    let etag = obj.etag;
    // Step 2: the first pack, from the pages, the manifest and the log of the old format.
    let work = staging.join("pack");
    fs::create_dir_all(&work).map_err(|e| io_err("create the upgrade's work dir", e))?;
    let db = work.join(DB_FILE);
    write_synced(&db, b"")?;
    if let Some(m) = head.manifest {
        fetch_pages(&**store, keys, cfg, m, &db)?;
    }
    let records = fetch_generation(
        &**store,
        keys,
        head.generation,
        cfg.upload_concurrency,
        retry,
        &AtomicU64::new(0),
    )?;
    let images = replay(&records, &head)?;
    apply_images_over(&work, &images, &head, enc, None)?;
    remove_if_there(&work.join(WAL_FILE))?;
    let size = fs::metadata(&db)
        .map_err(|e| io_err("stat the first pack", e))?
        .len();
    if size == 0 {
        return Err(Error::Unsupported(
            "upgrade of a database that holds no page yet (create it again instead)",
        ));
    }
    let page_size = db_facts(&work, enc)?.page_size;
    // The database id of format 5: a format 4 bucket keeps its own, every other one gets a new
    // id (its pack, codes, pointer and head are coded under it when encrypted).
    let new_keys = if head.format == FORMAT_V4 {
        keys.clone()
    } else {
        let k = keys.with_new_identity();
        k.init_packed_database_id()?;
        k
    };
    let database_id = new_keys.known_database_id().ok_or_else(|| {
        Error::Corrupt(format!("{head_key}: a format 4 head without database_id"))
    })?;
    let target = (head.generation + 1).max(head.next_generation_floor.unwrap_or(0));
    // Step 3: the pack and its codes.
    let pointer = first::upload_first(&**store, &new_keys, retry, &db, page_size, target)?;
    // Step 4: the pointer, while the head is still the takeover's.
    let replaced_pointer = write_pointer(&**store, keys, &new_keys, cfg, &pointer, &etag)?;
    // Step 5: the head at format 5, last.
    let new_head = Head {
        format: FORMAT_V5,
        generation: target,
        manifest: None,
        database_id: Some(database_id.clone()),
        ..head.clone()
    };
    let new_etag = put_head(&**store, &new_keys, cfg, &new_head, &etag)?;
    // From here on the bucket is a good format 5 bucket: the pointer names the pack. An error
    // tells the operator so, and names the next step.
    first::rewrite_after_head(
        &**store,
        &new_keys,
        retry,
        &FirstPack {
            pointer: pointer.clone(),
        },
    )
    .map_err(after_head)?;
    tracing::info!(
        target: "buckwal",
        from_format = head.format,
        from_generation = head.generation,
        generation = target,
        pack_key = %pointer.pack_key,
        bytes = size,
        replaced_pointer,
        ms = start.elapsed().as_millis() as u64,
        "upgrade finished"
    );
    // Step 6: the restored copy of the new head in `dir`.
    write_copy(&work, staging, dir, &new_head, &new_etag).map_err(after_head)?;
    Ok(UpgradeReport {
        from_format: head.format,
        from_generation: head.generation,
        generation: target,
        epoch: head.epoch,
        fence_seq: taken.fence_seq,
        database_id,
        pack_key: pointer.pack_key,
        bytes: size,
        replaced_pointer,
        elapsed: start.elapsed(),
    })
}

fn changed() -> Error {
    Error::Fenced(
        "upgrade: the head changed after the upgrade's takeover (another writer or upgrade took \
         the database over); the bucket keeps its old format; run the upgrade again"
            .into(),
    )
}

/// The error of a step after the head write, which the bucket no longer needs: the same kind,
/// with a text that tells the operator that the bucket is upgraded and what to do next.
fn after_head(e: Error) -> Error {
    let m = |m: String| {
        format!(
            "the bucket is already upgraded to format 5, but a step after the head write \
             failed: {m}. Do not run the upgrade again: restore the database into an empty \
             directory, or open an empty directory as the writer (sql --claim)"
        )
    };
    match e {
        Error::Fenced(s) => Error::Fenced(m(s)),
        Error::Timeout(s) => Error::Timeout(m(s)),
        Error::NotFound(s) => Error::NotFound(m(s)),
        Error::Unsupported(s) => Error::Io(m(s.to_string())),
        Error::UploadFailed(s) => Error::UploadFailed(m(s)),
        Error::Locked(s) => Error::Locked(m(s)),
        Error::AlreadyOpen(s) => Error::AlreadyOpen(m(s)),
        Error::Corrupt(s) => Error::Corrupt(m(s)),
        Error::Bucket(s) => Error::Bucket(m(s)),
        Error::Engine(s) => Error::Engine(m(s)),
        Error::Io(s) => Error::Io(m(s)),
        Error::Config(s) => Error::Config(m(s)),
        Error::Encryption(s) => Error::Encryption(m(s)),
        Error::Stale(s) => Error::Stale(m(s)),
    }
}

/// Before the takeover, so that a disk that is too small fences no writer: the filesystem that
/// holds the staging directory (`dir`'s parent) must have the database size, `page_count ×
/// page_size` of the head's manifest, plus 10% free (the rule of a pack run). A bucket that
/// never sealed has no manifest; its database is only the log of one generation, and the
/// check asks for nothing more.
fn check_free_disk(
    store: &dyn ObjectStore,
    keys: &Keys,
    cfg: &EngineConfig,
    head: &Head,
    dir: &Path,
) -> Result<()> {
    let Some(m) = head.manifest else {
        return Ok(());
    };
    let key = keys.manifest(m);
    let body = with_retry(&cfg.retry, &key, |_| store.get(&key))?
        .ok_or_else(|| Error::NotFound(key.clone()))?
        .body;
    let manifest = keys.decode_manifest(&body, m)?;
    let size = u64::from(manifest.page_count) * u64::from(manifest.page_size);
    let need = size + size / 10;
    let at = dir.parent().unwrap_or(dir);
    let free = crate::pack::run::free_disk(at)?;
    if free < need {
        return Err(Error::Config(format!(
            "{} has {free} bytes free; the upgrade needs {need} (the database, {size} bytes, \
             plus 10%) to build the first pack. Nothing was written to the bucket, and the \
             writer keeps running",
            at.display()
        )));
    }
    Ok(())
}

/// Every page that manifest `m` names, written into `db` at its place, `cfg.upload_concurrency`
/// GETs at once. Each page object is checked (`Keys::open_page`); its bytes are kept as stored.
fn fetch_pages(
    store: &dyn ObjectStore,
    keys: &Keys,
    cfg: &EngineConfig,
    m: u64,
    db: &Path,
) -> Result<()> {
    let key = keys.manifest(m);
    let body = with_retry(&cfg.retry, &key, |_| store.get(&key))?
        .ok_or_else(|| Error::NotFound(key.clone()))?
        .body;
    let manifest = keys.decode_manifest(&body, m)?;
    let pages: Vec<(u32, u64)> = (1..=manifest.page_count)
        .map(|p| match manifest.pages.get(&p) {
            Some(&g) => Ok((p, g)),
            None => Err(Error::Corrupt(format!(
                "{key}: page {p} is within page_count {} but has no entry",
                manifest.page_count
            ))),
        })
        .collect::<Result<_>>()?;
    let file = fs::OpenOptions::new()
        .write(true)
        .open(db)
        .map_err(|e| io_err("open the first pack", e))?;
    let page_size = u64::from(manifest.page_size);
    let next = AtomicUsize::new(0);
    let failed: Mutex<Option<Error>> = Mutex::new(None);
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    std::thread::scope(|s| {
        for _ in 0..cfg.upload_concurrency.max(1).min(pages.len()) {
            s.spawn(|| {
                tracing::dispatcher::with_default(&dispatch, || loop {
                    if failed.lock().unwrap().is_some() {
                        return;
                    }
                    let Some(&(p, g)) = pages.get(next.fetch_add(1, Relaxed)) else {
                        return;
                    };
                    let r = fetch_page(store, keys, cfg, p, g, page_size).and_then(|bytes| {
                        file.write_all_at(&bytes, (u64::from(p) - 1) * page_size)
                            .map_err(|e| io_err("write a page of the first pack", e))
                    });
                    if let Err(e) = r {
                        failed.lock().unwrap().get_or_insert(e);
                        return;
                    }
                })
            });
        }
    });
    if let Some(e) = failed.into_inner().unwrap() {
        return Err(e);
    }
    file.sync_all()
        .map_err(|e| io_err("fsync the first pack", e))
}

fn fetch_page(
    store: &dyn ObjectStore,
    keys: &Keys,
    cfg: &EngineConfig,
    p: u32,
    g: u64,
    page_size: u64,
) -> Result<Vec<u8>> {
    let key = keys.page(g, p);
    let obj = with_retry(&cfg.retry, &key, |_| store.get(&key))?
        .ok_or_else(|| Error::NotFound(key.clone()))?;
    let page = keys.open_page(p, g, &obj.body)?;
    if page.len() as u64 != page_size {
        return Err(Error::Corrupt(format!(
            "{key} is {} bytes, not the page size {page_size}",
            page.len()
        )));
    }
    Ok(page.to_vec())
}

/// The head is still the one the takeover wrote (`etag`), else `Fenced`.
fn check_head(store: &dyn ObjectStore, keys: &Keys, cfg: &EngineConfig, etag: &str) -> Result<()> {
    let key = keys.head();
    match with_retry(&cfg.retry, &key, |_| store.get(&key))? {
        Some(o) if o.etag == etag => Ok(()),
        _ => Err(changed()),
    }
}

/// How many rounds of a read and a conditional write of the pointer the upgrade makes before
/// it gives up.
const MAX_ROUNDS: u32 = 10;

/// Step 4: `pointer` as `pack-latest`: `If-None-Match` when there is none, else `If-Match` on
/// the ETag of the leftover just read. Returns whether a leftover was replaced.
///
/// Each round first checks that the head is still the takeover's (`etag`, read with `keys`).
/// A leftover pointer whose pack generation is at or above this run's is never replaced: only
/// a later upgrade run can have written it (each takeover raises the floor, so each later run
/// has a higher pack generation), and that run may already have written its format 5 head. This
/// run is then `Fenced`. So a run that stalls anywhere in this step cannot put its pointer over
/// the pointer of a newer run.
fn write_pointer(
    store: &dyn ObjectStore,
    keys: &Keys,
    new_keys: &Keys,
    cfg: &EngineConfig,
    pointer: &crate::pack::PackPointer,
    etag: &str,
) -> Result<bool> {
    let key = new_keys.pack_pointer();
    let body = new_keys.encode_pointer(pointer);
    let retry = &cfg.retry;
    for _ in 0..MAX_ROUNDS {
        check_head(store, keys, cfg, etag)?;
        #[cfg(test)]
        if let Some(f) = BEFORE_POINTER_READ.with(|h| h.borrow_mut().take()) {
            f();
        }
        let (put, replaced) = match with_retry(retry, &key, |_| store.get(&key))? {
            // An earlier attempt of this write landed.
            Some(o) if o.body == body => return Ok(false),
            Some(o)
                if new_keys
                    .pointer_pack_generation(&o.body)
                    .is_some_and(|g| g >= pointer.pack_generation) =>
            {
                return Err(changed());
            }
            Some(o) => (
                with_retry(retry, &key, |_| store.put_if_match(&key, &body, &o.etag))?,
                true,
            ),
            None => (
                with_retry(retry, &key, |_| store.put_if_absent(&key, &body))?,
                false,
            ),
        };
        if let CondPut::Created { .. } = put {
            if replaced {
                tracing::info!(target: "buckwal", key = %key, "upgrade: replaced a pointer of an earlier upgrade run");
            }
            return Ok(replaced);
        }
    }
    Err(Error::UploadFailed(format!(
        "{key}: it changed at each of {MAX_ROUNDS} reads"
    )))
}

/// Step 5: `head` with `If-Match` on `etag`. A 412 is ours only when the stored head is
/// exactly `head` (an earlier attempt landed); anything else is `Fenced`.
fn put_head(
    store: &dyn ObjectStore,
    keys: &Keys,
    cfg: &EngineConfig,
    head: &Head,
    etag: &str,
) -> Result<String> {
    let key = keys.head();
    let body = keys.encode_head(head);
    let retry = &cfg.retry;
    if let CondPut::Created { etag } =
        with_retry(retry, &key, |_| store.put_if_match(&key, &body, etag))?
    {
        return Ok(etag);
    }
    match with_retry(retry, &key, |_| store.get(&key))? {
        Some(o) if o.body == body => Ok(o.etag),
        _ => Err(changed()),
    }
}

/// Step 6: `<work>/app.db` (the pack) and the files a restore of `head` writes beside it, built
/// in `<staging>/copy`, then moved into `dir` as a restore moves its files (`move_restored`).
fn write_copy(work: &Path, staging: &Path, dir: &Path, head: &Head, etag: &str) -> Result<()> {
    let copy = staging.join("copy");
    fs::create_dir_all(&copy).map_err(|e| io_err("create the upgrade's copy dir", e))?;
    rename_or_copy(&work.join(DB_FILE), &copy.join(DB_FILE))?;
    // The log of the head's generation is empty: nothing is written to it before a claim.
    write_synced(&copy.join(WAL_FILE), b"")?;
    if head.journal_mode == JournalMode::Mvcc {
        write_synced(&copy.join(LOG_FILE), b"")?;
        repair_wal_without_log(&copy)?;
    }
    fsync_dir(&copy)?;
    LocalState {
        format: local_format(head),
        writer_id: head.writer_id.clone(),
        generation: head.generation,
        // The pack is the whole database file, and it is local.
        complete: true,
        // The first writer open claims the head under a new writer id, as after a restore.
        claimed: false,
        head_etag: etag.to_string(),
        epoch: head.epoch,
        claim_id: String::new(),
        seal_not_before_ms: 0,
        seal_quiet_ms: 0,
        next_generation_floor: 0,
    }
    .write_atomic(&copy)?;
    move_restored(&copy, dir)
}

fn remove_if_there(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_err("remove", e)),
    }
}

/// `Config` when `dir` holds an entry other than the lock file, `pack-work-*` directories and
/// the kept copy. A missing `dir` is empty.
fn refuse_entries(dir: &Path) -> Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io_err(&format!("list {}", dir.display()), e)),
    };
    for e in entries {
        let e = e.map_err(|e| io_err(&format!("list {}", dir.display()), e))?;
        let name = e.file_name();
        let allowed = name == LOCK_FILE
            || name
                .to_string_lossy()
                .starts_with(crate::pack::run::WORK_DIR_PREFIX)
            || name == crate::pack::kept::COPY_DIR;
        if !allowed {
            return Err(Error::Config(format!(
                "{} is not empty ({}): an upgrade writes the restored copy into a missing or \
                 empty directory",
                dir.display(),
                name.to_string_lossy()
            )));
        }
    }
    Ok(())
}

/// The name prefix of `dir`'s upgrade staging directories: `.<name>.upgrade-`.
fn staging_prefix(dir: &Path) -> Option<String> {
    dir.file_name()
        .map(|n| format!(".{}.upgrade-", n.to_string_lossy()))
}

/// Remove what an earlier upgrade into `dir` left next to it (a crash). Called with `dir` held.
fn sweep_staging_dirs(dir: &Path) -> Result<()> {
    let (Some(parent), Some(prefix)) = (dir.parent(), staging_prefix(dir)) else {
        return Ok(());
    };
    for e in fs::read_dir(parent).map_err(|e| io_err("list parent dir", e))? {
        let e = e.map_err(|e| io_err("list parent dir", e))?;
        if e.file_name().to_string_lossy().starts_with(&prefix) {
            fs::remove_dir_all(e.path()).map_err(|e| io_err("remove staging dir", e))?;
        }
    }
    Ok(())
}

/// `<parent>/.<name>.upgrade-<pid>-<nanos>`.
fn staging_dir(dir: &Path) -> Result<PathBuf> {
    let (Some(parent), Some(prefix)) = (dir.parent(), staging_prefix(dir)) else {
        return Err(Error::Config(format!(
            "cannot upgrade into {}: it has no parent directory",
            dir.display()
        )));
    };
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    Ok(parent.join(format!("{prefix}{}-{nanos}", std::process::id())))
}

#[cfg(test)]
thread_local! {
    static BEFORE_POINTER_READ: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// Tests only: the next upgrade on this thread runs `f` once, in its pointer write, just before
/// it reads the pointer (an upgrade that stalls there).
#[cfg(test)]
pub(crate) fn before_pointer_read_for_test(f: impl FnOnce() + 'static) {
    BEFORE_POINTER_READ.with(|h| *h.borrow_mut() = Some(Box::new(f)));
}
