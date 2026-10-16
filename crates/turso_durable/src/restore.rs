// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! Restore onto an empty directory: rebuild the current generation's `-wal` from its log and
//! leave every database page in the bucket, to be fetched on first read.
// The public `restore` (Task 10) is the first non-test user.
#![allow(dead_code)]

use crate::config::EngineConfig;
use crate::error::{Error, Result};
use crate::format::{apply_ops, decode_record, Head, Keys, LocalState, LogRecord, Manifest};
use crate::store::{with_retry, ObjectStore, RetryPolicy};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

/// Write `bytes` to `path` and fsync it.
fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = fs::File::create(path).map_err(|e| io_err("create", e))?;
    f.write_all(bytes)
        .and_then(|_| f.sync_all())
        .map_err(|e| io_err("write", e))
}

/// Restore the bucket's current state into `dir`, which must hold no database yet.
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
    let head = Head::decode(&get(&*store, retry, &keys.head())?)?;
    if let Some(g) = head.manifest {
        let manifest = Manifest::decode(&get(&*store, retry, &keys.manifest(g))?)?;
        let pg = manifest
            .pages
            .get(&1)
            .ok_or_else(|| Error::Corrupt(format!("manifest/{g} has no entry for page 1")))?;
        refuse_mvcc(&get(&*store, retry, &keys.page(*pg, 1))?)?;
    }
    let records = fetch_generation(
        &*store,
        keys,
        head.generation,
        cfg.upload_concurrency,
        retry,
    )?;
    let mut wal = Vec::new();
    for rec in &records {
        match rec {
            LogRecord::Extents { writer_id, syncs } if *writer_id == head.writer_id => {
                for ops in syncs {
                    apply_ops(&mut wal, ops)?;
                }
            }
            LogRecord::Extents { writer_id, .. } | LogRecord::Fence { writer_id } => {
                return Err(Error::Fenced(format!(
                    "log/{} holds a record of writer {writer_id:?}, not {:?}",
                    head.generation, head.writer_id
                )));
            }
        }
    }
    // Never sealed: the bucket holds no page, but the engine reads an empty database file as
    // a new database and ignores the WAL. Any page-1 frame bootstraps it (page size, reserved
    // space); page 1 itself is read from the WAL, which holds the current version.
    let db = match head.manifest {
        Some(_) => Vec::new(),
        None => match wal_page_one(&wal) {
            Some(p) => {
                refuse_mvcc(p)?;
                p.to_vec()
            }
            None => Vec::new(),
        },
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
    }
    .write_atomic(dir)?;
    let delta = store.stats().since(&before);
    Ok(RestoreReport {
        generation: head.generation,
        log_objects: records.len() as u64,
        gets: delta.gets,
        lists: delta.lists,
        bytes: delta.bytes_down,
        elapsed: start.elapsed(),
    })
}
