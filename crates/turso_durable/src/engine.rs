// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! Assemble the engine: the uploader, the bucket-backed database file and the IO that ships
//! the `-wal`, opened through `Database::open`. An MVCC database also gets its durable
//! storage (`LogDurableStorage`, over `app.db-log`), passed with `OpenOptions::durable_storage`.

use crate::bucket::Bucket;
use crate::config::{EngineConfig, JournalMode};
use crate::encryption::{check_page_one, local_page_one};
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE, LOG_FILE};
use crate::format::LocalState;
use crate::io::db_storage::StorageParts;
use crate::io::log_storage::LogDurableStorage;
use crate::io::{stash, BucketDbStorage, DurableIo};
use crate::uploader::{TrimSnapshot, Uploader};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::JoinHandle;
use turso_core::{
    Database, DatabaseOpts, EncryptionContext, EncryptionKey, EncryptionOpts, OpenFlags,
    OpenOptions, PlatformIO, SqliteDialect, IO,
};

/// The page size an MVCC database's log context is built with when no manifest names one.
const DEFAULT_PAGE_SIZE: usize = 4096;

/// The engine's database, with the key every connection to it needs (D16). turso builds each
/// connection's pager from the key that connection is given (database.rs:2386-2395,
/// 2465-2495), so every engine connection is made here. Without encryption, `connect` is
/// turso's plain `connect()` (both pass `None`, database.rs:2383-2395).
#[derive(Clone)]
pub(crate) struct EngineDb {
    db: Arc<Database>,
    key: Option<EncryptionKey>,
}

impl EngineDb {
    pub(crate) fn connect(&self) -> turso_core::Result<Arc<turso_core::Connection>> {
        self.db.connect_with_encryption(self.key.clone())
    }

    pub(crate) fn mvcc_enabled(&self) -> bool {
        self.db.mvcc_enabled()
    }
}

pub(crate) struct Engine {
    pub db: EngineDb,
    /// Tests inspect the page cache through it; the engine holds its own handle.
    #[cfg_attr(not(test), allow(dead_code))]
    pub storage: Arc<BucketDbStorage>,
    pub uploader: Arc<Uploader>,
    pub io: Arc<DurableIo>,
    /// How far the current generation's `-wal` reaches, in bytes (`ShippingFile`'s high-water
    /// mark): the maintenance trigger's second measure.
    pub wal_bytes: Arc<AtomicU64>,
    /// The background page fetcher (`EngineConfig::prefetch`); stopped and joined on drop.
    pub prefetch: Option<Prefetch>,
}

/// The prefetch thread writes the local database file, so it must be gone before the
/// directory is released (`durable.lock`, the process registry).
pub(crate) struct Prefetch {
    stop: Arc<AtomicBool>,
    /// Set by the thread as it ends (tests).
    #[cfg_attr(not(test), allow(dead_code))]
    pub done: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Some(p) = &mut self.prefetch {
            p.stop.store(true, SeqCst);
            if let Some(t) = p.thread.take() {
                // A panic on the prefetch thread left nothing to clean up.
                let _ = t.join();
            }
        }
    }
}

/// The typed error an engine call stashed, else the engine's own message.
pub(crate) fn engine_error(e: turso_core::LimboError) -> Error {
    stash::take().unwrap_or_else(|| Error::Engine(e.to_string()))
}

/// Open `<dir>/app.db` through the bucket wrappers. `state` is the directory's local state,
/// already reconciled with `bucket`. `trim_tx`, when given, wakes a trimmer
/// (`trim::spawn_trimmer`): the head at open first, then one snapshot per seal.
pub(crate) fn open_engine(
    dir: &Path,
    mut bucket: Bucket,
    state: &LocalState,
    cfg: &EngineConfig,
    trim_tx: Option<Sender<TrimSnapshot>>,
) -> Result<Engine> {
    // `db` sets it right after create or attach, before any append; this covers every other
    // caller (`Bucket::check_lease`).
    bucket.set_lease(cfg.lease);
    let db_path = dir
        .join(DB_FILE)
        .to_str()
        .ok_or_else(|| Error::Config(format!("{} is not UTF-8", dir.display())))?
        .to_string();
    // The engine opens its WAL at exactly this path (database.rs:1495).
    let wal_path = format!("{db_path}-wal");
    let platform: Arc<dyn IO> = Arc::new(PlatformIO::new().map_err(|e| io_err("platform IO", e))?);
    let store = bucket.store();
    let keys = bucket.keys().clone();
    let manifest = bucket.manifest().cloned();
    let page_size = manifest
        .as_ref()
        .map_or(DEFAULT_PAGE_SIZE, |m| m.page_size as usize);
    // D10: the open wakes the trimmer first; each seal wakes it again.
    if let Some(tx) = &trim_tx {
        // No trimmer listening is not an open failure.
        let _ = tx.send(TrimSnapshot {
            head: bucket.head().clone(),
            manifest: manifest.clone(),
        });
    }
    let uploader = Uploader::spawn(bucket, trim_tx, cfg.events.clone(), Arc::default());

    stash::clear();
    let file = platform
        .open_file(&db_path, OpenFlags::Create, false)
        .map_err(engine_error)?;
    let storage = Arc::new(BucketDbStorage::new(StorageParts {
        file,
        io: platform.clone(),
        store,
        keys,
        retry: cfg.retry.clone(),
        uploader: uploader.clone(),
        dir: dir.to_path_buf(),
        state: state.clone(),
        manifest,
        read_only: cfg.read_only,
    }));
    storage.adopt_persisted_dirty()?;
    // D16: page 1, as the engine will read it, against the encryption setting.
    check_page_one(
        local_page_one(dir, &storage)?.as_deref(),
        cfg.encryption.as_deref(),
    )?;
    let wal_bytes = Arc::new(AtomicU64::new(0));
    let mvcc = cfg.journal_mode == JournalMode::Mvcc;
    let log = if mvcc {
        // The log's context: the same cipher and key as `EncryptionOpts` below.
        let enc = match &cfg.encryption {
            Some(e) => Some(
                EncryptionContext::new(e.cipher, &e.key, page_size)
                    .map_err(|e| Error::Encryption(e.to_string()))?,
            ),
            None => None,
        };
        Some(Arc::new(LogDurableStorage::open(
            platform.clone(),
            &dir.join(LOG_FILE),
            Path::new(&wal_path),
            uploader.clone(),
            storage.clone(),
            enc,
            cfg.read_only,
        )?))
    } else {
        None
    };
    let io = Arc::new(DurableIo::new(
        platform,
        wal_path,
        uploader.clone(),
        storage.clone(),
        wal_bytes.clone(),
        cfg.read_only,
        log.clone(),
    ));
    let mut opts = OpenOptions::new(Arc::new(SqliteDialect))
        .storage(storage.clone())
        .flags(OpenFlags::Create);
    if let Some(log) = &log {
        let log: Arc<dyn turso_core::mvcc::persistent_storage::DurableStorage> = log.clone();
        opts = opts.durable_storage(log);
    }
    // All three of turso's switches (D16): the database option, the cipher and key at open
    // (a new database gets its reserved bytes from them), and the key on every connection.
    if let Some(enc) = &cfg.encryption {
        opts = opts
            .db_opts(DatabaseOpts::new().with_encryption(true))
            .encryption(EncryptionOpts {
                cipher: enc.cipher.to_string(),
                hexkey: hex::encode(enc.key.as_slice()),
            });
    }
    let db = EngineDb {
        db: Database::open(io.clone(), &db_path, opts).map_err(engine_error)?,
        key: cfg.encryption.as_ref().map(|e| e.key.clone()),
    };
    // The page-1 header and the bucket's mode were compared before (`db::refuse_mode_
    // mismatch`); this is the engine's own word on it.
    match (db.mvcc_enabled(), mvcc) {
        (true, false) => return Err(Error::Unsupported("MVCC")),
        (false, true) => return Err(Error::Unsupported("MVCC: switching back to WAL")),
        _ => {}
    }
    let prefetch = if cfg.prefetch {
        let s = storage.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let (stop2, done2) = (stop.clone(), done.clone());
        // The thread reports to the subscriber of the thread that opened the database.
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let thread = std::thread::Builder::new()
            .name("turso-durable-prefetch".into())
            .spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    s.prefetch(&stop2);
                    done2.store(true, SeqCst);
                })
            })
            .map_err(|e| io_err("spawn prefetch thread", e))?;
        Some(Prefetch {
            stop,
            done,
            thread: Some(thread),
        })
    } else {
        None
    };
    Ok(Engine {
        db,
        storage,
        uploader,
        io,
        wal_bytes,
        prefetch,
    })
}

#[cfg(test)]
pub(crate) mod tests;
