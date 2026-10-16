// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! Assemble the engine: the uploader, the bucket-backed database file and the IO that ships
//! the `-wal`, opened through `Database::open`.

use crate::bucket::Bucket;
use crate::config::EngineConfig;
use crate::error::{Error, Result};
use crate::format::LocalState;
use crate::io::db_storage::StorageParts;
use crate::io::{stash, BucketDbStorage, DurableIo};
use crate::uploader::{TrimSnapshot, Uploader};
use std::path::Path;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Instant;
use turso_core::{Database, OpenFlags, OpenOptions, PlatformIO, SqliteDialect, IO};

pub(crate) struct Engine {
    pub db: Arc<Database>,
    /// Tests inspect the page cache through it; the engine holds its own handle.
    #[cfg_attr(not(test), allow(dead_code))]
    pub storage: Arc<BucketDbStorage>,
    pub uploader: Arc<Uploader>,
    pub io: Arc<DurableIo>,
}

/// The typed error an engine call stashed, else the engine's own message.
pub(crate) fn engine_error(e: turso_core::LimboError) -> Error {
    stash::take().unwrap_or_else(|| Error::Engine(e.to_string()))
}

/// Open `<dir>/app.db` through the bucket wrappers. `state` is the directory's local state,
/// already reconciled with `bucket`. `trim_tx`, when given, feeds a trimmer
/// (`trim::spawn_trimmer`): the head at open first, then one snapshot per seal.
pub(crate) fn open_engine(
    dir: &Path,
    bucket: Bucket,
    state: &LocalState,
    cfg: &EngineConfig,
    trim_tx: Option<Sender<TrimSnapshot>>,
) -> Result<Engine> {
    let db_path = dir
        .join("app.db")
        .to_str()
        .ok_or_else(|| Error::Config(format!("{} is not UTF-8", dir.display())))?
        .to_string();
    // The engine opens its WAL at exactly this path (database.rs:1495).
    let wal_path = format!("{db_path}-wal");
    let platform: Arc<dyn IO> =
        Arc::new(PlatformIO::new().map_err(|e| Error::Io(format!("platform IO: {e}")))?);
    let store = bucket.store();
    let keys = bucket.keys().clone();
    let manifest = bucket.manifest().cloned();
    // D10: the trimmer starts from the head at open; each seal sends the next snapshot.
    if let Some(tx) = &trim_tx {
        // No trimmer listening is not an open failure.
        let _ = tx.send(TrimSnapshot {
            at: Instant::now(),
            head: bucket.head().clone(),
            manifest: manifest.clone(),
        });
    }
    let uploader = Uploader::spawn(bucket, trim_tx, cfg.events.clone());

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
        writer_id: state.writer_id.clone(),
        manifest,
        complete: state.complete,
        read_only: cfg.read_only,
    }));
    storage.adopt_persisted_dirty()?;
    let io = Arc::new(DurableIo::new(
        platform,
        wal_path,
        uploader.clone(),
        storage.clone(),
        cfg.read_only,
    ));
    let opts = OpenOptions::new(Arc::new(SqliteDialect))
        .storage(storage.clone())
        .flags(OpenFlags::Create);
    let db = Database::open(io.clone(), &db_path, opts).map_err(engine_error)?;
    if db.mvcc_enabled() {
        return Err(Error::Unsupported("MVCC"));
    }
    if cfg.prefetch {
        let s = storage.clone();
        std::thread::Builder::new()
            .name("turso-durable-prefetch".into())
            .spawn(move || s.prefetch())
            .map_err(|e| Error::Io(format!("spawn prefetch thread: {e}")))?;
    }
    Ok(Engine {
        db,
        storage,
        uploader,
        io,
    })
}

#[cfg(test)]
pub(crate) mod tests;
