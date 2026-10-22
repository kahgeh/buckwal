//! Creating a new MVCC database (`docs/mvcc-recon.md`, Stage-2 design, Creation). turso has
//! no open option for MVCC: the header decides (bytes 18/19 = 255). So the library builds the
//! empty MVCC database itself, in a staging directory `<dir>/app.db-creating`, with plain
//! local IO and no wrapper: `PRAGMA journal_mode='mvcc'`, then a TRUNCATE checkpoint. Only
//! then does the bucket see anything (`Bucket::create_sealed`), and only after its head landed
//! are the files moved into the database directory (`move_created`).
//!
//! A crash mid-switch can make an open of the staging files panic (P7), so a staging
//! directory is never opened again: the next open removes it (`clear_interrupted_create`).
use crate::encryption::Encryption;
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE, LOG_FILE, STATE_FILE};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use turso_core::{
    CheckpointMode, Database, DatabaseOpts, EncryptionOpts, OpenFlags, OpenOptions, PlatformIO,
    SqliteDialect, IO,
};

/// The staging directory of a creation, inside the database directory.
pub(crate) const CREATING_DIR: &str = "app.db-creating";

/// The files a creation moves into the database directory, in this order: the local state
/// last, so a directory with a state file is whole.
const CREATED: [&str; 3] = [LOG_FILE, DB_FILE, STATE_FILE];

/// The engine's logical-log header size (`LOG_HDR_SIZE`,
/// `turso_core-0.8.3/mvcc/persistent_storage/logical_log.rs:299`).
pub(crate) const LOG_HEADER_LEN: usize = 56;

/// The empty MVCC database a staging build made: its page size and every page of `app.db`,
/// as the file holds them (ciphertext when encrypted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Image {
    pub page_size: u32,
    pub pages: Vec<(u32, Vec<u8>)>,
}

impl Image {
    pub(crate) fn page_count(&self) -> u32 {
        self.pages.len() as u32
    }
}

pub(crate) fn staging_dir(dir: &Path) -> PathBuf {
    dir.join(CREATING_DIR)
}

fn engine(e: turso_core::LimboError) -> Error {
    Error::Engine(format!("building the MVCC database: {e}"))
}

/// Build the empty MVCC database in `staging` (created empty; an existing one is removed
/// first): turso_core over a plain `PlatformIO`, `PRAGMA journal_mode='mvcc'`, a TRUNCATE
/// checkpoint, close. With `enc`, the database is encrypted as `engine::open_engine` opens
/// one (the database option, the cipher and key at open, the key on the connection).
/// Afterwards `staging` holds `app.db` (bytes 18/19 = 255) and `app.db-log` holding only the
/// engine's 56-byte log header (and an empty `-wal`, never moved). Returns the pages of
/// `app.db`.
pub(crate) fn build_mvcc_image(staging: &Path, enc: Option<&Encryption>) -> Result<Image> {
    remove_dir_if_exists(staging)?;
    fs::create_dir_all(staging).map_err(|e| io_err("create staging dir", e))?;
    let db_path = staging.join(DB_FILE);
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
    let log_path = staging.join(LOG_FILE);
    let (page_size, header) = {
        let db = Database::open(io, path, opts).map_err(engine)?;
        let conn = db
            .connect_with_encryption(enc.map(|e| e.key.clone()))
            .map_err(engine)?;
        conn.execute("PRAGMA journal_mode='mvcc'").map_err(engine)?;
        // The switch wrote and synced the log header before its meta-table commits
        // (`turso_core-0.8.3/vdbe/execute.rs:19119-19261`).
        let header = fs::read(&log_path).map_err(|e| io_err("read staged app.db-log", e))?;
        conn.checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .map_err(engine)?;
        let ps = page_size_of(&conn)?;
        conn.close().map_err(engine)?;
        (ps, header)
    };
    if header.len() != LOG_HEADER_LEN {
        return Err(Error::Engine(format!(
            "the switch left a {}-byte app.db-log, not the {LOG_HEADER_LEN}-byte header",
            header.len()
        )));
    }
    // The TRUNCATE checkpoint cut the log to 0 bytes (`logical_log.rs:1048-1068`), and the
    // engine writes a header only at the first bootstrap and the first commit. The header the
    // switch wrote goes back: an MVCC database whose log holds only its header opens
    // (P5 (d)). Its salt is the switch's; no frame was ever written under it.
    fs::write(&log_path, &header).map_err(|e| io_err("write staged app.db-log", e))?;
    let file = fs::read(&db_path).map_err(|e| io_err("read staged app.db", e))?;
    if file.is_empty() || file.len() % page_size as usize != 0 {
        return Err(Error::Engine(format!(
            "the staged app.db is {} bytes, not whole {page_size}-byte pages",
            file.len()
        )));
    }
    let pages = file
        .chunks_exact(page_size as usize)
        .enumerate()
        .map(|(i, p)| (i as u32 + 1, p.to_vec()))
        .collect();
    sync_dir_files(staging)?;
    Ok(Image { page_size, pages })
}

fn page_size_of(conn: &Arc<turso_core::Connection>) -> Result<u32> {
    let rows = conn
        .query("PRAGMA page_size")
        .map_err(engine)?
        .ok_or_else(|| Error::Engine("PRAGMA page_size returned no statement".into()))?
        .run_collect_rows()
        .map_err(engine)?;
    let ps = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_int())
        .ok_or_else(|| Error::Engine("PRAGMA page_size returned no value".into()))?;
    u32::try_from(ps).map_err(|_| Error::Engine(format!("page size {ps}")))
}

fn sync_dir_files(staging: &Path) -> Result<()> {
    for f in [DB_FILE, LOG_FILE] {
        fs::File::open(staging.join(f))
            .and_then(|f| f.sync_all())
            .map_err(|e| io_err(&format!("fsync staged {f}"), e))?;
    }
    fsync_dir(staging)
}

fn fsync_dir(dir: &Path) -> Result<()> {
    fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io_err("fsync dir", e))
}

fn remove_dir_if_exists(d: &Path) -> Result<()> {
    match fs::remove_dir_all(d) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_err(&format!("remove {}", d.display()), e)),
    }
}

/// Move the staged `app.db-log`, `app.db` and local state (written into `staging` by the
/// caller) into `dir`, the state last, then remove `staging`. Runs only after the bucket's
/// head landed. A crash during it leaves `staging`, and `clear_interrupted_create` takes the
/// directory back to empty unless the state file already made it in.
pub(crate) fn move_created(staging: &Path, dir: &Path) -> Result<()> {
    for (moved, f) in CREATED.iter().enumerate() {
        crash_point(moved)?;
        fs::rename(staging.join(f), dir.join(f))
            .map_err(|e| io_err(&format!("move created {f}"), e))?;
    }
    crash_point(CREATED.len())?;
    fsync_dir(dir)?;
    remove_dir_if_exists(staging)?;
    fsync_dir(dir)
}

/// Tests: the crash `CRASH_AFTER_MOVING` injects once `moved` files are in place.
fn crash_point(moved: usize) -> Result<()> {
    #[cfg(test)]
    if CRASH_AFTER_MOVING.with(|c| c.get()) == Some(moved) {
        return Err(Error::Io(format!(
            "injected: crash after moving {moved} files"
        )));
    }
    let _ = moved;
    Ok(())
}

/// Remove what an interrupted creation left in `dir` (held by the caller), never opening it.
/// With `app.db-creating` present and no local state in `dir`, the files a creation moves in
/// (`app.db-log`, `app.db`) are removed too: they can only be that creation's, moved before
/// its state file. With the state file in `dir` the move finished; only the staging
/// directory is left to remove.
pub(crate) fn clear_interrupted_create(dir: &Path) -> Result<()> {
    let staging = staging_dir(dir);
    if !staging.exists() {
        return Ok(());
    }
    if !dir.join(STATE_FILE).exists() {
        for f in [LOG_FILE, DB_FILE] {
            match fs::remove_file(dir.join(f)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_err(&format!("remove {f}"), e)),
            }
        }
    }
    remove_dir_if_exists(&staging)?;
    fsync_dir(dir)
}

#[cfg(test)]
thread_local! {
    /// Tests: `move_created` stops with `Io` after moving this many files (`CREATED.len()`:
    /// all moved, before the staging directory is removed), as a crash there would. Per
    /// thread.
    pub(crate) static CRASH_AFTER_MOVING: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}
