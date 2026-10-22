//! Restart on the writer's own disk (D9). Before the engine opens `<dir>`, its `-wal` and the
//! bucket's current generation are brought back to the same bytes:
//! - the local state is behind the head, or the local `-wal` still holds the generation
//!   before the head's: that WAL is covered by the head's manifest, so it is replaced by the
//!   bucket's image `B` (`NewGeneration`);
//! - `-wal == B`: nothing to do (`Level`);
//! - `-wal` shorter than `B` (the local disk lost a tail): `B` is written locally (`Behind`);
//! - otherwise the `-wal` holds commits whose upload never finished; they are uploaded as the
//!   next record (`Ahead`), because the engine will recover them as committed.
//!
//! An MVCC directory does the same for `app.db-log` (record kind 3), file by file. A
//! new-generation `.db-log` image that is empty next to a non-empty local file is
//! `NewGeneration` too. Last, WAL frames next to a log under 56 bytes get a fresh log header
//! (`repair_wal_without_log`), or the engine refuses the open.
//!
//! It reads and writes `app.db-wal` with `std::fs`, which D4 allows only because the engine
//! has not opened the files yet and `durable.lock` keeps every other process out.

use crate::bucket::{Bucket, LogPos};
use crate::config::{EngineConfig, JournalMode};
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE, DIRTY_FILE, LOG_FILE, WAL_FILE};
use crate::format::{Head, LocalState, LogFile, Op};
use crate::io::db_storage::{truncate_dirty_file, READ_ONLY};
use crate::io::wal_file::EPOCH;
use crate::restore::{generation_image, page_one_seed, repair_wal_without_log, write_synced};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Reconciled {
    Level,
    Behind,
    Ahead { uploaded: LogPos },
    NewGeneration,
}

/// `Ahead` uploads the local `-wal` in chunks of this size where it differs from `B`.
const CHUNK: usize = 4096;

/// Bring `dir` level with the bucket's current generation (D9). `bucket` is attached as
/// `state.writer_id`; it is handed to `open_engine` afterwards. `Behind` and
/// `NewGeneration` persist `state`.
pub(crate) fn reconcile(
    dir: &Path,
    bucket: &mut Bucket,
    state: &mut LocalState,
    cfg: &EngineConfig,
) -> Result<Reconciled> {
    reconcile_files(dir, bucket, state, cfg).map(Outcomes::combined)
}

/// What each file of `dir` needed.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Outcomes {
    pub wal: Reconciled,
    /// None for a WAL database (it has no `.db-log`).
    pub log: Option<Reconciled>,
}

impl Outcomes {
    /// One outcome for the directory: the strongest of the files' (NewGeneration, Ahead,
    /// Behind, Level).
    fn combined(self) -> Reconciled {
        let rank = |r: &Reconciled| match r {
            Reconciled::Level => 0,
            Reconciled::Behind => 1,
            Reconciled::Ahead { .. } => 2,
            Reconciled::NewGeneration => 3,
        };
        match self.log {
            Some(log) if rank(&log) > rank(&self.wal) => log,
            _ => self.wal,
        }
    }
}

/// [`reconcile`], with each file's outcome.
pub(crate) fn reconcile_files(
    dir: &Path,
    bucket: &mut Bucket,
    state: &mut LocalState,
    cfg: &EngineConfig,
) -> Result<Outcomes> {
    let head = bucket.head().clone();
    crate::db::refuse_mode_mismatch(&head, None, cfg.journal_mode)?;
    if head.writer_id != state.writer_id {
        return Err(Error::Fenced(format!(
            "head names writer {:?}, not this directory's {:?}",
            head.writer_id, state.writer_id
        )));
    }
    if state.generation > head.generation {
        return Err(Error::Corrupt(format!(
            "local state is at generation {}, past the head's {}",
            state.generation, head.generation
        )));
    }
    // One seal behind: that seal's manifest covers the local WAL. Anything else (more than one
    // generation, or a head whose manifest is not the local generation's) cannot be leveled.
    if state.generation < head.generation
        && (head.generation != state.generation + 1 || head.manifest != Some(state.generation))
    {
        return Err(Error::Corrupt(format!(
            "{} is stale: its local state is at generation {}, but the head is at generation \
             {} with manifest {:?}, and only the generation right after a seal of the local one \
             can be taken in; restore again into a new directory",
            dir.display(),
            state.generation,
            head.generation,
            head.manifest
        )));
    }
    let images = generation_image(&*bucket.store(), bucket.keys(), &head, cfg)?;
    let mvcc = head.journal_mode == JournalMode::Mvcc;
    let wal_local = read_file(dir, WAL_FILE, "-wal")?;
    // Only an MVCC directory has a `.db-log`; a WAL one is never given the file.
    let log_local = if mvcc {
        read_file(dir, LOG_FILE, ".db-log")?
    } else {
        Vec::new()
    };
    let behind = state.generation < head.generation;
    let wal_new = behind || holds_older_generation(&wal_local, &images.wal);
    // The `.db-log` of the same rule (docs/mvcc-recon.md, Stage-2 design, Invariant): a
    // new-generation image that is empty next to a non-empty local file. No acked commit is in
    // such a file, and every frame it holds is in the sealed pages.
    let log_new = mvcc && (behind || (images.log.is_empty() && !log_local.is_empty()));
    if wal_new || log_new {
        // The seal of the local generation published `manifest/(head.generation - 1)`,
        // which covers every page that WAL and the dirty file name.
        // Only a change of generation (or a stale WAL of the generation before) is covered by
        // the manifest; a `.db-log` replaced alone leaves the dirty record of this generation.
        if head.manifest.is_some() && (behind || wal_new) {
            truncate_dirty_file(&dir.join(DIRTY_FILE))?;
        }
        // An unclaimed directory (restored, or only ever opened read-only) never sealed: its
        // `app.db` holds the restore's pages, plus whatever a read-only MVCC open's bootstrap
        // backfilled locally. Pages the new generation's manifest maps elsewhere must come
        // from the bucket. A claimed writer's `app.db` holds every page it sealed.
        if !state.claimed {
            state.complete = false;
        }
        if log_new {
            replace_file(dir, LOG_FILE, &images.log)?;
        }
        if wal_new {
            replace_file(dir, WAL_FILE, &images.wal)?;
        }
        state.generation = head.generation;
        state.write_atomic(dir)?;
    }
    let wal = if wal_new {
        Reconciled::NewGeneration
    } else {
        level_file(dir, bucket, state, cfg, File::Wal, &wal_local, &images.wal)?
    };
    let log = if !mvcc {
        None
    } else if log_new {
        Some(Reconciled::NewGeneration)
    } else {
        Some(level_file(
            dir,
            bucket,
            state,
            cfg,
            File::Log,
            &log_local,
            &images.log,
        )?)
    };
    if head.manifest.is_none() {
        let bytes = match wal {
            Reconciled::Level | Reconciled::Ahead { .. } => &wal_local,
            Reconciled::Behind | Reconciled::NewGeneration => &images.wal,
        };
        seed_database_file(dir, bytes, &head, cfg.journal_mode)?;
    }
    if mvcc && !cfg.read_only {
        // A crash between the seal's steps 6 and 8 leaves WAL frames next to a cut log.
        repair_wal_without_log(dir)?;
    }
    Ok(Outcomes { wal, log })
}

/// Level, Behind or Ahead for one file: `local` against the bucket's `image`.
fn level_file(
    dir: &Path,
    bucket: &mut Bucket,
    state: &mut LocalState,
    cfg: &EngineConfig,
    file: File,
    local: &[u8],
    image: &[u8],
) -> Result<Reconciled> {
    let (name, kind) = match file {
        File::Wal => (WAL_FILE, LogFile::Wal),
        File::Log => (LOG_FILE, LogFile::Log),
    };
    if local == image {
        Ok(Reconciled::Level)
    } else if local.len() < image.len() {
        // The local disk lost what the bucket has, so it may have lost database pages too:
        // pages not read yet in this process come from the bucket. Recorded before the file is
        // replaced, so a crash in between repeats this.
        state.complete = false;
        state.write_atomic(dir)?;
        replace_file(dir, name, image)?;
        Ok(Reconciled::Behind)
    } else if cfg.read_only {
        Err(Error::Unsupported(READ_ONLY))
    } else {
        let uploaded = bucket
            .append_log(kind, vec![catch_up_ops(image, local)])
            .map_err(|f| f.error)?
            .pos;
        Ok(Reconciled::Ahead { uploaded })
    }
}

#[derive(Clone, Copy)]
enum File {
    Wal,
    Log,
}

/// The local `-wal` still holds the generation before `bucket`'s (state and head already
/// moved on). The seal records G+1 locally before the restart write reaches the `-wal`, so a
/// crash in between leaves G's WAL next to state G+1; uploading it would put G's frames
/// into G+1.
/// - `bucket` holds a header: the local one carries other salts or checkpoint seq.
/// - `bucket` is empty (no record yet in this generation): any local bytes are not this
///   generation's. A write of this generation passes the header gate, and the first sync
///   ships it; bytes never shipped belong to no acknowledged commit either.
fn holds_older_generation(local: &[u8], bucket: &[u8]) -> bool {
    if local.is_empty() {
        return false;
    }
    if bucket.is_empty() {
        return true;
    }
    match (local.get(EPOCH), bucket.get(EPOCH)) {
        (Some(l), Some(b)) => l != b,
        _ => false,
    }
}

/// The ops that turn `bucket` into `local`: one write per run of `CHUNK`-byte chunks of
/// `local` that differ from `bucket` (or lie past its end), then a truncate when `bucket` is
/// longer.
fn catch_up_ops(bucket: &[u8], local: &[u8]) -> Vec<Op> {
    let mut ops: Vec<Op> = Vec::new();
    for (i, chunk) in local.chunks(CHUNK).enumerate() {
        let off = i * CHUNK;
        if bucket.get(off..off + chunk.len()) == Some(chunk) {
            continue;
        }
        match ops.last_mut() {
            Some(Op::Write { off: o, bytes }) if *o as usize + bytes.len() == off => {
                bytes.extend_from_slice(chunk)
            }
            _ => ops.push(Op::Write {
                off: off as u64,
                bytes: chunk.to_vec(),
            }),
        }
    }
    if bucket.len() > local.len() {
        ops.push(Op::Truncate {
            len: local.len() as u64,
        });
    }
    ops
}

/// `<dir>/<name>`'s bytes; empty for a missing file. `what` names it in an error.
fn read_file(dir: &Path, name: &str, what: &str) -> Result<Vec<u8>> {
    match fs::read(dir.join(name)) {
        Ok(b) => Ok(b),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(io_err(&format!("read {what}"), e)),
    }
}

/// Write `<dir>/<name>` as a temp file, fsync, rename, fsync `dir`.
fn replace_file(dir: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    write_synced(&tmp, bytes)?;
    fs::rename(&tmp, dir.join(name)).map_err(|e| io_err("rename", e))?;
    fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io_err("fsync dir", e))
}

/// Never sealed: an empty or missing database file next to a WAL with frames would be read
/// as a new database and the WAL deleted, so seed it as a restore does (`page_one_seed`).
fn seed_database_file(dir: &Path, wal: &[u8], head: &Head, mode: JournalMode) -> Result<()> {
    let len = match fs::metadata(dir.join(DB_FILE)) {
        Ok(m) => m.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => return Err(io_err("stat database file", e)),
    };
    if len > 0 {
        return Ok(());
    }
    let seed = page_one_seed(wal, head, mode)?;
    if seed.is_empty() {
        return Ok(());
    }
    replace_file(dir, DB_FILE, &seed)
}

#[cfg(test)]
mod tests;
