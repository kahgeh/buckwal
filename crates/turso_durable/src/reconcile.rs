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
//! It reads and writes `app.db-wal` with `std::fs`, which D4 allows only because the engine
//! has not opened the files yet and `durable.lock` keeps every other process out.

use crate::bucket::{Bucket, LogPos};
use crate::config::EngineConfig;
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE, DIRTY_FILE, WAL_FILE};
use crate::format::{LocalState, Op};
use crate::io::db_storage::{truncate_dirty_file, READ_ONLY};
use crate::io::wal_file::EPOCH;
use crate::restore::{generation_image, page_one_seed, write_synced};
use std::fs;
use std::path::Path;

#[derive(Debug, PartialEq, Eq)]
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
    let head = bucket.head().clone();
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
    let image = generation_image(&*bucket.store(), bucket.keys(), &head, cfg)?.0;
    let local = read_wal(dir)?;
    let outcome = if state.generation < head.generation || holds_older_generation(&local, &image) {
        // The seal of the local WAL's generation published `manifest/(head.generation - 1)`,
        // which covers every page that WAL and the dirty file name.
        if head.manifest.is_some() {
            truncate_dirty_file(&dir.join(DIRTY_FILE))?;
        }
        replace_file(dir, WAL_FILE, &image)?;
        state.generation = head.generation;
        state.write_atomic(dir)?;
        Reconciled::NewGeneration
    } else if local == image {
        Reconciled::Level
    } else if local.len() < image.len() {
        // The local disk lost what the bucket has, so it may have lost database pages too:
        // pages not read yet in this process come from the bucket. Recorded before the
        // `-wal` is replaced, so a crash in between repeats this.
        state.complete = false;
        state.write_atomic(dir)?;
        replace_file(dir, WAL_FILE, &image)?;
        Reconciled::Behind
    } else if cfg.read_only {
        return Err(Error::Unsupported(READ_ONLY));
    } else {
        let uploaded = bucket.append_log(vec![catch_up_ops(&image, &local)])?;
        Reconciled::Ahead { uploaded }
    };
    if head.manifest.is_none() {
        let wal = match outcome {
            Reconciled::Level | Reconciled::Ahead { .. } => &local,
            Reconciled::Behind | Reconciled::NewGeneration => &image,
        };
        seed_database_file(dir, wal)?;
    }
    Ok(outcome)
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

fn read_wal(dir: &Path) -> Result<Vec<u8>> {
    match fs::read(dir.join(WAL_FILE)) {
        Ok(b) => Ok(b),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(io_err("read -wal", e)),
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
fn seed_database_file(dir: &Path, wal: &[u8]) -> Result<()> {
    let len = match fs::metadata(dir.join(DB_FILE)) {
        Ok(m) => m.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => return Err(io_err("stat database file", e)),
    };
    if len > 0 {
        return Ok(());
    }
    let seed = page_one_seed(wal)?;
    if seed.is_empty() {
        return Ok(());
    }
    replace_file(dir, DB_FILE, &seed)
}

#[cfg(test)]
mod tests;
