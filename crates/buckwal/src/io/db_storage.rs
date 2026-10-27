// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! The database file: a local page cache backed by the bucket's sealed pages. Pages not yet
//! local are fetched on first read. Formats 1 to 4 only: pages the engine writes are recorded
//! dirty (in memory and in `<dir>/app.db-dirty`) and uploaded only when the generation is
//! sealed (D6). The legacy seal needs the dirty set, so this code stays for those formats.
//!
//! A format 5 bucket (`packed`) holds no page: no dirty page is recorded, and the dirty file
//! is never read or written. Its WAL restart is a generation switch (`switch_generation`),
//! which uploads no page. A copy restored from a pack reads the blocks that are not local yet from the pack
//! (`blocks`, the `BlockStore`) until every block is local; then nothing is fetched.
use super::block_store::{page_bytes, BlockStore};
use super::stash::stash;
use super::IoMode;
use super::{read_at, sync_file, write_at};
use crate::error::{Error, Result};
use crate::file_record::DbFiles;
use crate::files::{io_err, DIRTY_FILE};
use crate::format::{Keys, LocalState, Manifest};
use crate::store::{with_retry, ObjectStore, RetryPolicy};
use crate::uploader::Uploader;
use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{
    AtomicBool, AtomicU32, AtomicU64,
    Ordering::{Relaxed, SeqCst},
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use turso_core::io::FileSyncType;
use turso_core::{Buffer, Completion, DatabaseStorage, File, IOContext, IO};

/// What a read-only open's IO answers to every write the engine attempts.
pub(crate) const READ_ONLY: &str = "writes on a read-only open";

/// The pages a seal uploads: the in-memory dirty set, the persisted dirty set (u32 LE page
/// numbers; a torn trailing entry is ignored) and every page in a frame of the local `-wal`,
/// limited to `1..=page_count`.
pub(crate) fn seal_page_set(
    memory: &BTreeSet<u32>,
    persisted: &[u8],
    wal_pages: &BTreeSet<u32>,
    page_count: u32,
) -> BTreeSet<u32> {
    memory
        .iter()
        .copied()
        .chain(persisted_pages(persisted))
        .chain(wal_pages.iter().copied())
        .filter(|p| (1..=page_count).contains(p))
        .collect()
}

/// The page numbers in the dirty file's bytes (u32 LE; a torn trailing entry is ignored).
fn persisted_pages(persisted: &[u8]) -> impl Iterator<Item = u32> + '_ {
    persisted
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
}

/// The dirty file's bytes; none when it does not exist.
fn read_dirty_file(path: &Path) -> Result<Vec<u8>> {
    match fs::read(path) {
        Ok(b) => Ok(b),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(io_err("read dirty file", e)),
    }
}

/// What the dirty mutex guards.
struct Dirty {
    /// Pages written since the last seal.
    memory: BTreeSet<u32>,
    /// Pages written since the last append to the dirty file.
    unpersisted: BTreeSet<u32>,
    /// The local state as this process last wrote it (or found it at open): every later write
    /// starts from it, so none reads `app.db-durable` back. Only `write_state` changes it.
    state: LocalState,
}

pub(crate) struct BucketDbStorage {
    inner: turso_core::storage::database::DatabaseFile,
    file: Arc<dyn File>,
    io: Arc<dyn IO>,
    store: Arc<dyn ObjectStore>,
    keys: Keys,
    retry: RetryPolicy,
    uploader: Arc<Uploader>,
    dir: PathBuf,
    /// The manifest at open: where pages that are not local yet come from. Every page a
    /// later seal maps elsewhere was written locally in this process first.
    manifest: Option<Manifest>,
    /// Pages the bucket holds beyond the local file: the manifest's page count, then
    /// whatever a truncate sets. Pages above it are never fetched.
    bucket_pages: AtomicU32,
    /// Every page is local (no fetches).
    complete: AtomicBool,
    page_size: AtomicU32,
    /// Pages fetched or written in this process. Its mutex is the fetch mutex: held for a
    /// whole fetch, and by `write_page`, so a fetch never overwrites a newer write.
    local: Mutex<HashSet<u32>>,
    dirty: Mutex<Dirty>,
    /// The file record's helper (outside-change-detect): the one writer of `app.db-durable`.
    /// None for a replica, which keeps no record.
    files: Option<Arc<DbFiles>>,
    lazy_gets: AtomicU64,
    /// A read-only open: every engine write is refused (fetched pages are still cached),
    /// unless `mvcc`.
    mode: IoMode,
    /// An MVCC database: a read-only open lets the engine's bootstrap write the local file
    /// (backfill); the pages are tracked as dirty locally and nothing is uploaded.
    mvcc: bool,
    /// A format 5 bucket: no dirty page set, no dirty file, a generation switch at a WAL
    /// restart (the module doc).
    packed: bool,
    /// A format 5 copy restored from a pack whose blocks are not all local yet.
    blocks: Option<Arc<BlockStore>>,
    /// The trim grace and `Config::pack_keep`: the pack keep time a background fetch is
    /// measured against (`config::pack_keep_time`).
    keep: (Duration, Option<Duration>),
}

pub(crate) struct StorageParts {
    pub file: Arc<dyn File>,
    pub io: Arc<dyn IO>,
    pub store: Arc<dyn ObjectStore>,
    pub keys: Keys,
    pub retry: RetryPolicy,
    pub uploader: Arc<Uploader>,
    pub dir: PathBuf,
    /// The local state at open, as written to `app.db-durable` (a read-only open's may never
    /// be written).
    pub state: LocalState,
    pub manifest: Option<Manifest>,
    pub mode: IoMode,
    pub mvcc: bool,
    /// The bucket is format 5 (`BucketDbStorage::packed`).
    pub packed: bool,
    /// The pack's blocks (`BucketDbStorage::blocks`).
    pub blocks: Option<Arc<BlockStore>>,
    /// `BucketDbStorage::keep`.
    pub keep: (Duration, Option<Duration>),
}

impl BucketDbStorage {
    pub(crate) fn new(p: StorageParts) -> BucketDbStorage {
        let (page_size, page_count) = p
            .manifest
            .as_ref()
            .map_or((0, 0), |m| (m.page_size, m.page_count));
        let files = crate::file_record::lookup(&p.dir);
        BucketDbStorage {
            files,
            inner: turso_core::storage::database::DatabaseFile::new(p.file.clone()),
            file: p.file,
            io: p.io,
            store: p.store,
            keys: p.keys,
            retry: p.retry,
            uploader: p.uploader,
            dir: p.dir,
            manifest: p.manifest,
            bucket_pages: AtomicU32::new(page_count),
            complete: AtomicBool::new(p.state.complete),
            page_size: AtomicU32::new(page_size),
            local: Mutex::new(HashSet::new()),
            dirty: Mutex::new(Dirty {
                memory: BTreeSet::new(),
                unpersisted: BTreeSet::new(),
                state: p.state,
            }),
            lazy_gets: AtomicU64::new(0),
            mode: p.mode,
            mvcc: p.mvcc,
            packed: p.packed,
            blocks: p.blocks,
            keep: p.keep,
        }
    }

    /// The copy reads blocks from a pack: the background fetch completes it.
    pub(crate) fn has_blocks(&self) -> bool {
        self.blocks.is_some()
    }

    /// The key of the pack the copy still reads blocks from: some block is not local yet.
    /// `None` once every block is local (or the copy reads no pack). The writer's GC never
    /// deletes this pack (spec "GC").
    pub(crate) fn reading_pack(&self) -> Option<String> {
        self.blocks
            .as_ref()
            .filter(|b| !b.is_complete())
            .map(|b| b.pack_key())
    }

    /// The bucket is format 5: a WAL restart is a generation switch (`switch_generation`).
    pub(crate) fn packed(&self) -> bool {
        self.packed
    }

    /// Pages fetched from the bucket so far.
    #[cfg(test)]
    pub(crate) fn lazy_gets(&self) -> u64 {
        self.lazy_gets.load(Relaxed)
    }

    /// Every page is local: nothing is fetched any more.
    #[cfg(test)]
    pub(crate) fn is_complete(&self) -> bool {
        self.complete.load(SeqCst)
    }

    fn dirty_path(&self) -> PathBuf {
        self.dir.join(DIRTY_FILE)
    }

    /// The WAL's page size, for a seal before any page was written in this process.
    pub(crate) fn note_page_size(&self, ps: u32) {
        if ps != 0 {
            let _ = self.page_size.compare_exchange(0, ps, SeqCst, SeqCst);
        }
    }

    fn page_size(&self) -> Result<u32> {
        match self.page_size.load(SeqCst) {
            0 => Err(Error::Corrupt("database page size is unknown".into())),
            ps => Ok(ps),
        }
    }

    /// Make page `p` present in the local file, fetching it from the bucket once (from a
    /// pack: its whole block).
    pub(crate) fn ensure_local(&self, p: u32) -> Result<()> {
        if p == 0 || self.complete.load(SeqCst) {
            return Ok(());
        }
        if let Some(b) = &self.blocks {
            let (off, len) = page_bytes(p as usize, b.page_size() as usize);
            return b.ensure(off, len);
        }
        let mut local = self.local.lock().unwrap();
        if local.contains(&p) {
            return Ok(());
        }
        let Some(m) = &self.manifest else {
            return Ok(());
        };
        if p > m.page_count.min(self.bucket_pages.load(SeqCst)) {
            return Ok(());
        }
        let Some(&g) = m.pages.get(&p) else {
            return Err(Error::Corrupt(format!(
                "page {p} is within page_count {} but has no manifest entry",
                m.page_count
            )));
        };
        let key = self.keys.page(g, p);
        let obj = with_retry(&self.retry, &key, |_| self.store.get(&key))?
            .ok_or_else(|| Error::NotFound(key.clone()))?;
        let page = self.keys.open_page(p, g, &obj.body)?;
        if page.len() != m.page_size as usize {
            return Err(Error::Corrupt(format!(
                "{key} is {} bytes, not the page size {}",
                page.len(),
                m.page_size
            )));
        }
        let pos = (p as u64 - 1) * m.page_size as u64;
        write_at(&*self.file, &*self.io, pos, page.to_vec())
            .map_err(|e| io_err("write fetched page", e))?;
        self.lazy_gets.fetch_add(1, Relaxed);
        local.insert(p);
        Ok(())
    }

    /// Pages the dirty file names were written locally since the last seal, so the local
    /// file holds a version newer than the bucket's: never fetch them over it. Called at open,
    /// before the engine reads anything.
    pub(crate) fn adopt_persisted_dirty(&self) -> Result<()> {
        if self.packed {
            // Formats 1 to 4 only: nothing uploads pages.
            return Ok(());
        }
        let persisted = read_dirty_file(&self.dirty_path())?;
        let pages: BTreeSet<u32> = persisted_pages(&persisted).collect();
        // A replica never seals, so nothing truncates the file: every sync appends its pages
        // again. Rewrite it once, atomically, as the distinct pages when it holds repeats (or
        // a torn tail).
        if self.mode == IoMode::Replica && persisted.len() != pages.len() * 4 {
            let bytes: Vec<u8> = pages.iter().flat_map(|p| p.to_le_bytes()).collect();
            crate::reconcile::replace_file(&self.dir, DIRTY_FILE, &bytes)?;
        }
        self.local.lock().unwrap().extend(pages);
        Ok(())
    }

    /// The bytes of `count` pages of `page_size` from page `first` on are local: from a pack,
    /// each block they touch is fetched first (also before the engine writes them, so a fetch
    /// never overwrites a page the engine wrote). Without a pack, as `ensure_local`.
    fn ensure_pages(&self, first: usize, count: usize, page_size: usize) -> Result<()> {
        if self.complete.load(SeqCst) {
            return Ok(());
        }
        match &self.blocks {
            Some(b) => {
                let (off, _) = page_bytes(first, page_size);
                b.ensure(off, (count * page_size) as u64)
            }
            None => (0..count).try_for_each(|i| match u32::try_from(first + i) {
                Ok(p) => self.ensure_local(p),
                Err(_) => Ok(()),
            }),
        }
    }

    /// A copy restored from a pack: fetch every block that is not local, in 16 MiB ranges, 8
    /// at once (`BlockStore::fetch_all`); once all are local, record the copy complete, and a
    /// writer removes the pack files. The `buckwal.prefetch` span as `prefetch` makes it, its
    /// `buckwal.pages` the pages of the bytes fetched, and `buckwal.prefetch.past_half_keep`
    /// true when the fetch still ran at half the pack keep time. A copy on another machine than
    /// the writer (a read-only open, a replica) then logs a WARN `background fetch slower than
    /// the pack keep time` with the bytes left: GC can delete its pack once the keep time has
    /// passed (spec "Restore"). The writer's own GC keeps the pack it reads.
    fn fetch_pack(&self, blocks: &BlockStore, stop: &AtomicBool) {
        let span = tracing::info_span!(
            target: "buckwal",
            "buckwal.prefetch",
            buckwal.pages = tracing::field::Empty,
            buckwal.prefetch.past_half_keep = tracing::field::Empty,
            buckwal.outcome = tracing::field::Empty,
        );
        let _run = span.enter();
        let before = blocks.fetched();
        let half = crate::config::pack_keep_time(self.keep.0, self.keep.1, blocks.pack_size()) / 2;
        let past_half = AtomicBool::new(false);
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let fetched = std::thread::scope(|s| {
            let dispatch = tracing::dispatcher::get_default(Clone::clone);
            let span = span.clone();
            let past_half = &past_half;
            s.spawn(move || {
                // Ends at once when the fetch ends first (its sender is dropped).
                if done_rx.recv_timeout(half) != Err(std::sync::mpsc::RecvTimeoutError::Timeout) {
                    return;
                }
                past_half.store(true, SeqCst);
                if self.mode != IoMode::Writer {
                    tracing::dispatcher::with_default(&dispatch, || {
                        let _in = span.enter();
                        tracing::warn!(
                            target: "buckwal",
                            pack_key = %blocks.pack_key(),
                            bytes_left = blocks.bytes_left(),
                            keep_ms = (half * 2).as_millis() as u64,
                            "background fetch slower than the pack keep time"
                        );
                    });
                }
            });
            let r = blocks.fetch_all(stop);
            drop(done_tx);
            r
        });
        span.record("buckwal.prefetch.past_half_keep", past_half.load(SeqCst));
        let outcome = match fetched {
            Ok(true) => {
                let r = self.mark_complete().and_then(|_| match self.mode {
                    IoMode::Writer => super::block_store::remove_files(&self.dir),
                    _ => Ok(()),
                });
                match r {
                    Ok(()) => "complete",
                    Err(e) => e.outcome(),
                }
            }
            Ok(false) => "stopped",
            Err(e) => {
                tracing::warn!(target: "buckwal", error = %e, "background fetch of the pack failed");
                e.outcome()
            }
        };
        let page_size = u64::from(blocks.page_size().max(1));
        span.record("buckwal.pages", (blocks.fetched() - before) / page_size);
        span.record("buckwal.outcome", outcome);
    }

    /// Fetch every page the manifest names; once all are local, nothing is fetched again, in
    /// this process or a later one (`mark_complete`). Returns early, leaving the rest to be
    /// fetched on read, once `stop` is set. A copy restored from a pack fetches its blocks
    /// instead (`fetch_pack`).
    ///
    /// One `buckwal.prefetch` span covers the run, from its first page to its return, with
    /// `buckwal.pages` (pages fetched during the run) and `buckwal.outcome`: `complete`,
    /// `stopped`, `integrity` or `error`. It records once, never per page.
    pub(crate) fn prefetch(&self, stop: &AtomicBool) {
        if let Some(b) = &self.blocks {
            if !self.complete.load(SeqCst) {
                self.fetch_pack(b, stop);
            }
            return;
        }
        let Some(m) = &self.manifest else {
            return;
        };
        let span = tracing::info_span!(
            target: "buckwal",
            "buckwal.prefetch",
            buckwal.pages = tracing::field::Empty,
            buckwal.outcome = tracing::field::Empty,
        );
        let _run = span.enter();
        let mut fetched = 0u64;
        let mut outcome = "complete";
        for &p in m.pages.keys() {
            if stop.load(SeqCst) {
                outcome = "stopped";
                break;
            }
            let before = self.lazy_gets.load(Relaxed);
            let r = self.ensure_local(p);
            fetched += self.lazy_gets.load(Relaxed).saturating_sub(before);
            if let Err(e) = r {
                outcome = e.outcome();
                break;
            }
        }
        if outcome == "complete" {
            // A failure leaves `complete=false` on disk: the next seal records it.
            let _ = self.mark_complete();
        }
        span.record("buckwal.pages", fetched);
        span.record("buckwal.outcome", outcome);
    }

    /// Every page is local. A writer open fsyncs the fetched pages, then records
    /// `complete=true` in the local state at once, so a later process fetches nothing either.
    /// A read-only open keeps the flag in memory only: it never checkpoints, so its `app.db`
    /// holds just the manifest's pages while the generation's changes live in the local
    /// `-wal`, which a later open may replace with a newer generation (`NewGeneration`) whose
    /// pages must then be fetched. The in-memory flag is set even if persisting fails (the
    /// pages are local in this process).
    fn mark_complete(&self) -> Result<()> {
        let mut dirty = self.dirty.lock().unwrap();
        if self.mode != IoMode::Writer {
            self.complete.store(true, SeqCst);
            return Ok(());
        }
        let r = sync_file(&*self.file, &*self.io)
            .map_err(|e| io_err("fsync db", e))
            .and_then(|_| {
                let state = LocalState {
                    complete: true,
                    ..dirty.state.clone()
                };
                self.write_state(&mut dirty, state)
            });
        self.complete.store(true, SeqCst);
        r
    }

    /// The one way this process writes `app.db-durable` once the engine is open. Every writer
    /// must hold the dirty mutex (the `&mut Dirty` proves it), which the seal holds for its
    /// whole run, so the seal and `mark_complete` never write it at the same time or from a
    /// stale copy. `state` is built from `Dirty::state` and becomes it once written; the file
    /// is never read back.
    fn write_state(&self, dirty: &mut Dirty, state: LocalState) -> Result<()> {
        match &self.files {
            Some(files) => files.write_state(&state)?,
            None => state.write_atomic(&self.dir)?,
        }
        dirty.state = state;
        Ok(())
    }

    fn record_written(&self, first: u32, count: usize, page_size: usize) {
        self.page_size.store(page_size as u32, SeqCst);
        let pages = (0..count as u32).map(|i| first + i);
        self.local.lock().unwrap().extend(pages.clone());
        if self.packed {
            // Nothing uploads pages: no dirty set.
            return;
        }
        let mut d = self.dirty.lock().unwrap();
        d.memory.extend(pages.clone());
        d.unpersisted.extend(pages);
    }

    /// Append the pages dirtied since the last append to the dirty file and fsync it.
    fn persist_dirty(&self) -> Result<()> {
        if self.packed {
            return Ok(());
        }
        let mut d = self.dirty.lock().unwrap();
        if d.unpersisted.is_empty() {
            return Ok(());
        }
        let path = self.dirty_path();
        let created = !path.exists();
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| io_err("open dirty file", e))?;
        let bytes: Vec<u8> = d.unpersisted.iter().flat_map(|p| p.to_le_bytes()).collect();
        f.write_all(&bytes)
            .and_then(|_| f.sync_all())
            .map_err(|e| io_err("append dirty file", e))?;
        if created {
            fs::File::open(&self.dir)
                .and_then(|d| d.sync_all())
                .map_err(|e| io_err("fsync dir", e))?;
        }
        d.unpersisted.clear();
        Ok(())
    }

    /// Format 5, at a WAL restart: fsync the local file, then the generation switch (through
    /// the uploader, behind every log record already submitted), then record the new generation
    /// locally. No page and no manifest goes up, and there is no dirty set to forget. Returns
    /// the new generation.
    ///
    /// The fsync comes first: once the head names the next generation, this writer's own
    /// `app.db` is the only local copy of the generation that ended (its WAL is restarted next,
    /// and a reopen replaces it, `reconcile`).
    pub(crate) fn switch_generation(&self) -> Result<u64> {
        let mut dirty = self.dirty.lock().unwrap();
        sync_file(&*self.file, &*self.io).map_err(|e| io_err("fsync db", e))?;
        let generation = self.uploader.switch_generation()?;
        if let Some(b) = &self.blocks {
            b.set_generation(generation);
        }
        // As in `seal`: the head's ETag changed, and a takeover quiet of the generation that
        // ended is cleared. Writer id, epoch and claim id carry over.
        let state = LocalState {
            generation,
            complete: self.complete.load(SeqCst),
            claimed: true,
            head_etag: String::new(),
            seal_not_before_ms: 0,
            seal_quiet_ms: 0,
            ..dirty.state.clone()
        };
        self.write_state(&mut dirty, state)?;
        Ok(generation)
    }

    /// Formats 1 to 4. D6 steps 2-6: upload the generation's pages, manifest and head (through the uploader,
    /// behind every log record already submitted), fsync the local file, record the new
    /// generation locally, then forget the dirty set. Returns the new generation.
    pub(crate) fn seal(&self, wal_frame_pages: &BTreeSet<u32>) -> Result<u64> {
        let mut dirty = self.dirty.lock().unwrap();
        let ps = self.page_size()?;
        let size = DatabaseStorage::size(self).map_err(|e| io_err("db size", e))?;
        let page_count = u32::try_from(size / ps as u64)
            .map_err(|_| Error::Corrupt(format!("database of {size} bytes is too large")))?;
        let persisted = read_dirty_file(&self.dirty_path())?;
        let set = seal_page_set(&dirty.memory, &persisted, wal_frame_pages, page_count);
        let complete = self.complete.load(SeqCst);
        let local = self.local.lock().unwrap().clone();
        let mut pages = Vec::with_capacity(set.len());
        for p in set {
            // A page never made local in this process (only a stale frame names it) is
            // unchanged since the manifest: the bucket copy stays.
            //
            // Invariant this skip relies on: a page written to the local file in an earlier
            // process of this generation is written again in this one before any boundary.
            // turso 0.8.3 starts `nbackfills` at 0 when a WAL is opened without shared
            // coordination (sqlite3_ondisk.rs:1567, 2018) and restarts the log only when
            // `max_frame == nbackfills` (wal.rs:5287-5299), so every valid frame is
            // backfilled again (`write_page`, marking its page local) before the restart that
            // triggers this seal. Pages in the dirty file are also marked local at open
            // (`adopt_persisted_dirty`). Pinned by `seal_after_passive_checkpoint_crash_keeps_
            // pages` (reconcile tests), on a locally created and on a restored directory.
            if !complete && !local.contains(&p) {
                continue;
            }
            let pos = (p as u64 - 1) * ps as u64;
            let mut bytes = read_at(&*self.file, &*self.io, pos, ps as usize)
                .map_err(|e| io_err("read page for seal", e))?;
            // Past the end of the local file a page reads as zeros, as it does for the engine.
            bytes.resize(ps as usize, 0);
            pages.push((p, bytes));
        }
        let generation = self.uploader.seal_generation(pages, ps, page_count)?;
        sync_file(&*self.file, &*self.io).map_err(|e| io_err("fsync db", e))?;
        // Only a claimed writer seals (a read-only open never writes), and the head's ETag
        // changed with this seal, so none is kept. A takeover quiet (seal-not-before and its
        // length) applied to the generation just sealed: cleared. The writer id, the epoch and
        // the claim id are not this seal's to change: they carry over from the state in
        // memory, so nothing here reads the file after the head is published.
        let state = LocalState {
            generation,
            complete: self.complete.load(SeqCst),
            claimed: true,
            head_etag: String::new(),
            seal_not_before_ms: 0,
            seal_quiet_ms: 0,
            ..dirty.state.clone()
        };
        self.write_state(&mut dirty, state)?;
        truncate_dirty_file(&self.dirty_path())?;
        dirty.memory.clear();
        dirty.unpersisted.clear();
        Ok(generation)
    }
}

impl BucketDbStorage {
    /// The backstop of a read-only open: a WAL database's engine never gets to write the
    /// database file (an MVCC one's bootstrap backfills it).
    fn refuse_if_read_only(&self) -> turso_core::Result<()> {
        if self.mode == IoMode::ReadOnly && !self.mvcc {
            return Err(stash(Error::Unsupported(READ_ONLY)));
        }
        Ok(())
    }
}

pub(crate) fn truncate_dirty_file(path: &Path) -> Result<()> {
    match fs::OpenOptions::new().write(true).open(path) {
        Ok(f) => f
            .set_len(0)
            .and_then(|_| f.sync_all())
            .map_err(|e| io_err("truncate dirty file", e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_err("open dirty file", e)),
    }
}

impl DatabaseStorage for BucketDbStorage {
    fn read_header(&self, c: Completion) -> turso_core::Result<Completion> {
        self.ensure_local(1).map_err(stash)?;
        self.inner.read_header(c)
    }
    fn read_page(
        &self,
        page_idx: usize,
        io_ctx: &IOContext,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.ensure_pages(page_idx, 1, c.as_read().buf().len())
            .map_err(stash)?;
        self.inner.read_page(page_idx, io_ctx, c)
    }
    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<Buffer>,
        io_ctx: &IOContext,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.refuse_if_read_only()?;
        if self.blocks.is_some() {
            self.ensure_pages(page_idx, 1, buffer.len())
                .map_err(stash)?;
        }
        self.record_written(page_idx as u32, 1, buffer.len());
        self.inner.write_page(page_idx, buffer, io_ctx, c)
    }
    fn write_pages(
        &self,
        first_page_idx: usize,
        page_size: usize,
        buffers: Vec<Arc<Buffer>>,
        io_ctx: &IOContext,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.refuse_if_read_only()?;
        if self.blocks.is_some() {
            self.ensure_pages(first_page_idx, buffers.len(), page_size)
                .map_err(stash)?;
        }
        self.record_written(first_page_idx as u32, buffers.len(), page_size);
        self.inner
            .write_pages(first_page_idx, page_size, buffers, io_ctx, c)
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        // Uploads nothing. Formats 1 to 4: the pages go up when the generation is sealed, and
        // their numbers are made durable before the database file is.
        self.persist_dirty().map_err(stash)?;
        self.inner.sync(c, sync_type)
    }
    fn size(&self) -> turso_core::Result<u64> {
        let bucket = self.bucket_pages.load(SeqCst) as u64 * self.page_size.load(SeqCst) as u64;
        Ok(self.inner.size()?.max(bucket))
    }
    fn truncate(&self, len: usize, c: Completion) -> turso_core::Result<Completion> {
        self.refuse_if_read_only()?;
        if let Some(b) = &self.blocks {
            b.truncate(len as u64).map_err(stash)?;
        }
        if let Some(ps) = std::num::NonZeroU32::new(self.page_size.load(SeqCst)) {
            let pages = (len as u64 / ps.get() as u64).min(u32::MAX as u64) as u32;
            self.bucket_pages.store(pages, SeqCst);
        }
        self.inner.truncate(len, c)
    }
}
