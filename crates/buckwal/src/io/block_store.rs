//! The database file of a restored format 5 copy while its pack is not all local (Choice
//! C(a)): the restore returns at once, a read of a page whose block is not local fetches that
//! 1 MiB block (one range GET) and checks its code, and a background fetch completes the file
//! in 16 MiB ranges, 8 at once.
//!
//! A block is made local before the engine reads or writes any page in it, so a fetch never
//! overwrites a page the engine wrote. Which blocks are local is kept in `app.db-blocks`
//! (`BLOCKS_FILE`): a block's flag is written and synced only after its bytes are synced in
//! `app.db`, and before the engine can write to it, so a later process never fetches a block
//! again over a newer page. `app.db-pack` (`PACK_FILE`) holds the pointer and the codes, as the
//! bucket holds them (a coded pointer and coded codes are checked again at each load).
//!
//! A pack that GC deleted (a newer pack replaced it) is replaced by the newer pack when the
//! newer pack's generation is at most the generation the local file holds: every page that
//! changed between the two packs was then written locally, so its block is local, and every
//! other page is the same in both. A restore never replaces its pack: it starts again.
use crate::error::{Error, Result};
use crate::files::{io_err, BLOCKS_FILE, DB_FILE, PACK_FILE};
use crate::format::Keys;
use crate::pack::codes::{PackCodes, BLOCK_SIZE};
use crate::pack::run::PART_CONCURRENCY;
use crate::pack::PackPointer;
use crate::store::{with_retry, ObjectStore, RetryPolicy};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{
    AtomicBool, AtomicU64, AtomicUsize,
    Ordering::{Relaxed, SeqCst},
};
use std::sync::{Arc, Mutex};

/// How many times one fetch replaces a pack that is gone by a newer one.
const MAX_SWITCHES: u32 = 3;
/// The bytes of `BLOCKS_FILE` before the block flags: the limit, u64 LE.
const LIMIT_LEN: u64 = 8;

/// The pack the blocks come from: its pointer and its codes.
struct Source {
    pointer: PackPointer,
    codes: PackCodes,
}

/// The bytes of `PACK_FILE`: the pointer's length (u32 LE), the pointer, then the codes.
fn pack_file_bytes(pointer: &[u8], codes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + pointer.len() + codes.len());
    out.extend_from_slice(&(pointer.len() as u32).to_le_bytes());
    out.extend_from_slice(pointer);
    out.extend_from_slice(codes);
    out
}

/// The pointer and the codes `PACK_FILE` holds, decoded and checked as a read of them from the
/// bucket is.
fn decode_pack_file(keys: &Keys, bytes: &[u8]) -> Result<Source> {
    let bad = || Error::Corrupt(format!("{PACK_FILE} is damaged"));
    let n = u32::from_le_bytes(bytes.get(..4).ok_or_else(bad)?.try_into().unwrap()) as usize;
    let pointer = bytes.get(4..4 + n).ok_or_else(bad)?;
    let codes = &bytes[4 + n..];
    let pointer = keys.decode_pointer(pointer)?;
    let codes = PackCodes::decode(
        keys,
        codes,
        &pointer.pack_key,
        pointer.pack_generation,
        pointer.size,
    )?;
    Ok(Source { pointer, codes })
}

/// The page size bytes 16-17 of page 1 give (1 means 65536).
fn header_page_size(page1: &[u8]) -> Option<u32> {
    let field = u16::from_be_bytes(page1.get(16..18)?.try_into().ok()?);
    Some(if field == 1 { 65536 } else { u32::from(field) })
}

/// The pointer's page size against the database header in the pack's first block: they must be
/// equal, and the pack must hold whole pages.
fn check_page_size(p: &PackPointer, block0: &[u8]) -> Result<()> {
    let header = header_page_size(block0);
    if header != Some(p.page_size) || !p.size.is_multiple_of(u64::from(p.page_size)) {
        return Err(Error::Corrupt(format!(
            "pack pointer: page_size {} but {} holds {} bytes of {:?}-byte pages by its header",
            p.page_size, p.pack_key, p.size, header
        )));
    }
    Ok(())
}

/// `len` bytes of the pack `p` names from `offset` (a block boundary), in one range GET, each
/// block checked against `codes`. Fewer bytes at the end of the pack. `None` when the pack is
/// gone. A range of the wrong length, or a block that fails its code, is `Corrupt` (the
/// integrity error and record in an encrypted bucket), and so is an `offset` at or after the end
/// of the pack: no block of it is there.
pub(crate) fn fetch_verified(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    p: &PackPointer,
    codes: &PackCodes,
    offset: u64,
    len: u64,
) -> Result<Option<Vec<u8>>> {
    debug_assert_eq!(offset % BLOCK_SIZE, 0);
    if offset >= p.size {
        return Err(Error::Corrupt(format!(
            "{}: block {} is past the end of the pack ({} bytes)",
            p.pack_key,
            offset / BLOCK_SIZE,
            p.size
        )));
    }
    let want = (p.size - offset).min(len);
    let Some(bytes) = with_retry(retry, &p.pack_key, |_| {
        store.get_range(&p.pack_key, offset, want)
    })?
    else {
        return Ok(None);
    };
    let first = offset / BLOCK_SIZE;
    // A short range fails the code of its last block: a pack cut short.
    for (i, block) in bytes.chunks(BLOCK_SIZE as usize).enumerate() {
        codes.verify_block(
            keys,
            &p.pack_key,
            p.pack_generation,
            p.size,
            first + i as u64,
            block,
        )?;
    }
    if bytes.len() as u64 != want {
        let n = first + bytes.len() as u64 / BLOCK_SIZE;
        codes.verify_block(keys, &p.pack_key, p.pack_generation, p.size, n, &[])?;
    }
    Ok(Some(bytes))
}

pub(crate) struct BlockStore {
    store: Arc<dyn ObjectStore>,
    keys: Keys,
    retry: RetryPolicy,
    dir: PathBuf,
    /// `app.db`, for the fetched blocks (the engine writes through its own handle).
    db: File,
    source: Mutex<Arc<Source>>,
    /// One flag per block of the first pack, whatever pack the blocks come from now. Blocks
    /// beyond it were written locally.
    local: Vec<AtomicBool>,
    /// `BLOCKS_FILE`. Its mutex is the write lock: held to write fetched blocks into `app.db`,
    /// so a fetch writes a block only while it is not local, and by `truncate`.
    blocks_file: Mutex<File>,
    /// Bytes at or above it are never fetched: the pack's size, then whatever a truncate of
    /// the engine sets. It only goes down.
    limit: AtomicU64,
    /// The generation the local file holds at least: a newer pack up to it may replace a pack
    /// that is gone.
    generation: AtomicU64,
    /// A restore's store: a pack that is gone is never replaced (the restore starts again).
    restore: bool,
    /// Set when a range GET found the pack gone and no newer pack replaced it.
    gone: Arc<AtomicBool>,
    /// Pack bytes fetched by this store.
    fetched: AtomicU64,
}

impl BlockStore {
    /// A restore's block store in `dir`: `PACK_FILE` from the bucket's pointer and codes
    /// (`pointer_body`, `codes_body`, decoded as `pointer` and `codes`), `app.db` of the pack's
    /// size with no block local, and `BLOCKS_FILE`. `gone` is set when the pack is gone.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create(
        dir: &Path,
        store: Arc<dyn ObjectStore>,
        keys: &Keys,
        retry: &RetryPolicy,
        pointer: PackPointer,
        pointer_body: &[u8],
        codes: PackCodes,
        codes_body: &[u8],
        gone: Arc<AtomicBool>,
    ) -> Result<BlockStore> {
        crate::restore::write_synced(
            &dir.join(PACK_FILE),
            &pack_file_bytes(pointer_body, codes_body),
        )?;
        let blocks = pointer.size.div_ceil(BLOCK_SIZE);
        let mut flags = pointer.size.to_le_bytes().to_vec();
        flags.resize((LIMIT_LEN + blocks) as usize, 0);
        crate::restore::write_synced(&dir.join(BLOCKS_FILE), &flags)?;
        let db = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(DB_FILE))
            .map_err(|e| io_err("open app.db", e))?;
        db.set_len(pointer.size)
            .and_then(|_| db.sync_all())
            .map_err(|e| io_err("size app.db", e))?;
        crate::restore::fsync_dir(dir)?;
        let generation = pointer.pack_generation;
        Self::build(
            dir,
            store,
            keys,
            retry,
            Source { pointer, codes },
            &flags,
            generation,
            true,
            gone,
        )
    }

    /// The block store of a restored copy at an open, at local `generation`: `None` when
    /// `complete` (leftover pack files are removed) or when the directory holds no
    /// `PACK_FILE`.
    pub(crate) fn open(
        dir: &Path,
        store: Arc<dyn ObjectStore>,
        keys: &Keys,
        retry: &RetryPolicy,
        complete: bool,
        generation: u64,
    ) -> Result<Option<BlockStore>> {
        if complete {
            remove_files(dir)?;
            return Ok(None);
        }
        let bytes = match fs::read(dir.join(PACK_FILE)) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_err("read app.db-pack", e)),
        };
        let source = decode_pack_file(keys, &bytes)?;
        let flags = fs::read(dir.join(BLOCKS_FILE)).map_err(|e| io_err("read app.db-blocks", e))?;
        Self::build(
            dir,
            store,
            keys,
            retry,
            source,
            &flags,
            generation,
            false,
            Arc::default(),
        )
        .map(Some)
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        dir: &Path,
        store: Arc<dyn ObjectStore>,
        keys: &Keys,
        retry: &RetryPolicy,
        source: Source,
        flags: &[u8],
        generation: u64,
        restore: bool,
        gone: Arc<AtomicBool>,
    ) -> Result<BlockStore> {
        // The flags are of the first pack's blocks (a newer pack that replaced it can have more
        // or fewer): the limit, then one flag per block, and the limit is within them.
        let Some(limit) = flags.get(..8) else {
            return Err(Error::Corrupt(format!(
                "{BLOCKS_FILE} holds {} bytes, fewer than its limit",
                flags.len()
            )));
        };
        let limit = u64::from_le_bytes(limit.try_into().unwrap());
        let blocks = flags.len() as u64 - LIMIT_LEN;
        if limit > blocks * BLOCK_SIZE {
            return Err(Error::Corrupt(format!(
                "{BLOCKS_FILE}: its limit {limit} is beyond its {blocks} blocks"
            )));
        }
        let db = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join(DB_FILE))
            .map_err(|e| io_err("open app.db", e))?;
        let blocks_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join(BLOCKS_FILE))
            .map_err(|e| io_err("open app.db-blocks", e))?;
        Ok(BlockStore {
            store,
            keys: keys.clone(),
            retry: retry.clone(),
            dir: dir.to_path_buf(),
            db,
            source: Mutex::new(Arc::new(source)),
            local: flags[8..]
                .iter()
                .map(|f| AtomicBool::new(*f != 0))
                .collect(),
            blocks_file: Mutex::new(blocks_file),
            limit: AtomicU64::new(limit),
            generation: AtomicU64::new(generation),
            restore,
            gone,
            fetched: AtomicU64::new(0),
        })
    }

    /// The key of the pack the blocks come from now.
    pub(crate) fn pack_key(&self) -> String {
        self.source.lock().unwrap().pointer.pack_key.clone()
    }

    /// The pointer's page size (checked against the header with the first block).
    pub(crate) fn page_size(&self) -> u32 {
        self.source.lock().unwrap().pointer.page_size
    }

    /// Pack bytes this store fetched.
    pub(crate) fn fetched(&self) -> u64 {
        self.fetched.load(Relaxed)
    }

    /// The local file now holds generation `g` at least (a generation switch).
    pub(crate) fn set_generation(&self, g: u64) {
        self.generation.fetch_max(g, SeqCst);
    }

    /// Whether block `b` needs no fetch: it is local, or it starts at or above the limit.
    fn done(&self, b: usize) -> bool {
        self.local.get(b).is_none_or(|f| f.load(SeqCst))
            || b as u64 * BLOCK_SIZE >= self.limit.load(SeqCst)
    }

    /// Every block is local (or above the limit): nothing is fetched any more.
    pub(crate) fn is_complete(&self) -> bool {
        (0..self.local.len()).all(|b| self.done(b))
    }

    /// Make the bytes `offset..offset + len` of the file local, fetching each block of them
    /// that is not local yet (one range GET per block).
    pub(crate) fn ensure(&self, offset: u64, len: u64) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let first = offset / BLOCK_SIZE;
        let last = (offset + len - 1) / BLOCK_SIZE;
        for b in first..=last {
            self.ensure_block(b as usize)?;
        }
        Ok(())
    }

    /// Make block `b` local.
    pub(crate) fn ensure_block(&self, b: usize) -> Result<()> {
        if self.done(b) {
            return Ok(());
        }
        let bytes = self.fetch(b as u64 * BLOCK_SIZE, BLOCK_SIZE)?;
        self.write_fetched(b, &bytes)
    }

    /// The pack's bytes from `offset` (a block boundary), checked, from the current pack or a
    /// newer one that replaced it.
    fn fetch(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut switches = 0;
        loop {
            let source = self.source.lock().unwrap().clone();
            let p = &source.pointer;
            let got = fetch_verified(
                &*self.store,
                &self.keys,
                &self.retry,
                p,
                &source.codes,
                offset,
                len,
            )?;
            if let Some(bytes) = got {
                if offset == 0 {
                    check_page_size(p, &bytes)?;
                }
                self.fetched.fetch_add(bytes.len() as u64, Relaxed);
                return Ok(bytes);
            }
            if self.restore || switches == MAX_SWITCHES || !self.switch_from(p)? {
                self.gone.store(true, SeqCst);
                return Err(Error::NotFound(p.pack_key.clone()));
            }
            switches += 1;
        }
    }

    /// The pack `old` is gone: take the pack the pointer names now when its generation is at
    /// most the local file's (the module doc). False when there is none to take.
    fn switch_from(&self, old: &PackPointer) -> Result<bool> {
        let key = self.keys.pack_pointer();
        let Some(obj) = with_retry(&self.retry, &key, |_| self.store.get(&key))? else {
            return Ok(false);
        };
        // Another database's pointer is no pointer (spec rev 11, rule 3).
        let Some(p) = self.keys.read_pointer(&obj.body)? else {
            return Ok(false);
        };
        if p.pack_key == old.pack_key || p.pack_generation > self.generation.load(SeqCst) {
            return Ok(false);
        }
        self.check_local_page_size(&p, old)?;
        let Some(codes) = with_retry(&self.retry, &p.codes_key, |_| self.store.get(&p.codes_key))?
        else {
            return Ok(false);
        };
        let decoded = PackCodes::decode(
            &self.keys,
            &codes.body,
            &p.pack_key,
            p.pack_generation,
            p.size,
        )?;
        crate::reconcile::replace_file(
            &self.dir,
            PACK_FILE,
            &pack_file_bytes(&obj.body, &codes.body),
        )?;
        tracing::info!(
            target: "buckwal",
            from = %old.pack_key,
            to = %p.pack_key,
            "the pack is gone: blocks now come from the newer pack"
        );
        *self.source.lock().unwrap() = Arc::new(Source {
            pointer: p,
            codes: decoded,
        });
        Ok(true)
    }

    /// The newer pointer `p` against the local file: its page size must be the one the local
    /// header gives (the header of `old`'s first block while block 0 is not local).
    fn check_local_page_size(&self, p: &PackPointer, old: &PackPointer) -> Result<()> {
        let local = if self.done(0) {
            let mut header = [0u8; 18];
            self.db
                .read_exact_at(&mut header, 0)
                .map_err(|e| io_err("read the app.db header", e))?;
            header_page_size(&header)
        } else {
            Some(old.page_size)
        };
        if local != Some(p.page_size) {
            return Err(Error::Corrupt(format!(
                "pack pointer: page_size {} of {}, but the local database has {:?}-byte pages",
                p.page_size, p.pack_key, local
            )));
        }
        Ok(())
    }

    /// Write the fetched `bytes` (blocks from block `first` on) into `app.db`, each block only
    /// while it is not local and only below the limit, then sync `app.db`, then set and sync
    /// the blocks' flags.
    fn write_fetched(&self, first: usize, bytes: &[u8]) -> Result<()> {
        let flags = self.blocks_file.lock().unwrap();
        let limit = self.limit.load(SeqCst);
        let mut new = Vec::new();
        for (i, block) in bytes.chunks(BLOCK_SIZE as usize).enumerate() {
            let b = first + i;
            if b >= self.local.len() || self.local[b].load(SeqCst) {
                continue;
            }
            let start = b as u64 * BLOCK_SIZE;
            let end = (start + block.len() as u64).min(limit);
            if start < end {
                self.db
                    .write_all_at(&block[..(end - start) as usize], start)
                    .map_err(|e| io_err("write a fetched block", e))?;
            }
            new.push(b);
        }
        if new.is_empty() {
            return Ok(());
        }
        self.db.sync_data().map_err(|e| io_err("fsync app.db", e))?;
        for &b in &new {
            flags
                .write_all_at(&[1], LIMIT_LEN + b as u64)
                .map_err(|e| io_err("write app.db-blocks", e))?;
        }
        flags
            .sync_data()
            .map_err(|e| io_err("fsync app.db-blocks", e))?;
        for b in new {
            self.local[b].store(true, SeqCst);
        }
        Ok(())
    }

    /// The engine truncates the file to `len`: bytes at or above it are never fetched again,
    /// in this process or a later one. Called before the file is truncated.
    pub(crate) fn truncate(&self, len: u64) -> Result<()> {
        let flags = self.blocks_file.lock().unwrap();
        let limit = self.limit.load(SeqCst).min(len);
        flags
            .write_all_at(&limit.to_le_bytes(), 0)
            .and_then(|_| flags.sync_data())
            .map_err(|e| io_err("write app.db-blocks", e))?;
        self.limit.store(limit, SeqCst);
        Ok(())
    }

    /// Fetch every block that is not local, in ranges of the pack's part size (16 MiB), up to
    /// `PART_CONCURRENCY` (8) at once. Returns early once `stop` is set. Ok(true) when every
    /// block is local.
    pub(crate) fn fetch_all(&self, stop: &AtomicBool) -> Result<bool> {
        let part = self.source.lock().unwrap().pointer.part_size;
        let per_range = (part / BLOCK_SIZE).max(1) as usize;
        let ranges = self.local.len().div_ceil(per_range);
        let next = AtomicUsize::new(0);
        let failed: Mutex<Option<Error>> = Mutex::new(None);
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        std::thread::scope(|s| {
            for _ in 0..PART_CONCURRENCY.min(ranges) {
                s.spawn(|| {
                    tracing::dispatcher::with_default(&dispatch, || loop {
                        if stop.load(SeqCst) || failed.lock().unwrap().is_some() {
                            return;
                        }
                        let i = next.fetch_add(1, Relaxed);
                        if i >= ranges {
                            return;
                        }
                        let blocks = i * per_range..((i + 1) * per_range).min(self.local.len());
                        if let Err(e) = self.fetch_range(blocks) {
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
        Ok(self.is_complete())
    }

    /// Fetch the blocks of `blocks` that are not local, from the first to the last of them, in
    /// one range GET.
    fn fetch_range(&self, blocks: std::ops::Range<usize>) -> Result<()> {
        let mut todo = blocks.filter(|&b| !self.done(b));
        let Some(first) = todo.next() else {
            return Ok(());
        };
        let last = todo.last().unwrap_or(first);
        let bytes = self.fetch(
            first as u64 * BLOCK_SIZE,
            (last - first + 1) as u64 * BLOCK_SIZE,
        )?;
        self.write_fetched(first, &bytes)
    }
}

/// The database file the engine sees while a restore applies the generations after the pack
/// (`restore::apply_generation_over`): the local `app.db`, each block made local before a page
/// in it is read or written.
pub(crate) struct BlockDbStorage {
    inner: turso_core::storage::database::DatabaseFile,
    blocks: Arc<BlockStore>,
}

impl BlockDbStorage {
    pub(crate) fn new(file: Arc<dyn turso_core::File>, blocks: Arc<BlockStore>) -> Self {
        BlockDbStorage {
            inner: turso_core::storage::database::DatabaseFile::new(file),
            blocks,
        }
    }
}

/// The bytes of page `page_idx` (from 1) of `page_size` bytes.
pub(crate) fn page_bytes(page_idx: usize, page_size: usize) -> (u64, u64) {
    (
        (page_idx as u64).saturating_sub(1) * page_size as u64,
        page_size as u64,
    )
}

impl turso_core::DatabaseStorage for BlockDbStorage {
    fn read_header(&self, c: turso_core::Completion) -> turso_core::Result<turso_core::Completion> {
        self.blocks.ensure_block(0).map_err(super::stash::stash)?;
        self.inner.read_header(c)
    }
    fn read_page(
        &self,
        page_idx: usize,
        io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        let (off, len) = page_bytes(page_idx, c.as_read().buf().len());
        self.blocks.ensure(off, len).map_err(super::stash::stash)?;
        self.inner.read_page(page_idx, io_ctx, c)
    }
    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<turso_core::Buffer>,
        io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        let (off, len) = page_bytes(page_idx, buffer.len());
        self.blocks.ensure(off, len).map_err(super::stash::stash)?;
        self.inner.write_page(page_idx, buffer, io_ctx, c)
    }
    fn write_pages(
        &self,
        first_page_idx: usize,
        page_size: usize,
        buffers: Vec<Arc<turso_core::Buffer>>,
        io_ctx: &turso_core::IOContext,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        let (off, _) = page_bytes(first_page_idx, page_size);
        let len = (buffers.len() * page_size) as u64;
        self.blocks.ensure(off, len).map_err(super::stash::stash)?;
        self.inner
            .write_pages(first_page_idx, page_size, buffers, io_ctx, c)
    }
    fn sync(
        &self,
        c: turso_core::Completion,
        sync_type: turso_core::io::FileSyncType,
    ) -> turso_core::Result<turso_core::Completion> {
        self.inner.sync(c, sync_type)
    }
    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
    fn truncate(
        &self,
        len: usize,
        c: turso_core::Completion,
    ) -> turso_core::Result<turso_core::Completion> {
        self.blocks
            .truncate(len as u64)
            .map_err(super::stash::stash)?;
        self.inner.truncate(len, c)
    }
}

/// Remove `PACK_FILE` and `BLOCKS_FILE` from `dir`, those that exist (the file is complete).
pub(crate) fn remove_files(dir: &Path) -> Result<()> {
    let mut removed = false;
    for f in [PACK_FILE, BLOCKS_FILE] {
        match fs::remove_file(dir.join(f)) {
            Ok(()) => removed = true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err("remove a pack file", e)),
        }
    }
    if removed {
        crate::restore::fsync_dir(dir)?;
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    static NO_BACKGROUND_FETCH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether an open on this thread starts the background fetch of a pack (tests can turn it
/// off with [`NoBackgroundFetch`]).
pub(crate) fn background_fetch() -> bool {
    #[cfg(test)]
    if NO_BACKGROUND_FETCH.with(|f| f.get()) {
        return false;
    }
    true
}

/// Tests only: while alive, an open on this thread starts no background fetch of the pack.
#[cfg(test)]
pub(crate) struct NoBackgroundFetch;

#[cfg(test)]
impl NoBackgroundFetch {
    pub(crate) fn on() -> Self {
        NO_BACKGROUND_FETCH.with(|f| f.set(true));
        NoBackgroundFetch
    }
}

#[cfg(test)]
impl Drop for NoBackgroundFetch {
    fn drop(&mut self) {
        NO_BACKGROUND_FETCH.with(|f| f.set(false));
    }
}

#[cfg(test)]
mod tests;
