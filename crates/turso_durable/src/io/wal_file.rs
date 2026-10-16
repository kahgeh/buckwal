// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! The engine's `-wal` file. Every write and truncate is recorded as an op; `sync` ships the
//! ops as one log record and only then fsyncs (D3). A rewrite of the WAL header with new salts,
//! or a truncate to 0, starts a new generation: the old one is sealed first (D6).
use super::db_storage::{BucketDbStorage, READ_ONLY};
use super::read_at;
use super::stash::stash;
use crate::batching::Batching;
use crate::error::{Error, Result};
use crate::files::io_err;
use crate::format::Op;
use crate::uploader::Uploader;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use turso_core::io::FileSyncType;
use turso_core::{Buffer, Completion, File, IO};

const WAL_HEADER: usize = 32;
const FRAME_HEADER: usize = 24;
/// Bytes 12..24 of the WAL header: checkpoint sequence and the two salts. They change at
/// every WAL restart, which is a generation boundary (D6). Boundary detection here and
/// reconcile's "the local WAL holds an older generation" both compare exactly these bytes.
pub(crate) const EPOCH: std::ops::Range<usize> = 12..24;

/// The page number of every whole frame in a `-wal` image (page size from header bytes
/// 8..12, frames from offset 32, each `24 + page_size` bytes, page number in frame bytes
/// 0..4, all big-endian). A trailing partial frame is ignored.
pub(crate) fn wal_frame_pages(wal: &[u8]) -> BTreeSet<u32> {
    let mut pages = BTreeSet::new();
    if wal.len() < WAL_HEADER {
        return pages;
    }
    let page_size = u32::from_be_bytes(wal[8..12].try_into().unwrap()) as usize;
    if page_size == 0 {
        return pages;
    }
    let frame = FRAME_HEADER + page_size;
    let mut off = WAL_HEADER;
    while off + frame <= wal.len() {
        let page = u32::from_be_bytes(wal[off..off + 4].try_into().unwrap());
        if page != 0 {
            pages.insert(page);
        }
        off += frame;
    }
    pages
}

fn engine_io(e: turso_core::LimboError) -> Error {
    io_err("-wal", e)
}

pub(crate) struct ShippingFile {
    inner: Arc<dyn File>,
    io: Arc<dyn IO>,
    uploader: Arc<Uploader>,
    storage: Arc<BucketDbStorage>,
    /// Ops written since the last sync.
    pending: Mutex<Vec<Op>>,
    /// The current generation's WAL header (its first 32 bytes), once one is known.
    header: Mutex<Option<Vec<u8>>>,
    /// A read-only open: every write and truncate is refused.
    read_only: bool,
}

impl ShippingFile {
    /// Wrap the opened `-wal`; an existing header (the file holds at least 32 bytes) is the
    /// current generation's.
    pub(crate) fn new(
        inner: Arc<dyn File>,
        io: Arc<dyn IO>,
        uploader: Arc<Uploader>,
        storage: Arc<BucketDbStorage>,
        read_only: bool,
    ) -> turso_core::Result<ShippingFile> {
        let header = if inner.size()? >= WAL_HEADER as u64 {
            Some(read_at(&*inner, &*io, 0, WAL_HEADER)?)
        } else {
            None
        };
        Ok(ShippingFile {
            inner,
            io,
            uploader,
            storage,
            pending: Mutex::new(Vec::new()),
            header: Mutex::new(header),
            read_only,
        })
    }

    /// Ship the ops written since the last sync as one log record and wait for it. On
    /// failure they belong to the uploader's in-doubt record, so they are not requeued.
    fn ship_pending(&self) -> Result<()> {
        let ops = std::mem::take(&mut *self.pending.lock().unwrap());
        if ops.is_empty() {
            return Ok(());
        }
        self.uploader
            .submit_log(ops, Batching::Off)
            .wait()
            .map(|_| ())
    }

    /// Seal the generation the local file still holds (D6): its last ops, then the pages.
    fn seal(&self) -> Result<u64> {
        self.ship_pending()?;
        let len = self.inner.size().map_err(engine_io)?;
        let wal = read_at(&*self.inner, &*self.io, 0, len as usize).map_err(engine_io)?;
        if wal.len() >= WAL_HEADER {
            self.storage
                .note_page_size(u32::from_be_bytes(wal[8..12].try_into().unwrap()));
        }
        self.storage.seal(&wal_frame_pages(&wal))
    }

    /// A header write at offset 0. New checkpoint sequence or salts while a header is
    /// recorded: a new generation, so seal first. The first header, or the same one re-sent
    /// after a failed header sync, is not a boundary.
    fn on_header(&self, header: &[u8]) -> turso_core::Result<()> {
        let mut h = self.header.lock().unwrap();
        let boundary = match &*h {
            Some(old) if old.len() >= EPOCH.end && header.len() >= EPOCH.end => {
                old[EPOCH] != header[EPOCH]
            }
            _ => false,
        };
        if boundary {
            self.seal().map_err(stash)?;
        }
        *h = Some(header[..header.len().min(WAL_HEADER)].to_vec());
        Ok(())
    }

    /// The backstop of a read-only open: the engine never gets to change the `-wal`.
    fn refuse_if_read_only(&self) -> turso_core::Result<()> {
        if self.read_only {
            return Err(stash(Error::Unsupported(READ_ONLY)));
        }
        Ok(())
    }

    /// Record `ops` once the inner call that issued them returned `Ok`: a write or truncate
    /// refused synchronously never happened, so it must never be shipped. (A failure reported
    /// later, through its completion, panics the engine, D3.)
    fn record_if_ok(
        &self,
        ops: Vec<Op>,
        r: turso_core::Result<Completion>,
    ) -> turso_core::Result<Completion> {
        if r.is_ok() {
            self.pending.lock().unwrap().extend(ops);
        }
        r
    }
}

impl File for ShippingFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.refuse_if_read_only()?;
        if pos == 0 {
            self.on_header(buffer.as_slice())?;
        }
        let op = Op::Write {
            off: pos,
            bytes: buffer.as_slice().to_vec(),
        };
        self.record_if_ok(vec![op], self.inner.pwrite(pos, buffer, c))
    }
    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.refuse_if_read_only()?;
        if pos == 0 {
            if let Some(first) = buffers.first() {
                self.on_header(first.as_slice())?;
            }
        }
        let mut off = pos;
        let mut ops = Vec::with_capacity(buffers.len());
        for b in &buffers {
            ops.push(Op::Write {
                off,
                bytes: b.as_slice().to_vec(),
            });
            off += b.len() as u64;
        }
        self.record_if_ok(ops, self.inner.pwritev(pos, buffers, c))
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        // Upload before any completion exists: a failed upload is a synchronous `Err`, which
        // rolls the commit back, never a failed completion (which would panic, D3).
        self.ship_pending().map_err(stash)?;
        self.inner.sync(c, sync_type)
    }
    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.refuse_if_read_only()?;
        if len == 0 {
            let mut h = self.header.lock().unwrap();
            if h.is_some() {
                self.seal().map_err(stash)?;
            }
            *h = None;
        }
        self.record_if_ok(vec![Op::Truncate { len }], self.inner.truncate(len, c))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::tests::writer;
    use crate::store::MemStore;
    use turso_core::LimboError;

    /// A `-wal` whose every write and truncate fails synchronously.
    struct Refusing;

    fn refused() -> LimboError {
        LimboError::InternalError("refused".into())
    }

    impl File for Refusing {
        fn lock_file(&self, _: bool) -> turso_core::Result<()> {
            Ok(())
        }
        fn unlock_file(&self) -> turso_core::Result<()> {
            Ok(())
        }
        fn pread(&self, _: u64, _: Completion) -> turso_core::Result<Completion> {
            Err(refused())
        }
        fn pwrite(&self, _: u64, _: Arc<Buffer>, _: Completion) -> turso_core::Result<Completion> {
            Err(refused())
        }
        fn pwritev(
            &self,
            _: u64,
            _: Vec<Arc<Buffer>>,
            _: Completion,
        ) -> turso_core::Result<Completion> {
            Err(refused())
        }
        fn sync(&self, _: Completion, _: FileSyncType) -> turso_core::Result<Completion> {
            Err(refused())
        }
        fn size(&self) -> turso_core::Result<u64> {
            Ok(0)
        }
        fn truncate(&self, _: u64, _: Completion) -> turso_core::Result<Completion> {
            Err(refused())
        }
    }

    /// A write or truncate the inner file refused synchronously never happened: no op is
    /// left behind for the next sync to ship.
    #[test]
    fn refused_writes_record_no_op() {
        let mem = Arc::new(MemStore::new());
        let d = tempfile::tempdir().unwrap();
        let (e, _conn) = writer(mem, d.path());
        let f = ShippingFile::new(
            Arc::new(Refusing),
            Arc::new(turso_core::PlatformIO::new().unwrap()),
            e.uploader.clone(),
            e.storage.clone(),
            false,
        )
        .unwrap();
        let buf = || Arc::new(Buffer::new(vec![7u8; 64]));
        assert!(f
            .pwrite(4096, buf(), Completion::new_write(|_| {}))
            .is_err());
        assert!(f
            .pwritev(4096, vec![buf(), buf()], Completion::new_write(|_| {}))
            .is_err());
        assert!(f.truncate(4096, Completion::new_trunc(|_| {})).is_err());
        assert_eq!(f.pending.lock().unwrap().len(), 0, "phantom ops recorded");
    }
}
