// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! The IO layer (prototype 3 on bucket format v1): the engine's `-wal` file ships every
//! sync to the bucket before its fsync, and the database file is a local page cache backed
//! by the bucket's sealed pages.
pub(crate) mod db_storage;
pub(crate) mod durable_io;
pub(crate) mod log_file;
pub(crate) mod log_storage;
pub(crate) mod stash;
pub(crate) mod wal_file;

pub(crate) use db_storage::BucketDbStorage;
pub(crate) use durable_io::DurableIo;

use std::sync::atomic::{AtomicI32, Ordering::SeqCst};
use std::sync::Arc;
use turso_core::io::FileSyncType;
use turso_core::{Buffer, Completion, File, IO};

/// Read up to `len` bytes at `pos` through `file`, waiting on each completion. Fewer bytes
/// come back only at end of file.
pub(crate) fn read_at(
    file: &dyn File,
    io: &dyn IO,
    pos: u64,
    len: usize,
) -> turso_core::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        let want = len - out.len();
        let buf = Arc::new(Buffer::new_temporary(want));
        let got = Arc::new(AtomicI32::new(0));
        let got2 = got.clone();
        let c = Completion::new_read(buf.clone(), move |r| {
            if let Ok((_, n)) = r {
                got2.store(n, SeqCst);
            }
            None
        });
        let c = file.pread(pos + out.len() as u64, c)?;
        io.wait_for_completion(c)?;
        let n = (got.load(SeqCst).max(0) as usize).min(want);
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf.as_slice()[..n]);
    }
    Ok(out)
}

/// Write `bytes` at `pos` through `file` and wait for it.
pub(crate) fn write_at(
    file: &dyn File,
    io: &dyn IO,
    pos: u64,
    bytes: Vec<u8>,
) -> turso_core::Result<()> {
    let c = file.pwrite(
        pos,
        Arc::new(Buffer::new(bytes)),
        Completion::new_write(|_| {}),
    )?;
    io.wait_for_completion(c)
}

/// fsync `file` and wait for it.
pub(crate) fn sync_file(file: &dyn File, io: &dyn IO) -> turso_core::Result<()> {
    let c = file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)?;
    io.wait_for_completion(c)
}
