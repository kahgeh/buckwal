//! Log object codec: magic, kind, writer id, syncs of ops, sha256 trailer.
// Op and its wire encoding (tag 1: [u64 off][u32 len][bytes]; tag 2: [u64 len])
// are Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
use crate::error::{Error, Result};
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 8] = b"TDLOG\0\0\x01";
const KIND_EXTENTS: u8 = 1;
const KIND_FENCE: u8 = 2;
const KIND_LOG_EXTENTS: u8 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Op {
    Write { off: u64, bytes: Vec<u8> },
    Truncate { len: u64 },
}

/// Which local file an extents record's ops apply to: kind 1 is `app.db-wal`, kind 3 (bucket
/// format v2) is `app.db-log`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogFile {
    Wal,
    Log,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogRecord {
    Extents {
        file: LogFile,
        writer_id: String,
        syncs: Vec<Vec<Op>>,
    },
    Fence {
        writer_id: String,
        epoch: u64,
    },
}

fn corrupt(m: &str) -> Error {
    Error::Corrupt(format!("log record: {m}"))
}

fn put_writer_id(out: &mut Vec<u8>, id: &str) {
    assert!(
        id.is_ascii() && id.len() <= u8::MAX as usize,
        "writer id must be ASCII and at most 255 bytes"
    );
    out.push(id.len() as u8);
    out.extend(id.as_bytes());
}

pub(crate) fn encode_record(r: &LogRecord) -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    match r {
        LogRecord::Extents {
            file,
            writer_id,
            syncs,
        } => {
            out.push(match file {
                LogFile::Wal => KIND_EXTENTS,
                LogFile::Log => KIND_LOG_EXTENTS,
            });
            put_writer_id(&mut out, writer_id);
            out.extend((syncs.len() as u32).to_le_bytes());
            for ops in syncs {
                out.extend((ops.len() as u32).to_le_bytes());
                for op in ops {
                    match op {
                        Op::Write { off, bytes } => {
                            out.push(1);
                            out.extend(off.to_le_bytes());
                            out.extend((bytes.len() as u32).to_le_bytes());
                            out.extend(bytes);
                        }
                        Op::Truncate { len } => {
                            out.push(2);
                            out.extend(len.to_le_bytes());
                        }
                    }
                }
            }
        }
        LogRecord::Fence { writer_id, epoch } => {
            out.push(KIND_FENCE);
            put_writer_id(&mut out, writer_id);
            out.extend(0u32.to_le_bytes()); // sync_count, always 0
            out.extend(epoch.to_le_bytes());
        }
    }
    let sha = Sha256::digest(&out);
    out.extend(sha);
    out
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(corrupt("truncated"));
        }
        let (h, t) = self.0.split_at(n);
        self.0 = t;
        Ok(h)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

pub(crate) fn decode_record(b: &[u8]) -> Result<LogRecord> {
    if b.len() < MAGIC.len() + 32 {
        return Err(corrupt("too short"));
    }
    let (body, sha) = b.split_at(b.len() - 32);
    if body[..MAGIC.len()] != MAGIC[..] {
        return Err(corrupt("bad magic"));
    }
    if Sha256::digest(body).as_slice() != sha {
        return Err(corrupt("checksum mismatch"));
    }
    let mut r = Reader(&body[MAGIC.len()..]);
    let kind = r.u8()?;
    let id_len = r.u8()? as usize;
    let writer_id = r.take(id_len)?;
    if !writer_id.is_ascii() {
        return Err(corrupt("writer id is not ASCII"));
    }
    let writer_id = String::from_utf8_lossy(writer_id).into_owned();
    let rec = match kind {
        KIND_FENCE => {
            if r.u32()? != 0 {
                return Err(corrupt("fence with syncs"));
            }
            LogRecord::Fence {
                writer_id,
                epoch: r.u64()?,
            }
        }
        KIND_EXTENTS | KIND_LOG_EXTENTS => {
            let sync_count = r.u32()?;
            let mut syncs = Vec::new();
            for _ in 0..sync_count {
                let op_count = r.u32()?;
                let mut ops = Vec::new();
                for _ in 0..op_count {
                    match r.u8()? {
                        1 => {
                            let off = r.u64()?;
                            let len = r.u32()? as usize;
                            ops.push(Op::Write {
                                off,
                                bytes: r.take(len)?.to_vec(),
                            });
                        }
                        2 => ops.push(Op::Truncate { len: r.u64()? }),
                        t => return Err(corrupt(&format!("bad op tag {t}"))),
                    }
                }
                syncs.push(ops);
            }
            let file = if kind == KIND_LOG_EXTENTS {
                LogFile::Log
            } else {
                LogFile::Wal
            };
            LogRecord::Extents {
                file,
                writer_id,
                syncs,
            }
        }
        k => return Err(corrupt(&format!("unknown kind {k}"))),
    };
    if !r.0.is_empty() {
        return Err(corrupt("trailing bytes"));
    }
    Ok(rec)
}

/// Apply ops with file semantics: a write past the end zero-fills the gap;
/// truncate sets the length (zero-extending when it grows).
pub(crate) fn apply_ops(buf: &mut Vec<u8>, ops: &[Op]) -> Result<()> {
    for op in ops {
        match op {
            Op::Write { off, bytes } => {
                let off = usize::try_from(*off).map_err(|_| corrupt("write offset overflow"))?;
                let end = off
                    .checked_add(bytes.len())
                    .ok_or_else(|| corrupt("write offset overflow"))?;
                if buf.len() < end {
                    buf.resize(end, 0);
                }
                buf[off..end].copy_from_slice(bytes);
            }
            Op::Truncate { len } => buf.resize(
                usize::try_from(*len).map_err(|_| corrupt("truncate length overflow"))?,
                0,
            ),
        }
    }
    Ok(())
}
