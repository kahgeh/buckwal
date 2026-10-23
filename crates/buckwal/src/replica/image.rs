//! The WAL file rebuilt in memory from log records, and its committed prefix.

use crate::error::Result;
use crate::format::{apply_ops, Op};

const WAL_HEADER_LEN: usize = 32;
const FRAME_HEADER_LEN: usize = 24;

/// One whole WAL frame: number (1-based), 24-byte header and page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Frame {
    pub no: u64,
    pub header: [u8; 24],
    pub page: Vec<u8>,
}

/// The bytes of `app.db-wal` as the writer has synced them so far.
#[derive(Debug, Clone)]
pub(crate) struct WalImage {
    page_size: usize,
    bytes: Vec<u8>,
}

impl WalImage {
    pub(crate) fn new(page_size: usize) -> WalImage {
        WalImage {
            page_size,
            bytes: Vec::new(),
        }
    }

    /// Apply one sync's ops with file semantics (zero-fill past the end, truncate resizes).
    pub(crate) fn apply_sync(&mut self, ranges: &[Op]) -> Result<()> {
        apply_ops(&mut self.bytes, ranges)
    }

    fn frame_len(&self) -> usize {
        FRAME_HEADER_LEN + self.page_size
    }

    /// Whole frames present, partial trailing frame not counted.
    fn whole_frames(&self) -> usize {
        self.bytes.len().saturating_sub(WAL_HEADER_LEN) / self.frame_len()
    }

    fn frame_start(&self, no: usize) -> usize {
        WAL_HEADER_LEN + (no - 1) * self.frame_len()
    }

    /// Number of frames up to and including the last whole frame whose db_size
    /// (header bytes 4..8, big-endian) is non-zero.
    fn committed_count(&self) -> usize {
        (1..=self.whole_frames())
            .rev()
            .find(|&n| {
                let s = self.frame_start(n);
                self.bytes[s + 4..s + 8] != [0; 4]
            })
            .unwrap_or(0)
    }

    pub(crate) fn committed_len(&self) -> u64 {
        self.committed_count() as u64
    }

    pub(crate) fn committed_frames(&self) -> Vec<Frame> {
        (1..=self.committed_count())
            .map(|n| {
                let s = self.frame_start(n);
                let mut header = [0u8; FRAME_HEADER_LEN];
                header.copy_from_slice(&self.bytes[s..s + FRAME_HEADER_LEN]);
                Frame {
                    no: n as u64,
                    header,
                    page: self.bytes[s + FRAME_HEADER_LEN..s + self.frame_len()].to_vec(),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PS: usize = 512;
    const FL: usize = FRAME_HEADER_LEN + PS;

    fn frame_bytes(db_size: u32, fill: u8) -> Vec<u8> {
        let mut f = vec![fill; FL];
        f[..FRAME_HEADER_LEN].fill(0);
        f[4..8].copy_from_slice(&db_size.to_be_bytes());
        f
    }

    fn frame_off(n: usize) -> u64 {
        (WAL_HEADER_LEN + (n - 1) * FL) as u64
    }

    fn write(off: u64, bytes: Vec<u8>) -> Op {
        Op::Write { off, bytes }
    }

    fn header_op() -> Op {
        write(0, vec![7; WAL_HEADER_LEN])
    }

    #[test]
    fn empty_image_has_no_frames() {
        let img = WalImage::new(PS);
        assert_eq!(img.committed_len(), 0);
        assert!(img.committed_frames().is_empty());
        let mut img = WalImage::new(PS);
        img.apply_sync(&[header_op()]).unwrap();
        assert_eq!(img.committed_len(), 0);
    }

    #[test]
    fn frames_after_last_commit_are_held_back() {
        let mut img = WalImage::new(PS);
        img.apply_sync(&[
            header_op(),
            write(frame_off(1), frame_bytes(0, 1)),
            write(frame_off(2), frame_bytes(5, 2)),
            write(frame_off(3), frame_bytes(0, 3)),
        ])
        .unwrap();
        assert_eq!(img.committed_len(), 2);
        let frames = img.committed_frames();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].no, 1);
        assert_eq!(frames[1].no, 2);
        assert_eq!(frames[1].header[4..8], 5u32.to_be_bytes());
        assert_eq!(frames[1].page, vec![2; PS]);
    }

    #[test]
    fn rewind_below_held_back_frames_drops_them() {
        let mut img = WalImage::new(PS);
        img.apply_sync(&[
            header_op(),
            write(frame_off(1), frame_bytes(0, 1)),
            write(frame_off(2), frame_bytes(5, 2)),
            write(frame_off(3), frame_bytes(0, 3)),
        ])
        .unwrap();
        img.apply_sync(&[write(frame_off(3), frame_bytes(6, 9))])
            .unwrap();
        assert_eq!(img.committed_len(), 3);
        assert_eq!(img.committed_frames()[2].page, vec![9; PS]);
    }

    #[test]
    fn partial_frame_is_not_counted() {
        let mut img = WalImage::new(PS);
        img.apply_sync(&[
            header_op(),
            write(frame_off(1), frame_bytes(3, 1)),
            write(frame_off(2), frame_bytes(4, 2)[..100].to_vec()),
        ])
        .unwrap();
        assert_eq!(img.committed_len(), 1);
        img.apply_sync(&[write(frame_off(2) + 100, frame_bytes(4, 2)[100..].to_vec())])
            .unwrap();
        assert_eq!(img.committed_len(), 2);
    }

    #[test]
    fn zero_fill_past_end() {
        let mut img = WalImage::new(PS);
        img.apply_sync(&[header_op(), write(frame_off(3), frame_bytes(8, 3))])
            .unwrap();
        // frames 1 and 2 are zero (db_size 0) but sit below the commit at 3
        assert_eq!(img.committed_len(), 3);
        let frames = img.committed_frames();
        assert_eq!(frames[0].header, [0; 24]);
        assert_eq!(frames[1].page, vec![0; PS]);
        assert_eq!(frames[2].page, vec![3; PS]);
    }

    #[test]
    fn truncate_to_below_committed_prefix_shrinks_committed_len() {
        let mut img = WalImage::new(PS);
        img.apply_sync(&[
            header_op(),
            write(frame_off(1), frame_bytes(1, 1)),
            write(frame_off(2), frame_bytes(2, 2)),
            write(frame_off(3), frame_bytes(3, 3)),
        ])
        .unwrap();
        assert_eq!(img.committed_len(), 3);
        img.apply_sync(&[Op::Truncate {
            len: frame_off(2) + FL as u64,
        }])
        .unwrap();
        assert_eq!(img.committed_len(), 2);
        img.apply_sync(&[Op::Truncate { len: 0 }]).unwrap();
        assert_eq!(img.committed_len(), 0);
    }
}
