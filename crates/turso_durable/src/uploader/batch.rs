//! The uploader's waiting room: Window syncs gathered into one log record until the first of
//! their limits is reached. Pure: the caller supplies the clock and does the uploading.
use crate::batching::Batching;
use crate::bucket::LogPos;
use crate::error::Result;
use crate::format::Op;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

/// One submitted sync waiting to be uploaded: its ops, the setting it was committed with,
/// its commit number and where its result goes.
pub(crate) struct Pending {
    pub ops: Vec<Op>,
    pub batching: Batching,
    pub commit: u64,
    pub reply: Sender<Result<LogPos>>,
}

impl Pending {
    /// The bytes its writes carry; truncates count nothing.
    pub(crate) fn bytes(&self) -> u64 {
        self.ops
            .iter()
            .map(|op| match op {
                Op::Write { bytes, .. } => bytes.len() as u64,
                Op::Truncate { .. } => 0,
            })
            .sum()
    }
}

/// Syncs waiting to share one record, in push order. Each limit is the smallest among the
/// items, so an item's own limits hold whatever joins it.
pub(crate) struct Batch {
    items: Vec<Pending>,
    bytes: u64,
    max_commits: u64,
    max_bytes: u64,
    deadline: Option<Instant>,
}

impl Default for Batch {
    fn default() -> Self {
        Batch {
            items: Vec::new(),
            bytes: 0,
            max_commits: u64::MAX,
            max_bytes: u64::MAX,
            deadline: None,
        }
    }
}

impl Batch {
    /// Add `item`, pushed at `now`. True when the batch must be flushed now: the item is
    /// `Off`, or the count or the bytes reach the smallest `max_commits` or `max_bytes`
    /// among the items, or the item's `max_wait` is zero.
    pub(crate) fn push(&mut self, item: Pending, now: Instant) -> bool {
        self.bytes = self.bytes.saturating_add(item.bytes());
        let due = match item.batching {
            Batching::Off => true,
            Batching::Window {
                max_wait,
                max_commits,
                max_bytes,
            } => {
                self.max_commits = self.max_commits.min(u64::from(max_commits));
                self.max_bytes = self.max_bytes.min(max_bytes);
                // A wait too long to represent never comes due.
                if let Some(at) = now.checked_add(max_wait) {
                    self.deadline = Some(self.deadline.map_or(at, |d| d.min(at)));
                }
                max_wait == Duration::ZERO
            }
        };
        self.items.push(item);
        due || self.items.len() as u64 >= self.max_commits || self.bytes >= self.max_bytes
    }

    /// The earliest `pushed_at + max_wait` among the items.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    /// Every item, in push order; the batch starts over empty.
    pub(crate) fn take(&mut self) -> Vec<Pending> {
        std::mem::take(self).items
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn item(batching: Batching, bytes: usize) -> Pending {
        let (reply, _) = mpsc::channel();
        Pending {
            ops: vec![Op::Write {
                off: 0,
                bytes: vec![0; bytes],
            }],
            batching,
            commit: 0,
            reply,
        }
    }

    #[test]
    fn window_flushes_at_the_first_limit() {
        let w = |ms, n, b| Batching::Window {
            max_wait: Duration::from_millis(ms),
            max_commits: n,
            max_bytes: b,
        };
        let t0 = Instant::now();
        let mut b = Batch::default();
        assert!(!b.push(item(w(50, 3, 1 << 20), 10), t0));
        assert!(!b.push(item(w(50, 3, 1 << 20), 10), t0));
        assert!(
            b.push(item(w(50, 3, 1 << 20), 10), t0),
            "third commit reaches max_commits"
        );
        assert_eq!(b.take().len(), 3);
        assert!(!b.push(item(w(50, 64, 100), 60), t0));
        assert!(
            b.push(item(w(50, 64, 100), 60), t0),
            "120 bytes ≥ max_bytes 100"
        );
        b.take();
        b.push(item(w(50, 64, 1 << 20), 1), t0);
        b.push(item(w(5, 64, 1 << 20), 1), t0 + Duration::from_millis(1));
        assert_eq!(
            b.deadline(),
            Some(t0 + Duration::from_millis(6)),
            "the earliest item deadline wins"
        );
    }

    #[test]
    fn degenerate_limits_flush_immediately() {
        let t0 = Instant::now();
        for b in [
            Batching::Window {
                max_wait: Duration::from_millis(50),
                max_commits: 1,
                max_bytes: 1 << 20,
            },
            Batching::Window {
                max_wait: Duration::ZERO,
                max_commits: 64,
                max_bytes: 1 << 20,
            },
            Batching::Window {
                max_wait: Duration::from_millis(50),
                max_commits: 64,
                max_bytes: 1,
            },
            Batching::Off,
        ] {
            assert!(Batch::default().push(item(b, 4096), t0), "{b}");
        }
    }

    #[test]
    fn take_keeps_push_order_and_resets_the_limits() {
        let t0 = Instant::now();
        let tight = Batching::Window {
            max_wait: Duration::from_millis(5),
            max_commits: 2,
            max_bytes: 1 << 20,
        };
        let loose = Batching::Window {
            max_wait: Duration::from_secs(10),
            max_commits: 64,
            max_bytes: 1 << 20,
        };
        let mut b = Batch::default();
        let mut first = item(loose, 1);
        first.commit = 7;
        let mut second = item(tight, 1);
        second.commit = 8;
        assert!(!b.push(first, t0));
        assert!(
            b.push(second, t0),
            "the smallest max_commits among the items"
        );
        let taken: Vec<u64> = b.take().iter().map(|p| p.commit).collect();
        assert_eq!(taken, [7, 8]);
        assert_eq!(b.len(), 0);
        assert_eq!(b.deadline(), None);
        assert!(
            !b.push(item(loose, 1), t0),
            "the tight limits left with their item"
        );
        assert!(!b.push(item(loose, 1), t0));
        assert_eq!(b.deadline(), Some(t0 + Duration::from_secs(10)));
    }

    #[test]
    fn bytes_count_only_written_bytes() {
        let (reply, _) = mpsc::channel();
        let p = Pending {
            ops: vec![
                Op::Write {
                    off: 0,
                    bytes: vec![0; 10],
                },
                Op::Truncate { len: 4096 },
                Op::Write {
                    off: 99,
                    bytes: vec![0; 5],
                },
            ],
            batching: Batching::Off,
            commit: 1,
            reply,
        };
        assert_eq!(p.bytes(), 15);
    }
}
