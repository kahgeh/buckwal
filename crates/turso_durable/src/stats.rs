//! Upload counters: how many commits each log PUT carried and how long acks waited.
//! [`UploadStats`] is the public snapshot; `StatsCell` is the live, shared state behind it.
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Duration;

/// A snapshot of the upload counters since open.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct UploadStats {
    /// Commits whose ack returned `Ok`.
    pub commits_acked: u64,
    /// Commits in log records confirmed in the bucket.
    pub commits_uploaded: u64,
    /// Log records confirmed in the bucket that carry at least one commit. Records with no
    /// commit (a claim, a seal start, a reconcile catch-up) are not counted.
    pub log_puts: u64,
    /// The encoded size of those records.
    pub log_put_bytes: u64,
    /// `commits_uploaded / log_puts`; 0.0 when `log_puts == 0`.
    pub commits_per_log_put: f64,
    /// `log_put_bytes / log_puts`; 0.0 when `log_puts == 0`.
    pub bytes_per_log_put: f64,
    /// Ack wait quantiles, each the upper edge of its histogram sub-bucket (at most 6.25 %
    /// above the true value).
    pub ack_wait_p50: Duration,
    pub ack_wait_p95: Duration,
    pub ack_wait_p99: Duration,
}

const BUCKETS: usize = 64;
const SUBS: usize = 16;

/// A fixed-size histogram of waits in microseconds: 64 power-of-two buckets, bucket `b`
/// covering `[2^b, 2^(b+1))` µs (0 µs goes to bucket 0), each split into 16 linear
/// sub-buckets, so a quantile is at most 6.25 % above the value it stands for.
#[derive(Debug, Clone)]
pub(crate) struct AckHistogram {
    counts: [[u64; SUBS]; BUCKETS],
    total: u64,
}

impl Default for AckHistogram {
    fn default() -> Self {
        AckHistogram {
            counts: [[0; SUBS]; BUCKETS],
            total: 0,
        }
    }
}

impl AckHistogram {
    pub(crate) fn record(&mut self, wait: Duration) {
        let us = u64::try_from(wait.as_micros()).unwrap_or(u64::MAX).max(1);
        let b = us.ilog2() as usize;
        let s = (((us - (1 << b)) as u128 * SUBS as u128) >> b) as usize;
        self.counts[b][s] += 1;
        self.total += 1;
    }

    /// The upper edge of the sub-bucket holding the `q` quantile; zero when empty.
    pub(crate) fn quantile(&self, q: f64) -> Duration {
        if self.total == 0 {
            return Duration::ZERO;
        }
        let rank = ((q * self.total as f64).ceil() as u64).clamp(1, self.total);
        let mut seen = 0;
        for (b, subs) in self.counts.iter().enumerate() {
            for (s, n) in subs.iter().enumerate() {
                seen += n;
                if seen >= rank {
                    return upper_edge(b, s);
                }
            }
        }
        upper_edge(BUCKETS - 1, SUBS - 1)
    }
}

/// `2^b + (s + 1) · 2^b / 16` µs.
fn upper_edge(b: usize, s: usize) -> Duration {
    let nanos = (1u128 << b) * 1000 * (SUBS + s + 1) as u128 / SUBS as u128;
    // At most 2^64 µs: the seconds fit a u64.
    Duration::new(
        (nanos / 1_000_000_000) as u64,
        (nanos % 1_000_000_000) as u32,
    )
}

/// The live counters, shared by the uploader (puts) and the connections (acks).
#[derive(Debug, Default)]
pub(crate) struct StatsCell {
    commits_acked: AtomicU64,
    commits_uploaded: AtomicU64,
    log_puts: AtomicU64,
    log_put_bytes: AtomicU64,
    acks: Mutex<AckHistogram>,
}

impl StatsCell {
    /// A log record of `bytes` holding `commits` commits is in the bucket. A record with no
    /// commit is not counted.
    pub(crate) fn record_put(&self, commits: u64, bytes: u64) {
        if commits == 0 {
            return;
        }
        self.commits_uploaded.fetch_add(commits, Relaxed);
        self.log_puts.fetch_add(1, Relaxed);
        self.log_put_bytes.fetch_add(bytes, Relaxed);
    }

    /// One commit was acked after waiting `wait`.
    #[cfg_attr(not(test), allow(dead_code))] // Task 5: `Connection` acks
    pub(crate) fn record_ack(&self, wait: Duration) {
        self.acks.lock().unwrap().record(wait);
        self.commits_acked.fetch_add(1, Relaxed);
    }

    #[cfg_attr(not(test), allow(dead_code))] // Task 5: `Database::upload_stats`
    pub(crate) fn snapshot(&self) -> UploadStats {
        let acks = self.acks.lock().unwrap().clone();
        let log_puts = self.log_puts.load(Relaxed);
        let commits_uploaded = self.commits_uploaded.load(Relaxed);
        let log_put_bytes = self.log_put_bytes.load(Relaxed);
        let per_put = |n: u64| {
            if log_puts == 0 {
                0.0
            } else {
                n as f64 / log_puts as f64
            }
        };
        UploadStats {
            commits_acked: self.commits_acked.load(Relaxed),
            commits_uploaded,
            log_puts,
            log_put_bytes,
            commits_per_log_put: per_put(commits_uploaded),
            bytes_per_log_put: per_put(log_put_bytes),
            ack_wait_p50: acks.quantile(0.50),
            ack_wait_p95: acks.quantile(0.95),
            ack_wait_p99: acks.quantile(0.99),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_quantiles_are_within_a_sub_bucket() {
        let mut h = AckHistogram::default();
        for us in 1..=1000 {
            h.record(Duration::from_micros(us));
        }
        let p50 = h.quantile(0.50);
        let p99 = h.quantile(0.99);
        assert!(
            (Duration::from_micros(500)..=Duration::from_micros(532)).contains(&p50),
            "{p50:?}"
        );
        assert!(
            (Duration::from_micros(990)..=Duration::from_micros(1024)).contains(&p99),
            "{p99:?}"
        );
    }

    #[test]
    fn histogram_edges() {
        let mut h = AckHistogram::default();
        assert_eq!(h.quantile(0.5), Duration::ZERO, "empty");
        h.record(Duration::ZERO);
        assert!(h.quantile(1.0) <= Duration::from_micros(2));
        h.record(Duration::MAX);
        assert!(
            h.quantile(1.0) >= Duration::from_secs(1 << 40),
            "the top bucket"
        );
        // Every value lands within 6.25 % below the upper edge of its sub-bucket.
        for us in [1u64, 17, 255, 256, 1_000_003, 123_456_789] {
            let mut h = AckHistogram::default();
            h.record(Duration::from_micros(us));
            let edge = h.quantile(0.5).as_secs_f64() * 1e6;
            assert!(
                edge > us as f64 && edge <= us as f64 * 1.0625 + 1.0,
                "{us}: {edge}"
            );
        }
    }

    #[test]
    fn ratios_are_zero_without_puts() {
        let s = StatsCell::default();
        s.record_ack(Duration::from_millis(3));
        let snap = s.snapshot();
        assert_eq!(snap.log_puts, 0);
        assert_eq!(snap.commits_per_log_put, 0.0);
        assert_eq!(snap.bytes_per_log_put, 0.0);
        assert_eq!(snap.commits_acked, 1);
    }

    #[test]
    fn puts_count_only_records_with_commits() {
        let s = StatsCell::default();
        s.record_put(3, 300);
        s.record_put(1, 100);
        s.record_put(0, 50);
        let snap = s.snapshot();
        assert_eq!(
            (snap.log_puts, snap.commits_uploaded, snap.log_put_bytes),
            (2, 4, 400)
        );
        assert_eq!(snap.commits_per_log_put, 2.0);
        assert_eq!(snap.bytes_per_log_put, 200.0);
        assert_eq!(snap.commits_acked, 0);
    }

    #[test]
    fn snapshot_quantiles_are_ordered() {
        let s = StatsCell::default();
        for ms in 1..=100 {
            s.record_ack(Duration::from_millis(ms));
        }
        let snap = s.snapshot();
        assert_eq!(snap.commits_acked, 100);
        assert!(snap.ack_wait_p50 <= snap.ack_wait_p95);
        assert!(snap.ack_wait_p95 <= snap.ack_wait_p99);
        assert!(snap.ack_wait_p99 >= Duration::from_millis(99));
    }
}
