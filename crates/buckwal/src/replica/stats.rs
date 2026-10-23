//! What `Replica::stats` reports.
use std::time::SystemTime;

/// A snapshot of a replica's progress and cost. The pass and object counters run for the life
/// of the replica, across re-restores.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicaStats {
    /// The generation being followed.
    pub generation: u64,
    /// The last log record of that generation taken in (0 before any).
    pub seq: u64,
    /// When the last successful pass finished.
    pub last_pass_at: Option<SystemTime>,
    /// The last measured lag in milliseconds: now minus the `Last-Modified` of the newest
    /// applied log object, by the bucket's clock (`ObjectStore::stat`, whose resolution is 1 s).
    /// A pass that finds nothing new sets 0: the replica is as current as the bucket. `None`
    /// until a pass has measured it.
    pub lag_ms: Option<u64>,
    /// Passes started by a bucket notification (`ReplicaWaker::wake`).
    pub passes_notification: u64,
    /// Passes started by the slow poll timer. The first pass at start is in neither count.
    pub passes_slow_poll: u64,
    /// Calls of `ReplicaWaker::wake`.
    pub notifications_received: u64,
    /// Notification passes that took in nothing new.
    pub notification_passes_nothing_new: u64,
    /// Log objects fetched.
    pub objects_fetched: u64,
    /// Checkpoints given up while readers held the local WAL, to be tried again later.
    pub deferred_checkpoints: u64,
    /// Log records skipped by the fence rule.
    pub fenced_records_skipped: u64,
    pub re_restores: u64,
    /// When the last re-restore happened, and why.
    pub last_re_restore_at: Option<(SystemTime, String)>,
}
