//! The sync pass: list the generation's log after the last record seen, fetch the new records
//! in an unbroken seq run, rebuild the WAL image, apply each committed transaction, then look
//! at the head once. Read-only against the bucket: GET, stat (HEAD) and LIST only.
use std::sync::Arc;

use crate::{
    config::JournalMode,
    error::Error,
    format::{Head, Keys, LogFile, LogRecord},
    replica::{
        apply::{is_conflict, Applier, ApplyOutcome},
        fence::FenceRule,
        image::{Frame, WalImage},
    },
    store::{with_retry, ObjectStore, RetryPolicy},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Trigger {
    Start,
    Notification,
    SlowPoll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Reason {
    /// A log object the follower needs is gone (trimmed) or missing after the head moved on.
    Trimmed,
    /// The local state and the log disagree (a frame differs).
    Conflict,
    /// Re-reading the generation from seq 1 still left a gap in the local numbering.
    Gap,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StopReason {
    /// A frame already in the local WAL differs from the log's. The pass logged an ERROR line
    /// naming the generation and frame; the replica then re-restores (`Outcome::re_restore`).
    Conflict,
    /// A kind-3 (MVCC) record, or a head that says MVCC.
    Mvcc,
    /// A record or the head failed its integrity check; carries the object key.
    Integrity(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Outcome {
    Applied,
    Nothing,
    /// Never returned by the pass itself: for the checkpoint stages (Task 8).
    CheckpointDeferred,
    ReRestore(Reason),
    Stopped(StopReason),
    /// The head names a later generation. The pass has already applied every record it could
    /// list of this one; the caller finishes the generation (re-list, checkpoint, move on).
    GenerationMoved {
        head_generation: u64,
    },
    /// A request failed after its retries (or a local engine error). Nothing is lost; the next
    /// pass retries.
    Failed(String),
}

impl Outcome {
    /// The reason to re-restore, for a `ReRestore` and for `Stopped(Conflict)` (the spec turns
    /// a conflict into an ERROR line, then a re-restore).
    pub(crate) fn re_restore(&self) -> Option<Reason> {
        match self {
            Outcome::ReRestore(r) => Some(r.clone()),
            Outcome::Stopped(StopReason::Conflict) => Some(Reason::Conflict),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PassResult {
    pub outcome: Outcome,
    /// Log objects fetched.
    pub objects: u64,
    pub lists: u64,
    /// GET and HEAD requests: log objects plus the head check.
    pub gets: u64,
    /// New log records (any kind) were taken in.
    pub found_new: bool,
    /// Records skipped by the fence rule.
    pub skipped: u64,
    /// Not measured yet.
    pub lag_ms: Option<u64>,
    /// The error text behind a `Stopped`, `ReRestore` or `Failed`.
    pub detail: Option<String>,
}

#[derive(Default)]
struct Tally {
    objects: u64,
    lists: u64,
    gets: u64,
    found_new: bool,
    skipped: u64,
    applied_tx: u64,
    detail: Option<String>,
}

/// What the head said, once per pass.
struct HeadView {
    generation: u64,
    mvcc: bool,
}

enum Step {
    Go,
    Done(Outcome),
    /// The local numbering has a gap: re-read the generation from seq 1.
    Reread,
}

pub(crate) struct Follower {
    store: Arc<dyn ObjectStore>,
    keys: Keys,
    retry: RetryPolicy,
    applier: Applier,
    page_size: usize,
    generation: u64,
    /// Local frame number = generation frame number + offset.
    offset: i64,
    last_seq: u64,
    image: WalImage,
    /// Frames of the generation already applied (or already in `app.db`).
    applied: u64,
    head_etag: Option<String>,
    head_view: Option<(u64, bool)>,
    fence: FenceRule,
}

fn integrity_key(e: &Error, key: &str) -> Option<String> {
    matches!(e, Error::Corrupt(_)).then(|| key.to_string())
}

impl Follower {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        applier: Applier,
        page_size: usize,
        generation: u64,
        offset: i64,
    ) -> Follower {
        applier.set_generation(generation);
        Follower {
            store,
            keys,
            retry,
            applier,
            page_size,
            generation,
            offset,
            last_seq: 0,
            image: WalImage::new(page_size),
            applied: 0,
            head_etag: None,
            head_view: None,
            fence: FenceRule::default(),
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn last_seq(&self) -> u64 {
        self.last_seq
    }

    /// Follow `generation` from seq 1 with the given offset (the caller has written the
    /// position file).
    pub(crate) fn reset(&mut self, generation: u64, offset: i64) {
        self.generation = generation;
        self.offset = offset;
        self.applier.set_generation(generation);
        self.restart_log();
    }

    fn restart_log(&mut self) {
        self.last_seq = 0;
        self.image = WalImage::new(self.page_size);
        self.applied = 0;
        self.fence = FenceRule::default();
    }

    pub(crate) fn pass(&mut self, trigger: Trigger) -> PassResult {
        let mut t = Tally::default();
        tracing::debug!(target: "buckwal", ?trigger, generation = self.generation, "replica pass");
        let outcome = self.run(&mut t);
        PassResult {
            outcome,
            objects: t.objects,
            lists: t.lists,
            gets: t.gets,
            found_new: t.found_new,
            skipped: t.skipped,
            lag_ms: None,
            detail: t.detail,
        }
    }

    fn run(&mut self, t: &mut Tally) -> Outcome {
        let mut rereads = 0;
        let mut hole;
        // A previous pass may have stopped between fetching a record and applying it.
        match self.apply_ready(t) {
            Step::Go => {}
            Step::Done(o) => return o,
            Step::Reread => {
                self.restart_log();
                rereads += 1;
            }
        }
        'reread: loop {
            let prefix = self.keys.log_prefix(self.generation);
            t.lists += 1;
            let listed = match with_retry(&self.retry, &prefix, |_| self.store.list(&prefix)) {
                Ok(l) => l,
                Err(e) => return failed(t, e),
            };
            let mut seqs: Vec<u64> = listed
                .iter()
                .filter_map(|k| self.keys.parse_log(k))
                .filter(|(g, s)| *g == self.generation && *s > self.last_seq)
                .map(|(_, s)| s)
                .collect();
            seqs.sort_unstable();
            hole = false;
            for s in seqs {
                if s != self.last_seq + 1 {
                    hole = true;
                    break;
                }
                let key = self.keys.log(self.generation, s);
                t.gets += 1;
                let body = match with_retry(&self.retry, &key, |_| self.store.get(&key)) {
                    Ok(Some(o)) => o.body,
                    // Listed, then gone: trimmed (or not visible yet); decided with the head.
                    Ok(None) => {
                        hole = true;
                        break;
                    }
                    Err(e) => return failed(t, e),
                };
                t.objects += 1;
                let rec = match self.keys.decode_record(&body, self.generation, s) {
                    Ok(r) => r,
                    Err(e) => {
                        let k = integrity_key(&e, &key).unwrap_or(key);
                        tracing::error!(target: "buckwal", key = %k, error = %e, "replica: record failed its check");
                        t.detail = Some(e.to_string());
                        return Outcome::Stopped(StopReason::Integrity(k));
                    }
                };
                if let Some(o) = self.take_record(rec, &key, t) {
                    return o;
                }
                self.last_seq = s;
                t.found_new = true;
                match self.apply_ready(t) {
                    Step::Go => {}
                    Step::Done(o) => return o,
                    Step::Reread => {
                        if rereads > 0 {
                            t.detail = Some(format!(
                                "generation {}: gap in local frame numbering after re-reading \
                                 from seq 1",
                                self.generation
                            ));
                            return Outcome::ReRestore(Reason::Gap);
                        }
                        rereads += 1;
                        self.restart_log();
                        continue 'reread;
                    }
                }
            }
            break;
        }
        let head = match self.check_head(t) {
            Ok(h) => h,
            Err(o) => return o,
        };
        if head.mvcc {
            tracing::error!(target: "buckwal", "replica: the head says MVCC; replicas do not follow it");
            return Outcome::Stopped(StopReason::Mvcc);
        }
        let moved = head.generation > self.generation;
        if hole && moved {
            t.detail = Some(format!(
                "log/{} has a hole or a missing object after seq {} and the head is at \
                 generation {}",
                self.generation, self.last_seq, head.generation
            ));
            return Outcome::ReRestore(Reason::Trimmed);
        }
        if moved {
            return Outcome::GenerationMoved {
                head_generation: head.generation,
            };
        }
        if t.applied_tx > 0 {
            Outcome::Applied
        } else {
            Outcome::Nothing
        }
    }

    /// Fold one record into the image. `Some` stops the pass.
    fn take_record(&mut self, rec: LogRecord, key: &str, t: &mut Tally) -> Option<Outcome> {
        match rec {
            LogRecord::Fence { writer_id, epoch } => {
                if !self.fence.on_fence(&writer_id, epoch) {
                    t.skipped += 1;
                }
                None
            }
            LogRecord::Extents {
                file: LogFile::Log, ..
            } => {
                tracing::error!(target: "buckwal", key, "replica: an MVCC (kind 3) record; replicas do not follow MVCC databases");
                t.detail = Some(format!("{key}: kind 3 record"));
                Some(Outcome::Stopped(StopReason::Mvcc))
            }
            LogRecord::Extents {
                writer_id, syncs, ..
            } => {
                if !self.fence.admits(&writer_id) {
                    t.skipped += 1;
                    return None;
                }
                for ops in &syncs {
                    if let Err(e) = self.image.apply_sync(ops) {
                        t.detail = Some(e.to_string());
                        return Some(Outcome::Stopped(StopReason::Integrity(key.to_string())));
                    }
                }
                None
            }
        }
    }

    /// Apply the committed transactions the image holds beyond `applied`, one at a time.
    fn apply_ready(&mut self, t: &mut Tally) -> Step {
        if self.image.committed_len() < self.applied {
            let msg = format!(
                "replica conflict: generation {} lost committed frames (image {} < applied {})",
                self.generation,
                self.image.committed_len(),
                self.applied
            );
            tracing::error!(target: "buckwal", "{msg}");
            t.detail = Some(msg);
            return Step::Done(Outcome::Stopped(StopReason::Conflict));
        }
        let frames = self.image.committed_frames_from(self.applied);
        let mut tx: Vec<Frame> = Vec::new();
        for f in frames {
            let commit = f.header[4..8] != [0; 4];
            tx.push(f);
            if !commit {
                continue;
            }
            match self.applier.apply_commit(&tx, self.offset) {
                // Advance by the transaction's own frames, not by `frames` (0 when in app.db).
                Ok(ApplyOutcome::Applied { .. }) => {
                    self.applied += tx.len() as u64;
                    t.applied_tx += 1;
                    tx.clear();
                }
                Ok(ApplyOutcome::Gap) => return Step::Reread,
                Ok(ApplyOutcome::Incomplete) => return Step::Go,
                Err(e) if is_conflict(&e) => {
                    tracing::error!(target: "buckwal", generation = self.generation, error = %e, "replica conflict; the replica must re-restore");
                    t.detail = Some(e.to_string());
                    return Step::Done(Outcome::Stopped(StopReason::Conflict));
                }
                Err(e) => return Step::Done(failed(t, e)),
            }
        }
        Step::Go
    }

    /// One head check per pass: a stat against the last ETag, and a GET only when it changed
    /// (or on the first look). The store has no conditional GET; a stat is the same one round
    /// trip, and counts as a GET.
    fn check_head(&mut self, t: &mut Tally) -> std::result::Result<HeadView, Outcome> {
        let key = self.keys.head();
        t.gets += 1;
        if let (Some(etag), Some((generation, mvcc))) = (&self.head_etag, self.head_view) {
            match with_retry(&self.retry, &key, |_| self.store.stat(&key)) {
                Ok(Some(s)) if &s.etag == etag => return Ok(HeadView { generation, mvcc }),
                Ok(_) => t.gets += 1,
                Err(e) => return Err(failed(t, e)),
            }
        }
        let obj = match with_retry(&self.retry, &key, |_| self.store.get(&key)) {
            Ok(Some(o)) => o,
            Ok(None) => {
                return Err(failed(t, Error::NotFound(key)));
            }
            Err(e) => return Err(failed(t, e)),
        };
        let head: Head = match self.keys.decode_head(&obj.body) {
            Ok(h) => h,
            Err(e) => {
                t.detail = Some(e.to_string());
                return Err(match integrity_key(&e, &key) {
                    Some(k) => {
                        tracing::error!(target: "buckwal", key = %k, error = %e, "replica: head failed its check");
                        Outcome::Stopped(StopReason::Integrity(k))
                    }
                    None => Outcome::Failed(e.to_string()),
                });
            }
        };
        let view = (head.generation, head.journal_mode == JournalMode::Mvcc);
        self.head_etag = Some(obj.etag);
        self.head_view = Some(view);
        Ok(HeadView {
            generation: view.0,
            mvcc: view.1,
        })
    }
}

fn failed(t: &mut Tally, e: Error) -> Outcome {
    let msg = e.to_string();
    t.detail = Some(msg.clone());
    Outcome::Failed(msg)
}

#[cfg(test)]
mod tests;
