//! The sync pass: list the generation's log after the last record seen, fetch the new records
//! in an unbroken seq run, rebuild the WAL image, apply each committed transaction, then look
//! at the head once. Read-only against the bucket: GET, stat (HEAD) and LIST only.
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    config::JournalMode,
    error::{Error, Result},
    format::{Head, Keys, LogFile, LogRecord},
    replica::{
        apply::{is_conflict, Applier, ApplyOutcome, CheckpointResult},
        fence::FenceRule,
        image::{Frame, WalImage},
        position::{Position, Stage},
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

/// The `reason` of the `replica stopped` event and log line, one string per `StopReason`. Change
/// them here only.
pub(crate) mod reason {
    pub(crate) const CONFLICT: &str = "conflict";
    pub(crate) const MVCC: &str = "mvcc";
    pub(crate) const INTEGRITY: &str = "integrity";
    pub(crate) const CORRUPT: &str = "corrupt";
    /// The `reason` of the `replica fell behind, restoring again` WARN (`Reason::Trimmed` and
    /// `Reason::Gap`; a `Conflict` uses `CONFLICT`).
    pub(crate) const TRIMMED: &str = "trimmed";
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
    /// A record or the head is malformed in a way that is not an integrity failure (bad
    /// encoding, ops that cannot be applied to the image).
    Corrupt(String),
}

impl StopReason {
    /// The reason string for `replica stopped` (see `reason`).
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            StopReason::Conflict => reason::CONFLICT,
            StopReason::Mvcc => reason::MVCC,
            StopReason::Integrity(_) => reason::INTEGRITY,
            StopReason::Corrupt(_) => reason::CORRUPT,
        }
    }
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
    moved: bool,
    deferred: bool,
    detail: Option<String>,
}

/// What the head said, once per pass.
struct HeadView {
    generation: u64,
    mvcc: bool,
}

enum Flow {
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
    /// The last admitted record of the generation was a seal-start (an empty Extents record).
    sealed: bool,
    /// Where the position file lives; `None` writes none (the pure-pass tests).
    dir: Option<PathBuf>,
    replica_id: String,
    /// Passes a checkpoint may stay `Busy` before the follower carries on without it.
    /// How long a seal's checkpoint may stay `Busy` before the follower carries on without it
    /// (two slow-poll intervals, worked out by the caller).
    busy_budget: Duration,
    ckpt: Option<CkptTask>,
    /// Tests move the follower's clock forward.
    #[cfg(test)]
    clock_skew: Duration,
    /// The `-k` the position file last named for a deferred checkpoint (to skip rewrites).
    last_pending_k: Option<u64>,
    #[cfg(test)]
    hook: Option<StepHook>,
}

#[cfg(test)]
type StepHook = Box<dyn FnMut(Step) -> std::result::Result<(), String> + Send>;

/// A checkpoint the position file says is pending.
struct CkptTask {
    new_offset: i64,
    /// Busy attempts so far (counted in passes, for the detail text).
    passes: u32,
    /// When the first `Busy` came; the budget runs from it.
    first_busy: Option<Instant>,
    /// A deferred checkpoint of frames already applied: a `Busy` is silent, no budget.
    deferred: bool,
}

/// The points between position-file writes and checkpoint stages where a test can abort the
/// follower (returning `Err` from the hook) to emulate `kill -9`.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// About to write `ckpt_pending`.
    BeforeCkptPending,
    /// `ckpt_pending` is on disk; the checkpoint has not run.
    AfterCkptPending,
    /// The checkpoint finished; the `applying` write has not happened.
    AfterCheckpoint,
    /// `applying` is on disk.
    AfterApplying,
}

/// True for the error the library's integrity check returns (`integrity: <key>`).
fn is_integrity(e: &Error) -> bool {
    matches!(e, Error::Corrupt(m) if m.starts_with("integrity: "))
}

/// Which outcome a failed decode of the object at `key` is.
fn decode_failure(e: &Error, key: &str) -> Outcome {
    match e {
        _ if is_integrity(e) => {
            tracing::error!(target: "buckwal", key, error = %e, "replica: object failed its integrity check");
            Outcome::Stopped(StopReason::Integrity(key.to_string()))
        }
        Error::Corrupt(m) => {
            tracing::error!(target: "buckwal", key, error = %e, "replica: malformed object");
            Outcome::Stopped(StopReason::Corrupt(m.clone()))
        }
        _ => Outcome::Failed(e.to_string()),
    }
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
            sealed: false,
            dir: None,
            replica_id: String::new(),
            busy_budget: Duration::ZERO,
            #[cfg(test)]
            clock_skew: Duration::ZERO,
            ckpt: None,
            last_pending_k: None,
            #[cfg(test)]
            hook: None,
        }
    }

    /// Write the position file in `dir`; a seal's checkpoint may stay `Busy` for `busy_budget`
    /// (two slow-poll intervals, worked out by the caller), however many passes that takes.
    pub(crate) fn with_position(
        mut self,
        dir: PathBuf,
        replica_id: String,
        busy_budget: Duration,
    ) -> Follower {
        self.dir = Some(dir);
        self.replica_id = replica_id;
        self.busy_budget = busy_budget;
        self
    }

    /// Resume from the position file in `dir`. A missing file cannot resume (the caller
    /// re-restores).
    pub(crate) fn resume(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        applier: Applier,
        page_size: usize,
        dir: PathBuf,
        busy_budget: Duration,
    ) -> Result<Follower> {
        let Some(pos) = Position::read(&dir)? else {
            return Err(Error::Corrupt("replica position file is missing".into()));
        };
        let (generation, offset, rerun) = pos.resume(applier.local_wal_empty()?);
        let finished = pos.stage == Stage::CkptPending && !rerun;
        let new_offset = pos.new_offset;
        let mut f = Follower::new(store, keys, retry, applier, page_size, generation, offset)
            .with_position(dir, pos.replica_id, busy_budget);
        if rerun {
            f.ckpt = Some(CkptTask {
                new_offset,
                passes: 0,
                first_busy: None,
                deferred: false,
            });
        } else if finished {
            // The checkpoint finished before the kill; record it.
            f.write_pos(generation, Stage::Applying, offset, offset)?;
        }
        Ok(f)
    }

    #[cfg(test)]
    pub(crate) fn set_step_hook(
        &mut self,
        f: impl FnMut(Step) -> std::result::Result<(), String> + Send + 'static,
    ) {
        self.hook = Some(Box::new(f));
    }

    fn now(&self) -> Instant {
        #[cfg(test)]
        return Instant::now() + self.clock_skew;
        #[cfg(not(test))]
        Instant::now()
    }

    #[cfg(test)]
    pub(crate) fn advance_clock(&mut self, by: Duration) {
        self.clock_skew += by;
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    #[cfg(test)]
    /// Frames of the generation applied (or already in `app.db`).
    pub(crate) fn applied_frames(&self) -> u64 {
        self.applied
    }

    pub(crate) fn replica_id(&self) -> &str {
        &self.replica_id
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
        self.sealed = false;
    }

    pub(crate) fn pass(&mut self, trigger: Trigger) -> PassResult {
        let mut t = Tally::default();
        tracing::debug!(target: "buckwal", ?trigger, generation = self.generation, "replica pass");
        let outcome = self.drive(&mut t);
        PassResult {
            outcome,
            objects: t.objects,
            lists: t.lists,
            gets: t.gets,
            found_new: t.found_new || t.moved,
            skipped: t.skipped,
            lag_ms: None,
            detail: t.detail,
        }
    }

    #[cfg(test)]
    fn run_once_for_test(&mut self) -> PassResult {
        let mut t = Tally::default();
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

    #[cfg(test)]
    fn step(&mut self, s: Step) -> std::result::Result<(), String> {
        match self.hook.as_mut() {
            Some(h) => h(s),
            None => Ok(()),
        }
    }

    fn write_pos(&self, generation: u64, stage: Stage, offset: i64, new_offset: i64) -> Result<()> {
        let Some(dir) = &self.dir else { return Ok(()) };
        Position {
            generation,
            stage,
            offset,
            new_offset,
            replica_id: self.replica_id.clone(),
        }
        .write(dir)
    }

    /// One pass: finish a pending checkpoint, follow the log, finish a generation the head has
    /// moved past (any number in a row), try a deferred checkpoint.
    fn drive(&mut self, t: &mut Tally) -> Outcome {
        if let Some(o) = self.run_ckpt_task(t) {
            return o;
        }
        let mut moves = 0;
        let outcome = loop {
            let o = self.run(t);
            let Outcome::GenerationMoved { head_generation } = o else {
                break o;
            };
            if let Some(o) = self.finish_generation(head_generation, t) {
                return o;
            }
            t.moved = true;
            moves += 1;
            if moves >= 16 {
                let msg = format!(
                    "the head moved {moves} times in one pass; stopped at generation {}",
                    self.generation
                );
                tracing::warn!(target: "buckwal", "replica: {msg}");
                t.detail = Some(msg.clone());
                return Outcome::Failed(msg);
            }
        };
        if !matches!(outcome, Outcome::Applied | Outcome::Nothing) {
            return outcome;
        }
        if !t.deferred {
            if let Some(o) = self.try_deferred(t) {
                return o;
            }
        }
        match outcome {
            _ if t.deferred => Outcome::CheckpointDeferred,
            Outcome::Nothing if t.moved => Outcome::Applied,
            o => o,
        }
    }

    /// The head names `head_generation` > ours. Apply what is left of our log, then move: write
    /// `ckpt_pending`, checkpoint, write `applying`. `Some` ends the pass.
    fn finish_generation(&mut self, head_generation: u64, t: &mut Tally) -> Option<Outcome> {
        // The head GET followed the LIST, so a last record may have landed between them.
        let hole = match self.read_log(t) {
            Ok(h) => h,
            Err(o) => return Some(o),
        };
        // The sealing writer's seal-start (an empty record) is the last admitted record of a
        // sealed generation; without it the tail was trimmed. See the report for the limit.
        if hole || !self.sealed {
            t.detail = Some(format!(
                "log/{} {} and the head is at generation {head_generation}",
                self.generation,
                if hole {
                    "has a hole"
                } else {
                    "does not end in a seal-start record (trimmed)"
                }
            ));
            return Some(Outcome::ReRestore(Reason::Trimmed));
        }
        let last = match self.applier.local_last_frame() {
            Ok(l) => l as i64,
            Err(e) => return Some(failed(t, e)),
        };
        #[cfg(test)]
        if let Err(m) = self.step(Step::BeforeCkptPending) {
            return Some(failed_msg(t, m));
        }
        if let Err(e) = self.write_pos(head_generation, Stage::CkptPending, last, 0) {
            return Some(failed(t, e));
        }
        #[cfg(test)]
        if let Err(m) = self.step(Step::AfterCkptPending) {
            return Some(failed_msg(t, m));
        }
        self.reset(head_generation, last);
        self.last_pending_k = None;
        self.ckpt = Some(CkptTask {
            new_offset: 0,
            passes: 0,
            first_busy: None,
            deferred: false,
        });
        self.run_ckpt_task(t)
    }

    /// Run the pending checkpoint. `Some` ends the pass (Busy within the budget, or an error).
    fn run_ckpt_task(&mut self, t: &mut Tally) -> Option<Outcome> {
        let now = self.now();
        let budget = self.busy_budget;
        let task = self.ckpt.as_mut()?;
        let (new_offset, deferred) = (task.new_offset, task.deferred);
        match self.applier.checkpoint_truncate() {
            Err(e) => Some(failed(t, e)),
            Ok(CheckpointResult::Done) => {
                #[cfg(test)]
                if let Err(m) = self.step(Step::AfterCheckpoint) {
                    return Some(failed_msg(t, m));
                }
                if let Err(e) =
                    self.write_pos(self.generation, Stage::Applying, new_offset, new_offset)
                {
                    return Some(failed(t, e));
                }
                self.offset = new_offset;
                self.ckpt = None;
                self.last_pending_k = None;
                #[cfg(test)]
                if let Err(m) = self.step(Step::AfterApplying) {
                    return Some(failed_msg(t, m));
                }
                None
            }
            Ok(CheckpointResult::Busy) if deferred => {
                // The position file stays `ckpt_pending` with the old offset, which is right
                // until the checkpoint lands.
                self.ckpt = None;
                None
            }
            Ok(CheckpointResult::Busy) => {
                task.passes += 1;
                let first = *task.first_busy.get_or_insert(now);
                let waited = now.saturating_duration_since(first);
                if waited < budget {
                    t.detail = Some(format!(
                        "checkpoint busy: readers hold the local WAL (pass {}, {} ms of {} ms)",
                        task.passes,
                        waited.as_millis(),
                        budget.as_millis()
                    ));
                    return Some(if t.applied_tx > 0 || t.moved {
                        Outcome::Applied
                    } else {
                        Outcome::Nothing
                    });
                }
                // Readers keep it: number on past the local last frame and try again later.
                tracing::warn!(target: "buckwal", generation = self.generation, offset = self.offset, "replica: checkpoint deferred, readers hold the local WAL");
                if let Err(e) =
                    self.write_pos(self.generation, Stage::Applying, self.offset, self.offset)
                {
                    return Some(failed(t, e));
                }
                self.ckpt = None;
                t.deferred = true;
                #[cfg(test)]
                if let Err(m) = self.step(Step::AfterApplying) {
                    return Some(failed_msg(t, m));
                }
                None
            }
        }
    }

    /// A later pass, after a checkpoint was given up: `k` frames of this generation are
    /// applied, so write `ckpt_pending` (old offset, new offset `-k`) and checkpoint.
    fn try_deferred(&mut self, t: &mut Tally) -> Option<Outcome> {
        if self.dir.is_none() || self.ckpt.is_some() || self.offset <= 0 {
            return None;
        }
        let k = self.applied;
        // k == 0 would write new offset 0, which resume reads as a seal's checkpoint; wait for a
        // frame of this generation.
        if k == 0 {
            return None;
        }
        let new_offset = -(k as i64);
        if self.last_pending_k != Some(k) {
            #[cfg(test)]
            if let Err(m) = self.step(Step::BeforeCkptPending) {
                return Some(failed_msg(t, m));
            }
            if let Err(e) =
                self.write_pos(self.generation, Stage::CkptPending, self.offset, new_offset)
            {
                return Some(failed(t, e));
            }
            self.last_pending_k = Some(k);
            #[cfg(test)]
            if let Err(m) = self.step(Step::AfterCkptPending) {
                return Some(failed_msg(t, m));
            }
        }
        self.ckpt = Some(CkptTask {
            new_offset,
            passes: 0,
            first_busy: None,
            deferred: true,
        });
        self.run_ckpt_task(t)
    }

    fn run(&mut self, t: &mut Tally) -> Outcome {
        let hole = match self.read_log(t) {
            Ok(h) => h,
            Err(o) => return o,
        };
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

    /// Read the generation's log from after `last_seq` as an unbroken run and apply what is
    /// committed. `Ok(hole)`: a seq is missing or its object is gone.
    fn read_log(&mut self, t: &mut Tally) -> std::result::Result<bool, Outcome> {
        let mut rereads = 0;
        let mut hole;
        // A previous pass may have stopped between fetching a record and applying it.
        match self.apply_ready(t) {
            Flow::Go => {}
            Flow::Done(o) => return Err(o),
            Flow::Reread => {
                self.restart_log();
                rereads += 1;
            }
        }
        'reread: loop {
            let prefix = self.keys.log_prefix(self.generation);
            // Log keys are zero-padded, so key order is seq order: list only after the last
            // record seen (from seq 1, after the prefix itself).
            let start_after = if self.last_seq == 0 {
                prefix.clone()
            } else {
                self.keys.log(self.generation, self.last_seq)
            };
            t.lists += 1;
            let listed = match with_retry(&self.retry, &prefix, |_| {
                self.store.list_after(&prefix, &start_after)
            }) {
                Ok(l) => l,
                Err(e) => return Err(failed(t, e)),
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
                    Err(e) => return Err(failed(t, e)),
                };
                t.objects += 1;
                let rec = match self.keys.decode_record(&body, self.generation, s) {
                    Ok(r) => r,
                    Err(e) => {
                        t.detail = Some(e.to_string());
                        return Err(decode_failure(&e, &key));
                    }
                };
                if let Some(o) = self.take_record(rec, &key, s, t) {
                    return Err(o);
                }
                self.last_seq = s;
                t.found_new = true;
                match self.apply_ready(t) {
                    Flow::Go => {}
                    Flow::Done(o) => return Err(o),
                    Flow::Reread => {
                        if rereads > 0 {
                            t.detail = Some(format!(
                                "generation {}: gap in local frame numbering after re-reading \
                                 from seq 1",
                                self.generation
                            ));
                            return Err(Outcome::ReRestore(Reason::Gap));
                        }
                        rereads += 1;
                        self.restart_log();
                        continue 'reread;
                    }
                }
            }
            break;
        }
        Ok(hole)
    }

    /// Fold one record into the image. `Some` stops the pass.
    fn take_record(
        &mut self,
        rec: LogRecord,
        key: &str,
        seq: u64,
        t: &mut Tally,
    ) -> Option<Outcome> {
        match rec {
            LogRecord::Fence { writer_id, epoch } => {
                // A stale fence changes nothing and is not counted.
                if self.fence.on_fence(&writer_id, epoch) {
                    self.sealed = false;
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
                    // Empty records (seal-start) carry nothing to skip.
                    if syncs.iter().any(|ops| !ops.is_empty()) {
                        t.skipped += 1;
                        tracing::info!(
                            target: "buckwal",
                            generation = self.generation,
                            seq,
                            writer_id = %writer_id,
                            "fenced writer's record skipped"
                        );
                    }
                    return None;
                }
                // An empty record is the writer's seal-start: the log's end once a head moves.
                self.sealed = syncs.iter().all(|ops| ops.is_empty());
                for ops in &syncs {
                    if let Err(e) = self.image.apply_sync(ops) {
                        t.detail = Some(e.to_string());
                        return Some(Outcome::Stopped(StopReason::Corrupt(e.to_string())));
                    }
                }
                None
            }
        }
    }

    /// Apply the committed transactions the image holds beyond `applied`, one at a time.
    fn apply_ready(&mut self, t: &mut Tally) -> Flow {
        if self.image.committed_len() < self.applied {
            let msg = format!(
                "replica conflict: generation {} lost committed frames (image {} < applied {})",
                self.generation,
                self.image.committed_len(),
                self.applied
            );
            tracing::error!(target: "buckwal", "{msg}");
            t.detail = Some(msg);
            return Flow::Done(Outcome::Stopped(StopReason::Conflict));
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
                Ok(ApplyOutcome::Gap) => return Flow::Reread,
                Ok(ApplyOutcome::Incomplete) => return Flow::Go,
                Err(e) if is_conflict(&e) => {
                    tracing::error!(target: "buckwal", generation = self.generation, error = %e, "replica conflict; the replica must re-restore");
                    t.detail = Some(e.to_string());
                    return Flow::Done(Outcome::Stopped(StopReason::Conflict));
                }
                Err(e) => return Flow::Done(failed(t, e)),
            }
        }
        Flow::Go
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
                return Err(decode_failure(&e, &key));
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

#[cfg(test)]
fn failed_msg(t: &mut Tally, msg: String) -> Outcome {
    t.detail = Some(msg.clone());
    Outcome::Failed(msg)
}

fn failed(t: &mut Tally, e: Error) -> Outcome {
    let msg = e.to_string();
    t.detail = Some(msg.clone());
    Outcome::Failed(msg)
}

#[cfg(test)]
mod tests;
