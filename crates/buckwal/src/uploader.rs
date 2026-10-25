//! The background uploader: one thread that owns the [`Bucket`] and runs its jobs in FIFO
//! order, so a seal can never overtake a log object submitted before it.
//!
//! Window syncs wait in a [`Batch`] and share one log record with whatever joins them before
//! the first of their limits; an `Off` sync flushes at once, carrying the waiting ones. A
//! record that was never sent hands its syncs back (`AppendFailure::unsent`): the Window
//! ones stay owed, at the front of every later record, until they land.
//!
//! A `.db-log` sync (`submit_log_file`, record kind 3) is never batched, owed or dropped: it
//! is its own record, and ops that were never sent go back to the caller
//! (`LogFailure::unsent`), which keeps them for its next sync (the confirmed-sync rule).

use crate::batching::Batching;
use crate::bucket::{AppendFailure, Bucket, Counters, LogPos};
use crate::config::{defaults, Event, EventHook};
use crate::error::{Error, Result};
use crate::format::{Head, LogFile, Manifest, Op};
use crate::stats::StatsCell;
pub(crate) use batch::op_bytes;
use batch::{Batch, Pending};
pub(crate) use durable::Durable;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::field::Empty;
use tracing::{info_span, Span};

/// What the bucket looked like right after a seal (or at open): the trimmer's wake-up. The
/// trimmer reads the head and its ages from the bucket itself; tests read these.
#[derive(Debug, Clone)]
pub(crate) struct TrimSnapshot {
    #[cfg_attr(not(test), allow(dead_code))]
    pub head: Head,
    #[cfg_attr(not(test), allow(dead_code))]
    pub manifest: Option<Manifest>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct UploaderCounters {
    pub log_puts: u64,
    pub page_puts: u64,
    pub seals: u64,
    pub retries: u64,
}

/// The reply to one `submit_log`.
pub(crate) struct Ticket {
    /// The commit's number: 1, 2, … in submission order, per open.
    pub commit: u64,
    /// When it was submitted, for the ack wait.
    pub submitted: Instant,
    rx: Receiver<Result<LogPos>>,
}

fn stopped() -> Error {
    Error::UploadFailed("uploader stopped".into())
}

impl Ticket {
    /// Block until the log object is in the bucket (or its upload failed).
    pub(crate) fn wait(self) -> Result<LogPos> {
        self.rx.recv().unwrap_or_else(|_| Err(stopped()))
    }

    /// [`Ticket::wait`] inside a `buckwal.ack_wait` child of `commit`; `commit` is entered
    /// around it, so it is last exited after its ack wait ends.
    pub(crate) fn wait_in(self, commit: &Span) -> Result<LogPos> {
        commit.in_scope(|| {
            info_span!(target: "buckwal", parent: commit, "buckwal.ack_wait")
                .in_scope(|| self.wait())
        })
    }
}

/// Why a `submit_log_file` record is not confirmed, and whether its ops may be sent again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogFailure {
    pub error: Error,
    /// `Some(ops)`, as submitted, when no PUT of the record was attempted
    /// (`AppendFailure::unsent`): the caller sends them again. `None` when the record is in
    /// doubt at its seq (the bucket settles it before anything else is appended, so the
    /// ops must not go in another record), or when the writer is fenced (never sent again).
    pub unsent: Option<Vec<Op>>,
}

/// The reply to one `submit_log_file`.
pub(crate) struct LogTicket {
    /// Its commit number, from the same sequence as `Ticket::commit`.
    pub commit: u64,
    pub submitted: Instant,
    rx: Receiver<std::result::Result<LogPos, LogFailure>>,
}

impl LogTicket {
    /// Block until the record is in the bucket (or its upload failed).
    pub(crate) fn wait(self) -> std::result::Result<LogPos, LogFailure> {
        self.rx.recv().unwrap_or_else(|_| {
            Err(LogFailure {
                error: stopped(),
                unsent: None,
            })
        })
    }

    /// [`LogTicket::wait`] inside a `buckwal.ack_wait` child of `commit`; `commit` is entered
    /// around it, so it is last exited after its ack wait ends.
    pub(crate) fn wait_in(self, commit: &Span) -> std::result::Result<LogPos, LogFailure> {
        commit.in_scope(|| {
            info_span!(target: "buckwal", parent: commit, "buckwal.ack_wait")
                .in_scope(|| self.wait())
        })
    }
}

/// One `submit_log_file`: `ops` of `file` as one record of one sync, carrying `commits`
/// commits (for the stats).
struct FilePending {
    file: LogFile,
    ops: Vec<Op>,
    commits: u64,
    commit: u64,
    reply: Sender<std::result::Result<LogPos, LogFailure>>,
    span: Span,
}

enum Job {
    Log(Pending),
    File(FilePending),
    Settle(Sender<Result<u64>>),
    Seal {
        pages: Vec<(u32, Vec<u8>)>,
        page_size: u32,
        page_count: u32,
        reply: Sender<Result<u64>>,
        /// The caller's current span: the parent of `buckwal.checkpoint_upload`.
        parent: Span,
    },
    Switch {
        reply: Sender<Result<u64>>,
        /// The caller's current span: the parent of `buckwal.generation_switch`.
        parent: Span,
    },
}

pub(crate) struct Uploader {
    /// None once shut down.
    jobs: Mutex<Option<Sender<Job>>>,
    /// The last commit number handed out; assigned under `jobs`, so numbers follow the
    /// queue order.
    commits: AtomicU64,
    thread: Mutex<Option<JoinHandle<()>>>,
    generation: Arc<AtomicU64>,
    degraded: Arc<AtomicBool>,
    /// The last bucket operation (a log upload, a seal, or a tick's catch-up or probe)
    /// failed; cleared by the next that succeeds. While set, the thread wakes every
    /// `MAX_BACKOFF` to probe the bucket, so an idle database clears it once the bucket
    /// answers.
    last_failed: Arc<AtomicBool>,
    /// The end of the current generation's takeover quiet (`Bucket::quiet_until`, the
    /// deadline the seal waits for), `None` for none; set by the worker after every job.
    quiet_until: Arc<Mutex<Option<Instant>>>,
    stats: Arc<StatsCell>,
    /// The durable position and the background commits behind it.
    durable: Arc<Durable>,
    #[cfg_attr(not(test), allow(dead_code))]
    counters: Arc<Counters>,
}

impl Uploader {
    pub(crate) fn spawn(
        mut bucket: Bucket,
        trim_tx: Option<Sender<TrimSnapshot>>,
        events: Option<EventHook>,
        stats: Arc<StatsCell>,
    ) -> Arc<Uploader> {
        bucket.set_event_hook(events.clone());
        let generation = Arc::new(AtomicU64::new(bucket.head().generation));
        let degraded = Arc::new(AtomicBool::new(bucket.degraded()));
        let last_failed = Arc::new(AtomicBool::new(false));
        let quiet_until = Arc::new(Mutex::new(bucket.quiet_until()));
        let counters = bucket.counters();
        let (tx, rx) = mpsc::channel();
        let durable = Arc::new(Durable::default());
        let report = durable.clone();
        bucket.set_fence_report(Box::new(move || {
            report.dropped().map(|d| (d.count, d.first, d.last))
        }));
        let worker = Worker {
            durable: durable.clone(),
            bucket,
            batch: Batch::default(),
            owed: VecDeque::new(),
            generation: generation.clone(),
            degraded: degraded.clone(),
            last_failed: last_failed.clone(),
            quiet_until: quiet_until.clone(),
            stats: stats.clone(),
            events,
            trim_tx,
            doubt: None,
        };
        // The thread reports to the subscriber of the thread that opened the database.
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let thread = std::thread::Builder::new()
            .name("buckwal-uploader".into())
            .spawn(move || tracing::dispatcher::with_default(&dispatch, || worker.run(rx)))
            .expect("spawn uploader thread");
        Arc::new(Uploader {
            jobs: Mutex::new(Some(tx)),
            commits: AtomicU64::new(0),
            thread: Mutex::new(Some(thread)),
            generation,
            degraded,
            last_failed,
            quiet_until,
            stats,
            durable,
            counters,
        })
    }

    /// Queue `job`. When the uploader has stopped the job, and its reply sender, is dropped,
    /// so the waiter sees "uploader stopped".
    fn send(&self, job: Job) {
        if let Some(tx) = self.jobs.lock().unwrap().as_ref() {
            let _ = tx.send(job);
        }
    }

    /// Queue one sync's ops, committed with `batching`, and return at once. `Off` ships them
    /// (with any waiting Window or background syncs) as the next log record; a Window or
    /// background sync waits for its limits. A background sync counts as behind until its
    /// record is in the bucket.
    /// The current span is the sync's commit span.
    pub(crate) fn submit_log(&self, ops: Vec<Op>, batching: Batching) -> Ticket {
        let (reply, rx) = mpsc::channel();
        let jobs = self.jobs.lock().unwrap();
        let commit = self.commits.fetch_add(1, SeqCst) + 1;
        let submitted = Instant::now();
        if let Some(tx) = jobs.as_ref() {
            if matches!(batching, Batching::Background { .. }) {
                self.durable.add_behind(commit, submitted);
            }
            let _ = tx.send(Job::Log(Pending {
                ops,
                batching,
                commit,
                reply,
                // A queued background commit holds no handle on its span: the span closes
                // when its call returns, and its upload is a root span.
                span: if matches!(batching, Batching::Background { .. }) {
                    Span::none()
                } else {
                    Span::current()
                },
            }));
        }
        Ticket {
            commit,
            submitted,
            rx,
        }
    }

    /// Queue `ops` of `file` as one record of its own (never batched with WAL syncs) and
    /// return at once; it carries `commits` commits, as the stats count them. A record that
    /// was never sent hands its ops back (`LogFailure::unsent`) instead of being dropped or
    /// owed. The current span is its commit span.
    pub(crate) fn submit_log_file(&self, file: LogFile, ops: Vec<Op>, commits: u64) -> LogTicket {
        let (reply, rx) = mpsc::channel();
        let jobs = self.jobs.lock().unwrap();
        let commit = self.commits.fetch_add(1, SeqCst) + 1;
        let submitted = Instant::now();
        if let Some(tx) = jobs.as_ref() {
            let _ = tx.send(Job::File(FilePending {
                file,
                ops,
                commits,
                commit,
                reply,
                span: Span::current(),
            }));
        }
        LogTicket {
            commit,
            submitted,
            rx,
        }
    }

    /// Settle what the bucket owes, behind every job already queued, and wait
    /// (`Bucket::settle_all`): a record in doubt is settled at its seq, and a seal whose head
    /// is published is adopted (the generation moves on). Ok only when nothing is left to
    /// settle; returns the current generation.
    pub(crate) fn settle_log(&self) -> Result<u64> {
        let (reply, rx) = mpsc::channel();
        self.send(Job::Settle(reply));
        rx.recv().unwrap_or_else(|_| Err(stopped()))
    }

    /// Queue a seal behind every log object already submitted and wait for it; returns the
    /// new generation. Waiting and owed syncs are flushed first; the seal fails if owed ones
    /// remain.
    pub(crate) fn seal_generation(
        &self,
        pages: Vec<(u32, Vec<u8>)>,
        page_size: u32,
        page_count: u32,
    ) -> Result<u64> {
        let (reply, rx) = mpsc::channel();
        self.send(Job::Seal {
            pages,
            page_size,
            page_count,
            reply,
            parent: Span::current(),
        });
        rx.recv().unwrap_or_else(|_| Err(stopped()))
    }

    /// Queue a generation switch (format 5, `Bucket::switch_generation`) behind every log
    /// object already submitted and wait for it; returns the new generation. Waiting and owed
    /// syncs are flushed first; the switch fails if owed ones remain.
    pub(crate) fn switch_generation(&self) -> Result<u64> {
        let (reply, rx) = mpsc::channel();
        self.send(Job::Switch {
            reply,
            parent: Span::current(),
        });
        rx.recv().unwrap_or_else(|_| Err(stopped()))
    }

    /// The current generation, without waiting for queued jobs.
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(SeqCst)
    }

    /// Why the bucket looks unable to take a seal now, if it does: the uploader is degraded,
    /// or its last bucket operation failed. Maintenance does not start a run then.
    pub(crate) fn bucket_failing(&self) -> Option<&'static str> {
        if self.degraded() {
            Some("bucket degraded")
        } else if self.last_failed.load(SeqCst) {
            Some("last bucket operation failed")
        } else {
            None
        }
    }

    /// How long the current generation's takeover quiet still lasts (`Bucket::
    /// set_seal_not_before`): a seal started now would wait that long first, with the
    /// uploader thread held. Zero when there is none. Maintenance does not start a run then.
    /// It reads the same monotonic deadline the seal waits for, so a wall clock step changes
    /// neither.
    pub(crate) fn seal_quiet_remaining(&self) -> Duration {
        crate::db::lock(&self.quiet_until).map_or(Duration::ZERO, |until| {
            until.saturating_duration_since(Instant::now())
        })
    }

    /// The bucket has something to settle (fenced, a record in doubt, a seal's head write
    /// unresolved) or Window syncs are owed. Updated after every job, before its replies.
    pub(crate) fn degraded(&self) -> bool {
        self.degraded.load(SeqCst)
    }

    pub(crate) fn stats(&self) -> Arc<StatsCell> {
        self.stats.clone()
    }

    /// The highest commit number whose record is in the bucket (0 before the first).
    pub(crate) fn durable_commit(&self) -> u64 {
        self.durable.durable()
    }

    /// Block until the commit is durable ([`Durable::wait_durable`]).
    pub(crate) fn wait_durable(&self, commit: u64, timeout: Duration) -> Result<()> {
        self.durable.wait_durable(commit, timeout)
    }

    /// Block until at most `max_behind` background commits wait for the bucket.
    pub(crate) fn wait_behind(&self, max_behind: u32) -> Result<()> {
        self.durable.wait_behind(max_behind)
    }

    /// Background commits waiting for the bucket, and how long ago the oldest returned.
    pub(crate) fn behind(&self) -> (u64, Duration) {
        self.durable.behind()
    }

    #[cfg(test)]
    pub(crate) fn counters(&self) -> UploaderCounters {
        use std::sync::atomic::Ordering::Relaxed;
        let c = &self.counters;
        UploaderCounters {
            log_puts: c.log_puts.load(Relaxed),
            page_puts: c.page_puts.load(Relaxed),
            seals: c.seals.load(Relaxed),
            retries: c.retries.load(Relaxed),
        }
    }

    /// Run every queued job, flush the waiting and owed syncs, then stop the thread (joined,
    /// unless called on it). Later submissions fail.
    pub(crate) fn shutdown(&self) {
        // Dropping the sender ends the thread's receive loop once the queue is empty.
        drop(self.jobs.lock().unwrap().take());
        let Some(t) = self.thread.lock().unwrap().take() else {
            return;
        };
        // The last handle can be dropped on the uploader thread itself (say, by an event
        // hook). It cannot join itself; it exits by itself once the queue drains.
        if t.thread().id() == std::thread::current().id() {
            return;
        }
        // A panic on the uploader thread already failed its waiting tickets.
        let _ = t.join();
    }
}

impl Drop for Uploader {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Tells the waiters that the uploader thread has ended.
struct StopOnDrop(Arc<Durable>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.stop();
    }
}

/// The uploader thread's state.
struct Worker {
    durable: Arc<Durable>,
    bucket: Bucket,
    /// Window syncs waiting for their limits.
    batch: Batch,
    /// Window syncs whose record was never sent, oldest first: they go in front of
    /// everything newer.
    owed: VecDeque<Pending>,
    generation: Arc<AtomicU64>,
    degraded: Arc<AtomicBool>,
    last_failed: Arc<AtomicBool>,
    quiet_until: Arc<Mutex<Option<Instant>>>,
    stats: Arc<StatsCell>,
    events: Option<EventHook>,
    trim_tx: Option<Sender<TrimSnapshot>>,
    /// The commits of a record whose PUT went in doubt: the bucket settles it at its seq,
    /// and when it has, they are in the bucket (`settled`).
    doubt: Option<Vec<(Batching, u64)>>,
}

impl Worker {
    /// Run jobs in order until every sender is gone, then flush what waits and exit.
    ///
    /// While syncs are owed, the bucket is degraded or its last operation failed, it wakes
    /// every `MAX_BACKOFF` (2 s) to retry them, or probe the bucket (`tick`).
    ///
    /// One degraded state can last: a seal whose head write was ambiguous and whose head
    /// turns out to be published stays pending until the next seal adopts it
    /// (`Bucket::before_record` refuses appends meanwhile), so each wake GETs the head again
    /// (`catch_up`), one GET every 2 s, until that seal. That is rare (an ambiguous head PUT
    /// that landed) and ends at the next seal.
    fn run(mut self, rx: Receiver<Job>) {
        // Whatever ends the thread, a panic included, wakes everyone who waits on a record.
        let _stop = StopOnDrop(self.durable.clone());
        loop {
            let next = if let Some(mut deadline) = self.batch.deadline() {
                if !self.owed.is_empty() {
                    // Owed retries are not held back by a long window.
                    deadline = deadline.min(Instant::now() + defaults::MAX_BACKOFF);
                }
                rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
            } else if !self.owed.is_empty()
                || self.bucket.degraded()
                || self.last_failed.load(SeqCst)
            {
                rx.recv_timeout(defaults::MAX_BACKOFF)
            } else {
                rx.recv().map_err(|_| RecvTimeoutError::Disconnected)
            };
            match next {
                Ok(Job::Log(item)) => self.log(item),
                Ok(Job::File(item)) => self.log_file(item),
                Ok(Job::Settle(reply)) => {
                    let r = self.settle_log();
                    self.last_failed.store(r.is_err(), SeqCst);
                    self.publish_degraded();
                    let _ = reply.send(r);
                }
                Ok(Job::Seal {
                    pages,
                    page_size,
                    page_count,
                    reply,
                    parent,
                }) => {
                    let r = self.seal(pages, page_size, page_count, &parent);
                    drop(parent);
                    self.last_failed.store(r.is_err(), SeqCst);
                    self.publish_degraded();
                    let _ = reply.send(r);
                }
                Ok(Job::Switch { reply, parent }) => {
                    let r = self.switch(&parent);
                    drop(parent);
                    self.last_failed.store(r.is_err(), SeqCst);
                    self.publish_degraded();
                    let _ = reply.send(r);
                }
                Err(RecvTimeoutError::Timeout) => self.tick(),
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        // Shutdown: what waits is uploaded (or failed) before the thread stops.
        let _ = self.flush();
    }

    fn emit(&self, e: Event) {
        if let Some(h) = &self.events {
            h(&e);
        }
    }

    /// Publish `degraded` and the generation's takeover quiet (every
    /// append and seal goes through here before its replies).
    fn publish_degraded(&mut self) {
        if !self.bucket.degraded() {
            self.settled();
        }
        let d = self.bucket.degraded() || !self.owed.is_empty();
        self.degraded.store(d, SeqCst);
        *crate::db::lock(&self.quiet_until) = self.bucket.quiet_until();
    }

    /// The bucket has nothing left to settle. A record that went in doubt is in the bucket
    /// now (the settle put it at its seq): its commits are durable, and background ones are
    /// acked here. An earlier failure no longer holds once nothing is owed either.
    fn settled(&mut self) {
        if let Some(doubt) = self.doubt.take() {
            if let Some(last) = doubt.iter().map(|(_, c)| *c).max() {
                self.durable.confirmed(last);
            }
            for (batching, commit) in doubt {
                if matches!(batching, Batching::Background { .. }) {
                    self.stats.record_background_ack();
                    self.emit(Event::CommitAcked { commit, batching });
                }
            }
        }
        if self.owed.is_empty() && !self.last_failed.load(SeqCst) {
            self.durable.recovered();
        }
    }

    fn log(&mut self, item: Pending) {
        if self.batch.push(item, Instant::now()) {
            // Its replies were sent; a failure is theirs.
            let _ = self.flush();
            return;
        }
        self.publish_degraded();
        self.emit(Event::BatchWaiting {
            commits: self.batch.len() as u64,
        });
    }

    /// A wait ran out: a batch whose deadline passed is flushed, and owed syncs are retried
    /// (with the batch, whatever its deadline); otherwise the bucket catches up on what it has
    /// to settle (a published seal waiting to be adopted keeps it degraded, see `run`), or,
    /// after a failure that left nothing to settle, is probed (`Bucket::probe`).
    fn tick(&mut self) {
        match self.batch.deadline() {
            Some(d) if d <= Instant::now() || !self.owed.is_empty() => {
                let _ = self.flush();
            }
            Some(_) => {}
            None if !self.owed.is_empty() => {
                let _ = self.flush();
            }
            None => {
                if self.bucket.degraded() || self.last_failed.load(SeqCst) {
                    // A failure leaves it degraded (or failed); the next tick tries again.
                    // Not degraded, nothing is owed: the probe is one head GET, so an idle
                    // database learns that the bucket has recovered (maintenance waits
                    // for that, `Uploader::bucket_failing`).
                    let ok = self.bucket.probe().is_ok();
                    self.last_failed.store(!ok, SeqCst);
                }
                self.publish_degraded();
            }
        }
    }

    /// Upload the owed syncs, then the batch, as one record, and reply to each. Returns the
    /// error every item got, if any. Nothing to upload is `Ok`.
    ///
    /// The record's `buckwal.upload` span is a child of its first commit's span and follows
    /// from the others'; it closes, and drops its hold on the commit spans, before any reply.
    fn flush(&mut self) -> Result<()> {
        let mut items: Vec<Pending> = self.owed.drain(..).collect();
        items.extend(self.batch.take());
        if items.is_empty() {
            return Ok(());
        }
        let span = info_span!(
            target: "buckwal",
            parent: &items[0].span,
            "buckwal.upload",
            buckwal.commits = items.len() as u64,
            buckwal.commit_first = items.first().map_or(0, |p| p.commit),
            buckwal.commit_last = items.iter().map(|p| p.commit).max().unwrap_or(0),
            buckwal.bytes = Empty,
            buckwal.generation = Empty,
            buckwal.seq = Empty,
            buckwal.retries = Empty,
            buckwal.oldest_behind_ms = Empty,
            s3.status = Empty,
        );
        let mut syncs = Vec::with_capacity(items.len());
        let mut waiters = Vec::with_capacity(items.len());
        let mut commit_spans = Vec::with_capacity(items.len());
        for (i, p) in items.into_iter().enumerate() {
            if i > 0 {
                span.follows_from(&p.span);
            }
            syncs.push(p.ops);
            waiters.push((p.batching, p.commit, p.reply));
            commit_spans.push(p.span);
        }
        let r = span.in_scope(|| self.bucket.append_log(LogFile::Wal, syncs));
        match &r {
            Ok(a) => {
                span.record("buckwal.bytes", a.bytes);
            }
            Err(f) => span.in_scope(|| {
                tracing::warn!(
                    target: "buckwal",
                    error = %f.error,
                    commits = waiters.len() as u64,
                    "log upload failed"
                );
            }),
        }
        match r {
            Ok(a) => {
                drop(commit_spans);
                // Counted, reported and published before any reply, so a waiter that sees
                // its ack also sees the record.
                let commits: Vec<u64> = waiters.iter().map(|(_, c, _)| *c).collect();
                self.stats.record_put(commits.len() as u64, a.bytes);
                self.last_failed.store(false, SeqCst);
                self.durable
                    .confirmed(commits.iter().copied().max().unwrap_or(0));
                // If this append settled a record that went in doubt before it (the bucket settles it first),
                // its commits are older, so their acks come before the new record's.
                // `confirmed` never moves the position back, so its order here does not matter.
                if !self.bucket.degraded() {
                    self.settled();
                }
                // How long the commits still behind have waited: what a lost machine would
                // lose now. The span closes before any reply.
                span.record(
                    "buckwal.oldest_behind_ms",
                    self.durable.behind().1.as_millis() as u64,
                );
                drop(span);
                self.emit(Event::LogUploaded {
                    generation: a.pos.generation,
                    seq: a.pos.seq,
                    bytes: a.bytes,
                    commits,
                });
                // A background commit's call has returned: it is acked here, when it is durable.
                for (batching, commit, _) in &waiters {
                    if matches!(batching, Batching::Background { .. }) {
                        self.stats.record_background_ack();
                        self.emit(Event::CommitAcked {
                            commit: *commit,
                            batching: *batching,
                        });
                    }
                }
                self.publish_degraded();
                for (_, _, reply) in waiters {
                    // A dropped ticket is fine: the object is uploaded either way.
                    let _ = reply.send(Ok(a.pos));
                }
                Ok(())
            }
            Err(AppendFailure { error, unsent }) => {
                drop(span);
                match unsent {
                    // A fenced writer never sends again: nothing stays owed.
                    _ if matches!(error, Error::Fenced(_)) => {}
                    // Never sent: the Window syncs stay owed, in order. The Off ones are
                    // dropped: their sync fails, so the engine rolls them back.
                    Some(unsent) => {
                        // An owed sync keeps its commit span, so its later upload hangs from it.
                        let owed = unsent.into_iter().zip(&waiters).zip(&commit_spans);
                        for ((ops, (batching, commit, reply)), span) in owed {
                            if batching != &Batching::Off {
                                self.owed.push_back(Pending {
                                    ops,
                                    batching: *batching,
                                    commit: *commit,
                                    reply: reply.clone(),
                                    span: span.clone(),
                                });
                            }
                        }
                    }
                    // In doubt at its seq (the bucket settles it), or fenced.
                    None => {
                        if !matches!(error, Error::Fenced(_)) {
                            // An earlier record settled before this append, and its
                            // commits are still to be confirmed, so the new ones join them.
                            self.doubt
                                .get_or_insert_with(Vec::new)
                                .extend(waiters.iter().map(|(b, c, _)| (*b, *c)));
                        }
                    }
                }
                drop(commit_spans);
                self.last_failed.store(true, SeqCst);
                self.durable.failed(&error);
                self.publish_degraded();
                for (_, _, reply) in waiters {
                    let _ = reply.send(Err(error.clone()));
                }
                Err(error)
            }
        }
    }

    /// Flush first; while owed syncs remain the seal fails with the flush's error, so it
    /// never seals (or adopts a published head) over them. Then seal as in stage 1, inside a
    /// `buckwal.checkpoint_upload` span under `parent` (the bucket records its fields).
    fn seal(
        &mut self,
        pages: Vec<(u32, Vec<u8>)>,
        page_size: u32,
        page_count: u32,
        parent: &Span,
    ) -> Result<u64> {
        if let Err(e) = self.flush() {
            if !self.owed.is_empty() {
                return Err(e);
            }
        }
        let span = info_span!(
            target: "buckwal",
            parent: parent,
            "buckwal.checkpoint_upload",
            buckwal.generation = Empty,
            buckwal.seq = Empty,
            buckwal.pages = Empty,
            buckwal.retries = Empty,
            s3.status = Empty,
            buckwal.skipped_generation = Empty,
        );
        let g = span.in_scope(|| self.bucket.seal_generation(pages, page_size, page_count))?;
        drop(span);
        self.sealed(g);
        Ok(g)
    }

    /// Flush first, as `seal` does (owed syncs left fail the switch), then switch the generation (format 5) inside a
    /// `buckwal.generation_switch` span under `parent`: `buckwal.generation` (the generation
    /// that ends), `buckwal.next_generation`, `buckwal.seq` (its seal-start record),
    /// `buckwal.retries` and `s3.status` (recorded by the bucket and the stores), then
    /// `buckwal.elapsed_ms` and `buckwal.outcome`: `switched`, `fenced` (another writer took the
    /// database over) or `upload_failed` (any other failure; the switch is retried at the next
    /// WAL restart). Its duration is the extra wait of the commit that restarts the WAL.
    fn switch(&mut self, parent: &Span) -> Result<u64> {
        if let Err(e) = self.flush() {
            if !self.owed.is_empty() {
                return Err(e);
            }
        }
        let start = Instant::now();
        let span = info_span!(
            target: "buckwal",
            parent: parent,
            "buckwal.generation_switch",
            buckwal.generation = Empty,
            buckwal.next_generation = Empty,
            buckwal.seq = Empty,
            buckwal.retries = Empty,
            buckwal.elapsed_ms = Empty,
            buckwal.outcome = Empty,
            s3.status = Empty,
        );
        let r = span.in_scope(|| self.bucket.switch_generation());
        span.record("buckwal.elapsed_ms", start.elapsed().as_millis() as u64);
        span.record(
            "buckwal.outcome",
            match &r {
                Ok(_) => "switched",
                Err(Error::Fenced(_)) => "fenced",
                Err(_) => "upload_failed",
            },
        );
        drop(span);
        let g = r?;
        self.sealed(g);
        Ok(g)
    }

    /// The bucket moved to generation `g` (a seal, a switch, or an adopted one): publish it and wake
    /// the trimmer.
    fn sealed(&self, g: u64) {
        self.generation.store(g, SeqCst);
        if let Some(tx) = &self.trim_tx {
            // No trimmer listening is not an upload failure.
            let _ = tx.send(TrimSnapshot {
                head: self.bucket.head().clone(),
                manifest: self.bucket.manifest().cloned(),
            });
        }
    }

    /// `Uploader::settle_log` on the thread. Nothing is batched or owed in MVCC mode
    /// (Window is refused there), so there is nothing to flush first.
    fn settle_log(&mut self) -> Result<u64> {
        if self.bucket.settle_all()? {
            self.sealed(self.bucket.head().generation);
        }
        Ok(self.bucket.head().generation)
    }

    /// Upload one `submit_log_file` record and reply. WAL syncs waiting or owed go first, so
    /// records keep the submission order; if owed ones remain, this record is not sent.
    fn log_file(&mut self, p: FilePending) {
        let FilePending {
            file,
            ops,
            commits,
            commit,
            reply,
            span: commit_span,
        } = p;
        if let Err(error) = self.flush() {
            if !self.owed.is_empty() {
                let _ = reply.send(Err(LogFailure {
                    error,
                    unsent: Some(ops),
                }));
                return;
            }
        }
        let span = info_span!(
            target: "buckwal",
            parent: &commit_span,
            "buckwal.upload",
            buckwal.commits = commits,
            buckwal.commit_first = commit,
            buckwal.commit_last = commit,
            buckwal.bytes = Empty,
            buckwal.generation = Empty,
            buckwal.seq = Empty,
            buckwal.retries = Empty,
            buckwal.oldest_behind_ms = Empty,
            s3.status = Empty,
        );
        let r = span.in_scope(|| self.bucket.append_log(file, vec![ops]));
        match &r {
            Ok(a) => {
                span.record("buckwal.bytes", a.bytes);
            }
            Err(f) => span.in_scope(|| {
                tracing::warn!(
                    target: "buckwal",
                    error = %f.error,
                    commits,
                    "log upload failed"
                );
            }),
        }
        drop(span);
        drop(commit_span);
        match r {
            Ok(a) => {
                // Counted, reported and published before the reply, as in `flush`.
                self.stats.record_put(commits, a.bytes);
                self.last_failed.store(false, SeqCst);
                self.durable.confirmed(commit);
                self.emit(Event::LogUploaded {
                    generation: a.pos.generation,
                    seq: a.pos.seq,
                    bytes: a.bytes,
                    commits: vec![commit],
                });
                self.publish_degraded();
                let _ = reply.send(Ok(a.pos));
            }
            Err(AppendFailure { error, unsent }) => {
                // A fenced writer never sends again; one in doubt is the bucket's to settle.
                let unsent = match unsent {
                    _ if matches!(error, Error::Fenced(_)) => None,
                    Some(syncs) => Some(syncs.into_iter().flatten().collect()),
                    None => None,
                };
                self.last_failed.store(true, SeqCst);
                self.durable.failed(&error);
                self.publish_degraded();
                let _ = reply.send(Err(LogFailure { error, unsent }));
            }
        }
    }
}

mod batch;
mod durable;

#[cfg(test)]
mod tests;
