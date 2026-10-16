//! The background uploader: one thread that owns the [`Bucket`] and runs its jobs in FIFO
//! order, so a seal can never overtake a log object submitted before it.
//!
//! Window syncs wait in a [`Batch`] and share one log record with whatever joins them before
//! the first of their limits; an `Off` sync flushes at once, carrying the waiting ones. A
//! record that was never sent hands its syncs back (`AppendFailure::unsent`): the Window
//! ones stay owed, at the front of every later record, until they land.

use crate::batching::Batching;
use crate::bucket::{AppendFailure, Bucket, Counters, LogPos};
use crate::config::{defaults, Event, EventHook};
use crate::error::{Error, Result};
use crate::format::{Head, Manifest, Op};
use crate::stats::StatsCell;
use batch::{Batch, Pending};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;

/// What the bucket looked like right after a seal; the trimmer deletes what it no longer needs.
#[derive(Debug, Clone)]
pub(crate) struct TrimSnapshot {
    pub at: Instant,
    pub head: Head,
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
}

enum Job {
    Log(Pending),
    Seal {
        pages: Vec<(u32, Vec<u8>)>,
        page_size: u32,
        page_count: u32,
        reply: Sender<Result<u64>>,
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
    stats: Arc<StatsCell>,
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
        let counters = bucket.counters();
        let (tx, rx) = mpsc::channel();
        let worker = Worker {
            bucket,
            batch: Batch::default(),
            owed: VecDeque::new(),
            generation: generation.clone(),
            degraded: degraded.clone(),
            stats: stats.clone(),
            events,
            trim_tx,
        };
        let thread = std::thread::Builder::new()
            .name("turso-durable-uploader".into())
            .spawn(move || worker.run(rx))
            .expect("spawn uploader thread");
        Arc::new(Uploader {
            jobs: Mutex::new(Some(tx)),
            commits: AtomicU64::new(0),
            thread: Mutex::new(Some(thread)),
            generation,
            degraded,
            stats,
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
    /// (with any waiting Window syncs) as the next log record; a Window waits for its limits.
    pub(crate) fn submit_log(&self, ops: Vec<Op>, batching: Batching) -> Ticket {
        let (reply, rx) = mpsc::channel();
        let jobs = self.jobs.lock().unwrap();
        let commit = self.commits.fetch_add(1, SeqCst) + 1;
        let submitted = Instant::now();
        if let Some(tx) = jobs.as_ref() {
            let _ = tx.send(Job::Log(Pending {
                ops,
                batching,
                commit,
                reply,
            }));
        }
        Ticket {
            commit,
            submitted,
            rx,
        }
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
        });
        rx.recv().unwrap_or_else(|_| Err(stopped()))
    }

    /// The current generation, without waiting for queued jobs.
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(SeqCst)
    }

    /// The bucket has something to settle (fenced, a record in doubt, a seal's head write
    /// unresolved) or Window syncs are owed. Updated after every job, before its replies.
    pub(crate) fn degraded(&self) -> bool {
        self.degraded.load(SeqCst)
    }

    pub(crate) fn stats(&self) -> Arc<StatsCell> {
        self.stats.clone()
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

/// The uploader thread's state.
struct Worker {
    bucket: Bucket,
    /// Window syncs waiting for their limits.
    batch: Batch,
    /// Window syncs whose record was never sent, oldest first: they go in front of
    /// everything newer.
    owed: VecDeque<Pending>,
    generation: Arc<AtomicU64>,
    degraded: Arc<AtomicBool>,
    stats: Arc<StatsCell>,
    events: Option<EventHook>,
    trim_tx: Option<Sender<TrimSnapshot>>,
}

impl Worker {
    /// Run jobs in order until every sender is gone, then flush what waits and exit.
    fn run(mut self, rx: Receiver<Job>) {
        loop {
            let next = if let Some(mut deadline) = self.batch.deadline() {
                if !self.owed.is_empty() {
                    // Owed retries are not held back by a long window.
                    deadline = deadline.min(Instant::now() + defaults::MAX_BACKOFF);
                }
                rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
            } else if !self.owed.is_empty() || self.bucket.degraded() {
                rx.recv_timeout(defaults::MAX_BACKOFF)
            } else {
                rx.recv().map_err(|_| RecvTimeoutError::Disconnected)
            };
            match next {
                Ok(Job::Log(item)) => self.log(item),
                Ok(Job::Seal {
                    pages,
                    page_size,
                    page_count,
                    reply,
                }) => {
                    let r = self.seal(pages, page_size, page_count);
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

    fn publish_degraded(&self) {
        let d = self.bucket.degraded() || !self.owed.is_empty();
        self.degraded.store(d, SeqCst);
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
    /// to settle.
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
                if self.bucket.degraded() {
                    // A failure leaves it degraded; the next tick tries again.
                    let _ = self.bucket.catch_up();
                }
                self.publish_degraded();
            }
        }
    }

    /// Upload the owed syncs, then the batch, as one record, and reply to each. Returns the
    /// error every item got, if any. Nothing to upload is `Ok`.
    fn flush(&mut self) -> Result<()> {
        let mut items: Vec<Pending> = self.owed.drain(..).collect();
        items.extend(self.batch.take());
        if items.is_empty() {
            return Ok(());
        }
        let mut syncs = Vec::with_capacity(items.len());
        let mut waiters = Vec::with_capacity(items.len());
        for p in items {
            syncs.push(p.ops);
            waiters.push((p.batching, p.commit, p.reply));
        }
        match self.bucket.append_log(syncs) {
            Ok(a) => {
                // Counted, reported and published before any reply, so a waiter that sees
                // its ack also sees the record.
                let commits: Vec<u64> = waiters.iter().map(|(_, c, _)| *c).collect();
                self.stats.record_put(commits.len() as u64, a.bytes);
                self.emit(Event::LogUploaded {
                    generation: a.pos.generation,
                    seq: a.pos.seq,
                    bytes: a.bytes,
                    commits,
                });
                self.publish_degraded();
                for (_, _, reply) in waiters {
                    // A dropped ticket is fine: the object is uploaded either way.
                    let _ = reply.send(Ok(a.pos));
                }
                Ok(())
            }
            Err(AppendFailure { error, unsent }) => {
                match unsent {
                    // A fenced writer never sends again: nothing stays owed.
                    _ if matches!(error, Error::Fenced(_)) => {}
                    // Never sent: the Window syncs stay owed, in order. The Off ones are
                    // dropped: their sync fails, so the engine rolls them back.
                    Some(unsent) => {
                        for (ops, (batching, commit, reply)) in unsent.into_iter().zip(&waiters) {
                            if batching != &Batching::Off {
                                self.owed.push_back(Pending {
                                    ops,
                                    batching: *batching,
                                    commit: *commit,
                                    reply: reply.clone(),
                                });
                            }
                        }
                    }
                    // In doubt at its seq (the bucket settles it), or fenced.
                    None => {}
                }
                self.publish_degraded();
                for (_, _, reply) in waiters {
                    let _ = reply.send(Err(error.clone()));
                }
                Err(error)
            }
        }
    }

    /// Flush first; while owed syncs remain the seal fails with the flush's error, so it
    /// never seals (or adopts a published head) over them. Then seal as in stage 1.
    fn seal(&mut self, pages: Vec<(u32, Vec<u8>)>, page_size: u32, page_count: u32) -> Result<u64> {
        if let Err(e) = self.flush() {
            if !self.owed.is_empty() {
                return Err(e);
            }
        }
        let g = self.bucket.seal_generation(pages, page_size, page_count)?;
        self.generation.store(g, SeqCst);
        if let Some(tx) = &self.trim_tx {
            // No trimmer listening is not an upload failure.
            let _ = tx.send(TrimSnapshot {
                at: Instant::now(),
                head: self.bucket.head().clone(),
                manifest: self.bucket.manifest().cloned(),
            });
        }
        Ok(g)
    }
}

mod batch;

#[cfg(test)]
mod tests;
