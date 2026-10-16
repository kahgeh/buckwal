//! The background uploader: one thread that owns the [`Bucket`] and runs its jobs in FIFO
//! order, so a seal can never overtake a log object submitted before it.

use crate::bucket::{Bucket, Counters, LogPos};
use crate::config::EventHook;
use crate::error::{Error, Result};
use crate::format::{Head, Manifest, Op};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::mpsc::{self, Receiver, Sender};
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
    Log {
        ops: Vec<Op>,
        reply: Sender<Result<LogPos>>,
    },
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
    thread: Mutex<Option<JoinHandle<()>>>,
    generation: Arc<AtomicU64>,
    #[cfg_attr(not(test), allow(dead_code))]
    counters: Arc<Counters>,
}

impl Uploader {
    pub(crate) fn spawn(
        mut bucket: Bucket,
        trim_tx: Option<Sender<TrimSnapshot>>,
        events: Option<EventHook>,
    ) -> Arc<Uploader> {
        bucket.set_event_hook(events);
        let generation = Arc::new(AtomicU64::new(bucket.head().generation));
        let counters = bucket.counters();
        let (tx, rx) = mpsc::channel();
        let gen = generation.clone();
        let thread = std::thread::Builder::new()
            .name("turso-durable-uploader".into())
            .spawn(move || run(bucket, rx, gen, trim_tx))
            .expect("spawn uploader thread");
        Arc::new(Uploader {
            jobs: Mutex::new(Some(tx)),
            thread: Mutex::new(Some(thread)),
            generation,
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

    /// Queue one sync's ops as one log object.
    pub(crate) fn submit_log(&self, ops: Vec<Op>) -> Ticket {
        let (reply, rx) = mpsc::channel();
        self.send(Job::Log { ops, reply });
        Ticket { rx }
    }

    /// Queue a seal behind every log object already submitted and wait for it; returns the
    /// new generation.
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

    /// Run every queued job, then stop the thread (joined, unless called on it). Later
    /// submissions fail.
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

/// The uploader thread: run jobs in order until every sender is gone.
fn run(
    mut bucket: Bucket,
    rx: Receiver<Job>,
    generation: Arc<AtomicU64>,
    trim_tx: Option<Sender<TrimSnapshot>>,
) {
    for job in rx {
        match job {
            Job::Log { ops, reply } => {
                // A dropped ticket is fine: the object is uploaded either way.
                let _ = reply.send(bucket.append_log(vec![ops]));
            }
            Job::Seal {
                pages,
                page_size,
                page_count,
                reply,
            } => {
                let r = bucket.seal_generation(pages, page_size, page_count);
                if let Ok(g) = r {
                    generation.store(g, SeqCst);
                    if let Some(tx) = &trim_tx {
                        // No trimmer listening is not an upload failure.
                        let _ = tx.send(TrimSnapshot {
                            at: Instant::now(),
                            head: bucket.head().clone(),
                            manifest: bucket.manifest().cloned(),
                        });
                    }
                }
                let _ = reply.send(r);
            }
        }
    }
}

#[cfg(test)]
mod tests;
