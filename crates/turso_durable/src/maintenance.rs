//! The maintenance checkpoint: a writer open seals its generation itself once the log (or the
//! local `-wal`) passes a threshold, because the engine restarts the WAL only when a writer
//! finds it fully backfilled with no reader on it, which sustained concurrent commits (Window
//! commits above all) never leave room for.
//!
//! - [`Gate`]: every public `Connection` call enters it around its engine work (an open
//!   explicit transaction stays inside until it ends). Maintenance closes it, waits for the
//!   calls in flight to leave (at most `gate_wait`), checkpoints, and opens it again.
//! - [`Maintenance`]: the `turso-durable-maintenance` thread. `Connection::call` pokes it
//!   after each call (a few atomic loads); it runs `checkpoint(TRUNCATE)` on its own engine
//!   connection while the gate is drained. The TRUNCATE seals at `truncate(0)` through the
//!   same path as a user's checkpoint (`ShippingFile::truncate`), which flushes every batched
//!   commit first.
//!
//! No run starts while the bucket looks unable to take a seal (the uploader is degraded or
//! its last upload failed): the gate would stay closed for the seal's whole retry deadline.
//! Nor during a takeover quiet (`Uploader::seal_quiet_remaining`): the seal would wait it out
//! with the gate closed; that run is retried once the quiet has passed. A run that is skipped, cannot drain the gate, finds the engine busy or fails backs off
//! for `retry`; repeated gate timeouts and busy refusals double it up to `retry_max`
//! (growth does not fix their cause), and a failed run is retried sooner only once the
//! generation has grown by another threshold. Its outcome is an event, a
//! `durable.maintenance` span and, on failure, a warning: never an error to any caller.
use crate::batching::{ctx, Batching};
use crate::config::{defaults, Config, Event};
use crate::conn::BUSY_TIMEOUT;
use crate::db::{lock, Shared};
use crate::engine::engine_error;
use crate::error::Error;
use crate::io::stash;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::field::Empty;
use turso_core::{LimboError, SyncMode};

/// When maintenance runs, from [`Config`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct MaintenanceConfig {
    /// Run once the generation's log holds this many objects; 0 = never on this measure.
    pub log_objects: u64,
    /// Run once the generation's `-wal` reaches this many bytes; 0 = never on this measure.
    pub wal_bytes: u64,
    /// The longest a run holds the gate closed waiting for calls in flight.
    pub gate_wait: Duration,
    /// The least time between a skipped or failed run and the next.
    pub retry: Duration,
    /// The longest backoff (repeated gate timeouts and busy refusals double `retry`).
    pub retry_max: Duration,
}

impl MaintenanceConfig {
    pub(crate) fn from_config(c: &Config) -> MaintenanceConfig {
        MaintenanceConfig {
            log_objects: c.maintenance_log_objects,
            wal_bytes: c.maintenance_wal_bytes,
            gate_wait: c.maintenance_gate_wait,
            retry: defaults::MAINTENANCE_RETRY,
            retry_max: defaults::MAINTENANCE_RETRY_MAX,
        }
    }

    /// At least one measure is on.
    pub(crate) fn enabled(&self) -> bool {
        self.log_objects > 0 || self.wal_bytes > 0
    }

    /// The generation is due for maintenance.
    pub(crate) fn due(&self, log_objects: u64, wal_bytes: u64) -> bool {
        let over = |n: u64, threshold: u64| threshold > 0 && n >= threshold;
        over(log_objects, self.log_objects) || over(wal_bytes, self.wal_bytes)
    }

    /// The measures at which a run that failed at (`log_objects`, `wal_bytes`) may be retried
    /// before its backoff ends: one more threshold of growth (`u64::MAX`: never, for a
    /// measure that is off).
    fn bypass_at(&self, log_objects: u64, wal_bytes: u64) -> (u64, u64) {
        let at = |n: u64, threshold: u64| {
            if threshold == 0 {
                u64::MAX
            } else {
                n.saturating_add(threshold)
            }
        };
        (
            at(log_objects, self.log_objects),
            at(wal_bytes, self.wal_bytes),
        )
    }
}

/// The `n`th backoff in a row (from 1): `retry` doubled `n − 1` times, at most `max`.
pub(crate) fn backoff(retry: Duration, n: u32, max: Duration) -> Duration {
    let doublings = n.saturating_sub(1).min(31);
    retry.saturating_mul(1 << doublings).min(max)
}

#[derive(Debug, Default)]
struct GateState {
    closed: bool,
    in_flight: usize,
}

/// Holds new calls back while maintenance drains the ones in flight; see the module docs.
#[derive(Debug, Default)]
pub(crate) struct Gate {
    state: Mutex<GateState>,
    /// Signalled when the gate opens and when the last call in flight leaves.
    changed: Condvar,
}

/// One call (or open transaction) inside the gate; it leaves on drop.
#[derive(Debug)]
pub(crate) struct GateGuard {
    gate: Arc<Gate>,
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        let mut s = lock(&self.gate.state);
        s.in_flight -= 1;
        if s.in_flight == 0 {
            self.gate.changed.notify_all();
        }
    }
}

impl Gate {
    /// Enter, blocking while the gate is closed.
    pub(crate) fn enter(self: &Arc<Self>) -> GateGuard {
        let mut s = lock(&self.state);
        while s.closed {
            s = self.changed.wait(s).unwrap_or_else(|e| e.into_inner());
        }
        s.in_flight += 1;
        GateGuard { gate: self.clone() }
    }

    /// Close the gate and wait, at most `wait`, for every call in flight to leave. True once
    /// none is left (the gate stays closed until [`Gate::open`]); false after reopening it
    /// on timeout.
    pub(crate) fn close_and_drain(&self, wait: Duration) -> bool {
        let deadline = Instant::now() + wait;
        let mut s = lock(&self.state);
        s.closed = true;
        while s.in_flight > 0 {
            let now = Instant::now();
            if now >= deadline {
                s.closed = false;
                self.changed.notify_all();
                return false;
            }
            s = self
                .changed
                .wait_timeout(s, deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        true
    }

    /// Let calls in again.
    pub(crate) fn open(&self) {
        lock(&self.state).closed = false;
        self.changed.notify_all();
    }

    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        lock(&self.state).closed
    }
}

/// Opens the gate when dropped, so a panic in the checkpoint never leaves it closed.
struct Reopen<'a>(&'a Gate);

impl Drop for Reopen<'_> {
    fn drop(&mut self) {
        self.0.open();
    }
}

/// The thread's phase, read by [`Maintenance::poke`] without a lock.
const IDLE: u8 = 0;
/// Woken, or running.
const RUNNING: u8 = 1;
/// The last run was skipped or failed; the next waits for `retry`.
const BACKOFF: u8 = 2;

#[derive(Default)]
struct Wake {
    woken: bool,
    stop: bool,
}

struct Signal {
    wake: Mutex<Wake>,
    cv: Condvar,
    phase: AtomicU8,
    /// While backing off after a failed run: the log objects and `-wal` bytes at which a
    /// poke retries at once (`u64::MAX`: wait out the backoff).
    bypass_log: AtomicU64,
    bypass_wal: AtomicU64,
    /// Tests: the next run panics.
    #[cfg(test)]
    panic_next: std::sync::atomic::AtomicBool,
    /// Tests: the next run panics inside the checkpoint, with the gate closed.
    #[cfg(test)]
    checkpoint_panic_next: std::sync::atomic::AtomicBool,
}

impl Default for Signal {
    fn default() -> Signal {
        Signal {
            wake: Mutex::default(),
            cv: Condvar::new(),
            phase: AtomicU8::new(IDLE),
            bypass_log: AtomicU64::new(u64::MAX),
            bypass_wal: AtomicU64::new(u64::MAX),
            #[cfg(test)]
            panic_next: Default::default(),
            #[cfg(test)]
            checkpoint_panic_next: Default::default(),
        }
    }
}

/// The maintenance thread of one writer open; see the module docs.
pub(crate) struct Maintenance {
    cfg: MaintenanceConfig,
    signal: Arc<Signal>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

/// How a run ended.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Sealed,
    /// Not started: the bucket looked unable to take a seal.
    BucketFailing,
    /// Not started: a seal now would wait out this much of a takeover quiet.
    TakeoverQuiet(Duration),
    Busy,
    GateTimeout,
    /// The database was closing.
    Closed,
    Error,
}

impl Outcome {
    fn as_str(&self) -> &'static str {
        match self {
            Outcome::Sealed => "sealed",
            Outcome::BucketFailing => "bucket_failing",
            Outcome::TakeoverQuiet(_) => "takeover_quiet",
            Outcome::Busy => "busy",
            Outcome::GateTimeout => "gate_timeout",
            Outcome::Closed => "closed",
            Outcome::Error => "error",
        }
    }
}

impl Maintenance {
    /// Start the thread. It holds only `shared` (a `Weak`), so it never keeps the database
    /// open, and it reports to the subscriber of the thread that opened the database.
    pub(crate) fn spawn(shared: Weak<Shared>, cfg: MaintenanceConfig) -> Maintenance {
        let signal = Arc::new(Signal::default());
        let s = signal.clone();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        let thread = std::thread::Builder::new()
            .name("turso-durable-maintenance".into())
            .spawn(move || {
                #[cfg(test)]
                let _count = tests::PanicCount;
                tracing::dispatcher::with_default(&dispatch, || run(&shared, &cfg, &s))
            })
            .expect("spawn maintenance thread");
        Maintenance {
            cfg,
            signal,
            thread: Mutex::new(Some(thread)),
        }
    }

    /// After a call: wake the thread when the generation is due and no run is in progress,
    /// nor backing off (unless a failed run's generation has grown by another threshold
    /// since). Only atomic loads when nothing is due; it never blocks.
    pub(crate) fn poke(&self, log_objects: u64, wal_bytes: u64) {
        if !self.cfg.due(log_objects, wal_bytes) {
            return;
        }
        let s = &self.signal;
        let grown =
            || log_objects >= s.bypass_log.load(SeqCst) || wal_bytes >= s.bypass_wal.load(SeqCst);
        let from = match s.phase.load(SeqCst) {
            IDLE => IDLE,
            BACKOFF if grown() => BACKOFF,
            _ => return,
        };
        // Only the caller that moves it to RUNNING wakes it.
        if self
            .signal
            .phase
            .compare_exchange(from, RUNNING, SeqCst, SeqCst)
            .is_ok()
        {
            self.wake();
        }
    }

    /// Tests: the next run panics.
    #[cfg(test)]
    pub(crate) fn panic_next_run(&self) {
        self.signal.panic_next.store(true, SeqCst);
    }

    /// Tests: the next run panics inside the checkpoint, once the gate is closed and drained.
    #[cfg(test)]
    pub(crate) fn panic_next_checkpoint(&self) {
        self.signal.checkpoint_panic_next.store(true, SeqCst);
    }

    /// Wake the thread for a run (it checks the thresholds again first).
    pub(crate) fn wake(&self) {
        lock(&self.signal.wake).woken = true;
        self.signal.cv.notify_all();
    }

    /// Stop the thread and wait for a run in progress to end. Called on the thread itself
    /// (it dropped the last handle to the database), it does not wait: the thread exits
    /// after this run.
    pub(crate) fn stop(&self) {
        lock(&self.signal.wake).stop = true;
        self.signal.cv.notify_all();
        let Some(t) = lock(&self.thread).take() else {
            return;
        };
        if t.thread().id() == std::thread::current().id() {
            return;
        }
        // A panic on the thread left the gate open (`Reopen`) and nothing else behind.
        let _ = t.join();
    }
}

/// The thread: wait for a wake (or the end of a backoff), then run while the generation is
/// due. Exits once stopped or the database is gone.
fn run(shared: &Weak<Shared>, cfg: &MaintenanceConfig, signal: &Signal) {
    let mut retry_at: Option<Instant> = None;
    // Gate timeouts and busy refusals in a row: each doubles the backoff.
    let mut held_off = 0u32;
    loop {
        {
            let mut w = lock(&signal.wake);
            loop {
                if w.stop {
                    return;
                }
                if w.woken {
                    w.woken = false;
                    break;
                }
                match retry_at {
                    Some(at) if Instant::now() >= at => break,
                    Some(at) => {
                        let left = at.saturating_duration_since(Instant::now());
                        w = signal
                            .cv
                            .wait_timeout(w, left)
                            .unwrap_or_else(|e| e.into_inner())
                            .0;
                    }
                    None => w = signal.cv.wait(w).unwrap_or_else(|e| e.into_inner()),
                }
            }
        }
        signal.phase.store(RUNNING, SeqCst);
        let Some(shared) = shared.upgrade() else {
            return;
        };
        let (log_objects, wal_bytes) = shared.maintenance_measures();
        let outcome = cfg.due(log_objects, wal_bytes).then(|| {
            // A panic must not end maintenance for good (the gate is reopened by `Reopen`).
            #[cfg(test)]
            let inject_panic = signal.panic_next.swap(false, SeqCst);
            #[cfg(not(test))]
            let inject_panic = false;
            let generation = shared.uploader.generation();
            let span = tracing::info_span!(
                target: "turso_durable",
                "durable.maintenance",
                durable.generation = generation,
                durable.log_objects = log_objects,
                durable.wal_bytes = wal_bytes,
                durable.gate_wait_ms = Empty,
                durable.outcome = Empty,
            );
            let _entered = span.enter();
            catch_unwind(AssertUnwindSafe(|| {
                let run = Measured {
                    span: &span,
                    generation,
                    log_objects,
                    wal_bytes,
                };
                run_once(&shared, cfg, run, inject_panic, signal)
            }))
            .unwrap_or_else(|_| {
                // The run may have emitted `MaintenanceStarted`: it gets an end event too.
                span.record("durable.outcome", Outcome::Error.as_str());
                tracing::warn!(
                    target: "turso_durable",
                    generation,
                    "maintenance checkpoint panicked; retrying later"
                );
                // A hook that panics again must not end the thread.
                let _ = catch_unwind(AssertUnwindSafe(|| {
                    shared.emit(Event::MaintenanceSkipped {
                        generation,
                        reason: "panic".to_string(),
                    })
                }));
                Outcome::Error
            })
        });
        // Dropped here, not at the loop's end: when it is the last handle, the database
        // closes on this thread (`Shared::finish` then does not join it).
        drop(shared);
        let (mut bypass_log, mut bypass_wal) = (u64::MAX, u64::MAX);
        let delay = match outcome {
            None | Some(Outcome::Sealed) => None,
            Some(Outcome::GateTimeout | Outcome::Busy) => {
                held_off = held_off.saturating_add(1);
                Some(backoff(cfg.retry, held_off, cfg.retry_max))
            }
            Some(Outcome::Error) => {
                (bypass_log, bypass_wal) = cfg.bypass_at(log_objects, wal_bytes);
                Some(cfg.retry)
            }
            Some(Outcome::BucketFailing | Outcome::Closed) => Some(cfg.retry),
            // Retried once the quiet has passed, not every `retry` meanwhile.
            Some(Outcome::TakeoverQuiet(left)) => Some(cfg.retry.max(left)),
        };
        signal.bypass_log.store(bypass_log, SeqCst);
        signal.bypass_wal.store(bypass_wal, SeqCst);
        match delay {
            None => {
                held_off = 0;
                retry_at = None;
                signal.phase.store(IDLE, SeqCst);
            }
            Some(d) => {
                retry_at = Some(Instant::now() + d);
                signal.phase.store(BACKOFF, SeqCst);
            }
        }
    }
}

/// What a run was started for, and its span.
struct Measured<'a> {
    span: &'a tracing::Span,
    generation: u64,
    log_objects: u64,
    wal_bytes: u64,
}

/// One run: unless the bucket is failing or a takeover quiet lasts, close and drain the gate,
/// checkpoint, reopen.
///
/// It runs inside its `durable.maintenance` `span`, which the caller made (for
/// `generation`, `log_objects` and `wal_bytes`) and entered, so a panic's outcome is
/// recorded on it too.
fn run_once(
    shared: &Shared,
    cfg: &MaintenanceConfig,
    run: Measured<'_>,
    inject_panic: bool,
    signal: &Signal,
) -> Outcome {
    let Measured {
        span,
        generation,
        log_objects,
        wal_bytes,
    } = run;
    let quiet = shared.uploader.seal_quiet_remaining();
    let skip = match shared.uploader.bucket_failing() {
        Some(reason) => Some((Outcome::BucketFailing, reason)),
        None if !quiet.is_zero() => Some((Outcome::TakeoverQuiet(quiet), "takeover quiet")),
        None => None,
    };
    if let Some((outcome, reason)) = skip {
        span.record("durable.outcome", outcome.as_str());
        tracing::info!(
            target: "turso_durable",
            generation,
            reason,
            "maintenance checkpoint not started; retrying later"
        );
        shared.emit(Event::MaintenanceSkipped {
            generation,
            reason: reason.to_string(),
        });
        return outcome;
    }
    shared.emit(Event::MaintenanceStarted {
        generation,
        log_objects,
        wal_bytes,
    });
    if inject_panic {
        panic!("injected maintenance panic");
    }
    #[cfg(not(test))]
    let _ = signal;
    let start = Instant::now();
    let drained = shared.gate.close_and_drain(cfg.gate_wait);
    let gate_wait = start.elapsed();
    span.record("durable.gate_wait_ms", gate_wait.as_millis() as u64);
    let r = if drained {
        let _reopen = Reopen(&shared.gate);
        #[cfg(test)]
        if signal.checkpoint_panic_next.swap(false, SeqCst) {
            panic!("injected checkpoint panic");
        }
        checkpoint(shared)
    } else {
        Err((Outcome::GateTimeout, "gate_timeout".to_string()))
    };
    let sealed = shared.uploader.generation() != generation;
    let (outcome, reason) = match r {
        Ok(()) if sealed => (Outcome::Sealed, None),
        // TRUNCATE returned without ending the generation: not expected; retried later.
        Ok(()) => (
            Outcome::Error,
            Some("error: the checkpoint did not end the generation".to_string()),
        ),
        Err((o, reason)) => (o, Some(reason)),
    };
    span.record("durable.outcome", outcome.as_str());
    match reason {
        None => shared.emit(Event::MaintenanceFinished {
            generation,
            gate_wait,
            elapsed: start.elapsed(),
        }),
        Some(reason) => {
            // A failure warns; a run held off by calls or the engine is routine.
            if outcome == Outcome::Error {
                tracing::warn!(
                    target: "turso_durable",
                    generation,
                    reason = %reason,
                    "maintenance checkpoint failed; retrying later"
                );
            } else {
                tracing::info!(
                    target: "turso_durable",
                    generation,
                    reason = %reason,
                    "maintenance checkpoint skipped; retrying later"
                );
            }
            shared.emit(Event::MaintenanceSkipped { generation, reason });
        }
    }
    outcome
}

/// `checkpoint(TRUNCATE)` on a fresh engine connection (not a public `Connection`: not
/// counted, no gate), set up as `Connection::new` sets one up, inside an `Off` commit context.
fn checkpoint(shared: &Shared) -> Result<(), (Outcome, String)> {
    let error = |e: Error| (Outcome::Error, format!("error: {e}"));
    // `Database::close` took the engine and waits for this run (`Shared::finish`).
    let Some(db) = shared.engine_db() else {
        return Err((Outcome::Closed, "closed".to_string()));
    };
    let _ctx = ctx::enter(Batching::Off);
    stash::clear();
    let core = db.connect().map_err(|e| error(engine_error(e)))?;
    core.set_busy_timeout(BUSY_TIMEOUT);
    core.set_sync_mode(SyncMode::Full);
    let r = core
        .checkpoint(turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .map(|_| ())
        .map_err(|e| match (stash::take(), e) {
            (Some(stashed), _) => error(stashed),
            (None, LimboError::Busy | LimboError::BusySnapshot) => {
                (Outcome::Busy, "busy".to_string())
            }
            (None, e) => error(Error::Engine(e.to_string())),
        });
    stash::clear();
    let closed = core.close().map_err(|e| error(engine_error(e)));
    r.and(closed)
}

#[cfg(test)]
mod tests;
