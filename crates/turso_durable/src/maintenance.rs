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
//! A run that cannot drain the gate, finds the engine busy, or fails backs off for `retry`
//! (sooner once the generation passes twice a threshold). Its outcome is an event, a
//! `durable.maintenance` span and, on failure, a warning: never an error to any caller.
use crate::batching::{ctx, Batching};
use crate::config::{defaults, Config, Event};
use crate::conn::BUSY_TIMEOUT;
use crate::db::{lock, Shared};
use crate::engine::engine_error;
use crate::error::Error;
use crate::io::stash;
use std::sync::atomic::{AtomicU8, Ordering::SeqCst};
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
}

impl MaintenanceConfig {
    pub(crate) fn from_config(c: &Config) -> MaintenanceConfig {
        MaintenanceConfig {
            log_objects: c.maintenance_log_objects,
            wal_bytes: c.maintenance_wal_bytes,
            gate_wait: c.maintenance_gate_wait,
            retry: defaults::MAINTENANCE_RETRY,
        }
    }

    /// At least one measure is on.
    pub(crate) fn enabled(&self) -> bool {
        self.log_objects > 0 || self.wal_bytes > 0
    }

    /// The generation has reached `factor` × a threshold that is on.
    fn reached(&self, log_objects: u64, wal_bytes: u64, factor: u64) -> bool {
        let over = |n: u64, threshold: u64| threshold > 0 && n >= threshold.saturating_mul(factor);
        over(log_objects, self.log_objects) || over(wal_bytes, self.wal_bytes)
    }

    /// The generation is due for maintenance.
    pub(crate) fn due(&self, log_objects: u64, wal_bytes: u64) -> bool {
        self.reached(log_objects, wal_bytes, 1)
    }

    /// Past twice a threshold: a run backing off is retried at once.
    pub(crate) fn overdue(&self, log_objects: u64, wal_bytes: u64) -> bool {
        self.reached(log_objects, wal_bytes, 2)
    }
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

#[derive(Default)]
struct Signal {
    wake: Mutex<Wake>,
    cv: Condvar,
    phase: AtomicU8,
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
    Busy,
    GateTimeout,
    Error,
}

impl Outcome {
    fn as_str(&self) -> &'static str {
        match self {
            Outcome::Sealed => "sealed",
            Outcome::Busy => "busy",
            Outcome::GateTimeout => "gate_timeout",
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
    /// nor backing off (unless the generation is overdue). Only atomic loads when nothing is
    /// due; it never blocks.
    pub(crate) fn poke(&self, log_objects: u64, wal_bytes: u64) {
        if !self.cfg.due(log_objects, wal_bytes) {
            return;
        }
        let from = match self.signal.phase.load(SeqCst) {
            IDLE => IDLE,
            BACKOFF if self.cfg.overdue(log_objects, wal_bytes) => BACKOFF,
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
        let outcome = cfg
            .due(log_objects, wal_bytes)
            .then(|| run_once(&shared, cfg, log_objects, wal_bytes));
        // Dropped here, not at the loop's end: when it is the last handle, the database
        // closes on this thread (`Shared::finish` then does not join it).
        drop(shared);
        if matches!(outcome, None | Some(Outcome::Sealed)) {
            retry_at = None;
            signal.phase.store(IDLE, SeqCst);
        } else {
            retry_at = Some(Instant::now() + cfg.retry);
            signal.phase.store(BACKOFF, SeqCst);
        }
    }
}

/// One run: close and drain the gate, checkpoint, reopen.
fn run_once(shared: &Shared, cfg: &MaintenanceConfig, log_objects: u64, wal_bytes: u64) -> Outcome {
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
    shared.emit(Event::MaintenanceStarted {
        generation,
        log_objects,
        wal_bytes,
    });
    let start = Instant::now();
    let drained = shared.gate.close_and_drain(cfg.gate_wait);
    let gate_wait = start.elapsed();
    span.record("durable.gate_wait_ms", gate_wait.as_millis() as u64);
    let r = if drained {
        let _reopen = Reopen(&shared.gate);
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
            sealed,
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
    let Some(db) = shared.engine_db() else {
        return Err(error(Error::Config("the database is closed".into())));
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
