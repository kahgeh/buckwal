//! The writer's checkpoints (spec "The writer"), on the `buckwal-maintenance` thread.
//!
//! - PASSIVE, often: in a format 5 WAL database, once the generation's `-wal` holds
//!   `defaults::PASSIVE_CHECKPOINT_WAL_BYTES`, at most once per
//!   `defaults::PASSIVE_CHECKPOINT_INTERVAL`. It never closes the gate and never waits for a
//!   reader. When no reader holds the WAL, it backfills every frame, so the next commit
//!   restarts the WAL (a generation switch). The engine runs its own PASSIVE checkpoint after
//!   a commit only at 1000 frames past the backfill: a reader that held the newest frames at
//!   that time can keep the engine from a new try for 1000 more frames.
//! - The maintenance checkpoint, only at the WAL size limit (`maintenance_wal_bytes`): readers
//!   that never leave keep the engine from a WAL restart, so the WAL grows. The run closes the
//!   gate, waits for the calls in flight, and runs `checkpoint(TRUNCATE)`. The TRUNCATE ends
//!   the generation at `truncate(0)` through the same path as a user's checkpoint
//!   (`ShippingFile::truncate`), which flushes every batched commit first. In a format 5
//!   bucket that is a generation switch, which uploads no page; formats 1 to 4 still seal.
//!
//! - [`Gate`]: every public `Connection` call enters it around its engine work (an open
//!   explicit transaction stays inside until it ends). Maintenance closes it, waits for the
//!   calls in flight to leave (at most `gate_wait`), checkpoints, and opens it again.
//! - [`Maintenance`]: the thread. `Connection::call` pokes it after each call (a few atomic
//!   loads); it runs each checkpoint on its own engine connection.
//!
//! No maintenance run starts while the bucket looks unable to take a seal (the uploader is
//! degraded or its last upload failed): the gate would stay closed for the seal's whole retry
//! deadline. Nor during a takeover quiet (`Uploader::seal_quiet_remaining`): the seal would
//! wait it out with the gate closed; that run is retried once the quiet has passed. A run that
//! is skipped, cannot drain the gate, finds the engine busy or fails backs off for `retry`;
//! repeated gate timeouts and busy refusals double it up to `retry_max` (growth does not fix
//! their cause), and a failed run is retried sooner only once the `-wal` has grown by another
//! limit. Its outcome is an event, a `buckwal.maintenance` span and, on failure, a warning:
//! never an error to any caller. A PASSIVE checkpoint has no event and no span; a failed one
//! warns.
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

/// When the checkpoints run, from [`Config`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct MaintenanceConfig {
    /// The WAL size limit: a maintenance run once the generation's `-wal` reaches this many
    /// bytes; 0 = never.
    pub wal_bytes: u64,
    /// A PASSIVE checkpoint once the generation's `-wal` reaches this many bytes; 0 = never
    /// (formats 1 to 4, where each WAL restart seals, and MVCC).
    pub passive_wal_bytes: u64,
    /// The least time between two PASSIVE checkpoints.
    pub passive_interval: Duration,
    /// The longest a run holds the gate closed waiting for calls in flight.
    pub gate_wait: Duration,
    /// The least time between a skipped or failed run and the next.
    pub retry: Duration,
    /// The longest backoff (repeated gate timeouts and busy refusals double `retry`).
    pub retry_max: Duration,
}

impl MaintenanceConfig {
    /// The settings of `c`. `passive`: the writer runs PASSIVE checkpoints (a format 5 WAL
    /// database).
    pub(crate) fn from_config(c: &Config, passive: bool) -> MaintenanceConfig {
        MaintenanceConfig {
            wal_bytes: c.maintenance_wal_bytes,
            passive_wal_bytes: if passive {
                defaults::PASSIVE_CHECKPOINT_WAL_BYTES
            } else {
                0
            },
            passive_interval: defaults::PASSIVE_CHECKPOINT_INTERVAL,
            gate_wait: c.maintenance_gate_wait,
            retry: defaults::MAINTENANCE_RETRY,
            retry_max: defaults::MAINTENANCE_RETRY_MAX,
        }
    }

    /// One of the two checkpoints is on.
    pub(crate) fn enabled(&self) -> bool {
        self.wal_bytes > 0 || self.passive_wal_bytes > 0
    }

    /// The generation's `-wal` is at the WAL size limit: a maintenance run is due.
    pub(crate) fn due(&self, wal_bytes: u64) -> bool {
        self.wal_bytes > 0 && wal_bytes >= self.wal_bytes
    }

    /// The generation's `-wal` is large enough for a PASSIVE checkpoint (the interval aside).
    fn passive_due(&self, wal_bytes: u64) -> bool {
        self.passive_wal_bytes > 0 && wal_bytes >= self.passive_wal_bytes
    }

    /// The `-wal` bytes at which a run that failed at `wal_bytes` may be retried before its
    /// backoff ends: one more limit of growth.
    fn bypass_at(&self, wal_bytes: u64) -> u64 {
        wal_bytes.saturating_add(self.wal_bytes)
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
    /// While backing off after a failed run: the `-wal` bytes at which a poke retries at once
    /// (`u64::MAX`: wait out the backoff).
    bypass_wal: AtomicU64,
    /// The time from which the next PASSIVE checkpoint may run, in milliseconds after `epoch`.
    passive_after_ms: AtomicU64,
    epoch: Instant,
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
            bypass_wal: AtomicU64::new(u64::MAX),
            passive_after_ms: AtomicU64::new(0),
            epoch: Instant::now(),
            #[cfg(test)]
            panic_next: Default::default(),
            #[cfg(test)]
            checkpoint_panic_next: Default::default(),
        }
    }
}

impl Signal {
    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// The interval after the last PASSIVE checkpoint has passed.
    fn passive_allowed(&self) -> bool {
        self.now_ms() >= self.passive_after_ms.load(SeqCst)
    }

    /// A PASSIVE checkpoint (or a maintenance run) ended now: the next waits `interval`.
    fn passive_ran(&self, interval: Duration) {
        let after = self.now_ms().saturating_add(interval.as_millis() as u64);
        self.passive_after_ms.store(after, SeqCst);
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
pub(crate) enum Outcome {
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
            .name("buckwal-maintenance".into())
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

    /// After a call: wake the thread when the `-wal` is at the WAL size limit and no run is in
    /// progress, nor backing off (unless a failed run's `-wal` has grown by another limit
    /// since); or, with the thread idle, when a PASSIVE checkpoint is due and its interval
    /// has passed. Only atomic loads when nothing is due; it never blocks.
    pub(crate) fn poke(&self, wal_bytes: u64) {
        let s = &self.signal;
        let from = if self.cfg.due(wal_bytes) {
            match s.phase.load(SeqCst) {
                IDLE => IDLE,
                BACKOFF if wal_bytes >= s.bypass_wal.load(SeqCst) => BACKOFF,
                _ => return,
            }
        } else if self.cfg.passive_due(wal_bytes)
            && s.phase.load(SeqCst) == IDLE
            && s.passive_allowed()
        {
            IDLE
        } else {
            return;
        };
        // Only the caller that moves it to RUNNING wakes it.
        if s.phase
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
        let wal_bytes = shared.maintenance_wal_bytes();
        if !cfg.due(wal_bytes) {
            if cfg.passive_due(wal_bytes) && signal.passive_allowed() {
                run_passive(&shared);
                signal.passive_ran(cfg.passive_interval);
            }
            drop(shared);
            // Below the limit (a user's checkpoint or the engine restarted the WAL): no
            // maintenance run waits.
            held_off = 0;
            retry_at = None;
            signal.bypass_wal.store(u64::MAX, SeqCst);
            signal.phase.store(IDLE, SeqCst);
            continue;
        }
        let outcome = {
            // A panic must not end maintenance for good (the gate is reopened by `Reopen`).
            #[cfg(test)]
            let inject_panic = signal.panic_next.swap(false, SeqCst);
            #[cfg(not(test))]
            let inject_panic = false;
            let generation = shared.uploader.generation();
            let span = tracing::info_span!(
                target: "buckwal",
                "buckwal.maintenance",
                buckwal.generation = generation,
                buckwal.wal_bytes = wal_bytes,
                buckwal.gate_wait_ms = Empty,
                buckwal.outcome = Empty,
            );
            let _entered = span.enter();
            catch_unwind(AssertUnwindSafe(|| {
                let run = Measured {
                    span: &span,
                    generation,
                    wal_bytes,
                };
                run_once(&shared, cfg, run, inject_panic, signal)
            }))
            .unwrap_or_else(|_| {
                // The run may have emitted `MaintenanceStarted`: it gets an end event too.
                span.record("buckwal.outcome", Outcome::Error.as_str());
                tracing::warn!(
                    target: "buckwal",
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
        };
        // Dropped here, not at the loop's end: when it is the last handle, the database
        // closes on this thread (`Shared::finish` then does not join it).
        drop(shared);
        // The run checkpointed more than a PASSIVE checkpoint does.
        signal.passive_ran(cfg.passive_interval);
        let mut bypass_wal = u64::MAX;
        let delay = match outcome {
            Outcome::Sealed => None,
            Outcome::GateTimeout | Outcome::Busy => {
                held_off = held_off.saturating_add(1);
                Some(backoff(cfg.retry, held_off, cfg.retry_max))
            }
            Outcome::Error => {
                bypass_wal = cfg.bypass_at(wal_bytes);
                Some(cfg.retry)
            }
            Outcome::BucketFailing | Outcome::Closed => Some(cfg.retry),
            // Retried once the quiet has passed, not every `retry` meanwhile.
            Outcome::TakeoverQuiet(left) => Some(cfg.retry.max(left)),
        };
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
    wal_bytes: u64,
}

/// One run: unless the bucket is failing or a takeover quiet lasts, close and drain the gate,
/// checkpoint, reopen.
///
/// It runs inside its `buckwal.maintenance` `span`, which the caller made (for
/// `generation` and `wal_bytes`) and entered, so a panic's outcome is
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
        wal_bytes,
    } = run;
    let quiet = shared.uploader.seal_quiet_remaining();
    let skip = match shared.uploader.bucket_failing() {
        Some(reason) => Some((Outcome::BucketFailing, reason)),
        None if !quiet.is_zero() => Some((Outcome::TakeoverQuiet(quiet), "takeover quiet")),
        None => None,
    };
    if let Some((outcome, reason)) = skip {
        span.record("buckwal.outcome", outcome.as_str());
        tracing::info!(
            target: "buckwal",
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
    span.record("buckwal.gate_wait_ms", gate_wait.as_millis() as u64);
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
    span.record("buckwal.outcome", outcome.as_str());
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
                    target: "buckwal",
                    generation,
                    reason = %reason,
                    "maintenance checkpoint failed; retrying later"
                );
            } else {
                tracing::info!(
                    target: "buckwal",
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

/// `checkpoint(TRUNCATE)` on a fresh engine connection ([`engine_checkpoint`]).
fn checkpoint(shared: &Shared) -> Result<(), (Outcome, String)> {
    engine_checkpoint(
        shared,
        turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    )
}

/// The writer's PASSIVE checkpoint: no gate, and no wait for a reader (the engine copies only
/// the frames that no reader needs; a busy engine is `Busy` at once).
pub(crate) fn passive_checkpoint(shared: &Shared) -> Result<(), (Outcome, String)> {
    engine_checkpoint(
        shared,
        turso_core::CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    )
}

/// [`passive_checkpoint`] on the thread. A failure only warns: the next poke tries again after
/// the interval. A busy engine or a closing database is routine.
fn run_passive(shared: &Shared) {
    let generation = shared.uploader.generation();
    match passive_checkpoint(shared) {
        Ok(()) => {}
        Err((Outcome::Error, reason)) => tracing::warn!(
            target: "buckwal",
            generation,
            reason = %reason,
            "passive checkpoint failed; retrying later"
        ),
        Err((_, reason)) => tracing::debug!(
            target: "buckwal",
            generation,
            reason = %reason,
            "passive checkpoint skipped"
        ),
    }
}

/// A checkpoint in `mode` on a fresh engine connection (not a public `Connection`: not
/// counted, no gate; with the database's key, `EngineDb::connect`), set up as
/// `Connection::new` sets one up, inside an `Off` commit context.
fn engine_checkpoint(
    shared: &Shared,
    mode: turso_core::CheckpointMode,
) -> Result<(), (Outcome, String)> {
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
        .checkpoint(mode)
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
