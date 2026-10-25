//! The packer loop ([`run_packer`]; spec "Packs and the packer" and Choice E) and the writer's
//! pack gauges (spec "Observability").
//!
//! The loop checks once a minute (`Config::pack_check_interval`) whether a pack run is due
//! ([`check`], [`due`]) and, when it is, makes one (`run::pack_run_in`). A run is due when at
//! least one generation has ended since the newest pack, and either the log since that pack
//! passes `pack_log_percent` of its size or the pack is older than `pack_max_age`. With no pack
//! at all, the first generation that ends makes the first run.
//!
//! A writer of a format 5 bucket runs the loop on its `buckwal-packer` thread when
//! `Config::pack` is on (`BUCKWAL_PACK`, the default), and the gauges on its
//! `buckwal-pack-gauges` thread in any case ([`PackerThreads`]). Both only read the bucket and
//! write packs, the pack pointer and their own working directory: they never take a lock or a
//! gate of the writer, so they never block a commit. Each waits a whole interval before its
//! first look, so a writer that is open for less than a minute makes no request for them. A
//! failed run, an error and a panic end one check only: the loop logs one WARN per distinct
//! failure (a failure like the one before it is only counted, on the next run's span) and
//! tries again at the next check. `Database` drop stops both threads and joins them: a thread
//! that waits stops at once, and a run in progress stops before its upload
//! (`run::pack_run_in`), or finishes its pointer write once its upload has started.
use super::run::{pack_run_in, PackOutcome, PackReport};
use crate::bucket::Counters;
use crate::clock::Stamp;
use crate::config::Config;
use crate::error::Result;
use crate::format::Keys;
use crate::store::{with_retry, ObjectStore, RetryPolicy};
use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tracing::field::Empty;

/// Why a pack run is due.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reason {
    /// No pack yet, and a generation has ended.
    FirstPack,
    /// The log since the newest pack passes `pack_log_percent` of the pack's size.
    LogPercent,
    /// The newest pack is at least `pack_max_age` old.
    MaxAge,
}

impl Reason {
    fn as_str(self) -> &'static str {
        match self {
            Reason::FirstPack => "first_pack",
            Reason::LogPercent => "log_percent",
            Reason::MaxAge => "max_age",
        }
    }
}

/// The newest pack, as the pointer names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NewestPack {
    pub size: u64,
    /// The pointer's age by the bucket's clock (its Last-Modified).
    pub age: Duration,
}

/// What the trigger decides on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Trigger {
    /// The head names a generation above the pack generation (above 1 without a pack): at
    /// least one generation ended since the newest pack.
    pub ended: bool,
    /// `None`: no pack of this database.
    pub pack: Option<NewestPack>,
    /// The log bytes in generations at or above the pack generation.
    pub log_bytes: u64,
}

/// Whether a pack run is due, and why (spec "Packs and the packer"): a generation ended since
/// the newest pack, and the log passes `log_percent` of the pack's size or the pack is
/// `max_age` old. Without a pack, the first ended generation is enough.
pub(crate) fn due(t: &Trigger, log_percent: u64, max_age: Duration) -> Option<Reason> {
    if !t.ended {
        return None;
    }
    let Some(p) = t.pack else {
        return Some(Reason::FirstPack);
    };
    if p.age >= max_age {
        return Some(Reason::MaxAge);
    }
    let share = u128::from(p.size) * u128::from(log_percent);
    (u128::from(t.log_bytes) * 100 > share).then_some(Reason::LogPercent)
}

/// The pointer as a check or a gauge reads it: one GET with its age.
struct PointerNow {
    /// The pack generation and size of this database's pointer, and the pointer's age.
    own: Option<(u64, NewestPack)>,
}

fn read_pointer_aged(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
) -> Result<PointerNow> {
    let key = keys.pack_pointer();
    let Some((obj, age)) = with_retry(retry, &key, |_| store.get_aged(&key))? else {
        return Ok(PointerNow { own: None });
    };
    // A pointer of another database is no pointer (spec rev 11, rule 3).
    let own = keys
        .read_pointer(&obj.body)?
        .map(|p| (p.pack_generation, NewestPack { size: p.size, age }));
    Ok(PointerNow { own })
}

/// The check the loop makes once a minute: the head (one GET), the pointer (one GET with its
/// age), and only when a generation ended and the pack is younger than `pack_max_age`, one
/// listing of the log with sizes. `None`: no run is due (or the bucket holds no database).
pub(crate) fn check(config: &Config, store: &dyn ObjectStore) -> Result<Option<Reason>> {
    let keys = config.keys()?.for_open();
    let retry = config.engine(crate::io::IoMode::Writer).retry;
    let head_key = keys.head();
    let Some(head) = with_retry(&retry, &head_key, |_| store.get(&head_key))? else {
        return Ok(None);
    };
    let head = keys.decode_head_open(&head.body)?;
    let pointer = read_pointer_aged(store, &keys, &retry)?;
    let from = pointer.own.map_or(1, |(g, _)| g);
    let mut t = Trigger {
        ended: head.generation > from,
        pack: pointer.own.map(|(_, p)| p),
        log_bytes: 0,
    };
    let listing_decides = t.ended && t.pack.is_some_and(|p| p.age < config.pack_max_age);
    if listing_decides {
        let root = keys.log_root();
        let listed = with_retry(&retry, &root, |_| store.list_sized(&root))?;
        t.log_bytes = log_bytes_from(&keys, &listed, from);
    }
    Ok(due(&t, config.pack_log_percent, config.pack_max_age))
}

fn log_bytes_from(keys: &Keys, listed: &[(String, u64)], from: u64) -> u64 {
    listed
        .iter()
        .filter(|(k, _)| keys.parse_log(k).is_some_and(|(g, _)| g >= from))
        .map(|(_, n)| n)
        .sum()
}

/// Run the packer loop against the bucket `config` names until `stop` is set: a check at
/// once, then one per minute, and a pack run when one is due (see the module docs). Returns
/// promptly after `stop`; a run in progress stops before its upload. `Err` only for a
/// configuration that cannot work (`Config::validate`, the key file); a failed check or run
/// is logged and tried again.
pub fn run_packer(config: &Config, stop: Arc<AtomicBool>) -> Result<()> {
    let store = config.s3_store()?;
    run_packer_with_store(config, store, stop)
}

#[doc(hidden)]
pub fn run_packer_with_store(
    config: &Config,
    store: Arc<dyn ObjectStore>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    config.validate()?;
    crate::encryption::load(config)?;
    packer_loop(config, &store, &stop, Instant::now());
    Ok(())
}

/// Sleep until `at`, or until `stop` is set. True when `at` came first.
fn wait_until(at: Instant, stop: &AtomicBool) -> bool {
    const SLICE: Duration = Duration::from_millis(20);
    loop {
        if stop.load(SeqCst) {
            return false;
        }
        let now = Instant::now();
        if now >= at {
            return true;
        }
        std::thread::sleep(SLICE.min(at - now));
    }
}

/// The failures in a row of a loop, logged once per distinct failure.
#[derive(Default)]
struct Failures {
    /// Failed runs and checks since the last success.
    count: u64,
    /// The last failure logged.
    last: Option<String>,
}

impl Failures {
    /// One more failure: a WARN `what` when it is not the one logged last.
    fn fail(&mut self, what: &str, kind: String, log: impl FnOnce(u64)) {
        self.count += 1;
        if self.last.as_deref() != Some(kind.as_str()) {
            log(self.count);
            self.last = Some(kind);
        } else {
            tracing::debug!(target: "buckwal", failures = self.count, "{what} (as before)");
        }
    }

    /// A success after failures: one INFO line.
    fn ok(&mut self, what: &str) {
        if self.count > 0 {
            tracing::info!(target: "buckwal", failures = self.count, "{what}");
        }
        self.count = 0;
        self.last = None;
    }
}

/// What a failed run is, for the rule "one WARN per distinct failure": its step, and its key
/// unless the key is the run's own pack (each run has its own run id).
fn failure_kind(step: &str, key: Option<&str>) -> String {
    match key {
        Some(k) if !k.contains("pack/") => format!("{step} {k}"),
        _ => step.to_string(),
    }
}

/// What an error is, for the same rule: its text without the parts that change from one try
/// to the next (a request id, and numbers such as attempt counts and times).
fn error_kind(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find("(request id ") {
        out.push_str(&rest[..at]);
        rest = rest[at..].split_once(')').map_or("", |(_, r)| r);
    }
    out.push_str(rest);
    let mut kind = String::with_capacity(out.len());
    for c in out.chars() {
        if c.is_ascii_digit() {
            if !kind.ends_with('#') {
                kind.push('#');
            }
        } else {
            kind.push(c);
        }
    }
    kind
}

/// The loop: a check at `first`, then one every `config.pack_check_interval`.
fn packer_loop(config: &Config, store: &Arc<dyn ObjectStore>, stop: &AtomicBool, first: Instant) {
    let mut failures = Failures::default();
    let mut next = first;
    while wait_until(next, stop) {
        next = Instant::now() + config.pack_check_interval;
        let n = failures.count;
        let r = catch_unwind(AssertUnwindSafe(|| check_and_run(config, store, stop, n)));
        match r {
            // No run is due: a failure before it does not continue.
            Ok(Ok(None)) => failures.ok("pack runs succeed again"),
            Ok(Ok(Some(report))) => match &report.outcome {
                PackOutcome::Failed { step, .. } if step == "stopped" => return,
                PackOutcome::Failed { step, key } => {
                    let kind = failure_kind(step, key.as_deref());
                    failures.fail("pack run failed", kind, |count| {
                        tracing::warn!(
                            target: "buckwal",
                            step = %step,
                            key = ?key,
                            error = ?report.error,
                            failures = count,
                            "pack run failed"
                        );
                    });
                }
                _ => failures.ok("pack runs succeed again"),
            },
            Ok(Err(e)) => failures.fail("pack check failed", error_kind(&e.to_string()), |count| {
                tracing::warn!(target: "buckwal", step = "error", error = %e, failures = count, "pack run failed");
            }),
            Err(panic) => {
                let what = panic_text(&panic);
                failures.fail("pack check panicked", format!("panic {what}"), |count| {
                    tracing::error!(target: "buckwal", error = %what, failures = count, "packer panicked");
                });
            }
        }
    }
}

fn panic_text(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown".into())
}

/// One check, and one run when it is due. `None`: no run was due.
fn check_and_run(
    config: &Config,
    store: &Arc<dyn ObjectStore>,
    stop: &AtomicBool,
    failures: u64,
) -> Result<Option<PackReport>> {
    let Some(reason) = check(config, &**store)? else {
        return Ok(None);
    };
    tracing::info!(target: "buckwal", reason = reason.as_str(), "pack run due");
    let origin = Stamp::now();
    let report = pack_run_in(
        config,
        store.clone(),
        Arc::new(move || origin.age()),
        stop,
        failures,
    )?;
    Ok(Some(report))
}

/// The writer's pack gauges (spec "Observability"): each interval, one GET of the pointer,
/// with its age by the bucket's clock. `behind_bytes` is the log bytes in generations at or
/// above the pack generation: the sizes one listing returned (at the first gauge), plus the
/// writer's own log objects since (`Counters::take_landed`), each counted once by its key.
struct Gauges {
    store: Arc<dyn ObjectStore>,
    keys: Keys,
    retry: RetryPolicy,
    counters: Arc<Counters>,
    /// The log objects at or above the last pack generation: (generation, seq) to bytes.
    /// `None` before the first gauge lists the log.
    log: Option<BTreeMap<(u64, u64), u64>>,
}

/// One gauge: the pointer's pack generation, its age (`None` without a pack) and the bytes
/// behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Gauge {
    pack: Option<(u64, NewestPack)>,
    behind_bytes: u64,
}

impl Gauges {
    fn measure(&mut self) -> Result<Gauge> {
        let log = match &mut self.log {
            Some(log) => log,
            None => {
                // Record from before the listing: an object that is in both is counted once,
                // by its key. A failed listing leaves the record on, and the next gauge lists.
                self.counters.track_landed();
                let root = self.keys.log_root();
                let listed = with_retry(&self.retry, &root, |_| self.store.list_sized(&root))?;
                let log = listed
                    .into_iter()
                    .filter_map(|(k, n)| self.keys.parse_log(&k).map(|pos| (pos, n)))
                    .collect();
                self.log.insert(log)
            }
        };
        for (g, s, n) in self.counters.take_landed() {
            log.insert((g, s), n);
        }
        let pointer = read_pointer_aged(&*self.store, &self.keys, &self.retry)?;
        let from = pointer.own.map_or(1, |(g, _)| g);
        // GC deletes the log below the pack generation: it is not behind any more.
        log.retain(|(g, _), _| *g >= from);
        Ok(Gauge {
            pack: pointer.own,
            behind_bytes: log.values().sum(),
        })
    }

    /// One gauge in its `buckwal.pack.gauges` span: `buckwal.pack.behind_bytes`,
    /// `buckwal.pack.age_seconds` and `buckwal.pack.generation` (both absent without a pack),
    /// `buckwal.pack.size` and `buckwal.outcome` (`ok`, or `error` and `integrity` as other
    /// spans).
    fn emit(&mut self) -> Result<()> {
        let span = tracing::info_span!(
            target: "buckwal",
            "buckwal.pack.gauges",
            buckwal.pack.behind_bytes = Empty,
            buckwal.pack.age_seconds = Empty,
            buckwal.pack.generation = Empty,
            buckwal.pack.size = Empty,
            buckwal.outcome = Empty,
        );
        let r = span.in_scope(|| self.measure());
        match &r {
            Ok(g) => {
                span.record("buckwal.pack.behind_bytes", g.behind_bytes);
                span.record("buckwal.pack.size", g.pack.map_or(0, |(_, p)| p.size));
                if let Some((generation, p)) = g.pack {
                    span.record("buckwal.pack.age_seconds", p.age.as_secs());
                    span.record("buckwal.pack.generation", generation);
                }
                span.record("buckwal.outcome", "ok");
            }
            Err(e) => {
                span.record("buckwal.outcome", e.outcome());
            }
        }
        r.map(|_| ())
    }
}

fn gauge_loop(mut gauges: Gauges, interval: Duration, stop: &AtomicBool) {
    let mut failures = Failures::default();
    let mut next = Instant::now() + interval;
    while wait_until(next, stop) {
        next = Instant::now() + interval;
        match catch_unwind(AssertUnwindSafe(|| gauges.emit())) {
            Ok(Ok(())) => failures.ok("pack gauges work again"),
            Ok(Err(e)) => failures.fail("pack gauges failed", error_kind(&e.to_string()), |count| {
                tracing::warn!(target: "buckwal", error = %e, failures = count, "pack gauges failed");
            }),
            Err(panic) => {
                let what = panic_text(&panic);
                failures.fail("pack gauges panicked", format!("panic {what}"), |count| {
                    tracing::error!(target: "buckwal", error = %what, failures = count, "pack gauges panicked");
                });
            }
        }
    }
}

/// The packer thread and the pack gauges of one writer open (see the module docs).
pub(crate) struct PackerThreads {
    stop: Arc<AtomicBool>,
    packer: Option<JoinHandle<()>>,
    gauges: Option<JoinHandle<()>>,
}

/// Spawn `f` as thread `name`, under the subscriber of the thread that opened the database.
fn spawn_named(name: &str, f: impl FnOnce() + Send + 'static) -> JoinHandle<()> {
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || tracing::dispatcher::with_default(&dispatch, f))
        .unwrap_or_else(|e| panic!("spawn {name} thread: {e}"))
}

impl PackerThreads {
    /// Start the gauges and, with `config.pack`, the packer thread, for a writer whose
    /// bucket `store` holds; `keys` know the database id, `counters` are its bucket's.
    pub(crate) fn spawn(
        config: &Config,
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        counters: Arc<Counters>,
    ) -> PackerThreads {
        let stop = Arc::new(AtomicBool::new(false));
        let packer = config.pack.then(|| {
            let (config, store, stop) = (config.clone(), store.clone(), stop.clone());
            spawn_named("buckwal-packer", move || {
                let first = Instant::now() + config.pack_check_interval;
                packer_loop(&config, &store, &stop, first);
            })
        });
        let gauges = Gauges {
            store,
            keys,
            retry: config.engine(crate::io::IoMode::Writer).retry,
            counters,
            log: None,
        };
        let interval = config.pack_gauge_interval;
        let s = stop.clone();
        let gauges = Some(spawn_named("buckwal-pack-gauges", move || {
            gauge_loop(gauges, interval, &s)
        }));
        PackerThreads {
            stop,
            packer,
            gauges,
        }
    }

    /// Ask both threads to stop, without waiting.
    pub(crate) fn signal_stop(&self) {
        self.stop.store(true, SeqCst);
    }

    /// Stop both threads and wait for them.
    pub(crate) fn join(mut self) {
        self.signal_stop();
        for t in [self.packer.take(), self.gauges.take()]
            .into_iter()
            .flatten()
        {
            // A panic is caught per check; a thread that still panicked left nothing behind
            // but its working directory, which the next run's sweep removes.
            let _ = t.join();
        }
    }

    #[cfg(test)]
    pub(crate) fn running(&self) -> (bool, bool) {
        (self.packer.is_some(), self.gauges.is_some())
    }
}

impl Drop for PackerThreads {
    fn drop(&mut self) {
        self.signal_stop();
    }
}

#[cfg(test)]
mod tests;
