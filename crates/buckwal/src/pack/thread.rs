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
//! write packs, the pack pointer and the packer's own files in the pack directory
//! (`Config::pack_dir`, by default the writer's directory: the kept copy and its lock, or a
//! working directory): they never take a lock or a gate of the writer, so they never block a
//! commit. Each waits a whole interval before its
//! first look, so a writer that is open for less than a minute makes no request for them.
//!
//! A failed run, an error and a panic end one check only: the loop logs one WARN per distinct
//! failure (a failure like the one before it is only counted, on the next run's span). After a
//! failed run (or a run that returned an error or panicked) the loop backs off (spec rev 21):
//! the next check comes one interval later, then the wait doubles after each further failure,
//! up to `pack_max_age` ([`next_wait`]). A run that packs, finds nothing to do or is superseded
//! resets the wait. A check that fails before any run (the head or the pointer cannot be read)
//! keeps the one-minute cadence: it costs two small requests.
//!
//! `Database` drop stops both threads and joins them: a thread that waits wakes at once
//! (`Thread::unpark`), and a run in progress ends as `failed` at the step `closed`
//! (`run::pack_run_in`). A close is not a fault: it starts no back-off and logs no WARN.
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
        t.log_bytes = list_log_from(store, &keys, &retry, from)?.values().sum();
    }
    Ok(due(&t, config.pack_log_percent, config.pack_max_age))
}

/// One listing of the log of the generations from `from` on (`list_sized_after`, so the log
/// below the pack is not listed): the bytes per generation.
fn list_log_from(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    from: u64,
) -> Result<BTreeMap<u64, u64>> {
    let root = keys.log_root();
    // Every key of the generations below `from` sorts before this one.
    let after = format!("{}~", keys.log_prefix(from.saturating_sub(1)));
    let listed = with_retry(retry, &root, |_| store.list_sized_after(&root, &after))?;
    let mut per_generation = BTreeMap::new();
    for (k, n) in listed {
        if let Some((g, _)) = keys.parse_log(&k).filter(|(g, _)| *g >= from) {
            *per_generation.entry(g).or_default() += n;
        }
    }
    Ok(per_generation)
}

/// The keys of one listing of the log from `from` on, with their sizes (the gauge's first
/// merge needs the keys).
fn list_log_keys_from(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    from: u64,
) -> Result<Vec<((u64, u64), u64)>> {
    let root = keys.log_root();
    let after = format!("{}~", keys.log_prefix(from.saturating_sub(1)));
    let listed = with_retry(retry, &root, |_| store.list_sized_after(&root, &after))?;
    Ok(listed
        .into_iter()
        .filter_map(|(k, n)| keys.parse_log(&k).map(|pos| (pos, n)))
        .filter(|((g, _), _)| *g >= from)
        .collect())
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
    packer_loop(config, &store, &stop, Instant::now(), None);
    Ok(())
}

/// The longest a loop of `run_packer` sleeps before it looks at `stop` again: the caller's
/// flag (a signal handler, say) cannot wake it. A writer's threads are woken at once.
const STOP_POLL: Duration = Duration::from_millis(250);

/// Sleep until `at`, or until `stop` is set. True when `at` came first. The thread parks: the
/// writer's `PackerThreads::signal_stop` unparks it, and it looks at `stop` at least every
/// `STOP_POLL` for a flag that nobody unparks for.
fn wait_until(at: Instant, stop: &AtomicBool) -> bool {
    loop {
        if stop.load(SeqCst) {
            return false;
        }
        let now = Instant::now();
        if now >= at {
            return true;
        }
        std::thread::park_timeout(STOP_POLL.min(at - now));
    }
}

/// The wait before the next check after `failures` failed runs in a row: one `interval`
/// after none or one, then doubled after each further failure, up to `max` (`pack_max_age`),
/// and never below `interval`.
pub(crate) fn next_wait(interval: Duration, failures: u64, max: Duration) -> Duration {
    let doublings = failures.saturating_sub(1).min(32) as u32;
    interval
        .checked_mul(1u32 << doublings.min(31))
        .unwrap_or(Duration::MAX)
        .min(max)
        .max(interval)
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
fn failure_kind(step: &str, key: Option<&str>, pack_root: &str) -> String {
    match key {
        Some(k) if !k.starts_with(pack_root) => format!("{step} {k}"),
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

/// The loop: a check at `first`, then one every `config.pack_check_interval`, longer after
/// failed runs ([`next_wait`]).
fn packer_loop(
    config: &Config,
    store: &Arc<dyn ObjectStore>,
    stop: &AtomicBool,
    first: Instant,
    local_db: Option<&std::path::Path>,
) {
    let pack_root = config.keys_for(None).pack_root();
    let mut failures = Failures::default();
    // Failed runs in a row: the back-off counts them (`failed_runs_after`).
    let mut failed_runs = 0u64;
    let mut next = first;
    while wait_until(next, stop) {
        let started = Instant::now();
        #[cfg(test)]
        CHECK_STARTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((config.dir.clone(), started));
        let n = failures.count;
        let estimate = first_pack_estimate(local_db);
        let r = catch_unwind(AssertUnwindSafe(|| {
            check_and_run(config, store, stop, n, estimate)
        }));
        let checked = match r {
            // No run is due: a failure before it does not continue.
            Ok(Ok(None)) => {
                failures.ok("pack runs succeed again");
                Checked::NothingDue
            }
            Ok(Ok(Some(report))) => match &report.outcome {
                PackOutcome::Failed { step, .. } if step == CLOSED => return,
                PackOutcome::Failed { step, key } => {
                    let kind = failure_kind(step, key.as_deref(), &pack_root);
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
                    Checked::RunFailed
                }
                _ => {
                    failures.ok("pack runs succeed again");
                    Checked::Succeeded
                }
            },
            Ok(Err(e)) => {
                failures.fail("pack check failed", error_kind(&e.to_string()), |count| {
                    tracing::warn!(target: "buckwal", step = "error", error = %e, failures = count, "pack run failed");
                });
                // An error of the run itself (not of the check) backs off as a failed run.
                match e {
                    RunError::Run(_) => Checked::RunFailed,
                    RunError::Check(_) => Checked::CheckFailed,
                }
            }
            Err(panic) => {
                let what = panic_text(&panic);
                failures.fail("pack check panicked", format!("panic {what}"), |count| {
                    tracing::error!(target: "buckwal", error = %what, failures = count, "packer panicked");
                });
                Checked::RunFailed
            }
        };
        if stop.load(SeqCst) {
            return;
        }
        let before = failed_runs;
        failed_runs = failed_runs_after(failed_runs, checked);
        let wait = next_wait(config.pack_check_interval, failed_runs, config.pack_max_age);
        if failed_runs == 2 && before == 1 {
            tracing::info!(
                target: "buckwal",
                wait_secs = wait.as_secs_f64(),
                max_secs = config.pack_max_age.as_secs_f64(),
                "pack runs back off"
            );
        }
        next = started + wait;
    }
}

/// Tests only: the directory of each loop's packer and the time it started each check.
#[cfg(test)]
pub(crate) static CHECK_STARTS: std::sync::Mutex<Vec<(std::path::PathBuf, Instant)>> =
    std::sync::Mutex::new(Vec::new());

/// How one check of the loop ended, for the back-off.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Checked {
    /// No run was due.
    NothingDue,
    /// A run packed, found nothing to do or was superseded.
    Succeeded,
    /// A run ended as `failed` (not `closed`), returned an error or panicked.
    RunFailed,
    /// The check itself failed (the head or the pointer could not be read): no run started.
    CheckFailed,
}

/// The failed runs in a row after a check that ended as `c`. A failed check leaves the count
/// as it is: it downloads nothing, and a run failure, a check failure and a run failure still
/// back off.
pub(crate) fn failed_runs_after(prev: u64, c: Checked) -> u64 {
    match c {
        Checked::RunFailed => prev.saturating_add(1),
        Checked::CheckFailed => prev,
        Checked::NothingDue | Checked::Succeeded => 0,
    }
}

/// The size to check the free disk against for a database with no pack yet: the size of the
/// writer's local database file `db` (`None` for `buckwal pack` and `buckwal packer`, which
/// have no local database, and for a missing file).
pub(crate) fn first_pack_estimate(db: Option<&std::path::Path>) -> Option<u64> {
    std::fs::metadata(db?).ok().map(|m| m.len())
}

/// The step of a run that a close ended (`run::pack_run_in`).
const CLOSED: &str = super::run::CLOSED_STEP;

/// Why `check_and_run` returned no report: the check failed, or the run did.
enum RunError {
    Check(crate::error::Error),
    Run(crate::error::Error),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Check(e) | RunError::Run(e) => e.fmt(f),
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
    first_pack_estimate: Option<u64>,
) -> std::result::Result<Option<PackReport>, RunError> {
    let Some(reason) = check(config, &**store).map_err(RunError::Check)? else {
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
        first_pack_estimate,
    )
    .map_err(RunError::Run)?;
    Ok(Some(report))
}

/// The writer's pack gauges (spec "Observability"): each interval, one GET of the pointer,
/// with its age by the bucket's clock. `behind_bytes` is the log bytes in generations at or
/// above the pack generation. The first gauge lists the log once, from the pack generation on,
/// and merges the writer's own log objects that landed meanwhile by their keys
/// (`Counters::take_positions`). After that it keeps one sum per generation and adds the
/// writer's own uploads to their generation (`Counters::take_sums`). When the pointer moves
/// to a newer pack, it drops the generations below the new pack generation.
struct Gauges {
    store: Arc<dyn ObjectStore>,
    keys: Keys,
    retry: RetryPolicy,
    counters: Arc<Counters>,
    /// The log bytes per generation, at or above the last pack generation. `None` before
    /// the first listing.
    log: Option<BTreeMap<u64, u64>>,
}

/// One gauge: the pointer's pack generation and newest pack (`None` without a pack) and the
/// bytes behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Gauge {
    pack: Option<(u64, NewestPack)>,
    behind_bytes: u64,
}

impl Gauges {
    fn measure(&mut self) -> Result<Gauge> {
        let first = self.log.is_none();
        if first {
            // Record from before the listing: an object that is in both is counted once, by
            // its key.
            self.counters.track_landed();
        }
        let pointer = read_pointer_aged(&*self.store, &self.keys, &self.retry)?;
        let from = pointer.own.map_or(1, |(g, _)| g);
        if first {
            let listed = list_log_keys_from(&*self.store, &self.keys, &self.retry, from)?;
            let Some(own) = self.counters.take_positions() else {
                // More of the writer's own objects than the record keeps: list again.
                return Err(crate::error::Error::Bucket(
                    "pack gauges: the log grew too much during the first listing".into(),
                ));
            };
            let mut seen: std::collections::BTreeSet<(u64, u64)> = Default::default();
            let mut log = BTreeMap::new();
            for (pos, n) in listed {
                seen.insert(pos);
                *log.entry(pos.0).or_default() += n;
            }
            for (g, seq, n) in own {
                if seen.insert((g, seq)) {
                    *log.entry(g).or_default() += n;
                }
            }
            self.log = Some(log);
        }
        let log = self.log.as_mut().expect("listed above");
        for (g, n) in self.counters.take_sums() {
            *log.entry(g).or_default() += n;
        }
        // The pack holds the generations below its pack generation: they are not behind.
        log.retain(|g, _| *g >= from);
        Ok(Gauge {
            pack: pointer.own,
            behind_bytes: log.values().sum(),
        })
    }

    /// One gauge in its `buckwal.pack.gauges` span: `buckwal.pack.behind_bytes`, and with a
    /// pack `buckwal.pack.age_seconds`, `buckwal.pack.size_bytes` (the newest pack's size,
    /// from the pointer) and `buckwal.pack.generation`, then `buckwal.outcome` (`ok`, or
    /// `error` and `integrity` as other spans).
    fn emit(&mut self) -> Result<()> {
        let span = tracing::info_span!(
            target: "buckwal",
            "buckwal.pack.gauges",
            buckwal.pack.behind_bytes = Empty,
            buckwal.pack.age_seconds = Empty,
            buckwal.pack.size_bytes = Empty,
            buckwal.pack.generation = Empty,
            buckwal.outcome = Empty,
        );
        let r = span.in_scope(|| self.measure());
        match &r {
            Ok(g) => {
                span.record("buckwal.pack.behind_bytes", g.behind_bytes);
                if let Some((generation, p)) = g.pack {
                    span.record("buckwal.pack.age_seconds", p.age.as_secs());
                    span.record("buckwal.pack.size_bytes", p.size);
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
                let db = config.dir.join(crate::files::DB_FILE);
                packer_loop(&config, &store, &stop, first, Some(&db));
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

    /// Ask both threads to stop, without waiting: a thread that waits wakes at once.
    pub(crate) fn signal_stop(&self) {
        self.stop.store(true, SeqCst);
        for t in [&self.packer, &self.gauges].into_iter().flatten() {
            t.thread().unpark();
        }
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
