//! `bench`: per-commit latency and bucket traffic, one `key=value` line per metric. With
//! `--connections N`, N threads (one connection each) share the commits.
use crate::args::BenchArgs;
use crate::{config, err, ms, s3_store, say, say_all, stats_lines, CliResult};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use turso_durable::store::fault::FaultStore;
use turso_durable::{Database, Event, UploadStats, Value};

#[derive(Default)]
struct Counters {
    seals: Mutex<Vec<(u64, Option<Duration>)>>,
    maintenance: Maintenance,
}

/// The maintenance checkpoint's runs, from its events.
#[derive(Default)]
struct Maintenance {
    /// `MaintenanceStarted`, `MaintenanceFinished` (sealed), `MaintenanceSkipped`.
    counts: Mutex<(u64, u64, u64)>,
    /// The longest `gate_wait` of a run that sealed.
    gate_wait_max: Mutex<Duration>,
    /// The highest log seq seen: the most log objects a generation held, which a restore
    /// of it would read.
    log_objects_max: AtomicU64,
}

impl Maintenance {
    fn record(&self, e: &Event) {
        let counts = || self.counts.lock().unwrap();
        match e {
            Event::LogUploaded { seq, .. } => {
                self.log_objects_max.fetch_max(*seq, SeqCst);
            }
            Event::MaintenanceStarted { .. } => counts().0 += 1,
            Event::MaintenanceFinished { gate_wait, .. } => {
                counts().1 += 1;
                let mut m = self.gate_wait_max.lock().unwrap();
                *m = (*m).max(*gate_wait);
            }
            Event::MaintenanceSkipped { .. } => counts().2 += 1,
            _ => {}
        }
    }

    fn log_objects_max(&self) -> u64 {
        self.log_objects_max.load(SeqCst)
    }

    fn lines(&self) -> Vec<String> {
        let (runs, sealed, skipped) = *self.counts.lock().unwrap();
        vec![
            format!("maintenance_runs={runs}"),
            format!("maintenance_sealed={sealed}"),
            format!("maintenance_skipped={skipped}"),
            format!(
                "maintenance_gate_wait_max_ms={}",
                ms(*self.gate_wait_max.lock().unwrap())
            ),
        ]
    }
}

/// PUTs the store saw whose key contains `needle`.
fn puts(fault: &FaultStore, needle: &str) -> usize {
    fault
        .events()
        .iter()
        .filter(|e| e.starts_with("put") && e.contains(needle))
        .count()
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    let i = ((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[i]
}

/// The counters of `after` minus those of `before`, ratios recomputed from them. The ack
/// waits are `after`'s: a histogram cannot be subtracted.
fn since(before: &UploadStats, after: &UploadStats) -> UploadStats {
    let log_puts = after.log_puts - before.log_puts;
    let commits_uploaded = after.commits_uploaded - before.commits_uploaded;
    let log_put_bytes = after.log_put_bytes - before.log_put_bytes;
    let per_put = |x: u64| {
        if log_puts == 0 {
            0.0
        } else {
            x as f64 / log_puts as f64
        }
    };
    UploadStats {
        commits_acked: after.commits_acked - before.commits_acked,
        commits_uploaded,
        log_puts,
        log_put_bytes,
        commits_per_log_put: per_put(commits_uploaded),
        bytes_per_log_put: per_put(log_put_bytes),
        ..*after
    }
}

/// The engine refused the statement as busy: with several Off writers it sometimes does
/// so before its busy timeout (with or without maintenance). Nothing was committed; the
/// statement is retried.
fn is_busy(e: &turso_durable::Error) -> bool {
    matches!(e, turso_durable::Error::Engine(m) if m.starts_with("database is busy"))
}

/// How long one commit may keep being refused as busy before bench gives up with the error.
const BUSY_GIVE_UP: Duration = Duration::from_secs(10);
/// The pause before retrying a commit refused as busy.
const BUSY_PAUSE: Duration = Duration::from_millis(10);

/// Run `op`; while it is refused as busy, pause and run it again (counting each retry in
/// `busy`), until `give_up` has passed since the first attempt. Then, or on any other
/// error, return the error.
fn retry_busy<T>(
    busy: &AtomicU64,
    give_up: Duration,
    pause: Duration,
    mut op: impl FnMut() -> Result<T, turso_durable::Error>,
) -> Result<T, turso_durable::Error> {
    let start = Instant::now();
    loop {
        match op() {
            Err(e) if is_busy(&e) && start.elapsed() < give_up => {
                busy.fetch_add(1, SeqCst);
                std::thread::sleep(pause);
            }
            r => return r,
        }
    }
}

/// Commit until `next` passes `commits` or another thread failed; returns (commit index,
/// latency) for each commit. A commit the engine refuses as busy is retried after a pause
/// (counted in `busy`) for up to `BUSY_GIVE_UP`; its latency runs from its first attempt.
fn commit_loop(
    db: &Database,
    commits: u64,
    next: &AtomicU64,
    failed: &AtomicBool,
    busy: &AtomicU64,
) -> CliResult<Vec<(u64, Duration)>> {
    let conn = db.connect().map_err(err)?;
    let payload = Value::from_slice(&[0xabu8; 1024]).map_err(err)?;
    let mut lat = Vec::new();
    let r = loop {
        let i = next.fetch_add(1, SeqCst);
        if i >= commits || failed.load(SeqCst) {
            break Ok(());
        }
        let t = Instant::now();
        let r = retry_busy(busy, BUSY_GIVE_UP, BUSY_PAUSE, || {
            conn.execute(
                "INSERT OR REPLACE INTO bench(id, payload) VALUES (?, ?)",
                &[Value::from_i64(i as i64), payload.clone()],
            )
        });
        if let Err(e) = r {
            failed.store(true, SeqCst);
            break Err(err(e));
        }
        lat.push((i, t.elapsed()));
    };
    r?;
    conn.close().map_err(err)?;
    Ok(lat)
}

pub fn run(a: &BenchArgs) -> CliResult<()> {
    let commits = a.commits;
    if commits == 0 {
        return Err("--commits must be at least 1".into());
    }
    let counters = Arc::new(Counters::default());
    let mut cfg = config(&a.dir)?;
    let c = counters.clone();
    cfg.on_event = Some(Arc::new(move |e: &Event| match e {
        Event::SealStarted { pages, .. } => c.seals.lock().unwrap().push((*pages, None)),
        Event::SealFinished { elapsed, .. } => {
            if let Some(last) = c.seals.lock().unwrap().last_mut() {
                last.1 = Some(*elapsed);
            }
        }
        e => c.maintenance.record(e),
    }));
    let fault = Arc::new(FaultStore::new(s3_store(&cfg)?));
    let db = turso_durable::open_with_store(cfg, fault.clone()).map_err(err)?;
    if let Some(b) = a.batching {
        db.set_batching(b).map_err(err)?;
    }
    let conn = db.connect().map_err(err)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS bench(id INTEGER PRIMARY KEY, payload BLOB)",
        &[],
    )
    .map_err(err)?;
    conn.close().map_err(err)?;
    let before = db.upload_stats();
    let next = AtomicU64::new(0);
    let failed = AtomicBool::new(false);
    let busy = AtomicU64::new(0);
    let started = Instant::now();
    let results: Vec<CliResult<Vec<(u64, Duration)>>> = std::thread::scope(|s| {
        let threads: Vec<_> = (0..a.connections)
            .map(|_| s.spawn(|| commit_loop(&db, commits, &next, &failed, &busy)))
            .collect();
        let joined = threads.into_iter().map(|t| t.join());
        joined
            .map(|r| r.unwrap_or_else(|_| Err("a bench thread panicked".into())))
            .collect()
    });
    let total = started.elapsed();
    let mut lat = Vec::with_capacity(commits as usize);
    for r in results {
        lat.extend(r?);
    }
    let stats = since(&before, &db.upload_stats());
    // Before the close-time seal.
    let log_objects_max = counters.maintenance.log_objects_max();

    db.close().map_err(err)?;
    // The seal at close uploads pages, so count them after it.
    let page_puts = puts(&fault, "/pages/");

    let (slowest, max) = lat.iter().copied().max_by_key(|(_, d)| *d).unwrap();
    let mut sorted: Vec<Duration> = lat.iter().map(|(_, d)| *d).collect();
    sorted.sort();
    let seals = counters.seals.lock().unwrap().clone();
    say(&format!("commits={commits}"));
    say(&format!("commit_p50_ms={}", ms(pct(&sorted, 0.50))));
    say(&format!("commit_p95_ms={}", ms(pct(&sorted, 0.95))));
    say(&format!("commit_p99_ms={}", ms(pct(&sorted, 0.99))));
    say(&format!("commit_max_ms={}", ms(max)));
    say(&format!("commit_max_at={}", slowest + 1));
    say(&format!("busy_retries={}", busy.load(SeqCst)));
    say(&format!(
        "commits_per_sec={:.1}",
        commits as f64 / total.as_secs_f64()
    ));
    say_all("", &stats_lines(&stats));
    say(&format!(
        "log_puts_per_commit={:.3}",
        stats.log_puts as f64 / commits as f64
    ));
    say(&format!("bucket_puts={}", puts(&fault, "")));
    say(&format!("page_puts={page_puts}"));
    say(&format!("seals={}", seals.len()));
    say_all("", &counters.maintenance.lines());
    say(&format!("generation_log_objects_max={log_objects_max}"));
    for (i, (pages, el)) in seals.iter().enumerate() {
        let t = el.map(ms).unwrap_or_else(|| "unfinished".into());
        say(&format!(
            "seal_{}_ms={t} seal_{}_pages={pages}",
            i + 1,
            i + 1
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_engine_busy_refusal_is_retried() {
        use turso_durable::Error;
        assert!(is_busy(&Error::Engine("database is busy (locked)".into())));
        assert!(!is_busy(&Error::Engine("no such table: bench".into())));
        assert!(!is_busy(&Error::Timeout("database is busy".into())));
    }

    #[test]
    fn busy_is_retried_with_a_pause_until_a_cap() {
        use turso_durable::Error;
        let busy_err = || Error::Engine("database is busy (locked)".into());
        // Busy twice, then Ok: two retries counted.
        let n = AtomicU64::new(0);
        let mut left = 2;
        let r = retry_busy(&n, Duration::from_secs(5), Duration::from_millis(1), || {
            if left > 0 {
                left -= 1;
                Err(busy_err())
            } else {
                Ok(7)
            }
        });
        assert_eq!((r, n.load(SeqCst)), (Ok(7), 2));
        // Busy for good: gives up with the error once the cap has passed, pausing between.
        let n = AtomicU64::new(0);
        let t = Instant::now();
        let r: Result<(), Error> = retry_busy(
            &n,
            Duration::from_millis(50),
            Duration::from_millis(5),
            || Err(busy_err()),
        );
        assert_eq!(r, Err(busy_err()));
        assert!(
            t.elapsed() >= Duration::from_millis(50),
            "{:?}",
            t.elapsed()
        );
        let tries = n.load(SeqCst);
        assert!(
            (1..=11).contains(&tries),
            "{tries} retries in 50 ms at 5 ms"
        );
        // Any other error is returned at once.
        let n = AtomicU64::new(0);
        let r: Result<(), Error> = retry_busy(&n, Duration::from_secs(5), Duration::ZERO, || {
            Err(Error::Engine("no such table: bench".into()))
        });
        assert!(r.is_err());
        assert_eq!(n.load(SeqCst), 0);
    }

    #[test]
    fn maintenance_figures_count_runs_seals_skips_and_the_longest_gate_wait() {
        let m = Maintenance::default();
        assert_eq!(
            m.lines(),
            vec![
                "maintenance_runs=0",
                "maintenance_sealed=0",
                "maintenance_skipped=0",
                "maintenance_gate_wait_max_ms=0.000",
            ]
        );
        let started = Event::MaintenanceStarted {
            generation: 1,
            log_objects: 1000,
            wal_bytes: 0,
        };
        let finished = |gate_ms: u64| Event::MaintenanceFinished {
            generation: 1,
            gate_wait: Duration::from_millis(gate_ms),
            elapsed: Duration::from_millis(100),
        };
        let skipped = Event::MaintenanceSkipped {
            generation: 2,
            reason: "busy".into(),
        };
        let uploaded = |seq: u64| Event::LogUploaded {
            generation: 1,
            seq,
            bytes: 10,
            commits: vec![seq],
        };
        assert_eq!(m.log_objects_max(), 0);
        for e in [
            &uploaded(5),
            &uploaded(9),
            &uploaded(1),
            &started,
            &finished(7),
            &started,
            &skipped,
            &started,
            &finished(42),
            &Event::BatchWaiting { commits: 1 },
        ] {
            m.record(e);
        }
        assert_eq!(
            m.lines(),
            vec![
                "maintenance_runs=3",
                "maintenance_sealed=2",
                "maintenance_skipped=1",
                "maintenance_gate_wait_max_ms=42.000",
            ]
        );
        assert_eq!(m.log_objects_max(), 9, "the highest log seq seen");
    }

    #[test]
    fn since_subtracts_counters_and_keeps_ack_waits() {
        let before = UploadStats {
            commits_acked: 1,
            commits_uploaded: 1,
            log_puts: 1,
            log_put_bytes: 100,
            commits_per_log_put: 1.0,
            bytes_per_log_put: 100.0,
            ..UploadStats::default()
        };
        let after = UploadStats {
            commits_acked: 11,
            commits_uploaded: 11,
            log_puts: 5,
            log_put_bytes: 900,
            commits_per_log_put: 2.2,
            bytes_per_log_put: 180.0,
            ack_wait_p50: Duration::from_millis(3),
            ack_wait_p95: Duration::from_millis(7),
            ack_wait_p99: Duration::from_millis(9),
        };
        let d = since(&before, &after);
        assert_eq!(
            (
                d.commits_acked,
                d.commits_uploaded,
                d.log_puts,
                d.log_put_bytes
            ),
            (10, 10, 4, 800)
        );
        assert_eq!((d.commits_per_log_put, d.bytes_per_log_put), (2.5, 200.0));
        assert_eq!(d.ack_wait_p99, Duration::from_millis(9));
        let none = since(&after, &after);
        assert_eq!(
            (none.commits_per_log_put, none.bytes_per_log_put),
            (0.0, 0.0)
        );
    }
}
