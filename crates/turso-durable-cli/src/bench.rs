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

/// Commit until `next` passes `commits` or another thread failed; returns (commit index,
/// latency) for each commit.
fn commit_loop(
    db: &Database,
    commits: u64,
    next: &AtomicU64,
    failed: &AtomicBool,
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
        let r = conn.execute(
            "INSERT OR REPLACE INTO bench(id, payload) VALUES (?, ?)",
            &[Value::from_i64(i as i64), payload.clone()],
        );
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
        _ => {}
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
    let started = Instant::now();
    let results: Vec<CliResult<Vec<(u64, Duration)>>> = std::thread::scope(|s| {
        let threads: Vec<_> = (0..a.connections)
            .map(|_| s.spawn(|| commit_loop(&db, commits, &next, &failed)))
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
