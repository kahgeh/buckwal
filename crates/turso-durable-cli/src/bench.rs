//! `bench`: per-commit latency and bucket traffic, one `key=value` line per metric.
use crate::{config, err, s3_store, say, CliResult};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use turso_durable::store::fault::FaultStore;
use turso_durable::{Event, Value};

#[derive(Default)]
struct Counters {
    log_puts: AtomicU64,
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

fn ms(d: Duration) -> String {
    format!("{:.3}", d.as_secs_f64() * 1000.0)
}

fn pct(sorted: &[Duration], p: f64) -> Duration {
    let i = ((sorted.len() as f64 * p).ceil() as usize).clamp(1, sorted.len()) - 1;
    sorted[i]
}

pub fn run(dir: &Path, commits: u64) -> CliResult<()> {
    if commits == 0 {
        return Err("--commits must be at least 1".into());
    }
    let counters = Arc::new(Counters::default());
    let mut cfg = config(dir)?;
    let c = counters.clone();
    cfg.on_event = Some(Arc::new(move |e: &Event| match e {
        Event::LogUploaded { .. } => {
            c.log_puts.fetch_add(1, Relaxed);
        }
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
    let conn = db.connect().map_err(err)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS bench(id INTEGER PRIMARY KEY, payload BLOB)",
        &[],
    )
    .map_err(err)?;
    let log_before = counters.log_puts.load(Relaxed);
    let payload = Value::from_slice(&[0xabu8; 1024]).map_err(err)?;
    let mut lat = Vec::with_capacity(commits as usize);
    let started = Instant::now();
    for i in 0..commits {
        let t = Instant::now();
        conn.execute(
            "INSERT OR REPLACE INTO bench(id, payload) VALUES (?, ?)",
            &[Value::from_i64(i as i64), payload.clone()],
        )
        .map_err(err)?;
        lat.push(t.elapsed());
    }
    let total = started.elapsed();
    let log_puts = counters.log_puts.load(Relaxed) - log_before;

    conn.close().map_err(err)?;
    db.close().map_err(err)?;
    // The seal at close uploads pages, so count them after it.
    let page_puts = puts(&fault, "/pages/");

    let (slowest, max) = lat
        .iter()
        .enumerate()
        .max_by_key(|(_, d)| **d)
        .map(|(i, d)| (i, *d))
        .unwrap();
    let mut sorted = lat.clone();
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
    say(&format!("log_puts={log_puts}"));
    say(&format!(
        "log_puts_per_commit={:.3}",
        log_puts as f64 / commits as f64
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
