//! `telemetry::init` with no collector listening: commits neither fail nor slow down, and
//! dropping the guard stops the export promptly. Its own test binary, because `init`
//! installs the process's global subscriber.
use buckwal::store::MemStore;
use buckwal::telemetry::{init, TelemetryConfig};
use buckwal::{open_with_store, Config, Error, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};

const COMMITS: usize = 300;

/// The median time of `COMMITS` Off commits (each an INSERT) on a fresh MemStore database.
fn p50_commit(dir: &std::path::Path) -> Duration {
    let cfg = Config::new(dir, "http://127.0.0.1:1", "b", "k", "s");
    let db = open_with_store(cfg, Arc::new(MemStore::new())).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
        .unwrap();
    let mut times: Vec<Duration> = (0..COMMITS as i64)
        .map(|i| {
            let t = Instant::now();
            conn.execute("INSERT INTO t VALUES (?)", &[Value::from_i64(i)])
                .expect("a commit with the collector down");
            t.elapsed()
        })
        .collect();
    times.sort_unstable();
    times[COMMITS / 2]
}

#[test]
fn collector_down_never_slows_or_fails_commits() {
    let before = tempfile::tempdir().unwrap();
    let baseline = p50_commit(before.path());

    let guard = init(TelemetryConfig {
        enabled: true,
        endpoint: "http://127.0.0.1:1".into(),
        test_run_id: Some("otel-down".into()),
        ..TelemetryConfig::default()
    })
    .expect("init with the collector down");
    let after = tempfile::tempdir().unwrap();
    let exporting = p50_commit(after.path());
    let limit = baseline * 2 + Duration::from_millis(1);
    eprintln!("p50 before init {baseline:?}, after {exporting:?}, limit {limit:?}");
    assert!(
        exporting <= limit,
        "p50 {exporting:?} with export on, {baseline:?} before (limit {limit:?})"
    );

    let t = Instant::now();
    drop(guard);
    let stop = t.elapsed();
    eprintln!("guard dropped in {stop:?}");
    assert!(stop < Duration::from_millis(2500), "shutdown took {stop:?}");

    let again = init(TelemetryConfig {
        enabled: true,
        ..TelemetryConfig::default()
    });
    assert!(
        matches!(&again, Err(Error::Config(m)) if m.contains("already set")),
        "{again:?}"
    );
}
