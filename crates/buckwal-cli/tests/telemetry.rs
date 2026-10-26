//! A test in its own binary because it sets the process's global `tracing` subscriber
//! (`buckwal::telemetry::init`), which no other test of a binary may then set. Run through
//! `./scripts/test.sh` (`--test-threads=1`).
mod common;

use common::*;
use std::time::Duration;

/// The writer's packer thread and its pack gauges, in process against RustFS, with the
/// check and gauge intervals cut from 60 s and 5 minutes to 1 s: the thread packs the ended
/// generation by itself, and the writer sends `buckwal.pack.gauges` spans before and after
/// the pack. Under `BUCKWAL_OTEL=1` (as `scripts/test.sh` runs it) the process exports them
/// under the run's `BUCKWAL_TEST_RUN_ID`; `scripts/otel-check.sh` then checks their
/// attributes and runs the alert queries "packer not keeping up" and "restore gave up" on
/// them. Without `BUCKWAL_OTEL` the test still runs and exports nothing.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn pack_gauges_spans_reach_clickhouse() {
    let guard = buckwal::telemetry::TelemetryConfig::from_env()
        .and_then(buckwal::telemetry::init)
        .expect("telemetry");
    let ctx = Ctx::new("pack_gauges_spans_reach_clickhouse");
    let mut cfg = ctx.config(&ctx.dir("a"));
    cfg.pack_check_interval = Duration::from_secs(1);
    cfg.pack_gauge_interval = Duration::from_secs(1);
    let mut w = Writer::open(cfg);
    w.insert_range(1..=20);
    w.seal();
    w.insert_range(21..=25);
    let pointer = format!("{}/pack-latest", ctx.prefix());
    let store = ctx.store();
    wait_until(
        "the writer's packer thread to pack",
        Duration::from_secs(30),
        || {
            use buckwal::store::ObjectStore;
            store.get(&pointer).expect("get the pointer").map(|_| ())
        },
    );
    // Gauges after the pack: at least three, as the alert needs.
    std::thread::sleep(Duration::from_millis(3500));
    w.insert_range(26..=30);
    let lines: Vec<String> = w.acked.iter().map(|id| format!("ack {id}")).collect();
    w.close().expect("close writer");
    let v = restore_verify(&ctx, &lines);
    eprintln!(
        "pack_gauges_spans_reach_clickhouse: BUCKWAL_OTEL={:?}, run id {:?}; acked={} {}",
        std::env::var("BUCKWAL_OTEL").ok(),
        std::env::var("BUCKWAL_TEST_RUN_ID").ok(),
        v.acked,
        v.restored
    );
    guard.shutdown();
}
