//! Task 8: maintenance of an MVCC database (it measures the `.db-log`), and the stats.
use super::*;
use crate::config::{Event, EventHook};
use std::sync::Mutex;

type Seen = Arc<Mutex<Vec<Event>>>;

fn recorder() -> (EventHook, Seen) {
    let seen: Seen = Arc::default();
    let s = seen.clone();
    (
        Arc::new(move |e: &Event| s.lock().unwrap().push(e.clone())),
        seen,
    )
}

fn wait_event(seen: &Seen, what: &str, pred: impl Fn(&Event) -> bool) {
    let until = Instant::now() + Duration::from_secs(15);
    while !seen.lock().unwrap().iter().any(&pred) {
        assert!(
            Instant::now() < until,
            "no {what}: {:?}",
            seen.lock().unwrap()
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The local log's size after the CREATE TABLE and `n` inserts, measured on a database
/// without maintenance (the bytes of a commit do not vary).
fn log_len_after(n: i64) -> u64 {
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(mem(), mvcc(&dir), n);
    let len = read(&dir, "app.db-log").len() as u64;
    c.close().unwrap();
    db.close().unwrap();
    len
}

/// A config whose maintenance fires when the log passes `bytes` (no object-count measure).
fn maintained(dir: &Path, bytes: u64, hook: EventHook) -> Config {
    let mut c = mvcc(dir);
    c.maintenance_wal_bytes = bytes;
    c.on_event = Some(hook);
    c
}

#[test]
fn maintenance_checkpoints_mvcc_when_log_bytes_exceed_threshold() {
    // Reached by the fourth insert and not before.
    let threshold = log_len_after(3) + 1;
    assert!(log_len_after(4) >= threshold);
    let mem = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (hook, seen) = recorder();
    let (db, c) = mvcc_with_rows(mem.clone(), maintained(&dir, threshold, hook), 3);
    assert_eq!(db.generation(), 2, "not due yet");
    assert_eq!(db.shared.maintenance_measures().1, log_len_after(3));
    insert_c(&c, 4).unwrap();
    wait_event(&seen, "MaintenanceFinished", |e| {
        matches!(e, Event::MaintenanceFinished { .. })
    });
    {
        let ev = seen.lock().unwrap();
        let started = ev.iter().find_map(|e| match e {
            Event::MaintenanceStarted {
                generation,
                wal_bytes,
                ..
            } => Some((*generation, *wal_bytes)),
            _ => None,
        });
        let (generation, wal_bytes) = started.expect("started");
        assert_eq!(generation, 2);
        assert!(wal_bytes >= threshold, "{wal_bytes} < {threshold}");
        assert!(ev
            .iter()
            .any(|e| matches!(e, Event::MaintenanceFinished { generation: 2, .. })));
    }
    // Switched: generation +1 (no manifest), the new generation's log image empty, the local
    // log cut, and the measure back to 0.
    assert_eq!(db.generation(), 3);
    let b = bucket_image(&*mem);
    assert_eq!((b.head.generation, b.head.manifest), (3, None));
    assert_eq!(b.kinds, Vec::<u8>::new());
    assert!(b.log.is_empty());
    assert_eq!(read(&dir, "app.db-log"), Vec::<u8>::new());
    assert_eq!(db.shared.maintenance_measures().1, 0);
    assert_eq!(bucket_rows(&*mem, None), vec![1, 2, 3, 4]);
    // Commits go on into the new generation.
    insert_c(&c, 5).unwrap();
    assert_eq!(bucket_image(&*mem).kinds, [3]);
    assert_eq!(bucket_rows(&*mem, None), (1..=5).collect::<Vec<_>>());
    c.close().unwrap();
    db.close().unwrap();
}

/// The measure is the log's size on reopen.
#[test]
fn log_high_water_is_the_file_size_at_open() {
    let mem = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(mem.clone(), mvcc(&dir), 3);
    let len = read(&dir, "app.db-log").len() as u64;
    assert_eq!(db.shared.maintenance_measures().1, len);
    assert_eq!(
        db.shared.maintenance_measures().0,
        4,
        "kind-3 objects of the generation"
    );
    c.close().unwrap();
    db.close().unwrap();
    let on_disk = read(&dir, "app.db-log").len() as u64;
    let db = open_mvcc(mem, mvcc(&dir));
    assert_eq!(db.shared.maintenance_measures().1, on_disk);
    db.close().unwrap();
}

/// A seal published whose reply was lost makes the bucket look failing, so maintenance
/// skips; the next commit's sync adopts the head and is acked, and the next run seals.
#[test]
fn pending_seal_does_not_starve_maintenance() {
    let threshold = log_len_after(6);
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let (hook, seen) = recorder();
    let (db, c) = mvcc_with_rows(store.clone(), maintained(&dir, threshold + 1, hook), 6);
    assert_eq!(db.generation(), 2);
    // The seal of the run the seventh insert causes stores its head and loses the reply.
    store.only_keys_containing("/head");
    store.store_then_fail_next_puts(1);
    store.fail_puts_after(0);
    insert_c(&c, 7).unwrap();
    wait_event(
        &seen,
        "failed run",
        |e| matches!(e, Event::MaintenanceSkipped { reason, .. } if reason.starts_with("error")),
    );
    store.fail_puts_after(u64::MAX);
    store.store_then_fail_next_puts(0);
    store.only_keys_containing("");
    assert_eq!(head(&*store).generation, 3, "the head is published");
    assert_eq!(db.generation(), 2, "but not adopted");
    // The retry finds the bucket failing: skipped, with no commit to adopt the head yet.
    wait_event(
        &seen,
        "bucket_failing skip",
        |e| matches!(e, Event::MaintenanceSkipped { reason, .. } if reason.starts_with("bucket")),
    );
    assert_eq!(db.generation(), 2, "still pending");
    // A commit adopts it and is acked.
    let acked = db.upload_stats().commits_acked;
    insert_c(&c, 8).unwrap();
    assert_eq!(db.upload_stats().commits_acked, acked + 1);
    assert_eq!(db.generation(), 3, "the commit adopted the published head");
    // The next run seals.
    wait_event(&seen, "MaintenanceFinished", |e| {
        matches!(e, Event::MaintenanceFinished { .. })
    });
    assert_eq!(db.generation(), 4);
    assert_eq!(head(&*store).generation, 4);
    assert_eq!(bucket_rows(&*mem, None), (1..=8).collect::<Vec<_>>());
    assert_eq!(
        crash_rows(db, vec![c], &dir, None),
        (1..=8).collect::<Vec<_>>()
    );
}

/// An open `BEGIN CONCURRENT` transaction keeps the gate from draining: the run is skipped
/// as `gate_timeout`; once the transaction ends, a later run seals.
#[test]
fn open_begin_concurrent_makes_the_run_time_out_at_the_gate() {
    let threshold = log_len_after(3) + 1;
    let mem = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (hook, seen) = recorder();
    let mut cfg = maintained(&dir, threshold, hook);
    cfg.maintenance_gate_wait = Duration::from_millis(50);
    let (db, c) = mvcc_with_rows(mem.clone(), cfg, 3);
    let holder = db.connect().unwrap();
    holder.execute("BEGIN CONCURRENT", &[]).unwrap();
    holder.query("SELECT count(*) FROM t", &[]).unwrap();
    insert_c(&c, 4).unwrap();
    wait_event(
        &seen,
        "gate_timeout",
        |e| matches!(e, Event::MaintenanceSkipped { reason, .. } if reason == "gate_timeout"),
    );
    assert_eq!(db.generation(), 2);
    assert!(!db.shared.gate.is_closed(), "the gate is open again");
    // Others still commit meanwhile.
    insert_c(&c, 5).unwrap();
    holder.execute("COMMIT", &[]).unwrap();
    wait_event(&seen, "MaintenanceFinished", |e| {
        matches!(e, Event::MaintenanceFinished { .. })
    });
    assert_eq!(db.generation(), 3);
    assert_eq!(bucket_rows(&*mem, None), (1..=5).collect::<Vec<_>>());
    holder.close().unwrap();
    c.close().unwrap();
    db.close().unwrap();
}
