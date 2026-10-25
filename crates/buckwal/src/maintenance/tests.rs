use super::*;
use crate::format::LogFile;
use std::sync::atomic::AtomicUsize;
use std::sync::{mpsc, Mutex};
use std::thread;

/// Maintenance threads (of any test) that ended in a panic.
static THREAD_PANICS: AtomicUsize = AtomicUsize::new(0);

/// Dropped as a maintenance thread ends: counts it in `THREAD_PANICS` if it panicked.
pub(super) struct PanicCount;

impl Drop for PanicCount {
    fn drop(&mut self) {
        if thread::panicking() {
            THREAD_PANICS.fetch_add(1, SeqCst);
        }
    }
}

/// How long a thread blocked at the gate is given to (wrongly) get through.
const BLOCKED: Duration = Duration::from_millis(100);

#[test]
fn gate_blocks_new_calls_and_drains_in_flight() {
    let gate = Arc::new(Gate::default());

    // One call in flight; draining waits for it.
    let held = gate.enter();
    let (g2, (tx, rx)) = (gate.clone(), mpsc::channel());
    let drainer = thread::spawn(move || {
        let r = g2.close_and_drain(Duration::from_secs(10));
        tx.send(()).unwrap();
        r
    });
    // The gate closes while `held` is still in flight: a new call blocks.
    let until = Instant::now() + Duration::from_secs(5);
    while !gate.is_closed() {
        assert!(Instant::now() < until, "the gate never closed");
        thread::sleep(Duration::from_millis(1));
    }
    let (g3, (in_tx, in_rx)) = (gate.clone(), mpsc::channel());
    let newcomer = thread::spawn(move || {
        let guard = g3.enter();
        in_tx.send(()).unwrap();
        drop(guard);
    });
    assert!(
        in_rx.recv_timeout(BLOCKED).is_err(),
        "entered a closed gate"
    );
    assert!(
        rx.recv_timeout(BLOCKED).is_err(),
        "drained with a call in flight"
    );

    drop(held);
    assert!(drainer.join().unwrap(), "drained once the call left");
    assert!(
        in_rx.recv_timeout(BLOCKED).is_err(),
        "entered before maintenance reopened the gate"
    );
    gate.open();
    in_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    newcomer.join().unwrap();

    // A guard that stays held: false after `wait`, and the gate is open again.
    let held = gate.enter();
    let t = Instant::now();
    assert!(!gate.close_and_drain(Duration::from_millis(50)));
    assert!(
        t.elapsed() >= Duration::from_millis(50),
        "{:?}",
        t.elapsed()
    );
    assert!(!gate.is_closed());
    let (g4, (tx, rx)) = (gate.clone(), mpsc::channel());
    let after = thread::spawn(move || {
        let _g = g4.enter();
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(5))
        .expect("a reopened gate lets calls in");
    after.join().unwrap();
    drop(held);

    // Nothing in flight: drained at once.
    assert!(gate.close_and_drain(Duration::from_millis(1)));
    gate.open();
}

/// The WAL size limit alone makes a maintenance run due (0 turns it off); the PASSIVE
/// threshold alone makes a PASSIVE checkpoint due (0, formats 1 to 4 and MVCC, turns it off).
#[test]
fn due_follows_the_thresholds() {
    let cfg = |wal_bytes, passive_wal_bytes| MaintenanceConfig {
        wal_bytes,
        passive_wal_bytes,
        passive_interval: Duration::from_secs(1),
        gate_wait: Duration::from_millis(500),
        retry: Duration::from_secs(1),
        retry_max: Duration::from_secs(30),
    };
    let c = cfg(1 << 20, 0);
    assert!(c.enabled());
    assert!(!c.due((1 << 20) - 1));
    assert!(c.due(1 << 20));
    assert!(!c.passive_due(u64::MAX));
    assert_eq!(c.bypass_at(7), 7 + (1 << 20));
    let c = cfg(0, 4 << 20);
    assert!(c.enabled() && !c.due(u64::MAX));
    assert!(!c.passive_due((4 << 20) - 1) && c.passive_due(4 << 20));
    let c = cfg(0, 0);
    assert!(!c.enabled() && !c.due(u64::MAX) && !c.passive_due(u64::MAX));
    // From the settings: PASSIVE only when the caller says so.
    let s = crate::config::Config::new("/d", "http://e", "b", "k", "s");
    let on = MaintenanceConfig::from_config(&s, true);
    assert_eq!(
        (on.wal_bytes, on.passive_wal_bytes, on.passive_interval),
        (64 << 20, 4 << 20, Duration::from_secs(1))
    );
    assert_eq!(
        MaintenanceConfig::from_config(&s, false).passive_wal_bytes,
        0
    );
}

/// The maintenance thread can end up holding the last handle (the app dropped everything
/// while a run was starting): the database then closes on that thread, which must not join
/// itself, and the directory is released.
#[test]
fn last_handle_dropped_on_the_maintenance_thread_closes_the_database() {
    use crate::config::{Config, Event, EventHook};
    use crate::store::MemStore;
    use crate::Value;

    let dir = tempfile::tempdir().unwrap();
    let (started_tx, started_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let go_rx = Mutex::new(go_rx);
    let started_tx = Mutex::new(started_tx);
    let hook: EventHook = Arc::new(move |e: &Event| {
        if matches!(e, Event::MaintenanceStarted { .. }) {
            let _ = started_tx.lock().unwrap().send(());
            let _ = go_rx.lock().unwrap().recv_timeout(Duration::from_secs(10));
        }
    });
    let mut c = Config::new(dir.path(), "http://127.0.0.1:1", "b", "k", "s");
    c.on_event = Some(hook);
    c.maintenance_wal_bytes = 16 << 10;
    let store = Arc::new(MemStore::new());
    let db = crate::open_with_store(c.clone(), store.clone()).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x)", &[]).unwrap();
    for x in 0..10 {
        conn.execute("INSERT INTO t VALUES (?)", &[Value::from_i64(x)])
            .unwrap();
    }
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    // The run holds the only other handle now.
    drop(conn);
    drop(db);
    go_tx.send(()).unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    c.on_event = None;
    let db = loop {
        match crate::open_with_store(c.clone(), store.clone()) {
            Ok(db) => break db,
            Err(e) => {
                assert!(Instant::now() < until, "never released: {e:?}");
                thread::sleep(Duration::from_millis(5));
            }
        }
    };
    let conn = db.connect().unwrap();
    let n = conn.query("SELECT count(*) FROM t", &[]).unwrap()[0][0].as_int();
    assert_eq!(n, Some(10));
    // A thread that joined itself panics in `Shared::finish`, after the directory is
    // released; give its unwinding time to finish.
    thread::sleep(Duration::from_millis(200));
    assert_eq!(
        THREAD_PANICS.load(SeqCst),
        0,
        "a maintenance thread panicked"
    );
}

fn open_small(dir: &std::path::Path, hook: crate::config::EventHook) -> crate::Database {
    let mut c = crate::config::Config::new(dir, "http://127.0.0.1:1", "b", "k", "s");
    c.on_event = Some(hook);
    c.maintenance_wal_bytes = 16 << 10;
    crate::open_with_store(c, Arc::new(crate::store::MemStore::new())).unwrap()
}

fn recorder() -> (
    crate::config::EventHook,
    Arc<Mutex<Vec<crate::config::Event>>>,
) {
    let seen: Arc<Mutex<Vec<crate::config::Event>>> = Arc::default();
    let s = seen.clone();
    (
        Arc::new(move |e: &crate::config::Event| s.lock().unwrap().push(e.clone())),
        seen,
    )
}

fn wait_event(
    seen: &Mutex<Vec<crate::config::Event>>,
    pred: impl Fn(&crate::config::Event) -> bool,
) {
    let until = Instant::now() + Duration::from_secs(10);
    while !seen.lock().unwrap().iter().any(&pred) {
        assert!(
            Instant::now() < until,
            "no event: {:?}",
            seen.lock().unwrap()
        );
        thread::sleep(Duration::from_millis(2));
    }
}

/// A run that panics is caught: the thread backs off and a later run seals.
#[test]
fn a_panicking_run_backs_off_and_maintenance_goes_on() {
    use crate::config::Event;
    let dir = tempfile::tempdir().unwrap();
    let (hook, seen) = recorder();
    let db = open_small(dir.path(), hook);
    db.shared.maintenance().unwrap().panic_next_run();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x)", &[]).unwrap();
    for x in 0..10 {
        conn.execute("INSERT INTO t VALUES (?)", &[crate::Value::from_i64(x)])
            .unwrap();
    }
    wait_event(&seen, |e| matches!(e, Event::MaintenanceFinished { .. }));
    assert_eq!(THREAD_PANICS.load(SeqCst), 0, "the thread died");
    // The panicking run started, so it has an end event: a `panic` skip.
    let ev = seen.lock().unwrap().clone();
    let first_run: Vec<&Event> = ev
        .iter()
        .filter(|e| {
            matches!(
                e,
                Event::MaintenanceStarted { .. }
                    | Event::MaintenanceSkipped { .. }
                    | Event::MaintenanceFinished { .. }
            )
        })
        .take(2)
        .collect();
    assert!(
        matches!(
            first_run[..],
            [
                Event::MaintenanceStarted { .. },
                Event::MaintenanceSkipped { reason, .. }
            ] if reason == "panic"
        ),
        "{ev:?}"
    );
}

/// A panic inside the checkpoint, gate closed, still reopens the gate (`Reopen`): the run
/// is a `panic` skip and a new connection's write goes through.
#[test]
fn gate_reopens_when_the_checkpoint_panics() {
    use crate::config::Event;
    let dir = tempfile::tempdir().unwrap();
    let (hook, seen) = recorder();
    let db = open_small(dir.path(), hook);
    db.shared.maintenance().unwrap().panic_next_checkpoint();
    // Everything that could block on a gate left closed runs on this thread, so a failure
    // is an assertion, not a hang.
    let (loaded_tx, loaded_rx) = mpsc::channel();
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x)", &[]).unwrap();
        for x in 0..10 {
            conn.execute("INSERT INTO t VALUES (?)", &[crate::Value::from_i64(x)])
                .unwrap();
        }
        loaded_tx.send(()).unwrap();
        wait_event(
            &seen,
            |e| matches!(e, Event::MaintenanceSkipped { reason, .. } if reason == "panic"),
        );
        let c = db.connect().unwrap();
        let started = Instant::now();
        let r = c.execute("INSERT INTO t VALUES (99)", &[]);
        tx.send((r.map(|_| ()), started.elapsed())).unwrap();
        drop((c, conn, db));
    });
    loaded_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the gate stayed closed after the checkpoint panicked");
    let (r, took) = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("no `panic` skip, or the gate stayed closed");
    r.unwrap();
    assert!(took < Duration::from_secs(1), "{took:?}");
}

/// A commit's ack wait happens after it leaves the gate (`Connection::call`), so a Window
/// commit waiting out a slow upload does not hold maintenance off. Moving `await_acks`
/// inside the gate makes the drain below fail.
#[test]
fn ack_wait_runs_outside_the_gate() {
    use crate::batching::Batching;
    let dir = tempfile::tempdir().unwrap();
    let fault = Arc::new(crate::store::FaultStore::new(Arc::new(
        crate::store::MemStore::new(),
    )));
    let mut c = crate::config::Config::new(dir.path(), "http://127.0.0.1:1", "b", "k", "s");
    c.maintenance_wal_bytes = 0;
    c.batching = Batching::Window {
        max_wait: Duration::from_millis(1),
        max_commits: 64,
        max_bytes: 1 << 20,
    };
    let db = crate::open_with_store(c, fault.clone()).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x)", &[]).unwrap();
    fault.only_keys_containing("log/");
    fault.delay_puts(Duration::from_secs(1));
    let handle = thread::spawn(move || {
        let r = conn.execute("INSERT INTO t VALUES (1)", &[]);
        (r.map(|_| ()), conn)
    });
    thread::sleep(Duration::from_millis(200));
    assert!(
        !handle.is_finished(),
        "the commit finished before the delayed put"
    );
    let drained = db.shared.gate.close_and_drain(Duration::from_millis(100));
    db.shared.gate.open();
    assert!(
        drained,
        "the gate could not drain while the commit awaited its ack"
    );
    let (r, _conn) = handle.join().unwrap();
    r.unwrap();
}

/// A run that finds the database closing (`Database::close` took the engine) is a skip,
/// `closed`, not an error.
#[test]
fn a_run_racing_close_is_skipped_as_closed() {
    use crate::config::Event;
    let dir = tempfile::tempdir().unwrap();
    let (started_tx, started_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (go_rx, started_tx) = (Mutex::new(go_rx), Mutex::new(started_tx));
    let seen: Arc<Mutex<Vec<Event>>> = Arc::default();
    let s = seen.clone();
    let hook: crate::config::EventHook = Arc::new(move |e: &Event| {
        s.lock().unwrap().push(e.clone());
        if matches!(e, Event::MaintenanceStarted { .. }) {
            let _ = started_tx.lock().unwrap().send(());
            let _ = go_rx.lock().unwrap().recv_timeout(Duration::from_secs(10));
        }
    });
    let db = open_small(dir.path(), hook);
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x)", &[]).unwrap();
    for x in 0..10 {
        conn.execute("INSERT INTO t VALUES (?)", &[crate::Value::from_i64(x)])
            .unwrap();
    }
    started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    conn.close().unwrap();
    let closer = thread::spawn(move || db.close());
    // `close` takes the engine at once, then waits for the run (held in the hook) to end.
    // (Polling `connect` here would make `close` refuse: a connection would be open.)
    thread::sleep(Duration::from_millis(300));
    go_tx.send(()).unwrap();
    closer.join().unwrap().unwrap();
    let ev = seen.lock().unwrap().clone();
    let skipped: Vec<&String> = ev
        .iter()
        .filter_map(|e| match e {
            Event::MaintenanceSkipped { reason, .. } => Some(reason),
            _ => None,
        })
        .collect();
    assert_eq!(skipped, vec!["closed"], "{ev:?}");
}

#[test]
fn backoff_doubles_up_to_the_cap() {
    let (r, cap) = (Duration::from_secs(1), Duration::from_secs(30));
    let got: Vec<u64> = (1..=8).map(|n| backoff(r, n, cap).as_secs()).collect();
    assert_eq!(got, vec![1, 2, 4, 8, 16, 30, 30, 30]);
    assert_eq!(backoff(r, u32::MAX, cap), cap);
}

/// During a takeover quiet, a due generation is not sealed: the run is skipped with reason
/// `takeover quiet` before it closes the gate, so statements on other connections never wait
/// at it. Once the quiet has passed, maintenance seals.
#[test]
fn maintenance_skips_during_takeover_quiet() {
    use crate::config::{Config, Event};
    use crate::format::{encode_record, LocalState, LogRecord};
    use crate::store::{MemStore, ObjectStore};
    use crate::Value;

    let store = Arc::new(MemStore::new());
    // The old writer: rows, closed, then its seal-start record at the next seq, as if its
    // next seal had started and its process were gone with the page PUTs in flight.
    let a = tempfile::tempdir().unwrap();
    let mut ca = Config::new(a.path(), "http://127.0.0.1:1", "b", "k", "s");
    // Its head carries these: the takeover quiet is the larger of both writers' sums.
    ca.lease = Duration::from_secs(1);
    ca.request_timeout = Duration::from_millis(500);
    let db = crate::open_with_store(ca.clone(), store.clone()).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x)", &[]).unwrap();
    conn.close().unwrap();
    db.close().unwrap();
    let keys = ca.keys().unwrap();
    let gen = db_generation(&*store, &keys);
    let next = store.list(&keys.log_prefix(gen)).unwrap().len() as u64 + 1;
    let rec = LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: LocalState::read(a.path()).unwrap().unwrap().writer_id,
        syncs: Vec::new(),
        next_generation: None,
    };
    store
        .put_if_absent(&keys.log(gen, next), &encode_record(&rec))
        .unwrap();

    // B: a quiet of lease 2 s + request timeout 500 ms.
    let bt = tempfile::tempdir().unwrap();
    let b_dir = bt.path().join("db");
    let mut cb = Config::new(&b_dir, "http://127.0.0.1:1", "b", "k", "s");
    cb.lease = Duration::from_secs(2);
    cb.request_timeout = Duration::from_millis(500);
    let rep = crate::takeover_with_store(cb.clone(), store.clone()).unwrap();
    assert_eq!(rep.quiet, Duration::from_millis(2500), "{rep:?}");
    let (hook, seen) = recorder();
    cb.on_event = Some(hook);
    cb.maintenance_wal_bytes = 16 << 10;
    let db = crate::open_with_store(cb, store.clone()).unwrap();
    let conn = db.connect().unwrap();
    for x in 0..10 {
        conn.execute("INSERT INTO t VALUES (?)", &[Value::from_i64(x)])
            .unwrap();
    }
    wait_event(
        &seen,
        |e| matches!(e, Event::MaintenanceSkipped { reason, .. } if reason == "takeover quiet"),
    );
    let other = db.connect().unwrap();
    for x in 100..110 {
        let t = Instant::now();
        other
            .execute("INSERT INTO t VALUES (?)", &[Value::from_i64(x)])
            .unwrap();
        assert!(t.elapsed() < BLOCKED, "held {:?}", t.elapsed());
        assert!(!db.shared.gate.is_closed(), "the gate closed in the quiet");
    }
    {
        let ev = seen.lock().unwrap();
        assert!(
            !ev.iter()
                .any(|e| matches!(e, Event::MaintenanceStarted { .. })),
            "a run started in the quiet: {ev:?}"
        );
    }
    // After the quiet, maintenance seals (the generation is still due).
    wait_event(&seen, |e| matches!(e, Event::MaintenanceFinished { .. }));
    // Maintenance's seal is a takeover's first: the head skips a generation.
    assert_eq!(db.generation(), gen + 2);
    other.close().unwrap();
    conn.close().unwrap();
    db.close().unwrap();
}

/// The head's generation.
fn db_generation(store: &dyn crate::store::ObjectStore, keys: &crate::format::Keys) -> u64 {
    let body = store.get(&keys.head()).unwrap().unwrap().body;
    crate::format::Head::decode(&body).unwrap().generation
}

// --- Checkpoints of a format 5 writer (spec "The writer") ---------------------------------

/// A writer on an in-memory store, with the trimmer off (its GC pass reads the bucket on its
/// own thread), and its first connection with table `t(id, b)`.
fn writer_with(
    c: impl FnOnce(&mut crate::config::Config),
    store: Arc<crate::store::MemStore>,
    dir: &std::path::Path,
) -> (crate::Database, crate::Connection) {
    let mut cfg = crate::config::Config::new(dir, "http://127.0.0.1:1", "b", "k", "s");
    c(&mut cfg);
    let db = crate::open_with_store(cfg, store).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)", &[])
        .unwrap();
    (db, conn)
}

/// Insert one row with a blob of `bytes` (about `bytes / 4096` frames).
fn insert_blob(conn: &crate::Connection, bytes: usize) {
    conn.execute(
        &format!("INSERT INTO t(b) VALUES (randomblob({bytes}))"),
        &[],
    )
    .unwrap();
}

/// A reader kept the engine's own PASSIVE checkpoint (after a commit, at 1000 frames past the
/// backfill) from the newest frames. Then the reader left, and the WAL is less than 1000
/// frames past the backfill: the engine does not try again for 1000 more frames. The writer's
/// own PASSIVE checkpoint backfills the rest, so a later commit restarts the WAL (a generation
/// switch), long before the WAL size limit.
#[test]
fn writer_runs_passive_checkpoints_so_wal_restarts_without_readers() {
    let dir = tempfile::tempdir().unwrap();
    let _no_gc = crate::trim::NoWriterGc::on();
    let store = Arc::new(crate::store::MemStore::new());
    let (db, w) = writer_with(|_| {}, store, dir.path());
    // About 590 frames.
    insert_blob(&w, 2_400_000);
    let r = db.connect().unwrap();
    r.execute("BEGIN", &[]).unwrap();
    assert_eq!(
        r.query("SELECT count(*) FROM t", &[]).unwrap()[0][0].as_int(),
        Some(1)
    );
    // About 1180 frames: the engine's checkpoint backfills only up to the reader's snapshot.
    insert_blob(&w, 2_400_000);
    r.execute("COMMIT", &[]).unwrap();
    assert_eq!(db.generation(), 1);
    // Each small commit adds a frame or two: far fewer than the 1000 that the engine waits for.
    let until = Instant::now() + Duration::from_secs(3);
    let mut commits = 0;
    while db.generation() == 1 {
        assert!(
            Instant::now() < until,
            "no WAL restart after {commits} small commits"
        );
        w.execute("INSERT INTO t(b) VALUES (1)", &[]).unwrap();
        commits += 1;
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(db.generation(), 2);
    drop((r, w));
    db.close().unwrap();
}

/// The writer's PASSIVE checkpoint never waits for a reader: with a read transaction open on
/// frames older than the newest, it returns at once (well under the 5 s busy timeout and the
/// 500 ms gate wait), and the reader still reads its snapshot.
#[test]
fn passive_checkpoint_with_reader_never_waits() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(crate::store::MemStore::new());
    let (db, w) = writer_with(|_| {}, store, dir.path());
    for _ in 0..50 {
        w.execute("INSERT INTO t(b) VALUES (1)", &[]).unwrap();
    }
    let r = db.connect().unwrap();
    r.execute("BEGIN", &[]).unwrap();
    assert_eq!(
        r.query("SELECT count(*) FROM t", &[]).unwrap()[0][0].as_int(),
        Some(50)
    );
    for _ in 0..50 {
        w.execute("INSERT INTO t(b) VALUES (1)", &[]).unwrap();
    }
    let start = Instant::now();
    let res = passive_checkpoint(&db.shared);
    let took = start.elapsed();
    assert!(
        !matches!(res, Err((Outcome::Error, _))),
        "the PASSIVE checkpoint failed: {res:?}"
    );
    assert!(took < Duration::from_millis(250), "it waited {took:?}");
    assert_eq!(
        r.query("SELECT count(*) FROM t", &[]).unwrap()[0][0].as_int(),
        Some(50)
    );
    r.execute("COMMIT", &[]).unwrap();
    drop((r, w));
    db.close().unwrap();
}

/// Only the WAL size limit starts the maintenance checkpoint. More log objects than the old
/// `BUCKWAL_MAINTENANCE_LOG_OBJECTS` default (1000) start nothing while the WAL stays below
/// the limit (a reader keeps the engine from restarting it); the commit that takes the WAL past
/// the limit does.
#[test]
fn maintenance_runs_only_at_wal_size_limit() {
    use crate::config::Event;
    let dir = tempfile::tempdir().unwrap();
    let (hook, seen) = recorder();
    let store = Arc::new(crate::store::MemStore::new());
    let limit: u64 = 16 << 20;
    let (db, w) = writer_with(
        |c| {
            c.on_event = Some(hook);
            c.maintenance_wal_bytes = limit;
        },
        store,
        dir.path(),
    );
    let r = db.connect().unwrap();
    r.execute("BEGIN", &[]).unwrap();
    r.query("SELECT count(*) FROM t", &[]).unwrap();
    for x in 0..1100 {
        w.execute("INSERT INTO t(b) VALUES (?)", &[crate::Value::from_i64(x)])
            .unwrap();
    }
    // A poke after the last call would have woken the thread by now.
    thread::sleep(Duration::from_millis(200));
    let started = |seen: &Mutex<Vec<Event>>| {
        seen.lock()
            .unwrap()
            .iter()
            .filter_map(|e| match e {
                Event::MaintenanceStarted { wal_bytes, .. } => Some(*wal_bytes),
                _ => None,
            })
            .collect::<Vec<u64>>()
    };
    assert_eq!(
        started(&seen),
        Vec::<u64>::new(),
        "a run started below the limit"
    );
    assert_eq!(db.generation(), 1);
    insert_blob(&w, 10 << 20);
    wait_event(&seen, |e| matches!(e, Event::MaintenanceStarted { .. }));
    let at = started(&seen);
    assert!(at.iter().all(|b| *b >= limit), "{at:?}");
    // The reader holds the gate: the run waits it out, then a retry checkpoints.
    r.execute("COMMIT", &[]).unwrap();
    wait_event(&seen, |e| matches!(e, Event::MaintenanceFinished { .. }));
    assert_eq!(db.generation(), 2);
    drop((r, w));
    db.close().unwrap();
}

/// The maintenance checkpoint uploads no page and no manifest: from its start to its end the
/// bucket sees only three small PUTs, the generation switch's seal-start record and head move,
/// and the `-wal` truncate as the new generation's first record.
#[test]
fn maintenance_uploads_nothing() {
    use crate::config::{Event, EventHook};
    use crate::store::{ObjectStore, StatsSnap};
    let dir = tempfile::tempdir().unwrap();
    let _no_gc = crate::trim::NoWriterGc::on();
    let store = Arc::new(crate::store::MemStore::new());
    let snaps: Arc<Mutex<Vec<StatsSnap>>> = Arc::default();
    let (s, st) = (snaps.clone(), store.clone());
    let hook: EventHook = Arc::new(move |e: &Event| {
        if matches!(
            e,
            Event::MaintenanceStarted { .. } | Event::MaintenanceFinished { .. }
        ) {
            s.lock().unwrap().push(st.stats());
        }
    });
    let (db, w) = writer_with(
        |c| {
            c.on_event = Some(hook);
            c.maintenance_wal_bytes = 1 << 20;
        },
        store.clone(),
        dir.path(),
    );
    insert_blob(&w, 1_200_000);
    let until = Instant::now() + Duration::from_secs(10);
    while snaps.lock().unwrap().len() < 2 {
        assert!(Instant::now() < until, "no maintenance run ended");
        thread::sleep(Duration::from_millis(2));
    }
    let s = snaps.lock().unwrap().clone();
    let d = s[1].since(&s[0]);
    assert_eq!((d.puts, d.lists, d.deletes), (3, 0, 0), "{d:?}");
    assert!(d.bytes_up < 4096, "{d:?}");
    assert_eq!(db.generation(), 2);
    // The new generation's first record is the `-wal` truncate: no frame, no page.
    let keys = crate::format::Keys::new("");
    assert_eq!(
        store.list(&keys.log_prefix(2)).unwrap(),
        vec![keys.log(2, 1)]
    );
    let body = store.get(&keys.log(2, 1)).unwrap().unwrap().body;
    match crate::format::decode_record(&body).unwrap() {
        crate::format::LogRecord::Extents { syncs, .. } => assert!(
            syncs
                .iter()
                .flatten()
                .all(|op| matches!(op, crate::format::Op::Truncate { len: 0 })),
            "{syncs:?}"
        ),
        r => panic!("{r:?}"),
    }
    let bad: Vec<String> = store
        .list("")
        .unwrap()
        .into_iter()
        .filter(|k| k.starts_with(&keys.page_root()) || k.starts_with(&keys.manifest_root()))
        .collect();
    assert!(bad.is_empty(), "{bad:?}");
    drop(w);
    db.close().unwrap();
}

// --- A checkpoint's WAL fsync and another connection's commit -----------------------------

/// What a paused commit thread tells the test, and waits for.
struct Pause {
    paused: mpsc::Receiver<()>,
    go: mpsc::Sender<()>,
}

/// Run `f` on a new thread whose first shipping `-wal` sync with `pending_empty` as given
/// waits for the test's go. Returns the pause and the thread.
fn paused_at_sync<T: Send + 'static>(
    pending_empty: bool,
    f: impl FnOnce() -> T + Send + 'static,
) -> (Pause, thread::JoinHandle<T>) {
    let (paused_tx, paused) = mpsc::channel();
    let (go, go_rx) = mpsc::channel::<()>();
    let h = thread::spawn(move || {
        let mut once = Some((paused_tx, go_rx));
        crate::io::wal_file::BEFORE_SYNC.with(|h| {
            *h.borrow_mut() = Some(Box::new(move |empty| {
                if empty != pending_empty {
                    return;
                }
                if let Some((tx, rx)) = once.take() {
                    tx.send(()).unwrap();
                    rx.recv_timeout(Duration::from_secs(20)).unwrap();
                }
            }))
        });
        let r = f();
        crate::io::wal_file::BEFORE_SYNC.with(|h| *h.borrow_mut() = None);
        r
    });
    (Pause { paused, go }, h)
}

impl Pause {
    fn wait(&self) {
        self.paused
            .recv_timeout(Duration::from_secs(20))
            .expect("the thread never reached its sync");
    }
}

/// A writer on a fault store whose log PUTs fail fast, with a table `t(id, b)`.
fn faulty_writer(
    dir: &std::path::Path,
) -> (
    crate::Database,
    Arc<crate::store::FaultStore>,
    Arc<crate::store::MemStore>,
) {
    let mem = Arc::new(crate::store::MemStore::new());
    let fault = Arc::new(crate::store::FaultStore::new(mem.clone()));
    let mut c = crate::config::Config::new(dir, "http://127.0.0.1:1", "b", "k", "s");
    c.retry_deadline = Duration::from_millis(200);
    c.request_timeout = Duration::from_millis(50);
    let db = crate::open_with_store(c, fault.clone()).unwrap();
    db.connect()
        .unwrap()
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)", &[])
        .unwrap();
    (db, fault, mem)
}

/// The ids a restore of the bucket holds.
fn restored_ids(mem: Arc<crate::store::MemStore>) -> Vec<i64> {
    let to = tempfile::tempdir().unwrap();
    let c = crate::config::Config::new(to.path(), "http://127.0.0.1:1", "b", "k", "s");
    let db = crate::open_read_only_with_store(c, mem).unwrap();
    let conn = db.connect().unwrap();
    let ids = conn
        .query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect();
    drop(conn);
    db.close().unwrap();
    ids
}

/// Connection A wrote its commit's frames and waits for its own `-wal` fsync. The writer's
/// PASSIVE checkpoint fsyncs the WAL then, and the bucket refuses its upload for good. The checkpoint must not ship
/// (and lose) A's frames: A's own sync ships them, and A's acked commit restores.
#[test]
fn passive_checkpoint_never_ships_another_connections_commit() {
    let dir = tempfile::tempdir().unwrap();
    let (db, fault, mem) = faulty_writer(dir.path());
    let w = db.connect().unwrap();
    for id in 1..=20 {
        w.execute(
            "INSERT INTO t(id) VALUES (?)",
            &[crate::Value::from_i64(id)],
        )
        .unwrap();
    }
    let a_db = db.clone();
    let (pause, a) = paused_at_sync(false, move || {
        let c = a_db.connect().unwrap();
        c.execute("INSERT INTO t(id) VALUES (100)", &[])
    });
    pause.wait();
    // A permanent failure: the uploader drops a record it could not send.
    fault.only_keys_containing("log/");
    fault.fail_next_puts_permanently(u64::MAX);
    let checkpoint = passive_checkpoint(&db.shared);
    fault.fail_next_puts_permanently(0);
    pause.go.send(()).unwrap();
    let acked = a.join().unwrap();
    eprintln!("checkpoint: {checkpoint:?}, A: {acked:?}");
    // The ack means the record is in the bucket now, with the writer still open.
    let open_ids = restored_ids(mem.clone());
    drop(w);
    db.close().unwrap();
    let ids = restored_ids(mem);
    if acked.is_ok() {
        assert!(
            open_ids.contains(&100) && ids.contains(&100),
            "acked commit 100: in the bucket at its ack {}, after the close {}",
            open_ids.contains(&100),
            ids.contains(&100)
        );
    }
    assert!(acked.is_ok(), "{acked:?}");
}
