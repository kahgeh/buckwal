use super::*;
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

/// The thresholds decide when a generation is due; 0 turns a measure off.
#[test]
fn due_follows_the_thresholds() {
    let cfg = |log_objects, wal_bytes| MaintenanceConfig {
        log_objects,
        wal_bytes,
        gate_wait: Duration::from_millis(500),
        retry: Duration::from_secs(1),
    };
    let c = cfg(200, 1 << 20);
    assert!(c.enabled());
    assert!(!c.due(199, (1 << 20) - 1));
    assert!(c.due(200, 0));
    assert!(c.due(0, 1 << 20));
    assert!(!c.overdue(399, 0) && c.overdue(400, 0) && c.overdue(0, 2 << 20));
    let c = cfg(0, 1 << 20);
    assert!(c.enabled() && !c.due(u64::MAX, 0));
    let c = cfg(200, 0);
    assert!(c.enabled() && !c.due(0, u64::MAX));
    let c = cfg(0, 0);
    assert!(!c.enabled() && !c.due(u64::MAX, u64::MAX));
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
    c.maintenance_log_objects = 5;
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
