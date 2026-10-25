//! A checkpoint's `-wal` fsync and another connection's commit, through the engine.
use crate::io::wal_file::BEFORE_SYNC;
use crate::store::{FaultStore, MemStore};
use crate::{open_read_only_with_store, open_with_store, Config, Database, Value};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

/// What a paused commit thread tells the test, and waits for.
struct Pause {
    paused: mpsc::Receiver<()>,
    go: mpsc::Sender<()>,
}

impl Pause {
    fn wait(&self) {
        self.paused
            .recv_timeout(Duration::from_secs(20))
            .expect("the thread never reached its sync");
    }
}

/// Run `f` on a new thread whose first shipping `-wal` sync with `pending_empty` as given
/// waits for the test's go.
fn paused_at_sync<T: Send + 'static>(
    pending_empty: bool,
    f: impl FnOnce() -> T + Send + 'static,
) -> (Pause, thread::JoinHandle<T>) {
    let (paused_tx, paused) = mpsc::channel();
    let (go, go_rx) = mpsc::channel::<()>();
    let h = thread::spawn(move || {
        let mut once = Some((paused_tx, go_rx));
        BEFORE_SYNC.with(|h| {
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
        BEFORE_SYNC.with(|h| *h.borrow_mut() = None);
        r
    });
    (Pause { paused, go }, h)
}

/// A writer on a fault store whose log PUTs fail fast, with a table `t(id, b)`.
fn faulty_writer(dir: &std::path::Path) -> (Database, Arc<FaultStore>, Arc<MemStore>) {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let mut c = Config::new(dir, "http://127.0.0.1:1", "b", "k", "s");
    c.retry_deadline = Duration::from_millis(200);
    c.request_timeout = Duration::from_millis(50);
    let db = open_with_store(c, fault.clone()).unwrap();
    db.connect()
        .unwrap()
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB)", &[])
        .unwrap();
    (db, fault, mem)
}

/// The ids a restore of the bucket holds.
fn restored_ids(mem: Arc<MemStore>) -> Vec<i64> {
    let to = tempfile::tempdir().unwrap();
    let c = Config::new(to.path(), "http://127.0.0.1:1", "b", "k", "s");
    let db = open_read_only_with_store(c, mem).unwrap();
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

/// Connection B's commit passes 1000 frames, and its auto-checkpoint (after B released the
/// write lock) fsyncs the WAL while connection A's commit waits for its own fsync. The bucket
/// refuses uploads for good during B's checkpoint. A's acked commit must be in the bucket.
#[test]
fn auto_checkpoint_never_ships_another_connections_commit() {
    let dir = tempfile::tempdir().unwrap();
    let (db, fault, mem) = faulty_writer(dir.path());
    let w = db.connect().unwrap();
    w.execute("INSERT INTO t(id) VALUES (1)", &[]).unwrap();
    // B: about 1030 frames in one commit; its auto-checkpoint's sync finds no pending op.
    let b_db = db.clone();
    let (b_pause, b) = paused_at_sync(true, move || {
        let c = b_db.connect().unwrap();
        c.execute("INSERT INTO t(id, b) VALUES (2, randomblob(4200000))", &[])
    });
    b_pause.wait();
    let a_db = db.clone();
    let (a_pause, a) = paused_at_sync(false, move || {
        let c = a_db.connect().unwrap();
        c.execute("INSERT INTO t(id) VALUES (?)", &[Value::from_i64(100)])
    });
    a_pause.wait();
    fault.only_keys_containing("log/");
    fault.fail_next_puts_permanently(u64::MAX);
    b_pause.go.send(()).unwrap();
    let b_done = b.join().unwrap();
    fault.fail_next_puts_permanently(0);
    a_pause.go.send(()).unwrap();
    let acked = a.join().unwrap();
    eprintln!("B: {b_done:?}, A: {acked:?}");
    let in_bucket = restored_ids(mem).contains(&100);
    drop(w);
    db.close().unwrap();
    assert!(
        acked.is_err() || in_bucket,
        "A's commit 100 was acked and is not in the bucket"
    );
}
