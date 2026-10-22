//! Task 11: the limits of docs/mvcc-recon.md ("Limits in detail": P2, P2b, P3, P8g and the
//! checkpoint stop-the-world) pinned against today's turso_core and library, on in-memory
//! stores through the public API (an MVCC bucket). Each test parks a store PUT (a `BlockStore`
//! below, standing in for a slow upload) and observes who is held up, by "returned before the
//! release" / "returned after the release", never by timing alone. A change in turso_core or
//! in the library that moves one of these limits fails the matching test.
use std::path::Path;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use turso_durable::store::{
    CondPut, FaultStore, MemStore, Object, ObjectStat, ObjectStore, StatsSnap, StoreError,
};
use turso_durable::{
    open_read_only_with_store, open_with_store, CheckpointMode, Config, Connection, Database,
    Error, JournalMode,
};

const PREFIX: &str = "db";
/// How long "has not returned" is watched for before the release.
const WATCH: Duration = Duration::from_millis(400);
/// An upper bound for "returns after the release".
const GENEROUS: Duration = Duration::from_secs(20);

fn mvcc(dir: &Path) -> Config {
    let mut c = Config::new(dir, "http://127.0.0.1:1", "bucket", "key", "secret");
    c.prefix = PREFIX.into();
    c.retry_deadline = Duration::from_millis(300);
    c.request_timeout = Duration::from_millis(500);
    c.lease = Duration::from_secs(60);
    c.journal_mode = JournalMode::Mvcc;
    // Every checkpoint is the test's.
    c.maintenance_log_objects = 0;
    c.maintenance_wal_bytes = 0;
    c.gc_after_seal = false;
    c
}

type Armed = (String, Sender<String>, Receiver<()>);

/// A store that holds one PUT (any flavour) of a key containing the armed text until released,
/// as a slow upload would, then passes it on. A hold says so on its channel when it starts.
struct BlockStore {
    inner: Arc<dyn ObjectStore>,
    armed: Mutex<Option<Armed>>,
}

impl BlockStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            armed: Mutex::new(None),
        }
    }

    /// Hold the next PUT of a key containing `needle`: the receiver gets its key when it is
    /// held, a send on the sender lets it go on.
    fn hold_next(&self, needle: &str) -> (Receiver<String>, Sender<()>) {
        let (held_tx, held_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel();
        *self.armed.lock().unwrap() = Some((needle.to_string(), held_tx, go_rx));
        (held_rx, go_tx)
    }

    fn gate(&self, key: &str) {
        let hold = {
            let mut armed = self.armed.lock().unwrap();
            match armed.as_ref() {
                Some((s, _, _)) if key.contains(s.as_str()) => armed.take(),
                _ => None,
            }
        };
        if let Some((_, held_tx, go_rx)) = hold {
            let _ = held_tx.send(key.to_string());
            let _ = go_rx.recv();
        }
    }
}

impl ObjectStore for BlockStore {
    fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.gate(key);
        self.inner.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> Result<CondPut, StoreError> {
        self.gate(key);
        self.inner.put_if_absent(key, body)
    }
    fn put_if_match(&self, key: &str, body: &[u8], etag: &str) -> Result<CondPut, StoreError> {
        self.gate(key);
        self.inner.put_if_match(key, body, etag)
    }
    fn get(&self, key: &str) -> Result<Option<Object>, StoreError> {
        self.inner.get(key)
    }
    fn stat(&self, key: &str) -> Result<Option<ObjectStat>, StoreError> {
        self.inner.stat(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

/// An MVCC database on `store` with table `t` and row 1 acked.
fn setup(store: Arc<dyn ObjectStore>) -> (TempDir, Database, Connection) {
    let t = tempfile::tempdir().unwrap();
    let db = open_with_store(mvcc(&t.path().join("db")), store).unwrap();
    let c = db.connect().unwrap();
    c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    c.execute("INSERT INTO t VALUES (1, 'one')", &[]).unwrap();
    (t, db, c)
}

fn ids(c: &Connection) -> Vec<i64> {
    c.query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect()
}

/// Run `f` on a thread; the receiver gets its result when it returns.
fn spawn<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Receiver<T> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx
}

fn not_returned<T>(rx: &Receiver<T>, what: &str) {
    assert!(
        matches!(rx.recv_timeout(WATCH), Err(RecvTimeoutError::Timeout)),
        "{what} returned before the release"
    );
}

fn returned<T>(rx: &Receiver<T>, what: &str) -> T {
    rx.recv_timeout(GENEROUS)
        .unwrap_or_else(|_| panic!("{what} did not return after the release"))
}

/// Park the kind-3 PUT of the next commit: `insert` runs on a thread, the PUT is held.
fn park_insert(
    block: &BlockStore,
    c: Connection,
    sql: &'static [&'static str],
) -> (
    Sender<()>,
    Receiver<turso_durable::Result<()>>,
    Receiver<Connection>,
) {
    let (held, go) = block.hold_next("/log/");
    let (back_tx, back_rx) = mpsc::channel();
    let done = spawn(move || {
        let mut r = Ok(());
        for s in sql {
            r = r.and_then(|_| c.execute(s, &[]).map(|_| ()));
        }
        let _ = back_tx.send(c);
        r
    });
    let key = held.recv_timeout(GENEROUS).expect("the PUT is held");
    assert!(key.contains("/log/"), "{key}");
    (go, done, back_rx)
}

#[test]
fn slow_upload_stalls_writers_and_begin_concurrent_waits() {
    for plain in ["BEGIN", "autocommit"] {
        let mem = Arc::new(MemStore::new());
        let block = Arc::new(BlockStore::new(mem.clone()));
        let (_t, db, c1) = setup(block.clone());
        let (c2, c3) = (db.connect().unwrap(), db.connect().unwrap());

        // A plain commit parks in its upload.
        let stmts: &'static [&'static str] = if plain == "BEGIN" {
            &["BEGIN", "INSERT INTO t VALUES (2, 'two')", "COMMIT"]
        } else {
            &["INSERT INTO t VALUES (2, 'two')"]
        };
        let (go, done, _back) = park_insert(&block, c1, stmts);

        // A BEGIN CONCURRENT writer on another connection fails at once, before the release.
        let t0 = Instant::now();
        let attempt = spawn(move || {
            let r = (|| {
                c2.execute("BEGIN CONCURRENT", &[])?;
                c2.execute("INSERT INTO t VALUES (1000, 'x')", &[])?;
                c2.execute("COMMIT", &[]).map(|_| ())
            })();
            let _ = c2.execute("ROLLBACK", &[]);
            (r, c2)
        });
        // It returns while the commit is still parked (a regression waits here instead).
        let (r, c2) = attempt
            .recv_timeout(GENEROUS)
            .unwrap_or_else(|_| panic!("{plain}: BEGIN CONCURRENT did not fail at once"));
        let latency = t0.elapsed();
        let e = r.expect_err(&format!("{plain}: BEGIN CONCURRENT behind a parked commit"));
        assert!(
            format!("{e}").contains("Write-write conflict"),
            "{plain}: {e}"
        );
        assert!(
            done.try_recv().is_err(),
            "{plain}: the parked commit is still parked"
        );
        eprintln!("{plain}: BEGIN CONCURRENT failed in {latency:?}: {e}");

        go.send(()).unwrap();
        returned(&done, "the parked commit").unwrap();
        assert_eq!(ids(&c3), vec![1, 2]);
        drop(c2);
        drop(c3);
        drop(_back);
        db.close().unwrap();
    }

    // A BEGIN CONCURRENT behind a parked BEGIN CONCURRENT waits, then succeeds.
    let mem = Arc::new(MemStore::new());
    let block = Arc::new(BlockStore::new(mem.clone()));
    let (_t, db, c1) = setup(block.clone());
    let c2 = db.connect().unwrap();
    let (go, first, _back) = park_insert(
        &block,
        c1,
        &[
            "BEGIN CONCURRENT",
            "INSERT INTO t VALUES (2, 'two')",
            "COMMIT",
        ],
    );
    let second = spawn(move || {
        let r = (|| {
            c2.execute("BEGIN CONCURRENT", &[])?;
            c2.execute("INSERT INTO t VALUES (1000, 'x')", &[])?;
            c2.execute("COMMIT", &[]).map(|_| ())
        })();
        (r, c2)
    });
    not_returned(&second, "BEGIN CONCURRENT behind BEGIN CONCURRENT");
    assert!(first.try_recv().is_err());
    go.send(()).unwrap();
    returned(&first, "the parked BEGIN CONCURRENT").unwrap();
    let (r, c2) = returned(&second, "the waiting BEGIN CONCURRENT");
    r.expect("the waiting BEGIN CONCURRENT succeeds after the release");
    assert_eq!(ids(&c2), vec![1, 2, 1000]);
    drop(c2);
    drop(_back);
    db.close().unwrap();
}

#[test]
fn speculative_read_inside_begin_sees_unacked_commit() {
    for begin in ["BEGIN", "BEGIN CONCURRENT"] {
        let mem = Arc::new(MemStore::new());
        let block = Arc::new(BlockStore::new(mem.clone()));
        let (_t, db, c1) = setup(block.clone());
        let reader = db.connect().unwrap();
        let (go, done, _back) = park_insert(&block, c1, &["INSERT INTO t VALUES (2, 'held')"]);

        // The reader, inside an explicit transaction, returns the unacked row at once.
        let read = spawn(move || {
            reader.execute(begin, &[]).unwrap();
            let seen = reader.query("SELECT id FROM t WHERE id = 2", &[]);
            (seen, reader)
        });
        let (seen, reader) = read
            .recv_timeout(GENEROUS)
            .unwrap_or_else(|_| panic!("{begin}: the read waited for the upload"));
        assert_eq!(
            seen.unwrap().len(),
            1,
            "{begin}: the unacked row is read before the release"
        );
        assert!(done.try_recv().is_err(), "{begin}: the writer is not acked");
        reader.execute("ROLLBACK", &[]).unwrap();

        go.send(()).unwrap();
        returned(&done, "the writer").unwrap();
        drop(reader);
        drop(_back);
        db.close().unwrap();
    }
}

#[test]
fn autocommit_read_waits_out_the_upload() {
    let mem = Arc::new(MemStore::new());
    let block = Arc::new(BlockStore::new(mem.clone()));
    let (_t, db, c1) = setup(block.clone());
    let reader = db.connect().unwrap();
    let (go, done, _back) = park_insert(&block, c1, &["INSERT INTO t VALUES (2, 'held')"]);

    let read = spawn(move || {
        let rows = reader.query("SELECT id FROM t WHERE id = 2", &[]);
        (rows, reader)
    });
    not_returned(&read, "the autocommit SELECT of the held row");
    assert!(done.try_recv().is_err());

    go.send(()).unwrap();
    returned(&done, "the writer").unwrap();
    let (rows, reader) = returned(&read, "the autocommit SELECT");
    assert_eq!(rows.unwrap().len(), 1, "it reads the row once acked");
    drop(reader);
    drop(_back);
    db.close().unwrap();
}

#[test]
fn failed_upload_keeps_the_commit_locally_outcome_unknown() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let (t, db, c1) = setup(fault.clone());

    fault.only_keys_containing("/log/");
    fault.fail_next_puts_permanently(1);
    let e = c1
        .execute("INSERT INTO t VALUES (2, 'unknown')", &[])
        .expect_err("the commit whose upload failed returns Err");
    eprintln!("failed commit: {e}");
    assert!(!matches!(e, Error::Fenced(_)), "{e}");
    // The app got an Err, yet the commit is local: same connection and a fresh one see it.
    assert!(c1.is_autocommit());
    assert_eq!(ids(&c1), vec![1, 2], "same connection");
    let fresh = db.connect().unwrap();
    assert_eq!(ids(&fresh), vec![1, 2], "fresh connection");

    // The next successful commit ships it: a restore of the bucket has both rows.
    c1.execute("INSERT INTO t VALUES (3, 'three')", &[])
        .expect("the next commit succeeds");
    assert_eq!(ids(&c1), vec![1, 2, 3]);
    let v = tempfile::tempdir().unwrap();
    let ro = open_read_only_with_store(mvcc(&v.path().join("db")), mem.clone()).unwrap();
    let rc = ro.connect().unwrap();
    assert_eq!(
        ids(&rc),
        vec![1, 2, 3],
        "the restore holds the unknown commit"
    );
    rc.close().unwrap();
    ro.close().unwrap();
    fresh.close().unwrap();
    c1.close().unwrap();
    db.close().unwrap();
    drop(t);
}

#[test]
fn checkpoint_stops_readers_with_busy_after_timeout_on_slow_seal() {
    let mem = Arc::new(MemStore::new());
    let block = Arc::new(BlockStore::new(mem.clone()));
    let (_t, db, c1) = setup(block.clone());
    let reader = db.connect().unwrap();
    assert_eq!(ids(&reader), vec![1]);

    // The seal's first PUT is parked. The busy timeout is a fixed 5 s (`conn::BUSY_TIMEOUT`,
    // no config), so this test takes about 5 s.
    let (held, go) = block.hold_next("/log/");
    let ck = spawn(move || {
        let r = c1.checkpoint(CheckpointMode::Truncate);
        (r, c1)
    });
    held.recv_timeout(GENEROUS).expect("a seal PUT is held");
    let t0 = Instant::now();
    let r = reader.query("SELECT id FROM t", &[]);
    let waited = t0.elapsed();
    eprintln!("reader during the seal: {r:?} after {waited:?}");
    let e = r.expect_err("a read during the seal fails");
    assert!(format!("{e}").contains("busy"), "{e}");
    assert!(
        waited >= Duration::from_secs(4),
        "Busy only after the busy timeout, not at once: {waited:?}"
    );
    assert!(ck.try_recv().is_err(), "the checkpoint is still sealing");

    go.send(()).unwrap();
    let (rc, c1) = returned(&ck, "the checkpoint");
    rc.unwrap();
    assert_eq!(ids(&reader), vec![1], "a read succeeds after the seal");
    reader.close().unwrap();
    c1.close().unwrap();
    db.close().unwrap();
}
