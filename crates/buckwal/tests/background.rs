//! Black-box tests of the background setting, through `open_with_store` on a `MemStore` (a
//! `FaultStore` or a parking store over it): a background commit returns after its local
//! fsync and before its upload, the durable position moves only when a record lands and only
//! forward, and the bucket always holds a prefix of the commits.
use buckwal::store::{
    CondPut, FaultStore, MemStore, Object, ObjectStat, ObjectStore, StatsSnap, StoreError,
};
use buckwal::{
    open_read_only_with_store, open_with_store, restore_with_store, Batching, CheckpointMode,
    Config, Connection, Database, Error, Event, EventHook, JournalMode, Value,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// How long a call that must not return is watched.
const WATCH: Duration = Duration::from_millis(300);
/// An upper bound for a call that must return.
const GENEROUS: Duration = Duration::from_secs(20);

/// A store whose log PUTs, once parked, wait until released.
struct Park {
    inner: Arc<dyn ObjectStore>,
    parked: Mutex<bool>,
    cv: Condvar,
}

impl Park {
    fn new(inner: Arc<dyn ObjectStore>) -> Arc<Park> {
        Arc::new(Park {
            inner,
            parked: Mutex::new(false),
            cv: Condvar::new(),
        })
    }
    fn park(&self) {
        *self.parked.lock().unwrap() = true;
    }
    fn release(&self) {
        *self.parked.lock().unwrap() = false;
        self.cv.notify_all();
    }
    fn gate(&self, key: &str) {
        if !key.contains("log/") {
            return;
        }
        let g = self.parked.lock().unwrap();
        if !*g {
            return;
        }
        drop(self.cv.wait_while(g, |parked| *parked).unwrap());
    }
}

impl ObjectStore for Park {
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

/// Every event, with the thread that emitted it.
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<(ThreadId, Event)>>>);

impl Recorder {
    fn hook(&self) -> EventHook {
        let seen = self.0.clone();
        Arc::new(move |e: &Event| {
            seen.lock()
                .unwrap()
                .push((thread::current().id(), e.clone()))
        })
    }
    fn events(&self) -> Vec<(ThreadId, Event)> {
        self.0.lock().unwrap().clone()
    }
}

fn bg(wait_ms: u64, commits: u32, behind: u32) -> Batching {
    Batching::Background {
        max_wait: Duration::from_millis(wait_ms),
        max_commits: commits,
        max_bytes: 1 << 20,
        max_behind: behind,
    }
}

fn window(wait_ms: u64) -> Batching {
    Batching::Window {
        max_wait: Duration::from_millis(wait_ms),
        max_commits: 64,
        max_bytes: 1 << 20,
    }
}

fn config(dir: &Path) -> Config {
    Config::new(dir, "http://127.0.0.1:1", "b", "k", "s")
}

fn open(store: Arc<dyn ObjectStore>, rec: &Recorder) -> (TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let mut c = config(dir.path());
    c.on_event = Some(rec.hook());
    c.retry_deadline = Duration::from_millis(200);
    c.request_timeout = Duration::from_millis(50);
    let db = open_with_store(c, store).unwrap();
    (dir, db)
}

fn create(c: &Connection) {
    c.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
        .unwrap();
}

fn insert(c: &Connection, x: i64) -> buckwal::Result<u64> {
    c.execute("INSERT INTO t VALUES (?)", &[Value::from_i64(x)])
}

fn count(c: &Connection) -> i64 {
    c.query("SELECT count(*) FROM t", &[]).unwrap()[0][0]
        .as_int()
        .unwrap()
}

/// Restore the bucket into a fresh directory and read `t` there.
fn restore_rows(store: Arc<dyn ObjectStore>) -> Vec<i64> {
    let dir = tempfile::tempdir().unwrap();
    restore_with_store(&config(dir.path()), store.clone(), dir.path()).unwrap();
    let db = open_read_only_with_store(config(dir.path()), store).unwrap();
    let c = db.connect().unwrap();
    let rows = c.query("SELECT x FROM t ORDER BY x", &[]).unwrap();
    rows.iter().map(|r| r[0].as_int().unwrap()).collect()
}

/// Run `f` on its own thread and return its result, or `None` if it has not finished within
/// `within`; the thread is left to finish.
fn within<T: Send + 'static>(
    within: Duration,
    f: impl FnOnce() -> T + Send + 'static,
) -> (Option<T>, mpsc::Receiver<T>) {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    match rx.recv_timeout(within) {
        Ok(v) => (Some(v), rx),
        Err(_) => (None, rx),
    }
}

#[test]
fn a_background_commit_returns_before_its_put_and_is_durable_in_order() {
    let mem = Arc::new(MemStore::new());
    let park = Park::new(mem.clone());
    let rec = Recorder::default();
    let (_d, db) = open(park.clone(), &rec);
    let c = db.connect().unwrap();
    create(&c);
    c.set_batching(Some(bg(5, 64, 1000))).unwrap();
    let acked_before = db.upload_stats().commits_acked;
    park.park();
    let me = thread::current().id();
    let mut last = 0;
    for x in 1..=3 {
        let t = Instant::now();
        insert(&c, x).unwrap();
        assert!(t.elapsed() < Duration::from_secs(5), "it did not wait");
        let n = c.last_commit().expect("a commit number");
        assert!(n > last, "{n} after {last}");
        last = n;
    }
    assert!(db.durable_commit() < last, "nothing landed while parked");
    let s = db.upload_stats();
    assert_eq!(s.commits_behind, 3);
    assert_eq!(s.commits_acked, acked_before, "not acked before durable");
    let r = db.wait_durable(last, Duration::from_millis(50));
    assert!(matches!(r, Err(Error::Timeout(_))), "{r:?}");
    park.release();
    db.wait_durable(last, GENEROUS).unwrap();
    assert_eq!(db.durable_commit(), last);
    let s = db.upload_stats();
    assert_eq!((s.commits_behind, s.oldest_behind), (0, Duration::ZERO));
    assert_eq!(s.commits_acked, acked_before + 3, "acked when durable");
    // The acks came from the uploader thread, not the committing one.
    let acks: Vec<_> = rec
        .events()
        .into_iter()
        .filter_map(|(t, e)| match e {
            Event::CommitAcked { commit, batching } if commit > last - 3 => {
                Some((t, commit, batching))
            }
            _ => None,
        })
        .collect();
    assert_eq!(acks.len(), 3, "{acks:?}");
    assert!(acks
        .iter()
        .all(|(t, _, b)| *t != me && *b == bg(5, 64, 1000)));
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(restore_rows(mem), vec![1, 2, 3]);
}

#[test]
fn the_next_commit_waits_once_max_behind_commits_wait() {
    let mem = Arc::new(MemStore::new());
    let park = Park::new(mem);
    let (_d, db) = open(park.clone(), &Recorder::default());
    let c = db.connect().unwrap();
    create(&c);
    c.set_batching(Some(bg(5, 64, 2))).unwrap();
    park.park();
    insert(&c, 1).unwrap();
    insert(&c, 2).unwrap();
    let c = Arc::new(c);
    let c3 = c.clone();
    let (r, rx) = within(WATCH, move || insert(&c3, 3));
    assert!(r.is_none(), "a third commit waits while two wait");
    assert!(db.upload_stats().commits_behind >= 2);
    park.release();
    let r = rx.recv_timeout(GENEROUS).expect("it returns once two land");
    r.unwrap();
    assert!(db.upload_stats().commits_behind <= 2);
    db.wait_durable(c.last_commit().unwrap(), GENEROUS).unwrap();
}

#[test]
fn an_error_from_the_wait_leaves_the_commit_and_a_later_upload_carries_it() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let (_d, db) = open(fault.clone(), &Recorder::default());
    let c = db.connect().unwrap();
    create(&c);
    fault.only_keys_containing("log/");
    c.set_batching(Some(bg(5, 64, 1))).unwrap();
    fault.fail_puts_after(0);
    // Two commits: the first returns (one waits, the limit), the second waits for it.
    insert(&c, 1).unwrap();
    let r = insert(&c, 2);
    assert!(r.is_err(), "{r:?}");
    assert_eq!(
        count(&c),
        2,
        "an error from the wait is outcome unknown: it stays"
    );
    fault.fail_puts_after(u64::MAX);
    let t = Instant::now();
    loop {
        // Degraded commits take the blocking path, which uploads everything owed with them.
        match insert(&c, 3) {
            Ok(_) => break,
            Err(_) if t.elapsed() < GENEROUS => thread::sleep(Duration::from_millis(50)),
            Err(e) => panic!("{e}"),
        }
    }
    c.close().unwrap();
    db.close().unwrap();
    let rows = restore_rows(mem);
    assert!(
        rows.contains(&1) && rows.contains(&2) && rows.contains(&3),
        "{rows:?}"
    );
}

#[test]
fn a_degraded_uploader_makes_a_background_commit_an_off_commit() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let (_d, db) = open(fault.clone(), &Recorder::default());
    let c = db.connect().unwrap();
    create(&c);
    fault.only_keys_containing("log/");
    c.set_batching(Some(bg(5, 64, 1000))).unwrap();
    fault.fail_puts_after(0);
    // Healthy when it is made: returns after the fsync, and its upload then fails.
    insert(&c, 1).unwrap();
    let first = c.last_commit().unwrap();
    let t = Instant::now();
    let r = db.wait_durable(first, GENEROUS);
    assert!(
        matches!(r, Err(Error::UploadFailed(_))) && t.elapsed() < GENEROUS / 2,
        "{r:?} after {:?}",
        t.elapsed()
    );
    // Now degraded: the next commit uploads before its fsync and fails, rolled back.
    let r = insert(&c, 2);
    assert!(r.is_err(), "{r:?}");
    assert_eq!(count(&c), 1, "rolled back; the owed first stays");
    // Recovered: the next commit still takes the blocking path, carries the owed one with it,
    // and returns only once acked.
    fault.fail_puts_after(u64::MAX);
    insert(&c, 3).unwrap();
    assert!(
        db.durable_commit() >= c.last_commit().unwrap(),
        "a degraded commit returns acked"
    );
    assert_eq!(db.upload_stats().commits_behind, 0);
    // Healthy again: the commit returns after its fsync (its upload is still to come).
    insert(&c, 4).unwrap();
    db.wait_durable(c.last_commit().unwrap(), GENEROUS).unwrap();
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(restore_rows(mem), vec![1, 3, 4]);
}

/// A record whose PUT went in doubt (stored, the reply lost) is settled at its seq when the
/// bucket answers again: the position then moves past its commits, the commits are acked
/// (counted, and `CommitAcked` from the uploader), and the stale error is gone.
#[test]
fn a_record_settled_after_doubt_moves_the_position_and_clears_the_error() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let rec = Recorder::default();
    let (_d, db) = open(fault.clone(), &rec);
    let c = db.connect().unwrap();
    create(&c);
    fault.only_keys_containing("log/");
    c.set_batching(Some(bg(5, 64, 1000))).unwrap();
    let acked_before = db.upload_stats().commits_acked;
    // Every PUT of the next record is stored and then reports an error.
    fault.store_then_fail_next_puts(1000);
    insert(&c, 1).unwrap();
    let first = c.last_commit().unwrap();
    let r = db.wait_durable(first, GENEROUS);
    assert!(matches!(r, Err(Error::UploadFailed(_))), "{r:?}");
    assert!(db.durable_commit() < first);
    // The bucket answers again: the next tick settles the record at its seq.
    fault.store_then_fail_next_puts(0);
    // The error holds until the settle lands (a tick every 2 s), then the wait succeeds.
    let until = Instant::now() + GENEROUS;
    while db.wait_durable(first, Duration::from_millis(50)).is_err() {
        assert!(
            Instant::now() < until,
            "the settled record never became durable"
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(db.durable_commit(), first);
    let s = db.upload_stats();
    assert_eq!((s.commits_behind, s.oldest_behind), (0, Duration::ZERO));
    assert_eq!(s.commits_acked, acked_before + 1);
    let acked = rec
        .events()
        .into_iter()
        .any(|(_, e)| matches!(e, Event::CommitAcked { commit, .. } if commit == first));
    assert!(acked, "CommitAcked for the settled commit");
    // A new commit waits for its own record, with no stale error.
    insert(&c, 2).unwrap();
    db.wait_durable(c.last_commit().unwrap(), GENEROUS).unwrap();
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(restore_rows(mem), vec![1, 2]);
}

#[test]
fn off_window_and_background_commits_keep_the_log_in_commit_order() {
    for connections in [1usize, 8] {
        let mem = Arc::new(MemStore::new());
        let rec = Recorder::default();
        let (_d, db) = open(mem.clone(), &rec);
        let c = db.connect().unwrap();
        create(&c);
        let stop = Arc::new(AtomicBool::new(false));
        let sampler = {
            let (db, stop) = (db.clone(), stop.clone());
            thread::spawn(move || {
                let mut last = 0;
                while !stop.load(SeqCst) {
                    let d = db.durable_commit();
                    assert!(d >= last, "durable_commit moved back: {d} < {last}");
                    last = d;
                }
            })
        };
        let per = 30i64;
        let workers: Vec<_> = (0..connections as i64)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    let c = db.connect().unwrap();
                    for i in 0..per {
                        let setting = match i % 3 {
                            0 => Batching::Off,
                            1 => window(5),
                            _ => bg(5, 8, 16),
                        };
                        c.set_batching(Some(setting)).unwrap();
                        insert(&c, t * 1000 + i).unwrap();
                    }
                    let last = c.last_commit().unwrap();
                    c.close().unwrap();
                    last
                })
            })
            .collect();
        let last = workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .max()
            .unwrap();
        db.wait_durable(last, GENEROUS).unwrap();
        stop.store(true, SeqCst);
        sampler.join().unwrap();
        // The records hold the commits in strictly increasing order.
        let uploaded: Vec<u64> = rec
            .events()
            .into_iter()
            .filter_map(|(_, e)| match e {
                Event::LogUploaded { commits, .. } => Some(commits),
                _ => None,
            })
            .flatten()
            .collect();
        assert!(
            uploaded.windows(2).all(|w| w[0] < w[1]),
            "{connections} connections: {uploaded:?}"
        );
        c.close().unwrap();
        db.close().unwrap();
        let rows = restore_rows(mem);
        assert_eq!(rows.len(), connections * per as usize, "{connections}");
    }
}

#[test]
fn a_seal_uploads_waiting_background_commits_first_and_numbers_carry_on() {
    let mem = Arc::new(MemStore::new());
    let (_d, db) = open(mem.clone(), &Recorder::default());
    let c = db.connect().unwrap();
    create(&c);
    c.set_batching(Some(bg(10_000, 64, 1000))).unwrap();
    for x in 1..=3 {
        insert(&c, x).unwrap();
    }
    let before = c.last_commit().unwrap();
    assert!(db.durable_commit() < before, "they wait for their window");
    let generation = db.generation();
    let until = Instant::now() + GENEROUS;
    loop {
        match c.checkpoint(CheckpointMode::Truncate) {
            Err(Error::Engine(m)) if m.contains("busy") && Instant::now() < until => {
                thread::sleep(Duration::from_millis(5))
            }
            r => break r.unwrap(),
        }
    }
    assert!(db.generation() > generation, "it sealed");
    assert!(
        db.durable_commit() >= before,
        "the seal uploaded them first"
    );
    insert(&c, 4).unwrap();
    assert!(c.last_commit().unwrap() > before, "numbers carry on");
    db.wait_durable(c.last_commit().unwrap(), GENEROUS).unwrap();
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(restore_rows(mem), vec![1, 2, 3, 4]);
}

#[test]
fn close_uploads_every_waiting_background_commit() {
    let mem = Arc::new(MemStore::new());
    let (_d, db) = open(mem.clone(), &Recorder::default());
    let c = db.connect().unwrap();
    create(&c);
    c.set_batching(Some(bg(10_000, 64, 1000))).unwrap();
    for x in 1..=5 {
        insert(&c, x).unwrap();
    }
    assert!(db.upload_stats().commits_behind > 0);
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(restore_rows(mem), vec![1, 2, 3, 4, 5]);
}

#[test]
fn a_fence_drops_the_waiting_commits_and_the_position_stops() {
    let mem = Arc::new(MemStore::new());
    let (_d, db) = open(mem.clone(), &Recorder::default());
    let c = db.connect().unwrap();
    create(&c);
    let at = db.durable_commit();
    c.set_batching(Some(bg(10_000, 64, 1000))).unwrap();
    insert(&c, 1).unwrap();
    insert(&c, 2).unwrap();
    let last = c.last_commit().unwrap();
    let seqs: Vec<u64> = mem
        .list("log/0000000001/")
        .unwrap()
        .iter()
        .map(|k| k.rsplit('/').next().unwrap().parse().unwrap())
        .collect();
    let next = seqs.iter().max().unwrap() + 1;
    mem.put(
        &format!("log/0000000001/{next:020}"),
        b"another writer's record",
    )
    .unwrap();
    // An Off commit from another connection ships everything and finds the fence.
    let other = db.connect().unwrap();
    other.set_batching(Some(Batching::Off)).unwrap();
    let r = insert(&other, 3);
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
    let r = db.wait_durable(last, Duration::from_secs(5));
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
    assert_eq!(
        db.durable_commit(),
        at,
        "nothing of the dropped commits landed"
    );
}

#[test]
fn mvcc_refuses_the_background_setting() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = config(dir.path());
    c.journal_mode = JournalMode::Mvcc;
    let db = open_with_store(c, Arc::new(MemStore::new())).unwrap();
    let b = bg(5, 64, 10);
    let r = db.set_batching(b);
    assert!(
        matches!(r, Err(Error::Unsupported(m)) if m == "MVCC: Background batching"),
        "{r:?}"
    );
    let conn = db.connect().unwrap();
    let r = conn.set_batching(Some(b));
    assert!(matches!(r, Err(Error::Unsupported(_))), "{r:?}");
}

#[test]
fn the_pragma_sets_and_reads_a_background_setting() {
    let (_d, db) = open(Arc::new(MemStore::new()), &Recorder::default());
    let c = db.connect().unwrap();
    create(&c);
    c.execute_batch(
        "BEGIN; PRAGMA buckwal_batching = 'background 5ms 64 1MiB 1000'; INSERT INTO t VALUES (1);",
    )
    .unwrap();
    let text = c.query("PRAGMA buckwal_batching", &[]).unwrap();
    assert_eq!(
        text[0][0].cast_text(),
        Some("background 5ms 64 1MiB 1000".to_string())
    );
    c.execute("COMMIT", &[]).unwrap();
    db.wait_durable(c.last_commit().unwrap(), GENEROUS).unwrap();
}
