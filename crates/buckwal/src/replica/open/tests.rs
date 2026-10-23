use super::*;
use crate::engine::tests::PREFIX;
use crate::store::{CondPut, MemStore, Object, ObjectStat, StatsSnap, StoreError};
use crate::telemetry::testing::Capture;
use crate::{CheckpointMode, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::Condvar;

const POLL: Duration = Duration::from_millis(100);

fn wconfig(dir: &Path) -> Config {
    let mut c = Config::new(dir, "http://127.0.0.1:1", "bucket", "key", "secret");
    c.prefix = PREFIX.to_string();
    c.retry_deadline = Duration::from_secs(5);
    c
}

fn rconfig(dir: &Path) -> Config {
    let mut c = wconfig(dir);
    c.replica_poll_interval = POLL;
    c
}

/// A replica's view of the bucket: counts what it sends, and can hold a LIST at a gate.
struct Spy {
    inner: Arc<MemStore>,
    puts: AtomicU64,
    deletes: AtomicU64,
    lists: AtomicU64,
    gets: Mutex<Vec<String>>,
    gate: Mutex<Gate>,
    cv: Condvar,
    /// GETs of keys containing this fail (non-retryable) while it is set.
    fail_gets: Mutex<Option<String>>,
    failed_gets: AtomicU64,
}

#[derive(Default)]
struct Gate {
    blocked: bool,
    waiting: u64,
}

impl Spy {
    fn new(inner: Arc<MemStore>) -> Arc<Spy> {
        Arc::new(Spy {
            inner,
            puts: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            lists: AtomicU64::new(0),
            gets: Mutex::default(),
            gate: Mutex::default(),
            cv: Condvar::new(),
            fail_gets: Mutex::default(),
            failed_gets: AtomicU64::new(0),
        })
    }
    fn fail_gets_under(&self, part: &str) {
        *self.fail_gets.lock().unwrap() = Some(part.to_string());
    }
    fn heal(&self) {
        *self.fail_gets.lock().unwrap() = None;
    }
    fn page_gets_failed(&self) -> u64 {
        self.failed_gets.load(SeqCst)
    }
    fn block(&self) {
        self.gate.lock().unwrap().blocked = true;
    }
    fn unblock(&self) {
        self.gate.lock().unwrap().blocked = false;
        self.cv.notify_all();
    }
    /// Wait until a LIST waits at the gate.
    fn wait_blocked(&self) {
        let g = self.gate.lock().unwrap();
        let (_g, t) = self
            .cv
            .wait_timeout_while(g, Duration::from_secs(10), |g| g.waiting == 0)
            .unwrap();
        assert!(!t.timed_out(), "no LIST reached the gate");
    }
    fn pass_gate(&self) {
        let mut g = self.gate.lock().unwrap();
        if !g.blocked {
            return;
        }
        g.waiting += 1;
        self.cv.notify_all();
        g = self.cv.wait_while(g, |g| g.blocked).unwrap();
        g.waiting -= 1;
    }
    fn got(&self) -> Vec<String> {
        self.gets.lock().unwrap().clone()
    }
}

impl ObjectStore for Spy {
    fn put(&self, key: &str, body: &[u8]) -> std::result::Result<String, StoreError> {
        self.puts.fetch_add(1, SeqCst);
        self.inner.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> std::result::Result<CondPut, StoreError> {
        self.puts.fetch_add(1, SeqCst);
        self.inner.put_if_absent(key, body)
    }
    fn put_if_match(
        &self,
        key: &str,
        body: &[u8],
        etag: &str,
    ) -> std::result::Result<CondPut, StoreError> {
        self.puts.fetch_add(1, SeqCst);
        self.inner.put_if_match(key, body, etag)
    }
    fn get(&self, key: &str) -> std::result::Result<Option<Object>, StoreError> {
        self.gets.lock().unwrap().push(key.to_string());
        if let Some(part) = &*self.fail_gets.lock().unwrap() {
            if key.contains(part.as_str()) {
                self.failed_gets.fetch_add(1, SeqCst);
                return Err(StoreError::Permanent("injected".into()));
            }
        }
        self.inner.get(key)
    }
    fn stat(&self, key: &str) -> std::result::Result<Option<ObjectStat>, StoreError> {
        self.inner.stat(key)
    }
    fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StoreError> {
        self.inner.list(prefix)
    }
    fn list_after(
        &self,
        prefix: &str,
        after: &str,
    ) -> std::result::Result<Vec<String>, StoreError> {
        self.lists.fetch_add(1, SeqCst);
        self.pass_gate();
        self.inner.list_after(prefix, after)
    }
    fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
        self.deletes.fetch_add(1, SeqCst);
        self.inner.delete(key)
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

fn int(v: i64) -> Value {
    Value::from_i64(v)
}

/// A writer on `store` in a new temp dir with table `t` and `n` rows.
fn writer(
    store: &Arc<MemStore>,
    n: i64,
) -> (tempfile::TempDir, crate::Database, crate::Connection) {
    writer_with(store, n, |_| {})
}

fn writer_with(
    store: &Arc<MemStore>,
    n: i64,
    tune: impl FnOnce(&mut Config),
) -> (tempfile::TempDir, crate::Database, crate::Connection) {
    let t = tempfile::tempdir().unwrap();
    let mut c = wconfig(&t.path().join("w"));
    tune(&mut c);
    let db = crate::open_with_store(c, store.clone()).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    for i in 1..=n {
        add(&conn, i);
    }
    (t, db, conn)
}

fn add(c: &crate::Connection, id: i64) {
    c.execute("INSERT INTO t(id, v) VALUES (?, 'x')", &[int(id)])
        .unwrap();
}

fn count(c: &ReadConnection) -> i64 {
    c.query("SELECT count(*) FROM t", &[]).unwrap()[0][0]
        .as_int()
        .unwrap()
}

fn until(what: &str, mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(20);
    while !f() {
        assert!(Instant::now() < end, "timed out waiting for: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_rows(c: &ReadConnection, n: i64) {
    until(&format!("{n} rows"), || count(c) == n);
}

fn unsupported<T: std::fmt::Debug>(r: Result<T>) -> &'static str {
    match r {
        Err(Error::Unsupported(w)) => w,
        r => panic!("expected Unsupported, got {r:?}"),
    }
}

const READ_ONLY: &str = "writes on a read-only open";

#[test]
fn open_replica_follows_writer_commits() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 3);
    let r = tempfile::tempdir().unwrap();
    let replica = open_replica_with_store(rconfig(&r.path().join("r")), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 3);
    for i in 4..=10 {
        add(&wc, i);
    }
    wait_rows(&rc, 10);
    let s = replica.stats();
    assert_eq!(s.generation, 1);
    assert!(s.seq > 0 && s.objects_fetched > 0, "{s:?}");
    assert!(s.last_pass_at.is_some() && s.lag_ms.is_some(), "{s:?}");
    assert_eq!(s.re_restores, 0);
    rc.close().unwrap();
    replica.close().unwrap();
}

#[test]
fn connect_refuses_insert_update_ddl_pragma_write_attach_checkpoint() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, _wc) = writer(&store, 2);
    let r = tempfile::tempdir().unwrap();
    let replica = open_replica_with_store(rconfig(&r.path().join("r")), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 2);
    for sql in [
        "INSERT INTO t(id, v) VALUES (99, 'y')",
        "UPDATE t SET v = 'z'",
        "DELETE FROM t",
        "CREATE TABLE u(x)",
        "DROP TABLE t",
        "PRAGMA user_version = 7",
        "PRAGMA wal_checkpoint(TRUNCATE)",
        "BEGIN IMMEDIATE",
    ] {
        assert_eq!(unsupported(rc.query(sql, &[])), READ_ONLY, "{sql}");
    }
    assert_eq!(unsupported(rc.query("ATTACH 'x.db' AS x", &[])), "ATTACH");
    assert_eq!(
        unsupported(rc.run("UPDATE t SET v = 'z'", &[], &[])),
        READ_ONLY
    );
    assert_eq!(count(&rc), 2, "nothing changed");
    // Reads, describe and a read transaction work.
    assert!(rc.describe("SELECT * FROM t").unwrap().is_readonly);
    assert!(rc.is_autocommit());
    let n = rc
        .transaction(|tx| Ok(tx.query("SELECT count(*) FROM t", &[])?[0][0].as_int()))
        .unwrap();
    assert_eq!(n, Some(2));
    rc.close().unwrap();
    replica.close().unwrap();
}

#[test]
fn batching_pragma_is_refused() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, _wc) = writer(&store, 1);
    let r = tempfile::tempdir().unwrap();
    let replica = open_replica_with_store(rconfig(&r.path().join("r")), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    for sql in [
        "PRAGMA buckwal_batching",
        "PRAGMA buckwal_batching = off",
        "pragma BUCKWAL_BATCHING(off)",
    ] {
        assert_eq!(
            unsupported(rc.query(sql, &[])),
            "replica: PRAGMA buckwal_batching",
            "{sql}"
        );
    }
    assert_eq!(
        unsupported(rc.describe("PRAGMA buckwal_batching")),
        "replica: PRAGMA buckwal_batching"
    );
    assert_eq!(
        unsupported(rc.run("PRAGMA buckwal_batching", &[], &[])),
        "replica: PRAGMA buckwal_batching"
    );
    rc.close().unwrap();
    replica.close().unwrap();
}

fn key_config(dir: &Path, keydir: &Path, hex: &str, mk: fn(&Path) -> Config) -> Config {
    let path = keydir.join(format!("key-{hex}"));
    std::fs::write(&path, hex).unwrap();
    let mut c = mk(dir);
    c.encryption_key_file = Some(path);
    c
}

#[test]
fn wrong_key_refused_at_open_before_any_follow() {
    let store = Arc::new(MemStore::new());
    let k = tempfile::tempdir().unwrap();
    let t = tempfile::tempdir().unwrap();
    let good = "11".repeat(32);
    let db = crate::open_with_store(
        key_config(&t.path().join("w"), k.path(), &good, wconfig),
        store.clone(),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    add(&conn, 1);

    let r = tempfile::tempdir().unwrap();
    let spy = Spy::new(store.clone());
    let wrong = key_config(&r.path().join("r1"), k.path(), &"22".repeat(32), rconfig);
    assert!(matches!(
        open_replica_with_store(wrong, spy.clone()),
        Err(Error::Encryption(_))
    ));
    let none = rconfig(&r.path().join("r2"));
    assert!(matches!(
        open_replica_with_store(none, spy.clone()),
        Err(Error::Encryption(_))
    ));
    assert!(
        spy.got().iter().all(|k| k.ends_with("/head")),
        "before any follow only the head was read: {:?}",
        spy.got()
    );
    assert_eq!(spy.lists.load(SeqCst), 0);

    // The right key follows, decrypting each frame.
    let replica = open_replica_with_store(
        key_config(&r.path().join("r3"), k.path(), &good, rconfig),
        store.clone(),
    )
    .unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 1);
    add(&conn, 2);
    wait_rows(&rc, 2);
    rc.close().unwrap();
    replica.close().unwrap();
}

#[test]
fn mvcc_head_refused_with_unsupported() {
    let store = Arc::new(MemStore::new());
    let t = tempfile::tempdir().unwrap();
    let mut c = wconfig(&t.path().join("w"));
    c.journal_mode = JournalMode::Mvcc;
    let db = crate::open_with_store(c, store.clone()).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)", &[])
        .unwrap();
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    assert_eq!(
        unsupported(open_replica_with_store(rconfig(&dir), store.clone())),
        "replica: MVCC"
    );
    assert!(!dir.join("app.db").exists(), "nothing restored");
}

#[test]
fn mvcc_head_mid_follow_stops_the_follower_with_an_error_log() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, _wc) = writer(&store, 2);
    let r = tempfile::tempdir().unwrap();
    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let replica = open_replica_with_store(rconfig(&r.path().join("r")), store.clone()).unwrap();
        let rc = replica.connect().unwrap();
        wait_rows(&rc, 2);
        until("the start pass", || replica.stats().last_pass_at.is_some());
        let keys = Keys::new(PREFIX);
        let mut head = keys
            .decode_head(&store.get(&keys.head()).unwrap().unwrap().body)
            .unwrap();
        head.journal_mode = JournalMode::Mvcc;
        head.format = crate::format::FORMAT_V2;
        store.put(&keys.head(), &keys.encode_head(&head)).unwrap();
        assert_eq!(replica.stats().stopped, None);
        replica.waker().wake();
        until("the follower to stop", || replica.thread_finished());
        assert_eq!(replica.stats().stopped.as_deref(), Some("mvcc"));
        // Stats stay readable and connections still work on the last applied state.
        assert!(replica.stats().last_pass_at.is_some());
        assert_eq!(count(&replica.connect().unwrap()), 2);
        rc.close().unwrap();
        replica.close().unwrap();
        cap.spans()
    });
    let e = spans
        .events
        .iter()
        .find(|e| e.field("message") == Some("replica stopped"))
        .expect("replica stopped");
    assert_eq!(e.level, tracing::Level::ERROR);
    assert_eq!(e.field("reason"), Some("mvcc"));
    assert_eq!(e.field("generation"), Some("1"));
    assert!(e.field("seq").is_some());
}

#[test]
fn waker_burst_coalesces_to_one_extra_pass() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 1);
    let spy = Spy::new(store.clone());
    let r = tempfile::tempdir().unwrap();
    let mut c = rconfig(&r.path().join("r"));
    c.replica_poll_interval = Duration::from_secs(120);
    let replica = open_replica_with_store(c, spy.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 1);
    until("the start pass", || replica.stats().last_pass_at.is_some());
    let waker = replica.waker();
    assert_eq!(replica.stats().passes_notification, 0);
    let lists = spy.lists.load(SeqCst);

    spy.block();
    waker.wake();
    spy.wait_blocked();
    assert_eq!(replica.stats().passes_notification, 1);
    add(&wc, 2);
    for _ in 0..100 {
        waker.wake();
    }
    spy.unblock();
    wait_rows(&rc, 2);
    until("the extra pass", || {
        replica.stats().passes_notification == 2
    });
    // Nothing more follows.
    std::thread::sleep(Duration::from_millis(400));
    let s = replica.stats();
    assert_eq!(s.passes_notification, 2, "{s:?}");
    assert_eq!(s.notifications_received, 101);
    assert_eq!(spy.lists.load(SeqCst) - lists, 2, "two passes, two LISTs");
    // Both waker clones work from other threads.
    let w2 = waker.clone();
    std::thread::spawn(move || w2.wake()).join().unwrap();
    until("a pass for the wake", || {
        replica.stats().passes_notification == 3
    });
    rc.close().unwrap();
    replica.close().unwrap();
}

#[test]
fn slow_poll_alone_is_correct_and_warns_once() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 1);
    let r = tempfile::tempdir().unwrap();
    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let replica = open_replica_with_store(rconfig(&r.path().join("r")), store.clone()).unwrap();
        let rc = replica.connect().unwrap();
        wait_rows(&rc, 1);
        add(&wc, 2);
        add(&wc, 3);
        wait_rows(&rc, 3);
        until("three slow polls", || replica.stats().passes_slow_poll >= 3);
        let s = replica.stats();
        assert_eq!(s.notifications_received, 0);
        assert_eq!(s.passes_notification, 0);
        rc.close().unwrap();
        replica.close().unwrap();
        cap.spans()
    });
    let warns = spans
        .events
        .iter()
        .filter(|e| {
            e.field("message")
                == Some("no notification received yet, running on the slow poll only")
        })
        .count();
    assert_eq!(warns, 1);
}

#[test]
fn no_warning_when_a_notification_came_before_the_first_slow_poll() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, _wc) = writer(&store, 1);
    let r = tempfile::tempdir().unwrap();
    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let mut c = rconfig(&r.path().join("r"));
        c.replica_poll_interval = Duration::from_millis(300);
        let replica = open_replica_with_store(c, store.clone()).unwrap();
        replica.waker().wake();
        until("a slow poll", || replica.stats().passes_slow_poll >= 2);
        replica.close().unwrap();
        cap.spans()
    });
    assert!(!spans.events.iter().any(|e| e
        .field("message")
        .is_some_and(|m| m.starts_with("no notification"))));
}

#[test]
fn second_open_of_same_dir_is_refused_by_lock() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, _wc) = writer(&store, 1);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    assert!(matches!(
        open_replica_with_store(rconfig(&dir), store.clone()),
        Err(Error::AlreadyOpen(_))
    ));
    replica.close().unwrap();
    // Released: it opens again (and resumes).
    open_replica_with_store(rconfig(&dir), store.clone())
        .unwrap()
        .close()
        .unwrap();
}

#[test]
fn replica_sends_zero_put_or_delete() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 2);
    wc.checkpoint(CheckpointMode::Truncate).unwrap();
    add(&wc, 3);
    let spy = Spy::new(store.clone());
    let r = tempfile::tempdir().unwrap();
    let replica = open_replica_with_store(rconfig(&r.path().join("r")), spy.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 3);
    add(&wc, 4);
    wait_rows(&rc, 4);
    until("a slow poll", || replica.stats().passes_slow_poll >= 1);
    rc.close().unwrap();
    replica.close().unwrap();
    assert_eq!(spy.puts.load(SeqCst), 0);
    assert_eq!(spy.deletes.load(SeqCst), 0);
    assert!(spy.lists.load(SeqCst) > 0);
}

#[test]
fn reopen_resumes_from_the_position_file() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 3);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 3);
    rc.close().unwrap();
    replica.close().unwrap();
    let id = Position::read(&dir).unwrap().unwrap().replica_id;
    add(&wc, 4);

    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 4);
    assert_eq!(Position::read(&dir).unwrap().unwrap().replica_id, id);
    rc.close().unwrap();
    replica.close().unwrap();
}

#[test]
fn a_replica_whose_local_wal_is_ahead_of_the_bucket_still_opens() {
    // The writer's attach/reconcile would refuse this directory (its `-wal` is ahead of what
    // the bucket holds); a replica never goes through them.
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, _wc) = writer(&store, 3);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 3);
    rc.close().unwrap();
    replica.close().unwrap();
    // The bucket's log loses its newest record; the local WAL keeps it.
    let keys = Keys::new(PREFIX);
    let newest = store.list(&keys.log_prefix(1)).unwrap().pop().unwrap();
    store.delete(&newest).unwrap();
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    replica.close().unwrap();
}

#[test]
fn missing_position_file_at_open_restores_into_the_replica_dir() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 3);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 3);
    rc.close().unwrap();
    replica.close().unwrap();
    add(&wc, 4);
    for damage in [0, 1] {
        if damage == 0 {
            std::fs::remove_file(dir.join("app.db-replica")).unwrap();
        } else {
            std::fs::write(dir.join("app.db-replica"), b"garbage").unwrap();
        }
        let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
        let rc = replica.connect().unwrap();
        wait_rows(&rc, 4);
        assert!(Position::read(&dir).unwrap().is_some(), "written afresh");
        rc.close().unwrap();
        replica.close().unwrap();
    }
}

#[test]
fn a_directory_that_is_not_a_replicas_is_refused_and_left_alone() {
    let store = Arc::new(MemStore::new());
    let (wd, _wdb, _wc) = writer(&store, 1);
    // The writer's own directory.
    let wdir: PathBuf = wd.path().join("w");
    let cfg = rconfig(&wdir);
    drop(_wc);
    drop(_wdb);
    let before = std::fs::read(wdir.join("app.db-wal")).unwrap();
    assert!(matches!(
        open_replica_with_store(cfg, store.clone()),
        Err(Error::Config(m)) if m.contains("writer")
    ));
    assert_eq!(std::fs::read(wdir.join("app.db-wal")).unwrap(), before);
    // A directory holding something else.
    let r = tempfile::tempdir().unwrap();
    std::fs::write(r.path().join("notes.txt"), b"mine").unwrap();
    assert!(matches!(
        open_replica_with_store(rconfig(r.path()), store.clone()),
        Err(Error::Config(m)) if m.contains("notes.txt")
    ));
    assert!(r.path().join("notes.txt").exists());
}

#[test]
fn follows_across_seals_and_resumes_after_them() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 5);
    wc.checkpoint(CheckpointMode::Truncate).unwrap();
    add(&wc, 6);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    // Opened after a seal: pages come from the manifest, the rest from the log.
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 6);
    assert_eq!(replica.stats().generation, 2);
    // A seal while following.
    wc.checkpoint(CheckpointMode::Truncate).unwrap();
    add(&wc, 7);
    wait_rows(&rc, 7);
    until("generation 3", || replica.stats().generation == 3);
    rc.close().unwrap();
    replica.close().unwrap();

    // Stopped across another seal, then resumed from the position file.
    wc.checkpoint(CheckpointMode::Truncate).unwrap();
    add(&wc, 8);
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 8);
    until("generation 4", || replica.stats().generation == 4);
    assert_eq!(replica.stats().re_restores, 0);
    let ids: Vec<i64> = rc
        .query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect();
    assert_eq!(ids, (1..=8).collect::<Vec<_>>());
    rc.close().unwrap();
    replica.close().unwrap();
}

#[test]
fn refusals_before_the_restore_leave_the_directory_untouched() {
    let store = Arc::new(MemStore::new());
    let t = tempfile::tempdir().unwrap();
    let mut c = wconfig(&t.path().join("w"));
    c.journal_mode = JournalMode::Mvcc;
    let db = crate::open_with_store(c, store.clone()).unwrap();
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    std::fs::create_dir_all(&dir).unwrap();
    // An interrupted restore's marker is not swept by a refused open.
    std::fs::write(dir.join("app.db-restoring"), b"").unwrap();
    assert_eq!(
        unsupported(open_replica_with_store(rconfig(&dir), store.clone())),
        "replica: MVCC"
    );
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, ["app.db-restoring", "durable.lock"]);
    drop(db);
}

#[test]
fn files_without_a_local_state_are_refused_and_left_alone() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, _wc) = writer(&store, 1);
    for f in ["app.db-wal", "app.db-log", "app.db"] {
        let r = tempfile::tempdir().unwrap();
        std::fs::write(r.path().join(f), b"unshipped").unwrap();
        assert!(
            matches!(
                open_replica_with_store(rconfig(r.path()), store.clone()),
                Err(Error::Config(m)) if m.contains(f)
            ),
            "{f}"
        );
        assert_eq!(std::fs::read(r.path().join(f)).unwrap(), b"unshipped");
    }
}

#[test]
fn the_dirty_file_is_compacted_at_open() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 3);
    wc.checkpoint(CheckpointMode::Truncate).unwrap();
    add(&wc, 4);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 4);
    rc.close().unwrap();
    replica.close().unwrap();
    // Every sync appends its pages again: repeats and a torn tail.
    let path = dir.join("app.db-dirty");
    let mut bytes = std::fs::read(&path).unwrap_or_default();
    let distinct: std::collections::BTreeSet<u32> = bytes
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .chain([2, 3])
        .collect();
    for p in [3u32, 2, 3, 2, 3] {
        bytes.extend(p.to_le_bytes());
    }
    bytes.extend([9, 9]);
    std::fs::write(&path, &bytes).unwrap();

    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    let want: Vec<u8> = distinct.iter().flat_map(|p| p.to_le_bytes()).collect();
    assert_eq!(std::fs::read(&path).unwrap(), want);
    let rc = replica.connect().unwrap();
    assert_eq!(count(&rc), 4);
    rc.close().unwrap();
    replica.close().unwrap();
}

// ---- Task 9b: re-restore ----------------------------------------------------------------

const FELL_BEHIND: &str = "replica fell behind, restoring again";

/// A writer with 3 rows and a replica of it (behind a [`Spy`]) that has caught up.
struct Lag {
    store: Arc<MemStore>,
    spy: Arc<Spy>,
    _w: tempfile::TempDir,
    _wdb: crate::Database,
    wc: crate::Connection,
    _r: tempfile::TempDir,
    dir: PathBuf,
    replica: Replica,
}

fn lag() -> Lag {
    lag_with_poll(POLL)
}

fn lag_with_poll(poll: Duration) -> Lag {
    let store = Arc::new(MemStore::new());
    let (w, wdb, wc) = writer(&store, 3);
    let spy = Spy::new(store.clone());
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let mut c = rconfig(&dir);
    c.replica_poll_interval = poll;
    let replica = open_replica_with_store(c, spy.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 3);
    rc.close().unwrap();
    Lag {
        store,
        spy,
        _w: w,
        _wdb: wdb,
        wc,
        _r: r,
        dir,
        replica,
    }
}

impl Lag {
    /// Hold the replica's pass, write 5 more rows across a seal (generation 2), delete what
    /// generation 1's log held beyond the replica's seq (the trimmer's work), release.
    /// Returns the seq the replica had reached.
    fn fall_behind(&self) -> u64 {
        self.spy.block();
        self.replica.waker().wake();
        self.spy.wait_blocked();
        let applied = self.replica.stats().seq;
        for i in 4..=6 {
            add(&self.wc, i);
        }
        self.wc.checkpoint(CheckpointMode::Truncate).unwrap();
        for i in 7..=8 {
            add(&self.wc, i);
        }
        let keys = Keys::new(PREFIX);
        let keep = keys.log(1, applied);
        for k in ObjectStore::list(&*self.store, &keys.log_prefix(1)).unwrap() {
            if k > keep {
                ObjectStore::delete(&*self.store, &k).unwrap();
            }
        }
        self.spy.unblock();
        applied
    }

    fn wait_re_restore(&self) {
        until("a re-restore", || self.replica.stats().re_restores >= 1);
    }
}

fn fell_behind_events(
    s: &crate::telemetry::testing::Spans,
) -> Vec<&crate::telemetry::testing::EventRec> {
    s.events
        .iter()
        .filter(|e| e.field("message") == Some(FELL_BEHIND))
        .collect()
}

#[test]
fn lagging_past_trim_grace_re_restores_warns_once_and_catches_up() {
    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let l = lag();
        let applied = l.fall_behind();
        l.wait_re_restore();
        let rc = l.replica.connect().unwrap();
        wait_rows(&rc, 8);
        add(&l.wc, 9);
        wait_rows(&rc, 9);
        until("two passes after", || {
            l.replica.stats().passes_slow_poll >= 2
        });
        assert_eq!(l.replica.stats().re_restores, 1, "exactly one re-restore");
        assert_eq!(l.replica.stats().generation, 2);
        assert!(applied > 0);
        rc.close().unwrap();
        l.replica.close().unwrap();
        cap.spans()
    });
    let warns = fell_behind_events(&spans);
    assert_eq!(warns.len(), 1, "exactly one WARN: {warns:?}");
    let w = warns[0];
    assert_eq!(w.level, tracing::Level::WARN);
    assert_eq!(w.target, "buckwal");
    assert_eq!(w.field("generation"), Some("1"));
    assert_eq!(w.field("reason"), Some("trimmed"));
    assert!(w
        .field("seq")
        .is_some_and(|s| s.parse::<u64>().unwrap() > 0));
    assert!(w
        .field("behind_ms")
        .is_some_and(|s| s.parse::<u64>().is_ok()));
    assert!(w.field("replica_id").is_some_and(|s| !s.is_empty()));
}

/// Edit a log record of generation 1 that holds a frame so it differs from what a replica
/// applied.
fn corrupt_first_frame(store: &MemStore) {
    let keys = Keys::new(PREFIX);
    for seq in 1..=20 {
        let key = keys.log(1, seq);
        let body = ObjectStore::get(store, &key).unwrap().unwrap().body;
        let mut rec = keys.decode_record(&body, 1, seq).unwrap();
        let crate::format::LogRecord::Extents { syncs, .. } = &mut rec else {
            continue;
        };
        let Some(op) = syncs
            .iter_mut()
            .flatten()
            .filter_map(|o| match o {
                crate::format::Op::Write { bytes, .. } if bytes.len() >= 4096 => Some(bytes),
                _ => None,
            })
            .next()
        else {
            continue;
        };
        // A byte in the page's unused middle: the frame differs, the database stays valid.
        let i = (1500..3000)
            .find(|i| op[*i] == 0 && op[i + 1] == 0)
            .unwrap();
        op[i] = 0x5a;
        ObjectStore::put(store, &key, &keys.encode_record(&rec, 1, seq)).unwrap();
        return;
    }
    panic!("no record holds a frame");
}

#[test]
fn conflict_re_restores_after_error_log() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 3);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 3);
    rc.close().unwrap();
    replica.close().unwrap();
    corrupt_first_frame(&store);
    add(&wc, 4);

    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
        until("a re-restore", || replica.stats().re_restores >= 1);
        let rc = replica.connect().unwrap();
        wait_rows(&rc, 4);
        assert_eq!(replica.stats().stopped, None, "following again");
        let s = replica.stats();
        assert!(s
            .last_re_restore_at
            .as_ref()
            .unwrap()
            .1
            .contains("conflict"));
        rc.close().unwrap();
        replica.close().unwrap();
        cap.spans()
    });
    let err = spans
        .events
        .iter()
        .find(|e| e.level == tracing::Level::ERROR && e.field("generation").is_some())
        .expect("an ERROR naming the generation");
    assert_eq!(err.field("generation"), Some("1"));
    assert!(
        err.field("error").is_some_and(|e| e.contains("frame")),
        "{err:?}"
    );
    let warns = fell_behind_events(&spans);
    assert_eq!(warns.len(), 1);
    assert_eq!(warns[0].field("reason"), Some("conflict"));
}

#[test]
fn stats_after_re_restore_shows_count_reason_and_jumped_position() {
    let l = lag();
    let before = l.replica.stats();
    assert_eq!((before.generation, before.re_restores), (1, 0));
    assert!(before.last_re_restore_at.is_none());
    let passes = before.passes_slow_poll + before.passes_notification;
    let objects = before.objects_fetched;
    l.fall_behind();
    l.wait_re_restore();
    until("the jump", || {
        let s = l.replica.stats();
        s.generation == 2 && s.lag_ms.is_some()
    });
    let s = l.replica.stats();
    assert_eq!(s.re_restores, 1);
    let (at, why) = s.last_re_restore_at.clone().unwrap();
    assert!(at.elapsed().unwrap() < Duration::from_secs(60));
    assert!(
        why.starts_with("log/1/") && why.contains("trimmed: replica was "),
        "{why}"
    );
    assert!(s.seq > 0, "the position is the head's: {s:?}");
    assert!(
        s.passes_slow_poll + s.passes_notification >= passes,
        "carried on"
    );
    assert!(s.objects_fetched >= objects, "carried on");
    assert_eq!(s.stopped, None);
    l.replica.close().unwrap();
}

#[test]
fn old_connection_outside_a_transaction_is_rebound_to_the_new_copy() {
    let l = lag();
    let old = l.replica.connect().unwrap();
    assert_eq!(count(&old), 3);
    l.fall_behind();
    l.wait_re_restore();
    // The same connection, no transaction open: its next statement runs on the new copy.
    assert_eq!(count(&old), 8);
    add(&l.wc, 9);
    wait_rows(&old, 9);
    old.close().unwrap();
    l.replica.close().unwrap();
}

/// Implemented: a connection inside a transaction keeps its old snapshot (the old engine
/// serves it from its own files) until the transaction ends; it may instead get an error if the
/// old engine can no longer serve it. After the transaction the connection is rebound.
#[test]
fn a_transaction_across_the_swap_keeps_its_snapshot_or_errors_then_the_connection_rebinds() {
    let l = lag();
    let rc = l.replica.connect().unwrap();
    let r = rc.transaction(|tx| {
        assert_eq!(
            tx.query("SELECT count(*) FROM t", &[])?[0][0].as_int(),
            Some(3)
        );
        l.fall_behind();
        l.wait_re_restore();
        Ok(tx
            .query("SELECT count(*) FROM t", &[])
            .map(|r| r[0][0].as_int()))
    });
    match r {
        Ok(Ok(Some(3))) => {}
        Ok(Err(_)) | Err(_) => {}
        other => panic!("neither the old snapshot nor an error: {other:?}"),
    }
    assert!(rc.is_autocommit());
    assert_eq!(count(&rc), 8, "rebound once the transaction is over");
    rc.close().unwrap();
    l.replica.close().unwrap();
}

#[test]
fn a_dropped_replica_keeps_the_directory_locked_while_a_connection_lives() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, _wc) = writer(&store, 2);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 2);
    drop(replica);
    assert!(matches!(
        open_replica_with_store(rconfig(&dir), store.clone()),
        Err(Error::AlreadyOpen(_) | Error::Locked(_))
    ));
    assert_eq!(count(&rc), 2, "the connection still reads");
    drop(rc);
    open_replica_with_store(rconfig(&dir), store.clone())
        .unwrap()
        .close()
        .unwrap();
}

#[test]
fn behind_ms_after_a_restart_comes_from_the_last_pass_before_it() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 3);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 3);
    until("a pass", || replica.stats().last_pass_at.is_some());
    let applied = replica.stats().seq;
    rc.close().unwrap();
    replica.close().unwrap();
    std::thread::sleep(Duration::from_millis(600));
    for i in 4..=5 {
        add(&wc, i);
    }
    wc.checkpoint(CheckpointMode::Truncate).unwrap();
    add(&wc, 6);
    let keys = Keys::new(PREFIX);
    for k in ObjectStore::list(&*store, &keys.log_prefix(1)).unwrap() {
        if k > keys.log(1, applied) {
            ObjectStore::delete(&*store, &k).unwrap();
        }
    }
    let cap = Capture::new();
    let (spans, why) = tracing::subscriber::with_default(cap.subscriber(), || {
        let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
        until("a re-restore", || replica.stats().re_restores >= 1);
        let why = replica.stats().last_re_restore_at.unwrap().1;
        replica.close().unwrap();
        (cap.spans(), why)
    });
    let w = fell_behind_events(&spans)[0];
    let behind: u64 = w.field("behind_ms").unwrap().parse().unwrap();
    assert!(
        behind >= 550,
        "behind_ms {behind} counts the time the replica was down"
    );
    assert!(
        why.contains("replica was ") && !why.contains("was 0ms"),
        "{why}"
    );
}

#[test]
fn a_failing_re_restore_backs_off_and_logs_once_per_episode() {
    let cap = Capture::new();
    let (spans, attempts, passes) = tracing::subscriber::with_default(cap.subscriber(), || {
        // A long interval makes the backoff (a quarter of it, doubling) far longer than the
        // wake loop below, however loaded the machine is.
        let l = lag_with_poll(Duration::from_secs(4));
        l.spy.fail_gets_under("manifest/");
        l.fall_behind();
        until("a failed attempt", || l.spy.page_gets_failed() > 0);
        let waker = l.replica.waker();
        let before = l.spy.page_gets_failed();
        let passes0 = l.replica.stats().passes_notification;
        for _ in 0..40 {
            waker.wake();
            std::thread::sleep(Duration::from_millis(5));
        }
        let attempts = l.spy.page_gets_failed() - before;
        let passes = l.replica.stats().passes_notification - passes0;
        l.spy.heal();
        l.wait_re_restore();
        let rc = l.replica.connect().unwrap();
        wait_rows(&rc, 8);
        rc.close().unwrap();
        l.replica.close().unwrap();
        (cap.spans(), attempts, passes)
    });
    assert!(passes >= 5, "{passes} passes");
    assert!(
        attempts <= 6 && attempts < passes,
        "{attempts} attempts in {passes} passes"
    );
    assert_eq!(fell_behind_events(&spans).len(), 1, "one WARN per episode");
    let failed: Vec<_> = spans
        .events
        .iter()
        .filter(|e| {
            e.field("message") == Some("replica: restoring again failed, trying again later")
        })
        .collect();
    assert_eq!(failed.len(), 1, "the failure is logged once per episode");
    assert!(failed[0]
        .field("error")
        .is_some_and(|e| e.contains("injected")));
}

#[test]
fn a_crash_while_the_pointed_copy_is_cleared_still_re_restores_at_the_next_open() {
    // Clear steps: the pointer, then each file of the copy, then the directory's own files.
    for k in 1..=8 {
        let l = lag();
        l.fall_behind();
        l.wait_re_restore();
        let rc = l.replica.connect().unwrap();
        wait_rows(&rc, 8);
        rc.close().unwrap();
        l.replica.close().unwrap();
        clear_replica_dir_n(&l.dir, Some(k)).unwrap();
        let again = open_replica_with_store(rconfig(&l.dir), l.spy.clone())
            .unwrap_or_else(|e| panic!("open after {k} removals: {e}"));
        let rc = again.connect().unwrap();
        wait_rows(&rc, 8);
        rc.close().unwrap();
        again.close().unwrap();
        let names: Vec<_> = std::fs::read_dir(l.dir.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.len() <= 2, "after {k}: no orphan copy: {names:?}");
    }
}

#[test]
fn leftovers_of_the_old_copy_after_the_pointer_write_are_removed_at_open() {
    let l = lag();
    l.fall_behind();
    l.wait_re_restore();
    let rc = l.replica.connect().unwrap();
    wait_rows(&rc, 8);
    rc.close().unwrap();
    let dir = l.dir.clone();
    let spy = l.spy.clone();
    let copy = pointed_copy(&dir).unwrap();
    l.replica.close().unwrap();
    // A crash between the pointer write and the old copy's removal.
    for f in ["app.db", "app.db-wal", "app.db-durable", "app.db-replica"] {
        if copy.join(f).exists() {
            std::fs::copy(copy.join(f), dir.join(f)).unwrap();
        }
    }
    let again = open_replica_with_store(rconfig(&dir), spy).unwrap();
    for f in ["app.db", "app.db-wal", "app.db-durable", "app.db-replica"] {
        assert!(!dir.join(f).exists(), "{f} left over");
    }
    let rc = again.connect().unwrap();
    wait_rows(&rc, 8);
    rc.close().unwrap();
    again.close().unwrap();
}

#[test]
fn only_directories_shaped_like_copies_are_swept() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, _wc) = writer(&store, 1);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let mine = r.path().join(".r.data-notes");
    std::fs::create_dir(&mine).unwrap();
    let orphan = r.path().join(".r.data-12-345");
    std::fs::create_dir(&orphan).unwrap();
    open_replica_with_store(rconfig(&dir), store.clone())
        .unwrap()
        .close()
        .unwrap();
    assert!(mine.exists() && !orphan.exists());
}

#[test]
fn re_restore_removes_the_old_copy_sends_nothing_and_a_reopen_follows_the_new_one() {
    let l = lag();
    let parent = l.dir.parent().unwrap().to_path_buf();
    assert!(l.dir.join("app.db").exists());
    l.fall_behind();
    l.wait_re_restore();
    let rc = l.replica.connect().unwrap();
    wait_rows(&rc, 8);
    rc.close().unwrap();
    // The old copy is gone: the directory keeps the lock and the pointer to the new copy.
    for f in ["app.db", "app.db-wal", "app.db-durable", "app.db-replica"] {
        assert!(!l.dir.join(f).exists(), "{f} of the old copy was removed");
    }
    assert!(l.dir.join("durable.lock").exists());
    let siblings: Vec<_> = std::fs::read_dir(&parent)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(siblings.len(), 2, "r and the new copy only: {siblings:?}");
    assert_eq!(l.spy.puts.load(SeqCst), 0);
    assert_eq!(l.spy.deletes.load(SeqCst), 0);
    l.replica.close().unwrap();
    // The lock path is still the one directory; a reopen resumes on the new copy.
    let again = open_replica_with_store(rconfig(&l.dir), l.spy.clone()).unwrap();
    let rc = again.connect().unwrap();
    wait_rows(&rc, 8);
    add(&l.wc, 9);
    wait_rows(&rc, 9);
    assert_eq!(again.stats().re_restores, 0);
    rc.close().unwrap();
    again.close().unwrap();
    assert_eq!(l.spy.puts.load(SeqCst), 0);
}

#[test]
fn a_second_re_restore_removes_the_first_new_copy() {
    let l = lag();
    l.fall_behind();
    l.wait_re_restore();
    let rc = l.replica.connect().unwrap();
    wait_rows(&rc, 8);
    rc.close().unwrap();
    // Lag again: seal once more with the replica held, and trim.
    l.spy.block();
    l.replica.waker().wake();
    l.spy.wait_blocked();
    let applied = l.replica.stats().seq;
    add(&l.wc, 9);
    l.wc.checkpoint(CheckpointMode::Truncate).unwrap();
    add(&l.wc, 10);
    let keys = Keys::new(PREFIX);
    for k in ObjectStore::list(&*l.store, &keys.log_prefix(2)).unwrap() {
        if k > keys.log(2, applied) {
            ObjectStore::delete(&*l.store, &k).unwrap();
        }
    }
    l.spy.unblock();
    until("the second re-restore", || {
        l.replica.stats().re_restores >= 2
    });
    let rc = l.replica.connect().unwrap();
    wait_rows(&rc, 10);
    rc.close().unwrap();
    let siblings = std::fs::read_dir(l.dir.parent().unwrap()).unwrap().count();
    assert_eq!(siblings, 2, "r and the newest copy only");
    l.replica.close().unwrap();
}

#[test]
fn a_failed_re_restore_leaves_no_half_directory_and_the_replica_goes_on() {
    let l = lag();
    // Fail the restore: its first log GET fails for good (the replica only reads head and log).
    l.spy.fail_gets_under("manifest/");
    l.fall_behind();
    until("a failed attempt", || {
        l.spy.page_gets_failed() > 0 || l.replica.stats().re_restores > 0
    });
    l.spy.heal();
    l.wait_re_restore();
    let rc = l.replica.connect().unwrap();
    wait_rows(&rc, 8);
    rc.close().unwrap();
    let siblings: Vec<_> = std::fs::read_dir(l.dir.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(siblings.len(), 2, "no leftovers: {siblings:?}");
    l.replica.close().unwrap();
}

#[test]
fn a_crash_mid_restore_leaves_nothing_visible_at_the_next_open() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, _wc) = writer(&store, 3);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    replica.close().unwrap();
    // What a crash during a re-restore leaves beside the replica directory.
    let half = r.path().join(".r.data-1-1");
    std::fs::create_dir(&half).unwrap();
    std::fs::write(half.join("app.db"), b"half").unwrap();
    std::fs::write(half.join("app.db-restoring"), b"").unwrap();
    let staging = r.path().join("..r.data-1-1.restore-1-1");
    std::fs::create_dir(&staging).unwrap();
    std::fs::write(staging.join("app.db-wal"), b"half").unwrap();
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    assert!(!half.exists() && !staging.exists());
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 3);
    rc.close().unwrap();
    replica.close().unwrap();
}

#[test]
fn a_crash_after_any_removal_of_the_clear_still_re_restores_at_the_next_open() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 3);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 3);
    rc.close().unwrap();
    replica.close().unwrap();
    let files = replica_file_count(&dir);
    assert!(files >= 4, "{files}");
    for k in 1..=files {
        // The clear dies after its k-th removal.
        clear_replica_dir_n(&dir, Some(k)).unwrap();
        add(&wc, 100 + k as i64);
        let replica = open_replica_with_store(rconfig(&dir), store.clone())
            .unwrap_or_else(|e| panic!("open after {k} removals: {e}"));
        let rc = replica.connect().unwrap();
        wait_rows(&rc, 3 + k as i64);
        rc.close().unwrap();
        replica.close().unwrap();
    }
}

#[test]
fn a_refused_directory_says_what_to_delete() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, _wc) = writer(&store, 1);
    let r = tempfile::tempdir().unwrap();
    std::fs::write(r.path().join("app.db"), b"x").unwrap();
    let Err(Error::Config(m)) = open_replica_with_store(rconfig(r.path()), store.clone()) else {
        panic!("expected a refusal");
    };
    assert!(m.contains("app.db") && m.contains("delete"), "{m}");
}

// ---- Telemetry (Task 10) ----

use crate::telemetry::testing::{SpanRec, Spans};

const SYNC: &str = "buckwal.replica.sync";

fn sync_spans(s: &Spans) -> Vec<&SpanRec> {
    s.named(SYNC)
}

/// Wait until at least `n` sync spans have closed.
fn until_sync_spans(cap: &Capture, n: usize) {
    until(&format!("{n} sync spans"), || {
        cap.spans()
            .named(SYNC)
            .iter()
            .filter(|s| s.closed.is_some())
            .count()
            >= n
    });
}

fn num(s: &SpanRec, name: &str) -> u64 {
    s.field(name)
        .unwrap_or_else(|| panic!("{} has no {name}: {:?}", s.name, s.fields))
        .parse()
        .unwrap_or_else(|_| panic!("{name} is not a number: {:?}", s.field(name)))
}

fn check_sync_span(s: &SpanRec) {
    assert_eq!(s.target, "buckwal");
    assert_eq!(s.level, tracing::Level::INFO);
    assert!(
        s.field("buckwal.replica_id").is_some_and(|i| !i.is_empty()),
        "{s:?}"
    );
    for n in [
        "buckwal.generation",
        "buckwal.seq",
        "buckwal.objects",
        "buckwal.lists",
        "buckwal.gets",
        "buckwal.skipped",
    ] {
        num(s, n);
    }
    assert!(
        ["true", "false"].contains(&s.field("buckwal.found_new").unwrap_or("")),
        "{s:?}"
    );
    assert!(
        ["notification", "slow_poll", "start"].contains(&s.field("buckwal.trigger").unwrap_or("")),
        "{s:?}"
    );
    assert!(
        [
            "applied",
            "nothing",
            "checkpoint_deferred",
            "re_restore",
            "error"
        ]
        .contains(&s.field("buckwal.outcome").unwrap_or("")),
        "{s:?}"
    );
}

#[test]
fn sync_span_carries_all_attributes() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 3);
    let r = tempfile::tempdir().unwrap();
    let cap = Capture::new();
    let (spans, stats) = tracing::subscriber::with_default(cap.subscriber(), || {
        let mut c = rconfig(&r.path().join("r"));
        c.replica_poll_interval = Duration::from_secs(120);
        let replica = open_replica_with_store(c, store.clone()).unwrap();
        let rc = replica.connect().unwrap();
        wait_rows(&rc, 3);
        until_sync_spans(&cap, 1);
        // A notification that finds nothing.
        replica.waker().wake();
        until_sync_spans(&cap, 2);
        // A notification that finds a row.
        add(&wc, 4);
        replica.waker().wake();
        wait_rows(&rc, 4);
        until_sync_spans(&cap, 3);
        let stats = replica.stats();
        rc.close().unwrap();
        replica.close().unwrap();
        (cap.spans(), stats)
    });
    let all = sync_spans(&spans);
    assert_eq!(all.len(), 3, "{all:?}");
    all.iter().for_each(|s| check_sync_span(s));
    let (start, nothing, applied) = (all[0], all[1], all[2]);
    assert_eq!(start.field("buckwal.trigger"), Some("start"));
    assert_eq!(nothing.field("buckwal.trigger"), Some("notification"));
    assert_eq!(nothing.field("buckwal.outcome"), Some("nothing"));
    assert_eq!(nothing.field("buckwal.found_new"), Some("false"));
    assert_eq!(nothing.field("buckwal.lag_ms"), Some("0"));
    assert_eq!(nothing.field("buckwal.objects"), Some("0"));
    assert!(num(nothing, "buckwal.lists") >= 1);
    assert_eq!(applied.field("buckwal.trigger"), Some("notification"));
    assert_eq!(applied.field("buckwal.outcome"), Some("applied"));
    assert_eq!(applied.field("buckwal.found_new"), Some("true"));
    assert_eq!(num(applied, "buckwal.objects"), 1);
    assert!(num(applied, "buckwal.gets") >= 1);
    assert_eq!(applied.field("buckwal.skipped"), Some("0"));
    // The values are the ones stats() reports after that pass.
    assert_eq!(num(applied, "buckwal.generation"), stats.generation);
    assert_eq!(num(applied, "buckwal.seq"), stats.seq);
    assert_eq!(
        num(applied, "buckwal.lag_ms"),
        stats.lag_ms.expect("a measured lag")
    );
    assert_eq!(
        applied.field("buckwal.replica_id"),
        start.field("buckwal.replica_id")
    );
}

#[test]
fn sync_span_trigger_slow_poll() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, _wc) = writer(&store, 1);
    let r = tempfile::tempdir().unwrap();
    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let replica = open_replica_with_store(rconfig(&r.path().join("r")), store.clone()).unwrap();
        until("a slow poll", || replica.stats().passes_slow_poll >= 1);
        until_sync_spans(&cap, 2);
        replica.close().unwrap();
        cap.spans()
    });
    let all = sync_spans(&spans);
    all.iter().for_each(|s| check_sync_span(s));
    assert!(all
        .iter()
        .any(|s| s.field("buckwal.trigger") == Some("slow_poll")));
}

#[test]
fn sync_span_outcome_error() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 1);
    let spy = Spy::new(store.clone());
    let r = tempfile::tempdir().unwrap();
    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let mut c = rconfig(&r.path().join("r"));
        c.replica_poll_interval = Duration::from_secs(120);
        let replica = open_replica_with_store(c, spy.clone()).unwrap();
        until_sync_spans(&cap, 1);
        add(&wc, 2);
        spy.fail_gets_under("/log/");
        replica.waker().wake();
        until_sync_spans(&cap, 2);
        spy.heal();
        replica.close().unwrap();
        cap.spans()
    });
    let all = sync_spans(&spans);
    assert_eq!(all[1].field("buckwal.outcome"), Some("error"), "{all:?}");
    check_sync_span(all[1]);
}

#[test]
fn sync_span_outcome_re_restore() {
    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let l = lag();
        l.fall_behind();
        l.wait_re_restore();
        until_sync_spans(&cap, 3);
        l.replica.close().unwrap();
        cap.spans()
    });
    let all = sync_spans(&spans);
    all.iter().for_each(|s| check_sync_span(s));
    assert!(
        all.iter()
            .any(|s| s.field("buckwal.outcome") == Some("re_restore")),
        "{all:?}"
    );
}

#[test]
fn sync_span_outcome_checkpoint_deferred() {
    let store = Arc::new(MemStore::new());
    let (_w, _wdb, wc) = writer(&store, 3);
    let r = tempfile::tempdir().unwrap();
    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let replica = open_replica_with_store(rconfig(&r.path().join("r")), store.clone()).unwrap();
        let rc = replica.connect().unwrap();
        wait_rows(&rc, 3);
        // A reader holds its snapshot across a seal; the checkpoint is deferred once the
        // follower's busy budget (two poll intervals) is spent.
        rc.transaction(|tx| {
            tx.query("SELECT count(*) FROM t", &[])?;
            wc.checkpoint(CheckpointMode::Truncate).unwrap();
            add(&wc, 4);
            until("a deferred checkpoint", || {
                replica.stats().deferred_checkpoints >= 1
            });
            Ok(())
        })
        .unwrap();
        until_sync_spans(&cap, 2);
        rc.close().unwrap();
        replica.close().unwrap();
        cap.spans()
    });
    let all = sync_spans(&spans);
    all.iter().for_each(|s| check_sync_span(s));
    assert!(
        all.iter()
            .any(|s| s.field("buckwal.outcome") == Some("checkpoint_deferred")),
        "{:?}",
        all.iter()
            .map(|s| s.field("buckwal.outcome"))
            .collect::<Vec<_>>()
    );
}

#[test]
fn re_restore_span_parents_restore_span() {
    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let l = lag();
        let applied = l.fall_behind();
        l.wait_re_restore();
        let stats = l.replica.stats();
        l.replica.close().unwrap();
        (cap.spans(), applied, stats)
    });
    let (spans, applied, stats) = spans;
    let rr = spans.named("buckwal.replica.re_restore");
    assert_eq!(rr.len(), 1, "{rr:?}");
    let rr = rr[0];
    assert_eq!(rr.target, "buckwal");
    assert_eq!(rr.level, tracing::Level::INFO);
    assert_eq!(rr.field("buckwal.reason"), Some("trimmed"));
    assert_eq!(num(rr, "buckwal.generation"), 1);
    assert_eq!(num(rr, "buckwal.seq"), applied);
    assert_eq!(num(rr, "buckwal.generation_after"), stats.generation);
    // The follower of the new copy has read no log yet; its first pass re-reads from seq 1.
    num(rr, "buckwal.seq_after");
    num(rr, "buckwal.behind_ms");
    assert!(rr
        .field("buckwal.replica_id")
        .is_some_and(|i| !i.is_empty()));
    let restore = spans.with_parent(rr.id, "buckwal.restore");
    assert_eq!(restore.field("buckwal.outcome"), Some("ok"));
    // The same WARN and span agree on the id and behind_ms.
    let w = fell_behind_events(&spans)[0];
    assert_eq!(w.field("replica_id"), rr.field("buckwal.replica_id"));
    assert_eq!(w.field("behind_ms"), rr.field("buckwal.behind_ms"));
}

#[test]
fn replica_logs_carry_no_token_or_row_data() {
    const MARKER: &str = "ROWDATA-marker-5e1c0b77";
    const TOKEN: &str = "TOKEN-like-secret-9d3f21ab";
    let key_hex = "ab".repeat(32);
    let store = Arc::new(MemStore::new());
    let k = tempfile::tempdir().unwrap();
    let t = tempfile::tempdir().unwrap();
    let mut wc_cfg = key_config(&t.path().join("w"), k.path(), &key_hex, wconfig);
    wc_cfg.secret_key = TOKEN.to_string();
    let db = crate::open_with_store(wc_cfg, store.clone()).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    let put_marker = |id: i64| {
        conn.execute(
            &format!("INSERT INTO t(id, v) VALUES (?, '{MARKER}')"),
            &[int(id)],
        )
        .unwrap()
    };
    put_marker(1);
    let r = tempfile::tempdir().unwrap();
    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let mut c = key_config(&r.path().join("r"), k.path(), &key_hex, rconfig);
        c.secret_key = TOKEN.to_string();
        c.replica_poll_interval = Duration::from_secs(120);
        let replica = open_replica_with_store(c, store.clone()).unwrap();
        let rc = replica.connect().unwrap();
        wait_rows(&rc, 1);
        put_marker(2);
        replica.waker().wake();
        wait_rows(&rc, 2);
        until_sync_spans(&cap, 2);
        until("the second pass", || replica.stats().seq > 1);
        // Replay an old record at the next seq: an integrity failure and a stop.
        let seq = replica.stats().seq;
        let keys = Keys::new(PREFIX);
        let old = store.get(&keys.log(1, seq)).unwrap().unwrap().body;
        put_marker(3);
        store.put(&keys.log(1, seq + 1), &old).unwrap();
        replica.waker().wake();
        until("the follower to stop", || replica.thread_finished());
        assert_eq!(replica.stats().stopped.as_deref(), Some("integrity"));
        rc.close().unwrap();
        replica.close().unwrap();
        cap.spans()
    });
    let out = ours(&spans);
    for secret in [MARKER, TOKEN, key_hex.as_str()] {
        assert!(!out.contains(secret), "{secret} leaked into: {out}");
    }
    // And the same across a re-restore.
    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let l = lag();
        l.wc.execute(
            &format!("INSERT INTO t(id, v) VALUES (100, '{MARKER}')"),
            &[],
        )
        .unwrap();
        l.fall_behind();
        l.wait_re_restore();
        l.replica.close().unwrap();
        cap.spans()
    });
    assert!(!ours(&spans).contains(MARKER));
}

/// Everything buckwal itself emitted (target `buckwal`), as one string. The engine's own
/// tracing is not ours and carries the SQL text.
fn ours(s: &Spans) -> String {
    let spans: Vec<_> = s.spans.iter().filter(|s| s.target == "buckwal").collect();
    let events: Vec<_> = s.events.iter().filter(|e| e.target == "buckwal").collect();
    assert!(!spans.is_empty() && !events.is_empty());
    format!("{spans:?} {events:?}")
}

fn stopped_events(s: &Spans) -> Vec<&crate::telemetry::testing::EventRec> {
    s.events
        .iter()
        .filter(|e| e.field("message") == Some("replica stopped"))
        .collect()
}

#[test]
fn stopped_reasons_integrity_and_corrupt_carry_the_object_key() {
    for (reason, encrypted) in [("integrity", true), ("corrupt", false)] {
        let store = Arc::new(MemStore::new());
        let k = tempfile::tempdir().unwrap();
        let t = tempfile::tempdir().unwrap();
        let hex = "cd".repeat(32);
        let mk = |d: &Path, f: fn(&Path) -> Config| {
            if encrypted {
                key_config(d, k.path(), &hex, f)
            } else {
                f(d)
            }
        };
        let db = crate::open_with_store(mk(&t.path().join("w"), wconfig), store.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
            .unwrap();
        add(&conn, 1);
        let r = tempfile::tempdir().unwrap();
        let cap = Capture::new();
        let (spans, seq) = tracing::subscriber::with_default(cap.subscriber(), || {
            let mut c = mk(&r.path().join("r"), rconfig);
            c.replica_poll_interval = Duration::from_secs(120);
            let replica = open_replica_with_store(c, store.clone()).unwrap();
            let rc = replica.connect().unwrap();
            wait_rows(&rc, 1);
            until("the start pass", || replica.stats().seq > 0);
            let seq = replica.stats().seq;
            let keys = Keys::new(PREFIX);
            let old = store.get(&keys.log(1, seq)).unwrap().unwrap().body;
            add(&conn, 2);
            let body: &[u8] = if encrypted { &old } else { b"garbage" };
            store.put(&keys.log(1, seq + 1), body).unwrap();
            replica.waker().wake();
            until("the follower to stop", || replica.thread_finished());
            assert_eq!(replica.stats().stopped.as_deref(), Some(reason));
            rc.close().unwrap();
            replica.close().unwrap();
            (cap.spans(), seq)
        });
        let ev = stopped_events(&spans);
        assert_eq!(ev.len(), 1, "{reason}: {:?}", spans.events);
        assert_eq!(ev[0].level, tracing::Level::ERROR);
        assert_eq!(ev[0].target, "buckwal");
        assert_eq!(ev[0].field("reason"), Some(reason));
        assert_eq!(ev[0].field("generation"), Some("1"));
        assert_eq!(ev[0].field("seq"), Some(seq.to_string().as_str()));
        let key = Keys::new(PREFIX).log(1, seq + 1);
        assert_eq!(ev[0].field("key"), Some(key.as_str()), "{reason}");
        // The last sync span ended in error.
        let all = sync_spans(&spans);
        assert_eq!(all.last().unwrap().field("buckwal.outcome"), Some("error"));
    }
}
