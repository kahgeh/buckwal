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
        })
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
