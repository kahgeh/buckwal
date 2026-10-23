use super::export_filter;
use super::testing::{Capture, SpanRec, Spans};
use crate::batching::Batching;
use crate::config::Config;
use crate::conn::CheckpointMode;
use crate::store::{CondPut, FaultStore, MemStore, Object, ObjectStore, StatsSnap, StoreError};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tracing::Level;
use turso_core::Value;

fn config(dir: &Path) -> Config {
    Config::new(dir, "http://127.0.0.1:1", "b", "k", "s")
}

fn window_50ms() -> Batching {
    Batching::Window {
        max_wait: Duration::from_millis(50),
        max_commits: 64,
        max_bytes: 1 << 20,
    }
}

/// A MemStore whose log PUTs, once armed, wait at a gate until it opens: the uploader thread
/// is held there while later jobs queue up behind it.
#[derive(Default)]
struct GateStore {
    inner: MemStore,
    gate: Mutex<Gate>,
    cv: Condvar,
}

#[derive(Default)]
struct Gate {
    armed: bool,
    open: bool,
    waiting: usize,
}

impl GateStore {
    fn arm(&self) {
        self.gate.lock().unwrap().armed = true;
    }
    /// Block until a log PUT waits at the gate.
    fn wait_for_a_put(&self) {
        let g = self.gate.lock().unwrap();
        let (_g, t) = self
            .cv
            .wait_timeout_while(g, Duration::from_secs(10), |g| g.waiting == 0)
            .unwrap();
        assert!(!t.timed_out(), "no log PUT reached the gate");
    }
    fn open(&self) {
        self.gate.lock().unwrap().open = true;
        self.cv.notify_all();
    }
    fn pass(&self, key: &str) {
        let mut g = self.gate.lock().unwrap();
        if !g.armed || !key.contains("log/") {
            return;
        }
        g.waiting += 1;
        self.cv.notify_all();
        drop(self.cv.wait_while(g, |g| !g.open).unwrap());
    }
}

impl ObjectStore for GateStore {
    fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> Result<CondPut, StoreError> {
        self.pass(key);
        self.inner.put_if_absent(key, body)
    }
    fn put_if_match(&self, key: &str, body: &[u8], etag: &str) -> Result<CondPut, StoreError> {
        self.inner.put_if_match(key, body, etag)
    }
    fn get(&self, key: &str) -> Result<Option<Object>, StoreError> {
        self.inner.get(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
    fn stat(
        &self,
        key: &str,
    ) -> std::result::Result<Option<crate::store::ObjectStat>, crate::store::StoreError> {
        self.inner.stat(key)
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

/// Spawn `f` on a thread under the current dispatcher, as the library's own threads run.
fn spawn_traced(f: impl FnOnce() + Send + 'static) -> std::thread::JoinHandle<()> {
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    std::thread::spawn(move || tracing::dispatcher::with_default(&dispatch, f))
}

fn insert(c: &crate::Connection, x: i64) -> crate::Result<u64> {
    c.execute("INSERT INTO t VALUES (?)", &[Value::from_i64(x)])
}

/// Under a capture subscriber: open on a MemStore with `b`, create `t`, then `inserts`
/// inserts. The snapshot is taken before the database closes.
fn capture_run(b: Batching, inserts: i64) -> Spans {
    let cap = Capture::new();
    let dir = tempfile::tempdir().unwrap();
    tracing::subscriber::with_default(cap.subscriber(), || {
        let mut c = config(dir.path());
        c.batching = b;
        let db = crate::open_with_store(c, Arc::new(MemStore::new())).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        for i in 0..inserts {
            insert(&conn, i).unwrap();
        }
        cap.spans()
    })
}

/// Open on a FaultStore over a MemStore whose failures are short (a 200 ms retry deadline),
/// with faults limited to log objects, and create `t`; then run `f` under the capture.
fn capture_faulty(f: impl FnOnce(&FaultStore, &crate::Connection)) -> Spans {
    let cap = Capture::new();
    let dir = tempfile::tempdir().unwrap();
    tracing::subscriber::with_default(cap.subscriber(), || {
        let fault = Arc::new(FaultStore::new(Arc::new(MemStore::new())));
        let mut c = config(dir.path());
        c.retry_deadline = Duration::from_millis(200);
        c.request_timeout = Duration::from_millis(50);
        let db = crate::open_with_store(c, fault.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        fault.only_keys_containing("log/");
        f(&fault, &conn);
        cap.spans()
    })
}

#[test]
fn off_commit_has_its_three_children_and_one_upload() {
    let spans = capture_run(Batching::Off, 1);
    let c = spans.last_named("buckwal.commit");
    assert_eq!(c.target, "buckwal");
    assert_eq!(c.level, Level::INFO);
    assert_eq!(c.field("buckwal.batching"), Some("off"));
    assert_eq!(c.field("buckwal.outcome"), Some("ok"));
    assert!(c.field("buckwal.commit_no").is_some());
    assert!(c.field("buckwal.bytes").unwrap().parse::<u64>().unwrap() > 0);
    for child in [
        "buckwal.local_write",
        "buckwal.local_fsync",
        "buckwal.ack_wait",
    ] {
        assert_eq!(spans.children_of(c.id, child).len(), 1, "{child}");
    }
    let u = spans.with_parent(c.id, "buckwal.upload");
    assert_eq!(u.target, "buckwal");
    assert_eq!(u.field("buckwal.commits"), Some("1"));
    assert_eq!(u.field("s3.status"), Some("200"));
    assert_eq!(u.field("buckwal.retries"), Some("0"));
    assert!(u.field("buckwal.bytes").unwrap().parse::<u64>().unwrap() > 0);
    assert!(u.field("buckwal.seq").is_some() && u.field("buckwal.generation").is_some());
    let child = |name| spans.by_id(spans.children_of(c.id, name)[0]);
    let (write, fsync) = (child("buckwal.local_write"), child("buckwal.local_fsync"));
    assert!(
        write.closed.is_some_and(|t| t <= fsync.created),
        "local_write closes at sync entry"
    );
    let ack = child("buckwal.ack_wait");
    assert!(ack.closed.is_some() && ack.closed <= c.closed);
    assert!(c.closed.is_some());
    // An info record at open, from the library.
    assert!(
        spans.events.iter().any(|e| e.level == Level::INFO
            && e.target == "buckwal"
            && e.field("writer_id").is_some()
            && e.field("generation").is_some()),
        "no open record: {:?}",
        spans.events
    );
}

#[test]
fn window_upload_span_counts_its_commits_and_links_the_rest() {
    let cap = Capture::new();
    let dir = tempfile::tempdir().unwrap();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let store = Arc::new(GateStore::default());
        let db = crate::open_with_store(config(dir.path()), store.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        // A pilot commit (max_commits 1, so it is uploaded at once) holds the uploader at the
        // gate. The four commits made meanwhile queue behind it, so the uploader takes all of
        // them into one window as soon as the gate opens, however slowly they were made.
        store.arm();
        let pilot = {
            let db = db.clone();
            spawn_traced(move || {
                let c = db.connect().unwrap();
                let one = Batching::Window {
                    max_wait: Duration::from_millis(50),
                    max_commits: 1,
                    max_bytes: 1 << 20,
                };
                c.set_batching(Some(one)).unwrap();
                insert(&c, 100).unwrap();
            })
        };
        store.wait_for_a_put();
        let workers: Vec<_> = (0..4)
            .map(|i| {
                let db = db.clone();
                spawn_traced(move || {
                    let c = db.connect().unwrap();
                    c.set_batching(Some(window_50ms())).unwrap();
                    insert(&c, i).unwrap();
                })
            })
            .collect();
        // Each commit's number is recorded once it is submitted to the uploader.
        let deadline = Instant::now() + Duration::from_secs(10);
        while cap
            .spans()
            .named("buckwal.commit")
            .iter()
            .filter(|c| {
                c.field("buckwal.batching") == Some("window 50ms 64 1MiB")
                    && c.field("buckwal.commit_no").is_some()
            })
            .count()
            < 4
        {
            assert!(
                Instant::now() < deadline,
                "the four commits were not submitted"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        store.open();
        for w in workers.into_iter().chain([pilot]) {
            w.join().unwrap();
        }
        cap.spans()
    });
    let commits: Vec<_> = spans
        .named("buckwal.commit")
        .into_iter()
        .filter(|c| c.field("buckwal.batching") == Some("window 50ms 64 1MiB"))
        .collect();
    assert_eq!(commits.len(), 4, "{commits:#?}");
    let ids: Vec<usize> = commits.iter().map(|c| c.id).collect();
    for c in &commits {
        assert_eq!(c.field("buckwal.outcome"), Some("ok"));
        assert_eq!(spans.children_of(c.id, "buckwal.ack_wait").len(), 1);
    }
    let uploads: Vec<_> = spans
        .named("buckwal.upload")
        .into_iter()
        .filter(|u| u.field("buckwal.commits") == Some("4"))
        .collect();
    assert_eq!(uploads.len(), 1, "{:#?}", spans.named("buckwal.upload"));
    let u = uploads[0];
    let parent = u.parent.expect("the upload has a parent");
    assert!(ids.contains(&parent), "parent {parent} not in {ids:?}");
    assert_eq!(u.parent_name, Some("buckwal.commit"));
    let mut linked = u.follows_from.clone();
    linked.sort_unstable();
    let mut rest: Vec<usize> = ids.iter().copied().filter(|&i| i != parent).collect();
    rest.sort_unstable();
    assert_eq!(linked, rest);
    assert_eq!(u.field("s3.status"), Some("200"));
}

#[test]
fn checkpoint_upload_and_restore_spans_carry_bucket_fields() {
    let cap = Capture::new();
    let dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let store: Arc<dyn ObjectStore> = Arc::new(MemStore::new());
        let db = crate::open_with_store(config(dir.path()), store.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        for i in 0..3 {
            insert(&conn, i).unwrap();
        }
        conn.checkpoint(CheckpointMode::Truncate).unwrap();
        // One more commit, so generation 2's log has a record to replay.
        insert(&conn, 3).unwrap();
        crate::restore_with_store(&config(restored.path()), store.clone(), restored.path())
            .unwrap();
        // Counted before the database closes (its close-time seal appends to log/2).
        let keys = crate::format::Keys::new("");
        let logged = |g| store.list(&keys.log_prefix(g)).unwrap().len().to_string();
        (cap.spans(), [logged(1), logged(2)])
    });
    let (spans, logged) = spans;
    let logged = |g: usize| logged[g - 1].clone();
    let cp = spans.last_named("buckwal.checkpoint_upload");
    assert_eq!(cp.target, "buckwal");
    assert_eq!(cp.field("buckwal.generation"), Some("1"));
    // The seal-start record is generation 1's last.
    assert_eq!(cp.field("buckwal.seq"), Some(logged(1).as_str()));
    assert!(cp.field("buckwal.pages").unwrap().parse::<u64>().unwrap() > 0);
    assert_eq!(cp.field("buckwal.retries"), Some("0"));
    assert_eq!(cp.field("s3.status"), Some("200"));
    assert!(cp.closed.is_some());
    assert!(
        spans.events.iter().any(|e| e.level == Level::INFO
            && e.target == "buckwal"
            && e.field("pages").is_some()
            && e.field("ms").is_some()),
        "no seal record"
    );
    let r = spans.last_named("buckwal.restore");
    assert_eq!(r.target, "buckwal");
    assert_eq!(r.field("buckwal.generation"), Some("2"));
    assert!(r.field("buckwal.gets").unwrap().parse::<u64>().unwrap() > 0);
    assert!(r.field("buckwal.log_objects").is_some());
    // The highest seq replayed: generation 2's last record.
    assert_ne!(logged(2), "0");
    assert_eq!(r.field("buckwal.seq"), Some(logged(2).as_str()));
    assert_eq!(r.field("buckwal.retries"), Some("0"));
    assert!(r.field("buckwal.bytes").is_some());
    assert!(
        spans.events.iter().any(|e| e.level == Level::INFO
            && e.target == "buckwal"
            && e.field("log_objects").is_some()),
        "no restore record"
    );
}

/// A MemStore whose next `fail` GETs fail transiently.
#[derive(Default)]
struct FlakyGets {
    inner: MemStore,
    fail: Mutex<u64>,
}

impl ObjectStore for FlakyGets {
    fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> Result<CondPut, StoreError> {
        self.inner.put_if_absent(key, body)
    }
    fn put_if_match(&self, key: &str, body: &[u8], etag: &str) -> Result<CondPut, StoreError> {
        self.inner.put_if_match(key, body, etag)
    }
    fn get(&self, key: &str) -> Result<Option<Object>, StoreError> {
        let mut fail = self.fail.lock().unwrap();
        if *fail > 0 {
            *fail -= 1;
            return Err(StoreError::Transient(format!(
                "injected GET failure for {key}"
            )));
        }
        drop(fail);
        self.inner.get(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
    fn stat(
        &self,
        key: &str,
    ) -> std::result::Result<Option<crate::store::ObjectStat>, crate::store::StoreError> {
        self.inner.stat(key)
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

#[test]
fn restore_span_counts_its_retries_and_an_empty_log_is_seq_0() {
    let cap = Capture::new();
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let keys = crate::format::Keys::new("");
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let store = Arc::new(FlakyGets::default());
        let db = crate::open_with_store(config(dir.path()), store.clone()).unwrap();
        // Nothing committed yet: generation 1's log is empty.
        assert!(store.list(&keys.log_prefix(1)).unwrap().is_empty());
        crate::restore_with_store(&config(a.path()), store.clone(), a.path()).unwrap();
        let empty = cap.spans().last_named("buckwal.restore").clone();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        // Two failed GETs, retried within the restore's deadline.
        *store.fail.lock().unwrap() = 2;
        crate::restore_with_store(&config(b.path()), store, b.path()).unwrap();
        (empty, cap.spans())
    });
    let (empty, spans) = spans;
    assert_eq!(empty.field("buckwal.log_objects"), Some("0"));
    assert_eq!(empty.field("buckwal.seq"), Some("0"));
    assert_eq!(empty.field("buckwal.retries"), Some("0"));
    let r = spans.last_named("buckwal.restore");
    assert_eq!(r.field("buckwal.retries"), Some("2"));
    assert_ne!(r.field("buckwal.seq"), Some("0"));
}

#[test]
fn retries_are_counted_on_the_upload_span() {
    let spans = capture_faulty(|fault, conn| {
        fault.fail_next_puts(2);
        insert(conn, 1).unwrap();
    });
    let c = spans.last_named("buckwal.commit");
    let u = spans.with_parent(c.id, "buckwal.upload");
    assert_eq!(u.field("buckwal.retries"), Some("2"));
    assert_eq!(u.field("s3.status"), Some("200"));
}

#[test]
fn failed_flush_logs_a_warning_and_marks_the_commit_error() {
    let spans = capture_faulty(|fault, conn| {
        fault.fail_puts_after(0);
        assert!(insert(conn, 1).is_err());
    });
    assert!(
        spans
            .events
            .iter()
            .any(|e| e.level == Level::WARN && e.target == "buckwal"),
        "no warning: {:?}",
        spans.events
    );
    let decided: Vec<_> = spans
        .named("buckwal.commit")
        .into_iter()
        .filter_map(|c| c.field("buckwal.outcome"))
        .collect();
    assert_eq!(decided.last(), Some(&"error"), "{decided:?}");
}

#[test]
fn engine_tracing_is_not_ours() {
    let spans = capture_run(Batching::Off, 1);
    let keep = export_filter();
    let engine_spans = spans
        .spans
        .iter()
        .filter(|s| s.target.starts_with("turso_core"));
    for s in engine_spans {
        assert!(!keep(&s.target), "{s:?}");
    }
    for e in spans
        .events
        .iter()
        .filter(|e| e.target.starts_with("turso_core"))
    {
        assert!(!keep(&e.target), "{e:?}");
    }
    for s in spans
        .spans
        .iter()
        .filter(|s| s.name.starts_with("durable."))
    {
        assert!(keep(&s.target), "{s:?}");
    }
    for t in [
        "turso_core",
        "turso_core::storage::pager",
        "opentelemetry",
        "opentelemetry_sdk",
        "opentelemetry-otlp",
        "opentelemetry_appender_tracing",
        "tonic",
        "tonic::transport",
        "h2",
        "hyper",
        "hyper_util::client",
        "tower",
        "tower::buffer::worker",
        "reqwest",
    ] {
        assert!(!keep(t), "{t}");
    }
    assert!(keep("buckwal::uploader"));
}

#[test]
fn a_fenced_writer_logs_one_error() {
    use crate::bucket::Bucket;
    use crate::format::{encode_record, Keys, LogFile, LogRecord, Op};
    use crate::store::RetryPolicy;
    let cap = Capture::new();
    tracing::subscriber::with_default(cap.subscriber(), || {
        let mem = Arc::new(MemStore::new());
        let keys = Keys::new("db");
        let mut b = Bucket::create(
            mem.clone(),
            keys.clone(),
            RetryPolicy::default(),
            1,
            "w1".into(),
        )
        .unwrap();
        let foreign = encode_record(&LogRecord::Fence {
            writer_id: "w2".into(),
            epoch: 1,
        });
        mem.put(&keys.log(1, 1), &foreign).unwrap();
        let sync = || {
            vec![vec![Op::Write {
                off: 0,
                bytes: vec![1; 4],
            }]]
        };
        assert!(b.append_log(LogFile::Wal, sync()).is_err());
        assert!(
            b.append_log(LogFile::Wal, sync()).is_err(),
            "fenced for good"
        );
    });
    let errors: Vec<_> = cap
        .spans()
        .events
        .into_iter()
        .filter(|e| e.level == Level::ERROR)
        .collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].target, "buckwal");
    assert!(errors[0].field("error").unwrap().contains("another writer"));
}

/// A callsite first hit on a thread without a subscriber (another test's library thread,
/// say) must still be seen by a capture: tracing caches each callsite's interest, and with
/// at most one dispatcher registered it asks only the registering thread's.
#[test]
fn capture_sees_a_callsite_first_hit_on_a_plain_thread() {
    fn probe() {
        tracing::info!(target: "buckwal::probe", "probe");
    }
    let cap = Capture::new();
    tracing::subscriber::with_default(cap.subscriber(), || {
        std::thread::spawn(probe).join().unwrap();
        probe();
    });
    let probes = cap
        .spans()
        .events
        .iter()
        .filter(|e| e.target == "buckwal::probe")
        .count();
    assert_eq!(probes, 1);
}

#[test]
fn attaching_as_another_writer_logs_one_error_with_the_library_target() {
    use crate::bucket::Bucket;
    use crate::format::Keys;
    use crate::store::RetryPolicy;
    let cap = Capture::new();
    tracing::subscriber::with_default(cap.subscriber(), || {
        let mem = Arc::new(MemStore::new());
        let keys = Keys::new("db");
        let policy = RetryPolicy::default();
        Bucket::create(mem.clone(), keys.clone(), policy.clone(), 1, "w1".into()).unwrap();
        assert!(Bucket::attach(mem, keys, policy, 1, "w2", 0).is_err());
    });
    let errors: Vec<_> = cap
        .spans()
        .events
        .into_iter()
        .filter(|e| e.level == Level::ERROR)
        .collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].target, "buckwal");
}

/// An `Off` commit is in the bucket once its blocking wait returns, so its span says `ok`
/// even when a later statement of the same call fails and its ack is never collected.
#[test]
fn an_off_commit_in_a_failed_call_closes_with_outcome_ok() {
    let cap = Capture::new();
    let dir = tempfile::tempdir().unwrap();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let db = crate::open_with_store(config(dir.path()), Arc::new(MemStore::new())).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        assert!(conn
            .execute_batch("INSERT INTO t VALUES (7); SELECT * FROM missing")
            .is_err());
        assert_eq!(conn.query("SELECT x FROM t", &[]).unwrap().len(), 1);
        cap.spans()
    });
    let c = spans
        .named("buckwal.commit")
        .into_iter()
        .rev()
        .find(|c| c.closed.is_some())
        .expect("a closed commit");
    assert_eq!(c.field("buckwal.outcome"), Some("ok"), "{c:?}");
}

/// A MemStore that logs every PUT (of any flavour, as "put") and `get` (as a store with its
/// own logging would).
#[derive(Default)]
struct ProbeStore {
    inner: MemStore,
}

impl ObjectStore for ProbeStore {
    fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        tracing::info!(target: "buckwal::probe", key, op = "put", "probe");
        self.inner.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> Result<CondPut, StoreError> {
        tracing::info!(target: "buckwal::probe", key, op = "put", "probe");
        self.inner.put_if_absent(key, body)
    }
    fn put_if_match(&self, key: &str, body: &[u8], etag: &str) -> Result<CondPut, StoreError> {
        tracing::info!(target: "buckwal::probe", key, op = "put", "probe");
        self.inner.put_if_match(key, body, etag)
    }
    fn get(&self, key: &str) -> Result<Option<Object>, StoreError> {
        tracing::info!(target: "buckwal::probe", key, op = "get", "probe");
        self.inner.get(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
    fn stat(
        &self,
        key: &str,
    ) -> std::result::Result<Option<crate::store::ObjectStat>, crate::store::StoreError> {
        self.inner.stat(key)
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

/// The scoped threads of a seal (page PUTs) and of a restore (log GETs) run under the
/// caller's dispatcher.
#[test]
fn seal_and_restore_threads_run_under_the_callers_subscriber() {
    let cap = Capture::new();
    let dir = tempfile::tempdir().unwrap();
    let restored = tempfile::tempdir().unwrap();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let store = Arc::new(ProbeStore::default());
        let db = crate::open_with_store(config(dir.path()), store.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        for i in 0..3 {
            insert(&conn, i).unwrap();
        }
        conn.checkpoint(CheckpointMode::Truncate).unwrap();
        for i in 3..6 {
            insert(&conn, i).unwrap();
        }
        crate::restore_with_store(&config(restored.path()), store, restored.path()).unwrap();
        cap.spans()
    });
    let probes = |op: &str, part: &str| {
        spans
            .events
            .iter()
            .filter(|e| {
                e.target == "buckwal::probe"
                    && e.field("op") == Some(op)
                    && e.field("key").is_some_and(|k| k.contains(part))
            })
            .count()
    };
    assert!(probes("put", "pages/") > 0, "no page PUT seen");
    assert!(probes("get", "log/") > 0, "no log GET seen");
}

/// The maintenance thread reports under the opening thread's subscriber: a
/// `buckwal.maintenance` span with its fields, the seal's `buckwal.checkpoint_upload` under it.
#[test]
fn maintenance_span_carries_its_fields_and_parents_the_seal() {
    let cap = Capture::new();
    let dir = tempfile::tempdir().unwrap();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let mut c = config(dir.path());
        c.maintenance_log_objects = 5;
        let db = crate::open_with_store(c, Arc::new(MemStore::new())).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        for i in 0..10 {
            insert(&conn, i).unwrap();
        }
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let spans = cap.spans();
            let done = spans
                .named("buckwal.maintenance")
                .iter()
                .any(|s| s.closed.is_some());
            if done {
                break spans;
            }
            assert!(Instant::now() < until, "no maintenance span closed");
            std::thread::sleep(Duration::from_millis(5));
        }
    });
    let m = *spans
        .named("buckwal.maintenance")
        .iter()
        .find(|s| s.closed.is_some())
        .unwrap();
    assert_eq!((m.target.as_str(), m.level), ("buckwal", Level::INFO));
    assert_eq!(m.field("buckwal.generation"), Some("1"));
    assert!(
        m.field("buckwal.log_objects")
            .unwrap()
            .parse::<u64>()
            .unwrap()
            >= 5
    );
    assert!(
        m.field("buckwal.wal_bytes")
            .unwrap()
            .parse::<u64>()
            .unwrap()
            > 0
    );
    assert!(m.field("buckwal.gate_wait_ms").is_some());
    assert_eq!(m.field("buckwal.outcome"), Some("sealed"));
    // Through the engine's own spans (`pager_checkpoint`), which are never exported.
    let under_m = |s: &SpanRec| {
        let mut parent = s.parent;
        while let Some(p) = parent {
            if p == m.id {
                return true;
            }
            parent = spans.spans[p].parent;
        }
        false
    };
    let seals = spans.named("buckwal.checkpoint_upload");
    assert!(seals.iter().any(|s| under_m(s)), "{seals:?}");
}

/// A run that panics still closes its `buckwal.maintenance` span, with outcome `error`.
#[test]
fn a_panicking_maintenance_run_closes_its_span_as_error() {
    let cap = Capture::new();
    let dir = tempfile::tempdir().unwrap();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let mut c = config(dir.path());
        c.maintenance_log_objects = 5;
        let db = crate::open_with_store(c, Arc::new(MemStore::new())).unwrap();
        db.shared.maintenance().unwrap().panic_next_run();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        for i in 0..10 {
            insert(&conn, i).unwrap();
        }
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let spans = cap.spans();
            if spans
                .named("buckwal.maintenance")
                .iter()
                .any(|s| s.closed.is_some())
            {
                break spans;
            }
            assert!(Instant::now() < until, "no maintenance span closed");
            std::thread::sleep(Duration::from_millis(5));
        }
    });
    let m = spans.named("buckwal.maintenance");
    let first = m.iter().find(|s| s.closed.is_some()).unwrap();
    assert_eq!(first.field("buckwal.outcome"), Some("error"), "{m:?}");
}

// --- The export pipeline -------------------------------------------------------------

use super::pipeline::{build_for_test, Providers};
use super::testing::keep_interest_per_call;
use super::{endpoint, init, TelemetryConfig};
use crate::error::Error;
use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _};
use opentelemetry::{InstrumentationScope, Key};
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{
    InMemoryLogExporter, LogProcessor, SdkLoggerProvider, SimpleLogProcessor,
};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SpanData, SpanExporter};
use opentelemetry_sdk::Resource;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::Subscriber;

fn test_pipeline(
    run_id: Option<&str>,
) -> (
    impl Subscriber + Send + Sync + 'static,
    Providers,
    InMemorySpanExporter,
    InMemoryLogExporter,
) {
    keep_interest_per_call();
    let spans = InMemorySpanExporter::default();
    let logs = InMemoryLogExporter::default();
    let (sub, p) = build_for_test(spans.clone(), logs.clone(), run_id.map(str::to_string));
    (sub, p, spans, logs)
}

#[test]
fn log_records_carry_their_own_time_and_the_run_id() {
    let (sub, p, _spans, logs) = test_pipeline(Some("run-42"));
    let before = SystemTime::now();
    tracing::subscriber::with_default(sub, || tracing::info!(target: "buckwal", "hello"));
    let after = SystemTime::now();
    p.force_flush();
    let r = &logs.get_emitted_logs().unwrap()[0].record;
    let t = r.timestamp().expect("record time set");
    assert!(before <= t && t <= after);
    assert!(r.attributes_iter().any(|(k, v)| k.as_str() == "test_run_id"
        && matches!(v, AnyValue::String(s) if s.as_str() == "run-42")));
}

#[test]
fn a_record_with_its_own_time_keeps_it() {
    use super::pipeline::StampedLogs;
    let exported = InMemoryLogExporter::default();
    let stamped = StampedLogs::new(SimpleLogProcessor::new(exported.clone()), None);
    let scope = InstrumentationScope::builder("t").build();
    let logger = SdkLoggerProvider::builder().build().logger("t");
    let own = UNIX_EPOCH + Duration::from_secs(1_000);
    let observed = UNIX_EPOCH + Duration::from_secs(2_000);

    let mut r = logger.create_log_record();
    r.set_timestamp(own);
    r.set_observed_timestamp(observed);
    stamped.emit(&mut r, &scope);
    let mut r = logger.create_log_record();
    r.set_observed_timestamp(observed);
    stamped.emit(&mut r, &scope);
    let mut r = logger.create_log_record();
    let before = SystemTime::now();
    stamped.emit(&mut r, &scope);
    let after = SystemTime::now();

    let got: Vec<_> = exported
        .get_emitted_logs()
        .unwrap()
        .into_iter()
        .map(|l| l.record.timestamp().expect("a record time"))
        .collect();
    assert_eq!(got[0], own, "its own time");
    assert_eq!(got[1], observed, "its observed time");
    assert!(before <= got[2] && got[2] <= after, "the current time");
    assert!(exported.get_emitted_logs().unwrap()[0]
        .record
        .attributes_iter()
        .all(|(k, _)| k.as_str() != "test_run_id"));
}

/// An InMemorySpanExporter that keeps the resource the provider hands it.
#[derive(Debug)]
struct ResourceSpy {
    inner: InMemorySpanExporter,
    seen: Arc<Mutex<Option<Resource>>>,
}

impl SpanExporter for ResourceSpy {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        self.inner.export(batch)
    }
    fn set_resource(&mut self, resource: &Resource) {
        *self.seen.lock().unwrap() = Some(resource.clone());
        self.inner.set_resource(resource);
    }
}

#[test]
fn spans_carry_service_name_and_run_id_and_engine_spans_are_dropped() {
    keep_interest_per_call();
    let spans = InMemorySpanExporter::default();
    let seen = Arc::new(Mutex::new(None));
    let logs = InMemoryLogExporter::default();
    let spy = ResourceSpy {
        inner: spans.clone(),
        seen: seen.clone(),
    };
    let (sub, p) = build_for_test(spy, logs.clone(), Some("run-42".into()));
    let dir = tempfile::tempdir().unwrap();
    tracing::subscriber::with_default(sub, || {
        let db = crate::open_with_store(config(dir.path()), Arc::new(MemStore::new())).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        insert(&conn, 1).unwrap();
        drop(conn);
        drop(db);
    });
    p.force_flush();
    let got = spans.get_finished_spans().unwrap();
    let names: Vec<&str> = got.iter().map(|s| s.name.as_ref()).collect();
    for want in ["buckwal.commit", "buckwal.upload"] {
        assert!(names.contains(&want), "{want} not in {names:?}");
    }
    for s in &got {
        let attrs: HashMap<&str, String> = s
            .attributes
            .iter()
            .map(|kv| (kv.key.as_str(), kv.value.to_string()))
            .collect();
        assert_eq!(attrs.get("test_run_id").map(String::as_str), Some("run-42"));
        assert_eq!(s.instrumentation_scope.name(), "buckwal");
        let target = attrs.get("target").expect("a target attribute");
        assert!(target.starts_with("buckwal"), "{target}");
        assert!(!target.starts_with("turso_core"), "{target}");
    }
    let resource = seen.lock().unwrap().clone().expect("a resource");
    assert_eq!(
        resource
            .get(&Key::new("service.name"))
            .map(|v| v.to_string()),
        Some("buckwal".to_string())
    );
    // The log records name the same service: the open record, at least.
    let records = logs.get_emitted_logs().unwrap();
    assert!(!records.is_empty());
    for l in &records {
        assert_eq!(
            l.resource
                .get(&Key::new("service.name"))
                .map(|v| v.to_string()),
            Some("buckwal".to_string())
        );
        assert!(l.record.target().is_some_and(|t| t == "buckwal"));
    }
}

#[test]
fn from_env_parses_and_refuses_bad_values() {
    let lookup = |pairs: &[(&'static str, &'static str)]| {
        let env: HashMap<&str, &str> = pairs.iter().copied().collect();
        TelemetryConfig::from_lookup(|k| env.get(k).map(|v| v.to_string()))
    };
    let c = lookup(&[
        ("BUCKWAL_OTEL", "1"),
        ("BUCKWAL_OTEL_ENDPOINT", "http://h:1"),
        ("BUCKWAL_OTEL_LEVEL", "warn"),
        ("BUCKWAL_TEST_RUN_ID", "r"),
    ])
    .unwrap();
    assert_eq!(
        c,
        TelemetryConfig {
            enabled: true,
            endpoint: "http://h:1".into(),
            level: Level::WARN,
            test_run_id: Some("r".into()),
        }
    );
    let d = lookup(&[]).unwrap();
    assert_eq!(d, TelemetryConfig::default());
    assert!(!d.enabled);
    assert_eq!(d.endpoint, "http://localhost:4317");
    assert_eq!(d.level, Level::INFO);
    assert_eq!(d.test_run_id, None);
    assert!(!lookup(&[("BUCKWAL_OTEL", "false")]).unwrap().enabled);
    assert!(lookup(&[("BUCKWAL_OTEL", "TRUE")]).unwrap().enabled);
    for (var, bad) in [("BUCKWAL_OTEL", "yes"), ("BUCKWAL_OTEL_LEVEL", "loud")] {
        match lookup(&[(var, bad)]) {
            Err(Error::Config(m)) => assert!(m.contains(var) && m.contains(bad), "{m}"),
            r => panic!("{var}={bad}: {r:?}"),
        }
    }
}

#[test]
fn otel_accepts_on_and_off() {
    let lookup = |v: &'static str| {
        TelemetryConfig::from_lookup(|k| (k == "BUCKWAL_OTEL").then(|| v.to_string()))
    };
    for v in ["on", "On", "ON", "1", "true"] {
        assert!(lookup(v).unwrap().enabled, "{v}");
    }
    for v in ["off", "OFF", "0", "false"] {
        assert!(!lookup(v).unwrap().enabled, "{v}");
    }
    match lookup("yes") {
        Err(Error::Config(m)) => assert!(m.contains("on/off"), "{m}"),
        r => panic!("yes: {r:?}"),
    }
}

/// Disabled, `init` installs nothing; an https endpoint is refused before anything is.
/// (Enabled `init` installs a global subscriber, so it is tested in `tests/otel_down.rs`.)
#[test]
fn init_disabled_installs_nothing_and_https_is_refused() {
    // The global default, seen from a thread with no scoped one.
    let global_is_none = || {
        std::thread::spawn(|| {
            tracing::dispatcher::get_default(|d| d.is::<tracing::subscriber::NoSubscriber>())
        })
        .join()
        .unwrap()
    };
    let guard = init(TelemetryConfig::default()).unwrap();
    assert!(global_is_none());
    drop(guard);
    let https = TelemetryConfig {
        enabled: true,
        endpoint: "https://collector:4317".into(),
        ..TelemetryConfig::default()
    };
    assert!(matches!(init(https), Err(Error::Config(m)) if m.contains("http://")));
    assert!(global_is_none());
}

/// A malformed endpoint fails exporter building with `Config`, even when `init` runs inside
/// an async context, where dropping the export runtime plainly would panic.
#[test]
fn init_with_a_malformed_endpoint_is_a_config_error_inside_a_runtime() {
    let bad = || TelemetryConfig {
        enabled: true,
        endpoint: "http://bad host:1".into(),
        ..TelemetryConfig::default()
    };
    assert!(matches!(init(bad()), Err(Error::Config(m)) if m.contains("bad host")));
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let r = rt.block_on(async { init(bad()) });
    assert!(matches!(r, Err(Error::Config(m)) if m.contains("bad host")));
}

/// The endpoint is trimmed once; the trimmed value is both checked and used.
#[test]
fn the_endpoint_is_trimmed_before_it_is_checked_and_used() {
    let cfg = |e: &str| TelemetryConfig {
        enabled: true,
        endpoint: e.into(),
        ..TelemetryConfig::default()
    };
    assert_eq!(
        endpoint(&cfg("  http://collector:4317 \n")).unwrap(),
        "http://collector:4317"
    );
    assert!(matches!(
        endpoint(&cfg(" https://c:4317")),
        Err(Error::Config(_))
    ));
}

#[test]
fn takeover_exports_takeover_and_fence_spans() {
    let cap = Capture::new();
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (spans, rep) = tracing::subscriber::with_default(cap.subscriber(), || {
        let store: Arc<dyn ObjectStore> = Arc::new(MemStore::new());
        let db = crate::open_with_store(config(a.path()), store.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        insert(&conn, 1).unwrap();
        let rep = crate::takeover_with_store(config(b.path()), store.clone()).unwrap();
        (cap.spans(), rep)
    });
    let takeovers = spans.named("buckwal.takeover");
    assert_eq!(takeovers.len(), 1, "{takeovers:?}");
    let t = takeovers[0];
    assert_eq!(t.target, "buckwal");
    assert_eq!(t.level, Level::INFO);
    let (epoch, seq) = (rep.epoch.to_string(), rep.fence_seq.to_string());
    assert_eq!(
        t.field("buckwal.generation"),
        Some(rep.generation.to_string().as_str())
    );
    assert_eq!(t.field("buckwal.epoch"), Some(epoch.as_str()));
    assert_eq!(t.field("buckwal.seq"), Some(seq.as_str()));
    assert_eq!(t.field("buckwal.retries"), Some("0"));
    assert_eq!(t.field("buckwal.outcome"), Some("ok"));
    assert_eq!(t.field("s3.status"), Some("200"));
    assert!(t.closed.is_some());
    assert_eq!(spans.named("buckwal.fence").len(), 1);
    let f = spans.with_parent(t.id, "buckwal.fence");
    assert_eq!(f.target, "buckwal");
    assert_eq!(f.field("buckwal.generation"), t.field("buckwal.generation"));
    assert_eq!(f.field("buckwal.seq"), Some(seq.as_str()));
    assert_eq!(f.field("buckwal.epoch"), Some(epoch.as_str()));
    assert_eq!(f.field("buckwal.retries"), Some("0"));
    assert_eq!(f.field("s3.status"), Some("200"));
}

/// A takeover whose head write landed and whose fence then failed still exports the new
/// epoch on its `buckwal.takeover` span (the outcome is `error`, and no fence seq).
#[test]
fn takeover_failing_at_the_fence_exports_its_epoch() {
    let cap = Capture::new();
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mem = Arc::new(MemStore::new());
    let (spans, r) = tracing::subscriber::with_default(cap.subscriber(), || {
        let db = crate::open_with_store(config(a.path()), mem.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        insert(&conn, 1).unwrap();
        let fault = Arc::new(FaultStore::new(mem.clone()));
        fault.only_keys_containing("log/");
        fault.fail_next_puts_permanently(1);
        let r = crate::takeover_with_store(config(b.path()), fault);
        (cap.spans(), r)
    });
    assert!(matches!(r, Err(crate::Error::UploadFailed(_))), "{r:?}");
    let body = mem.get("head").unwrap().unwrap().body;
    let epoch = crate::format::Head::decode(&body)
        .unwrap()
        .epoch
        .to_string();
    let takeovers = spans.named("buckwal.takeover");
    assert_eq!(takeovers.len(), 1, "{takeovers:?}");
    let t = takeovers[0];
    assert_eq!(t.field("buckwal.outcome"), Some("error"));
    assert_eq!(t.field("buckwal.epoch"), Some(epoch.as_str()));
}

/// M3: a takeover resumed through `open` (its fence failed, so the generation's last record is
/// still the old writer's) is a takeover too: the open's fence runs in a `buckwal.takeover`
/// span with outcome `ok`, the fence's seq and the head's epoch, and emits `Event::TookOver`.
#[test]
fn a_takeover_resumed_through_open_exports_a_takeover_span() {
    let cap = Capture::new();
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mem = Arc::new(MemStore::new());
    let took: Arc<Mutex<Vec<crate::Event>>> = Arc::default();
    let (spans, db) = tracing::subscriber::with_default(cap.subscriber(), || {
        let db = crate::open_with_store(config(a.path()), mem.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        insert(&conn, 1).unwrap();
        let fault = Arc::new(FaultStore::new(mem.clone()));
        fault.only_keys_containing("log/");
        fault.fail_next_puts_permanently(1);
        let r = crate::takeover_with_store(config(b.path()), fault);
        assert!(matches!(r, Err(crate::Error::UploadFailed(_))), "{r:?}");
        let mut c = config(b.path());
        let t = took.clone();
        c.on_event = Some(Arc::new(move |e: &crate::Event| {
            if let crate::Event::TookOver { .. } = e {
                t.lock().unwrap().push(e.clone());
            }
        }));
        let resumed = crate::open_with_store(c, mem.clone()).unwrap();
        (cap.spans(), resumed)
    });
    let head = crate::format::Head::decode(&mem.get("head").unwrap().unwrap().body).unwrap();
    let takeovers = spans.named("buckwal.takeover");
    assert_eq!(
        takeovers.len(),
        2,
        "the takeover, then its resumption: {takeovers:?}"
    );
    let t = takeovers[1];
    assert_eq!(t.field("buckwal.outcome"), Some("ok"));
    assert_eq!(
        t.field("buckwal.generation"),
        Some(head.generation.to_string().as_str())
    );
    assert_eq!(
        t.field("buckwal.epoch"),
        Some(head.epoch.to_string().as_str())
    );
    let f = spans.with_parent(t.id, "buckwal.fence");
    assert_eq!(f.field("buckwal.seq"), t.field("buckwal.seq"));
    let events = took.lock().unwrap().clone();
    assert!(
        matches!(
            &events[..],
            [crate::Event::TookOver { generation, epoch, fence_seq }]
                if *generation == head.generation
                    && *epoch == head.epoch
                    && Some(fence_seq.to_string().as_str()) == t.field("buckwal.seq")
        ),
        "{events:?}"
    );
    db.close().unwrap();
}

/// `gc` runs in a `buckwal.gc` span: the head's generation, the manifest trimmed against and
/// its age, what was deleted, and the outcome.
#[test]
fn gc_exports_a_gc_span() {
    let cap = Capture::new();
    let a = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemStore::new());
    let mut cfg = config(a.path());
    cfg.gc_after_seal = false;
    let min_age = cfg.engine(crate::io::IoMode::Writer).gc_min_age();
    let (spans, young, old) = tracing::subscriber::with_default(cap.subscriber(), || {
        let db = crate::open_with_store(cfg.clone(), mem.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        for x in 1..=2 {
            insert(&conn, x).unwrap();
            conn.checkpoint(CheckpointMode::Truncate).unwrap();
        }
        conn.close().unwrap();
        db.close().unwrap();
        let young = crate::gc_with_store(&cfg, mem.clone()).unwrap();
        mem.advance_clock(min_age);
        let old = crate::gc_with_store(&cfg, mem.clone()).unwrap();
        (cap.spans(), young, old)
    });
    assert_eq!(young.outcome, crate::GcOutcome::TooYoung);
    assert_eq!(old.outcome, crate::GcOutcome::Trimmed);
    let gcs = spans.named("buckwal.gc");
    assert_eq!(gcs.len(), 2, "{gcs:?}");
    for g in &gcs {
        assert_eq!(g.target, "buckwal");
        assert_eq!(g.level, Level::INFO);
        assert_eq!(
            g.field("buckwal.generation"),
            Some(old.generation.to_string().as_str())
        );
        assert!(g.closed.is_some());
    }
    assert_eq!(gcs[0].field("buckwal.outcome"), Some("too_young"));
    assert_eq!(gcs[0].field("buckwal.manifest"), None);
    assert_eq!(gcs[0].field("buckwal.deleted"), Some("0"));
    let t = gcs[1];
    assert_eq!(t.field("buckwal.outcome"), Some("trimmed"));
    assert_eq!(
        t.field("buckwal.manifest"),
        Some(old.trimmed_against.unwrap().to_string().as_str())
    );
    assert_eq!(
        t.field("buckwal.deleted"),
        Some(old.deleted.to_string().as_str())
    );
    assert!(old.deleted > 0);
    let age_ms: u128 = t.field("buckwal.age_ms").unwrap().parse().unwrap();
    assert!(age_ms >= min_age.as_millis(), "{age_ms}");
}

/// A pass that fails reports `error`.
#[test]
fn failed_gc_exports_error_outcome() {
    let cap = Capture::new();
    let a = tempfile::tempdir().unwrap();
    let mem = Arc::new(MemStore::new());
    let mut cfg = config(a.path());
    cfg.gc_after_seal = false;
    let (spans, r) = tracing::subscriber::with_default(cap.subscriber(), || {
        let db = crate::open_with_store(cfg.clone(), mem.clone()).unwrap();
        db.close().unwrap();
        // A head that does not decode.
        mem.put("head", b"not a head").unwrap();
        let r = crate::gc_with_store(&cfg, mem.clone());
        (cap.spans(), r)
    });
    assert!(r.is_err(), "{r:?}");
    let gcs = spans.named("buckwal.gc");
    assert_eq!(gcs.len(), 1, "{gcs:?}");
    assert_eq!(gcs[0].field("buckwal.outcome"), Some("error"));
}

/// Open a database under a test provider and return the finished span names and the
/// resource the exporter was given.
fn spans_and_resource_of_a_takeover() -> (Vec<String>, Resource) {
    keep_interest_per_call();
    let spans = InMemorySpanExporter::default();
    let seen = Arc::new(Mutex::new(None));
    let spy = ResourceSpy {
        inner: spans.clone(),
        seen: seen.clone(),
    };
    let (sub, p) = build_for_test(spy, InMemoryLogExporter::default(), None);
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mem = Arc::new(MemStore::new());
    tracing::subscriber::with_default(sub, || {
        let db = crate::open_with_store(config(a.path()), mem.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
            .unwrap();
        insert(&conn, 1).unwrap();
        drop(conn);
        let taken = crate::takeover_with_store(config(b.path()), mem.clone()).unwrap();
        drop(taken);
        drop(db);
    });
    p.force_flush();
    let names = spans
        .get_finished_spans()
        .unwrap()
        .iter()
        .map(|s| s.name.to_string())
        .collect();
    let resource = seen.lock().unwrap().clone().expect("a resource");
    (names, resource)
}

#[test]
fn service_name_is_buckwal() {
    let (_, resource) = spans_and_resource_of_a_takeover();
    assert_eq!(
        resource
            .get(&Key::new("service.name"))
            .map(|v| v.to_string()),
        Some("buckwal".to_string())
    );
}

#[test]
fn span_names_use_buckwal_prefix() {
    let (names, _) = spans_and_resource_of_a_takeover();
    assert!(names.iter().any(|n| n == "buckwal.takeover"), "{names:?}");
    assert!(
        names.iter().all(|n| !n.starts_with("durable.")),
        "{names:?}"
    );
}
