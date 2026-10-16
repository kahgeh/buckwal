use super::export_filter;
use super::testing::{Capture, Spans};
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
    let c = spans.last_named("durable.commit");
    assert_eq!(c.target, "turso_durable");
    assert_eq!(c.level, Level::INFO);
    assert_eq!(c.field("durable.batching"), Some("off"));
    assert_eq!(c.field("durable.outcome"), Some("ok"));
    assert!(c.field("durable.commit_no").is_some());
    assert!(c.field("durable.bytes").unwrap().parse::<u64>().unwrap() > 0);
    for child in [
        "durable.local_write",
        "durable.local_fsync",
        "durable.ack_wait",
    ] {
        assert_eq!(spans.children_of(c.id, child).len(), 1, "{child}");
    }
    let u = spans.with_parent(c.id, "durable.upload");
    assert_eq!(u.target, "turso_durable");
    assert_eq!(u.field("durable.commits"), Some("1"));
    assert_eq!(u.field("s3.status"), Some("200"));
    assert_eq!(u.field("durable.retries"), Some("0"));
    assert!(u.field("durable.bytes").unwrap().parse::<u64>().unwrap() > 0);
    assert!(u.field("durable.seq").is_some() && u.field("durable.generation").is_some());
    let child = |name| spans.by_id(spans.children_of(c.id, name)[0]);
    let (write, fsync) = (child("durable.local_write"), child("durable.local_fsync"));
    assert!(
        write.closed.is_some_and(|t| t <= fsync.created),
        "local_write closes at sync entry"
    );
    let ack = child("durable.ack_wait");
    assert!(ack.closed.is_some() && ack.closed <= c.closed);
    assert!(c.closed.is_some());
    // An info record at open, from the library.
    assert!(
        spans.events.iter().any(|e| e.level == Level::INFO
            && e.target.starts_with("turso_durable")
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
            .named("durable.commit")
            .iter()
            .filter(|c| {
                c.field("durable.batching") == Some("window 50ms 64 1MiB")
                    && c.field("durable.commit_no").is_some()
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
        .named("durable.commit")
        .into_iter()
        .filter(|c| c.field("durable.batching") == Some("window 50ms 64 1MiB"))
        .collect();
    assert_eq!(commits.len(), 4, "{commits:#?}");
    let ids: Vec<usize> = commits.iter().map(|c| c.id).collect();
    for c in &commits {
        assert_eq!(c.field("durable.outcome"), Some("ok"));
        assert_eq!(spans.children_of(c.id, "durable.ack_wait").len(), 1);
    }
    let uploads: Vec<_> = spans
        .named("durable.upload")
        .into_iter()
        .filter(|u| u.field("durable.commits") == Some("4"))
        .collect();
    assert_eq!(uploads.len(), 1, "{:#?}", spans.named("durable.upload"));
    let u = uploads[0];
    let parent = u.parent.expect("the upload has a parent");
    assert!(ids.contains(&parent), "parent {parent} not in {ids:?}");
    assert_eq!(u.parent_name, Some("durable.commit"));
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
        crate::restore_with_store(&config(restored.path()), store, restored.path()).unwrap();
        cap.spans()
    });
    let cp = spans.last_named("durable.checkpoint_upload");
    assert_eq!(cp.target, "turso_durable");
    assert_eq!(cp.field("durable.generation"), Some("1"));
    assert!(cp.field("durable.pages").unwrap().parse::<u64>().unwrap() > 0);
    assert_eq!(cp.field("durable.retries"), Some("0"));
    assert_eq!(cp.field("s3.status"), Some("200"));
    assert!(cp.closed.is_some());
    assert!(
        spans.events.iter().any(|e| e.level == Level::INFO
            && e.target.starts_with("turso_durable")
            && e.field("pages").is_some()
            && e.field("ms").is_some()),
        "no seal record"
    );
    let r = spans.last_named("durable.restore");
    assert_eq!(r.target, "turso_durable");
    assert_eq!(r.field("durable.generation"), Some("2"));
    assert!(r.field("durable.gets").unwrap().parse::<u64>().unwrap() > 0);
    assert!(r.field("durable.log_objects").is_some());
    assert!(r.field("durable.bytes").is_some());
    assert!(
        spans.events.iter().any(|e| e.level == Level::INFO
            && e.target.starts_with("turso_durable")
            && e.field("log_objects").is_some()),
        "no restore record"
    );
}

#[test]
fn retries_are_counted_on_the_upload_span() {
    let spans = capture_faulty(|fault, conn| {
        fault.fail_next_puts(2);
        insert(conn, 1).unwrap();
    });
    let c = spans.last_named("durable.commit");
    let u = spans.with_parent(c.id, "durable.upload");
    assert_eq!(u.field("durable.retries"), Some("2"));
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
            .any(|e| e.level == Level::WARN && e.target.starts_with("turso_durable")),
        "no warning: {:?}",
        spans.events
    );
    let decided: Vec<_> = spans
        .named("durable.commit")
        .into_iter()
        .filter_map(|c| c.field("durable.outcome"))
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
    for t in ["turso_core", "turso_core::storage::pager", "tonic", "h2"] {
        assert!(!keep(t), "{t}");
    }
    assert!(keep("turso_durable::uploader"));
}

#[test]
fn a_fenced_writer_logs_one_error() {
    use crate::bucket::Bucket;
    use crate::format::{encode_record, Keys, LogRecord, Op};
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
        });
        mem.put(&keys.log(1, 1), &foreign).unwrap();
        let sync = || {
            vec![vec![Op::Write {
                off: 0,
                bytes: vec![1; 4],
            }]]
        };
        assert!(b.append_log(sync()).is_err());
        assert!(b.append_log(sync()).is_err(), "fenced for good");
    });
    let errors: Vec<_> = cap
        .spans()
        .events
        .into_iter()
        .filter(|e| e.level == Level::ERROR)
        .collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].target.starts_with("turso_durable"));
    assert!(errors[0].field("error").unwrap().contains("another writer"));
}

/// A callsite first hit on a thread without a subscriber (another test's library thread,
/// say) must still be seen by a capture: tracing caches each callsite's interest, and with
/// at most one dispatcher registered it asks only the registering thread's.
#[test]
fn capture_sees_a_callsite_first_hit_on_a_plain_thread() {
    fn probe() {
        tracing::info!(target: "turso_durable::probe", "probe");
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
        .filter(|e| e.target == "turso_durable::probe")
        .count();
    assert_eq!(probes, 1);
}
