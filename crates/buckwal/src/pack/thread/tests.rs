//! The packer loop, its trigger, the writer's packer thread and the pack gauges, against the
//! in-memory store.
use super::*;
use crate::config::{Event, EventHook};
use crate::db::{open_with_store, Database};
use crate::format::Keys;
use crate::pack::run::{pack_run_in, Clock, WORK_DIR_PREFIX};
use crate::pack::{PackOutcome, PackPointer};
use crate::store::{
    CondPut, MemStore, MultipartDone, Object, ObjectStat, ObjectStore, PartSource, StatsSnap,
    StoreError,
};
use crate::telemetry::testing::Capture;
use crate::{CheckpointMode, Connection, Value};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;

const PREFIX: &str = "t";

fn config(dir: &Path) -> Config {
    let mut c = Config::new(dir, "http://127.0.0.1:1", "bucket", "key", "secret");
    c.prefix = PREFIX.to_string();
    c.retry_deadline = Duration::from_secs(2);
    c
}

fn keys() -> Keys {
    Keys::new(PREFIX)
}

fn tempdir() -> TempDir {
    tempfile::tempdir().unwrap()
}

const MIB: u64 = 1 << 20;

// ---- The trigger, as a pure function ----

fn pack(size: u64, age_secs: u64) -> Option<NewestPack> {
    Some(NewestPack {
        size,
        age: Duration::from_secs(age_secs),
    })
}

fn decide(ended: bool, pack: Option<NewestPack>, log_bytes: u64) -> Option<Reason> {
    let t = Trigger {
        ended,
        pack,
        log_bytes,
    };
    due(&t, 10, Duration::from_secs(3600))
}

#[test]
fn trigger_log_past_10_percent_of_the_pack() {
    assert_eq!(decide(true, pack(100 * MIB, 60), 10 * MIB), None, "at 10%");
    assert_eq!(
        decide(true, pack(100 * MIB, 60), 10 * MIB + 1),
        Some(Reason::LogPercent)
    );
    // No overflow at large sizes.
    assert_eq!(
        decide(true, pack(u64::MAX / 2, 60), u64::MAX),
        Some(Reason::LogPercent)
    );
}

#[test]
fn trigger_pack_older_than_max_age() {
    assert_eq!(decide(true, pack(100 * MIB, 3599), 0), None);
    assert_eq!(decide(true, pack(100 * MIB, 3600), 0), Some(Reason::MaxAge));
}

#[test]
fn trigger_needs_an_ended_generation() {
    assert_eq!(decide(false, pack(MIB, 100_000), 100 * MIB), None);
    assert_eq!(decide(false, None, 100 * MIB), None);
}

#[test]
fn trigger_without_a_pack_is_the_first_run_once_a_generation_ended() {
    assert_eq!(decide(true, None, 0), Some(Reason::FirstPack));
    assert_eq!(decide(false, None, 0), None);
}

// ---- A store that records calls and holds or fails the pack upload ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Upload {
    Pass,
    /// Each multipart upload fails (a 403, say).
    Fail,
    /// Each multipart upload waits until the test passes it.
    Hold,
}

struct TestStore {
    inner: Arc<MemStore>,
    calls: Mutex<Vec<String>>,
    upload: Mutex<Upload>,
    changed: Condvar,
    uploads_started: std::sync::atomic::AtomicU64,
    /// Set once a log object of the bucket is read (a pack run's scan).
    on_log_get: Mutex<Option<Arc<AtomicBool>>>,
    /// Set at the first HEAD of a pack (its size check, before the pointer write).
    on_pack_stat: Mutex<Option<Arc<AtomicBool>>>,
    /// When each multipart upload started.
    upload_times: Mutex<Vec<Instant>>,
    /// Bytes put and got around each multipart upload, outside any pack run, on the same
    /// in-memory store (another thread's traffic, as a writer makes).
    unrelated_bytes: std::sync::atomic::AtomicU64,
}

impl TestStore {
    fn new(inner: Arc<MemStore>) -> Arc<TestStore> {
        Arc::new(TestStore {
            inner,
            calls: Mutex::default(),
            upload: Mutex::new(Upload::Pass),
            changed: Condvar::new(),
            uploads_started: Default::default(),
            on_log_get: Mutex::default(),
            on_pack_stat: Mutex::default(),
            upload_times: Mutex::default(),
            unrelated_bytes: Default::default(),
        })
    }

    fn set_upload(&self, u: Upload) {
        *self.upload.lock().unwrap() = u;
        self.changed.notify_all();
    }

    fn record(&self, op: &str, key: &str) {
        self.calls.lock().unwrap().push(format!("{op} {key}"));
    }

    fn calls_of(&self, op: &str, key_part: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.starts_with(&format!("{op} ")) && c.contains(key_part))
            .count()
    }

    fn clear_calls(&self) {
        self.calls.lock().unwrap().clear();
    }
}

impl ObjectStore for TestStore {
    fn put(&self, key: &str, body: &[u8]) -> std::result::Result<String, StoreError> {
        self.record("put", key);
        self.inner.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> std::result::Result<CondPut, StoreError> {
        self.record("put_if_absent", key);
        self.inner.put_if_absent(key, body)
    }
    fn put_if_match(
        &self,
        key: &str,
        body: &[u8],
        etag: &str,
    ) -> std::result::Result<CondPut, StoreError> {
        self.record("put_if_match", key);
        self.inner.put_if_match(key, body, etag)
    }
    fn get(&self, key: &str) -> std::result::Result<Option<Object>, StoreError> {
        self.record("get", key);
        if key.contains("/log/") {
            if let Some(flag) = &*self.on_log_get.lock().unwrap() {
                flag.store(true, SeqCst);
            }
        }
        self.inner.get(key)
    }
    fn get_aged(&self, key: &str) -> std::result::Result<Option<(Object, Duration)>, StoreError> {
        self.record("get_aged", key);
        self.inner.get_aged(key)
    }
    fn stat(&self, key: &str) -> std::result::Result<Option<ObjectStat>, StoreError> {
        self.record("stat", key);
        if key.contains("/pack/") {
            if let Some(flag) = self.on_pack_stat.lock().unwrap().take() {
                flag.store(true, SeqCst);
            }
        }
        self.inner.stat(key)
    }
    fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StoreError> {
        self.record("list", prefix);
        self.inner.list(prefix)
    }
    fn list_after(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> std::result::Result<Vec<String>, StoreError> {
        self.record("list_after", prefix);
        self.inner.list_after(prefix, start_after)
    }
    fn list_sized(&self, prefix: &str) -> std::result::Result<Vec<(String, u64)>, StoreError> {
        self.record("list_sized", prefix);
        self.inner.list_sized(prefix)
    }
    fn list_sized_after(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> std::result::Result<Vec<(String, u64)>, StoreError> {
        self.record("list_sized_after", prefix);
        self.inner.list_sized_after(prefix, start_after)
    }
    fn put_multipart(
        &self,
        key: &str,
        source: &mut dyn PartSource,
        part_size: usize,
        concurrency: usize,
    ) -> std::result::Result<MultipartDone, StoreError> {
        self.record("put_multipart", key);
        self.upload_times.lock().unwrap().push(Instant::now());
        self.uploads_started.fetch_add(1, SeqCst);
        let n = self.unrelated_bytes.load(SeqCst) as usize;
        if n > 0 {
            let other = format!("{PREFIX}/unrelated");
            self.inner.put(&other, &vec![7u8; n])?;
            self.inner.get(&other)?;
        }
        let mut u = self.upload.lock().unwrap();
        while *u == Upload::Hold {
            // A held upload still reads its parts, as a slow link does: a part read that fails
            // fails the upload.
            let mut b = [0u8; 1];
            if source.read_at(0, &mut b).is_err() {
                return Err(StoreError::Permanent(format!(
                    "PUT {key}: read part 1 of the source"
                )));
            }
            u = self
                .changed
                .wait_timeout(u, Duration::from_millis(20))
                .unwrap()
                .0;
        }
        if *u == Upload::Fail {
            return Err(StoreError::Permanent(format!(
                "PUT {key}: HTTP 403 AccessDenied (request id r1)"
            )));
        }
        drop(u);
        self.inner
            .put_multipart(key, source, part_size, concurrency)
    }
    fn get_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> std::result::Result<Option<Vec<u8>>, StoreError> {
        self.record("get_range", key);
        self.inner.get_range(key, offset, len)
    }
    fn abort_incomplete(
        &self,
        prefix: &str,
        older_than: Duration,
    ) -> std::result::Result<u64, StoreError> {
        self.record("abort_incomplete", prefix);
        self.inner.abort_incomplete(prefix, older_than)
    }
    fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
        self.record("delete", key);
        self.inner.delete(key)
    }
    fn delete_if_match(&self, key: &str, etag: &str) -> std::result::Result<bool, StoreError> {
        self.record("delete_if_match", key);
        self.inner.delete_if_match(key, etag)
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

// ---- Writers ----

struct Writer {
    db: Database,
    c: Connection,
    dir: TempDir,
}

fn writer_on(store: Arc<dyn ObjectStore>, cfg: impl FnOnce(&Path) -> Config) -> Writer {
    let dir = tempdir();
    let db = open_with_store(cfg(dir.path()), store).unwrap();
    let c = db.connect().unwrap();
    c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    Writer { db, c, dir }
}

/// A writer with no packer thread of its own (`pack: false`).
fn plain_writer(store: Arc<dyn ObjectStore>) -> Writer {
    writer_on(store, |d| {
        let mut c = config(d);
        c.pack = false;
        c
    })
}

impl Writer {
    fn insert(&self, ids: std::ops::RangeInclusive<i64>) {
        for id in ids {
            self.c
                .execute(
                    "INSERT INTO t(id, v) VALUES (?, ?)",
                    &[Value::from_i64(id), Value::build_text(format!("row {id}"))],
                )
                .unwrap();
        }
    }

    fn end_generation(&self) {
        let g = self.db.generation();
        self.c.checkpoint(CheckpointMode::Truncate).unwrap();
        assert!(self.db.generation() > g, "the checkpoint moved the head");
    }
}

fn pointer_of(store: &dyn ObjectStore) -> Option<PackPointer> {
    let body = store.get(&keys().pack_pointer()).unwrap()?.body;
    Some(keys().adopting(store).decode_pointer(&body).unwrap())
}

fn packs(store: &dyn ObjectStore) -> Vec<String> {
    store.list(&keys().pack_root()).unwrap()
}

/// Wait up to `secs` for `f`.
fn eventually(secs: u64, mut f: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < end {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    f()
}

fn packer_cfg(t: &TempDir, name: &str) -> Config {
    config(&t.path().join(name))
}

/// The log bytes of the bucket in generations at or above `from`, by a listing.
fn log_bytes_from(store: &dyn ObjectStore, from: u64) -> u64 {
    store
        .list_sized(&keys().log_root())
        .unwrap()
        .iter()
        .filter(|(k, _)| keys().parse_log(k).is_some_and(|(g, _)| g >= from))
        .map(|(_, n)| n)
        .sum()
}

fn clock() -> Clock {
    let origin = Instant::now();
    Arc::new(move || origin.elapsed())
}

// ---- The check the loop makes once a minute ----

#[test]
fn loop_starts_a_run_when_log_passes_10_percent() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    w.end_generation();
    let t = tempdir();
    let mut cfg = packer_cfg(&t, "p");
    // The first pack.
    assert_eq!(check(&cfg, &*mem).unwrap(), Some(Reason::FirstPack));
    crate::pack::pack_with_store(&cfg, mem.clone()).unwrap();
    let p = pointer_of(&*mem).unwrap();
    w.insert(4..=40);
    w.end_generation();
    let log = log_bytes_from(&*mem, p.pack_generation);
    assert!(log > 0);
    // Just at the share: not due; just past it: due.
    cfg.pack_log_percent = log * 100 / p.size;
    while log * 100 > p.size * cfg.pack_log_percent {
        cfg.pack_log_percent += 1;
    }
    assert_eq!(check(&cfg, &*mem).unwrap(), None, "{log} of {}", p.size);
    cfg.pack_log_percent -= 1;
    let ts = TestStore::new(mem.clone());
    assert_eq!(check(&cfg, &*ts).unwrap(), Some(Reason::LogPercent));
    // It lists the log from the pack generation on, not the whole log.
    assert_eq!(ts.calls_of("list_sized_after", "/log/"), 1);
    assert_eq!(ts.calls_of("list_sized", ""), 0);
    // The default 10% of the real pack.
    let cfg = packer_cfg(&t, "p");
    let want = (log * 100 > p.size * 10).then_some(Reason::LogPercent);
    assert_eq!(check(&cfg, &*mem).unwrap(), want);
}

#[test]
fn loop_starts_a_run_when_pack_older_than_max_age_and_a_generation_ended() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    w.end_generation();
    let t = tempdir();
    let mut cfg = packer_cfg(&t, "p");
    cfg.pack_log_percent = 1_000_000;
    crate::pack::pack_with_store(&cfg, mem.clone()).unwrap();
    w.insert(4..=5);
    w.end_generation();
    assert_eq!(check(&cfg, &*mem).unwrap(), None, "a young pack");
    // The pointer's Last-Modified, by the bucket's clock, is now an hour old.
    mem.advance_clock(Duration::from_secs(3600));
    assert_eq!(check(&cfg, &*mem).unwrap(), Some(Reason::MaxAge));
}

#[test]
fn loop_does_nothing_without_ended_generation() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    let t = tempdir();
    let mut cfg = packer_cfg(&t, "p");
    cfg.pack_log_percent = 0;
    let ts = TestStore::new(mem.clone());
    // No pack and no ended generation.
    assert_eq!(check(&cfg, &*ts).unwrap(), None);
    w.end_generation();
    crate::pack::pack_with_store(&cfg, mem.clone()).unwrap();
    // Commits in the head's generation only, an old pack and a log past any share.
    w.insert(4..=40);
    mem.advance_clock(Duration::from_secs(7200));
    ts.clear_calls();
    assert_eq!(check(&cfg, &*ts).unwrap(), None);
    // The check read the head and the pointer, and listed nothing.
    let calls = ts.calls.lock().unwrap().clone();
    assert!(calls.iter().all(|c| !c.starts_with("list")), "{calls:?}");
    assert_eq!(calls.len(), 2, "{calls:?}");
}

// ---- The span and the event of a pack run ----

#[test]
fn pack_span_has_outcome_and_step_times() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    w.end_generation();
    let t = tempdir();
    let events: Arc<Mutex<Vec<Event>>> = Arc::default();
    let e = events.clone();
    let mut cfg = packer_cfg(&t, "p");
    cfg.on_event =
        Some(Arc::new(move |ev: &Event| e.lock().unwrap().push(ev.clone())) as EventHook);
    let cap = Capture::new();
    let r = tracing::subscriber::with_default(cap.subscriber(), || {
        crate::pack::pack_with_store(&cfg, mem.clone()).unwrap()
    });
    assert_eq!(r.outcome, PackOutcome::Packed);
    let p = pointer_of(&*mem).unwrap();
    let spans = cap.spans();
    let s = spans.last_named("buckwal.pack");
    assert_eq!(s.field("buckwal.outcome"), Some("packed"));
    assert_eq!(
        s.field("buckwal.pack.generation"),
        Some(&*p.pack_generation.to_string())
    );
    assert_eq!(s.field("buckwal.pack.key"), Some(p.pack_key.as_str()));
    assert_eq!(s.field("buckwal.pack.size"), Some(&*p.size.to_string()));
    assert_eq!(
        s.field("buckwal.pack.bytes_up"),
        Some(&*r.bytes_up.to_string())
    );
    assert_eq!(
        s.field("buckwal.pack.bytes_down"),
        Some(&*r.bytes_down.to_string())
    );
    assert_eq!(s.field("buckwal.pack.failures"), Some("0"));
    let share: f64 = s
        .field("buckwal.pack.free_page_share")
        .unwrap()
        .parse()
        .unwrap();
    assert!((0.0..1.0).contains(&share), "{share}");
    for step in ["read", "apply", "upload", "verify", "publish"] {
        let ms = s.field(&format!("buckwal.pack.{step}_ms"));
        assert!(
            ms.is_some_and(|v| v.parse::<u64>().is_ok()),
            "{step}: {ms:?}"
        );
    }
    assert!(s.field("buckwal.elapsed_ms").is_some());
    assert!(s.field("buckwal.pack.failed_step").is_none());
    assert!(
        matches!(
            &events.lock().unwrap()[..],
            [Event::Packed { pack_generation, bytes, .. }]
                if *pack_generation == p.pack_generation && *bytes == p.size
        ),
        "{:?}",
        events.lock().unwrap()
    );

    // Nothing ended since: `nothing_to_do`, no event.
    let r = tracing::subscriber::with_default(cap.subscriber(), || {
        crate::pack::pack_with_store(&cfg, mem.clone()).unwrap()
    });
    assert_eq!(r.outcome, PackOutcome::NothingToDo);
    let spans = cap.spans();
    let s = spans.last_named("buckwal.pack");
    assert_eq!(s.field("buckwal.outcome"), Some("nothing_to_do"));
    assert!(s.field("buckwal.pack.upload_ms").is_none());
    assert_eq!(events.lock().unwrap().len(), 1);

    // A failed upload: `failed`, its step, its key and the store's error.
    w.insert(4..=5);
    w.end_generation();
    let ts = TestStore::new(mem.clone());
    ts.set_upload(Upload::Fail);
    let r = tracing::subscriber::with_default(cap.subscriber(), || {
        crate::pack::pack_with_store(&cfg, ts.clone()).unwrap()
    });
    let PackOutcome::Failed { step, key } = &r.outcome else {
        panic!("{r:?}")
    };
    assert_eq!(step, "upload");
    let spans = cap.spans();
    let s = spans.last_named("buckwal.pack");
    assert_eq!(s.field("buckwal.outcome"), Some("failed"));
    assert_eq!(s.field("buckwal.pack.failed_step"), Some("upload"));
    assert_eq!(s.field("buckwal.pack.failed_key"), key.as_deref());
    assert!(s
        .field("buckwal.error")
        .is_some_and(|e| e.contains("AccessDenied")));
    assert_eq!(events.lock().unwrap().len(), 1);
}

// ---- The loop ----

/// The loop on its own thread; `stop` ends it.
fn spawn_loop(cfg: Config, store: Arc<dyn ObjectStore>) -> (Arc<AtomicBool>, JoinHandle<()>) {
    let stop = Arc::new(AtomicBool::new(false));
    let s = stop.clone();
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    let h = std::thread::spawn(move || {
        tracing::dispatcher::with_default(&dispatch, || {
            run_packer_with_store(&cfg, store, s).unwrap()
        })
    });
    (stop, h)
}

#[test]
fn two_packer_loops_at_once_leave_one_newest_pack() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    w.end_generation();
    let ts = TestStore::new(mem.clone());
    // Both runs upload before either moves the pointer.
    ts.set_upload(Upload::Hold);
    let t = tempdir();
    let mut loops = Vec::new();
    for name in ["a", "b"] {
        let mut cfg = packer_cfg(&t, name);
        cfg.pack_check_interval = Duration::from_millis(50);
        loops.push(spawn_loop(cfg, ts.clone()));
    }
    assert!(eventually(10, || ts.uploads_started.load(SeqCst) == 2));
    ts.set_upload(Upload::Pass);
    assert!(eventually(10, || pointer_of(&*mem).is_some()));
    // Let both loops make a few more checks.
    std::thread::sleep(Duration::from_millis(300));
    for (stop, h) in loops {
        stop.store(true, SeqCst);
        h.join().unwrap();
    }
    let p = pointer_of(&*mem).unwrap();
    assert_eq!(packs(&*mem), [p.pack_key.clone(), p.codes_key.clone()]);
    assert_eq!(ts.uploads_started.load(SeqCst), 2, "one run each");
}

#[test]
fn failing_run_does_not_stop_the_loop_and_logs_once() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    w.end_generation();
    let ts = TestStore::new(mem.clone());
    ts.set_upload(Upload::Fail);
    let t = tempdir();
    let mut cfg = packer_cfg(&t, "p");
    cfg.pack_check_interval = Duration::from_millis(30);
    let cap = Capture::new();
    let (stop, h) =
        tracing::subscriber::with_default(cap.subscriber(), || spawn_loop(cfg, ts.clone()));
    assert!(eventually(10, || ts.uploads_started.load(SeqCst) >= 4));
    // The bucket heals: the next check packs.
    ts.set_upload(Upload::Pass);
    assert!(eventually(10, || pointer_of(&*mem).is_some()));
    stop.store(true, SeqCst);
    h.join().unwrap();
    let spans = cap.spans();
    let warns: Vec<_> = spans
        .events
        .iter()
        .filter(|e| e.level == tracing::Level::WARN)
        .collect();
    assert_eq!(warns.len(), 1, "{warns:?}");
    assert_eq!(warns[0].field("message"), Some("pack run failed"));
    assert_eq!(warns[0].field("step"), Some("upload"));
    // Every failed run is in a span, with the failures in a row before it.
    let failed: Vec<_> = spans
        .named("buckwal.pack")
        .into_iter()
        .filter(|s| s.field("buckwal.outcome") == Some("failed"))
        .map(|s| s.field("buckwal.pack.failures").unwrap().to_string())
        .collect();
    assert!(failed.len() >= 4, "{failed:?}");
    assert_eq!(&failed[..3], ["0", "1", "2"]);
    let last = spans.last_named("buckwal.pack");
    assert_eq!(last.field("buckwal.outcome"), Some("packed"));
    assert!(spans
        .events
        .iter()
        .any(|e| e.field("message") == Some("pack runs succeed again")));
}

#[test]
fn loop_stops_promptly_while_it_waits() {
    let mem = Arc::new(MemStore::new());
    let t = tempdir();
    let cfg = packer_cfg(&t, "p"); // checks once a minute
    let (stop, h) = spawn_loop(cfg, mem.clone());
    std::thread::sleep(Duration::from_millis(100));
    let at = Instant::now();
    stop.store(true, SeqCst);
    h.join().unwrap();
    assert!(
        at.elapsed() < Duration::from_millis(500),
        "{:?}",
        at.elapsed()
    );
}

#[test]
fn stop_abandons_a_run_and_removes_its_working_dir() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    w.end_generation();
    let ts = TestStore::new(mem.clone());
    let stop = Arc::new(AtomicBool::new(false));
    // The stop comes while the run reads the log.
    *ts.on_log_get.lock().unwrap() = Some(stop.clone());
    let t = tempdir();
    let cfg = packer_cfg(&t, "p");
    let r = pack_run_in(&cfg, ts.clone(), clock(), &stop, 0, None).unwrap();
    assert_eq!(
        r.outcome,
        PackOutcome::Failed {
            step: "closed".into(),
            key: None
        }
    );
    assert_eq!(ts.uploads_started.load(SeqCst), 0);
    assert!(pointer_of(&*mem).is_none() && packs(&*mem).is_empty());
    let left: Vec<_> = std::fs::read_dir(&cfg.dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with(WORK_DIR_PREFIX))
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

// ---- The writer's thread ----

fn fast_packer(d: &Path) -> Config {
    let mut c = config(d);
    c.pack_check_interval = Duration::from_millis(50);
    c
}

#[test]
fn writer_thread_packs_and_a_restore_uses_the_pack() {
    let mem = Arc::new(MemStore::new());
    let w = writer_on(mem.clone(), fast_packer);
    assert_eq!(w.db.shared.packer_threads(), (true, true));
    w.insert(1..=20);
    w.end_generation();
    assert!(eventually(10, || pointer_of(&*mem).is_some()));
    let p = pointer_of(&*mem).unwrap();
    assert_eq!(p.pack_generation, w.db.generation());
    w.insert(21..=25);
    drop(w.c);
    drop(w.db);
    let t = tempdir();
    let r = crate::restore::restore_with_store(
        &config(&t.path().join("r")),
        mem.clone(),
        &t.path().join("r"),
    )
    .unwrap();
    assert_eq!(r.pack_key.as_deref(), Some(p.pack_key.as_str()));
    let db = open_with_store(config(&t.path().join("r")), mem.clone()).unwrap();
    let c = db.connect().unwrap();
    let rows = c.query("SELECT count(*) FROM t", &[]).unwrap();
    assert_eq!(rows[0][0].as_int(), Some(25));
}

#[test]
fn bucket_pack_0_starts_no_thread() {
    let mem = Arc::new(MemStore::new());
    let ts = TestStore::new(mem.clone());
    let w = writer_on(ts.clone(), |d| {
        let mut c = fast_packer(d);
        c.pack = false;
        c
    });
    // The gauges still run: they tell an owner that no packer runs.
    assert_eq!(w.db.shared.packer_threads(), (false, true));
    w.insert(1..=3);
    w.end_generation();
    std::thread::sleep(Duration::from_millis(500));
    assert!(pointer_of(&*mem).is_none());
    assert_eq!(ts.calls_of("put_multipart", "pack/"), 0);
}

#[test]
fn read_only_and_legacy_writers_start_no_packer() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    w.end_generation();
    let t = tempdir();
    let ro = crate::db::open_read_only_with_store(fast_packer(t.path()), mem.clone()).unwrap();
    assert_eq!(ro.shared.packer_threads(), (false, false));
    // A bucket of formats 1 to 4 seals: it has no packs.
    let legacy = {
        let _legacy = crate::format::LegacySeal::on();
        writer_on(Arc::new(MemStore::new()), fast_packer)
    };
    assert_eq!(legacy.db.shared.packer_threads(), (false, false));
}

#[test]
fn thread_never_blocks_a_commit() {
    let mem = Arc::new(MemStore::new());
    let ts = TestStore::new(mem.clone());
    ts.set_upload(Upload::Hold);
    let w = writer_on(ts.clone(), fast_packer);
    w.insert(1..=3);
    w.end_generation();
    // A pack run is in its upload, and it waits there.
    assert!(eventually(10, || ts.uploads_started.load(SeqCst) == 1));
    let mut slowest = Duration::ZERO;
    for id in 4..=40 {
        let at = Instant::now();
        w.insert(id..=id);
        slowest = slowest.max(at.elapsed());
    }
    // A generation switch too.
    let at = Instant::now();
    w.end_generation();
    slowest = slowest.max(at.elapsed());
    assert!(slowest < Duration::from_millis(500), "{slowest:?}");
    assert!(pointer_of(&*mem).is_none(), "the run still waits");
    ts.set_upload(Upload::Pass);
    assert!(eventually(10, || pointer_of(&*mem).is_some()));
}

#[test]
fn writer_packer_stops_promptly_on_drop() {
    let mem = Arc::new(MemStore::new());
    let w = writer_on(mem.clone(), config); // checks once a minute
    w.insert(1..=3);
    let at = Instant::now();
    drop(w.c);
    drop(w.db);
    assert!(at.elapsed() < Duration::from_secs(2), "{:?}", at.elapsed());
    drop(w.dir);
}

#[test]
fn writer_packer_waits_a_whole_interval_before_its_first_check() {
    let mem = Arc::new(MemStore::new());
    let ts = TestStore::new(mem.clone());
    let w = writer_on(ts.clone(), |d| {
        let mut c = config(d);
        c.pack_check_interval = Duration::from_secs(2);
        c
    });
    w.insert(1..=3);
    w.end_generation();
    std::thread::sleep(Duration::from_millis(500));
    // A check reads the pointer with its age (the open's create reads it with a plain GET).
    assert_eq!(ts.calls_of("get_aged", "pack-latest"), 0, "no check yet");
    assert!(eventually(10, || pointer_of(&*mem).is_some()));
}

// ---- The gauges ----

#[test]
fn gauges_use_pointer_last_modified_and_listing_sizes() {
    let _gc = crate::trim::NoWriterGc::on();
    let mem = Arc::new(MemStore::new());
    let ts = TestStore::new(mem.clone());
    let cap = Capture::new();
    let w = tracing::subscriber::with_default(cap.subscriber(), || {
        writer_on(ts.clone(), |d| {
            let mut c = config(d);
            c.pack = false;
            c.pack_gauge_interval = Duration::from_millis(100);
            c
        })
    });
    w.insert(1..=10);
    w.end_generation();
    let t = tempdir();
    crate::pack::pack_with_store(&packer_cfg(&t, "p"), mem.clone()).unwrap();
    let p = pointer_of(&*mem).unwrap();
    mem.advance_clock(Duration::from_secs(1800));
    let listed_before = ts.calls_of("list_sized_after", "/log/");
    ts.clear_calls();
    let gauges = |cap: &Capture| cap.spans().named("buckwal.pack.gauges").len();
    let before = gauges(&cap);
    // The writer's own commits after the first gauge's listing.
    assert!(eventually(10, || gauges(&cap) > before));
    w.insert(11..=30);
    w.end_generation();
    w.insert(31..=35);
    let n = gauges(&cap);
    assert!(eventually(10, || gauges(&cap) >= n + 3));
    let spans = cap.spans();
    let s = spans.last_named("buckwal.pack.gauges");
    assert_eq!(s.field("buckwal.outcome"), Some("ok"));
    assert_eq!(
        s.field("buckwal.pack.behind_bytes"),
        Some(&*log_bytes_from(&*mem, p.pack_generation).to_string())
    );
    let age: u64 = s
        .field("buckwal.pack.age_seconds")
        .unwrap()
        .parse()
        .unwrap();
    assert!((1800..1830).contains(&age), "{age}");
    assert_eq!(
        s.field("buckwal.pack.generation"),
        Some(&*p.pack_generation.to_string())
    );
    assert_eq!(
        s.field("buckwal.pack.size_bytes"),
        Some(&*p.size.to_string())
    );
    // One listing in this open, and one pointer GET per gauge.
    let all = spans.named("buckwal.pack.gauges").len() - before;
    assert_eq!(listed_before + ts.calls_of("list_sized_after", "/log/"), 1);
    assert_eq!(ts.calls_of("list_sized", ""), 0);
    assert_eq!(ts.calls_of("list", ""), 0);
    assert_eq!(ts.calls_of("list_after", ""), 0);
    let gets = ts.calls_of("get_aged", "pack-latest");
    assert!(gets == all || gets == all + 1, "{gets} gets, {all} gauges");
    assert_eq!(ts.calls_of("get", "pack-latest"), 0);
    assert_eq!(ts.calls_of("stat", "pack-latest"), 0);
    drop(w);
}

#[test]
fn gauges_without_a_pack_count_the_whole_log() {
    let _gc = crate::trim::NoWriterGc::on();
    let mem = Arc::new(MemStore::new());
    let cap = Capture::new();
    let w = tracing::subscriber::with_default(cap.subscriber(), || {
        writer_on(mem.clone(), |d| {
            let mut c = config(d);
            c.pack = false;
            c.pack_gauge_interval = Duration::from_millis(50);
            c
        })
    });
    w.insert(1..=5);
    w.end_generation();
    let n = cap.spans().named("buckwal.pack.gauges").len();
    assert!(eventually(10, || cap
        .spans()
        .named("buckwal.pack.gauges")
        .len()
        >= n + 2));
    let spans = cap.spans();
    let s = spans.last_named("buckwal.pack.gauges");
    assert_eq!(
        s.field("buckwal.pack.behind_bytes"),
        Some(&*log_bytes_from(&*mem, 1).to_string())
    );
    assert_eq!(s.field("buckwal.pack.age_seconds"), None, "no pack");
    assert_eq!(s.field("buckwal.pack.generation"), None);
    assert_eq!(s.field("buckwal.pack.size_bytes"), None);
}

#[test]
fn a_failure_like_the_one_before_is_the_same_failure() {
    // A new request id, attempt count or time does not make a new failure.
    let a =
        "bucket: GET t/head: HTTP 403: AccessDenied (request id 17A2B) after 3 attempts in 2.1s";
    let b =
        "bucket: GET t/head: HTTP 403: AccessDenied (request id 99FFE) after 5 attempts in 4.0s";
    assert_eq!(error_kind(a), error_kind(b));
    assert_ne!(
        error_kind(a),
        error_kind("bucket: GET t/head: HTTP 404: NoSuchBucket (request id 1)")
    );
    // A run's own pack key changes with each run: only the step counts.
    let root = keys().pack_root();
    assert_eq!(
        failure_kind("upload", Some("t/pack/0000000003/abc"), &root),
        failure_kind("upload", Some("t/pack/0000000003/def"), &root)
    );
    assert_ne!(
        failure_kind(
            "log_incomplete",
            Some("t/log/0000000003/00000000000000000002"),
            &root
        ),
        failure_kind("upload", None, &root)
    );
    // Only a key under the pack root is a run's own pack.
    assert_ne!(
        failure_kind("head", Some("t/log/pack/1"), &root),
        failure_kind("head", Some("t/log/pack/2"), &root)
    );
}

// ---- Fix round 1 ----

#[test]
fn gauges_leave_out_the_generations_a_pack_run_packed() {
    let _gc = crate::trim::NoWriterGc::on();
    let mem = Arc::new(MemStore::new());
    let cap = Capture::new();
    let w = tracing::subscriber::with_default(cap.subscriber(), || {
        writer_on(mem.clone(), |d| {
            let mut c = config(d);
            c.pack = false;
            c.pack_gauge_interval = Duration::from_millis(50);
            c
        })
    });
    w.insert(1..=10);
    w.end_generation();
    w.insert(11..=20);
    w.end_generation();
    let gauges = || cap.spans().named("buckwal.pack.gauges").len();
    let behind = || -> u64 {
        let spans = cap.spans();
        spans
            .last_named("buckwal.pack.gauges")
            .field("buckwal.pack.behind_bytes")
            .unwrap()
            .parse()
            .unwrap()
    };
    let n = gauges();
    assert!(eventually(10, || gauges() >= n + 2));
    let all = behind();
    assert_eq!(all, log_bytes_from(&*mem, 1));
    let t = tempdir();
    crate::pack::pack_with_store(&packer_cfg(&t, "p"), mem.clone()).unwrap();
    let p = pointer_of(&*mem).unwrap();
    assert!(p.pack_generation > 2);
    let n = gauges();
    assert!(eventually(10, || gauges() >= n + 2));
    let left = behind();
    assert_eq!(left, log_bytes_from(&*mem, p.pack_generation));
    assert!(left < all, "{left} of {all}");
}

#[test]
fn gauges_wait_a_whole_interval_before_the_first_span() {
    let mem = Arc::new(MemStore::new());
    let ts = TestStore::new(mem.clone());
    let cap = Capture::new();
    let _w = tracing::subscriber::with_default(cap.subscriber(), || {
        writer_on(ts.clone(), |d| {
            let mut c = config(d);
            c.pack = false;
            c.pack_gauge_interval = Duration::from_secs(2);
            c
        })
    });
    std::thread::sleep(Duration::from_millis(500));
    assert!(cap.spans().named("buckwal.pack.gauges").is_empty());
    assert_eq!(
        ts.calls_of("list_sized_after", "/log/"),
        0,
        "no listing at open"
    );
    assert!(eventually(10, || !cap
        .spans()
        .named("buckwal.pack.gauges")
        .is_empty()));
}

#[test]
fn landed_record_is_bounded() {
    let c = crate::bucket::Counters::default();
    let pos = |g, s| crate::bucket::LogPos {
        generation: g,
        seq: s,
    };
    // Off: nothing is kept.
    c.note_landed(pos(1, 1), 10);
    c.track_landed();
    assert_eq!(c.take_positions(), Some(vec![]));
    // After the first merge, one sum per generation.
    for s in 1..=50_000 {
        c.note_landed(pos(1 + s % 3, s), 2);
    }
    let sums = c.take_sums();
    assert_eq!(sums.len(), 3);
    assert_eq!(sums.values().sum::<u64>(), 100_000);
    assert!(c.take_sums().is_empty());
    // A listing that does not come: the positions stop at the cap, and the next listing
    // starts again.
    c.track_landed();
    for s in 1..=(crate::bucket::MAX_LANDED_POSITIONS as u64 + 10) {
        c.note_landed(pos(4, s), 1);
    }
    assert_eq!(c.take_positions(), None, "too many: list again");
    c.track_landed();
    c.note_landed(pos(4, 1), 1);
    assert_eq!(c.take_positions(), Some(vec![(4, 1, 1)]));
}

#[test]
fn close_during_an_upload_returns_at_once_and_aborts_the_upload() {
    let mem = Arc::new(MemStore::new());
    let ts = TestStore::new(mem.clone());
    ts.set_upload(Upload::Hold);
    let cap = Capture::new();
    let w =
        tracing::subscriber::with_default(cap.subscriber(), || writer_on(ts.clone(), fast_packer));
    w.insert(1..=3);
    w.end_generation();
    assert!(eventually(10, || ts.uploads_started.load(SeqCst) == 1));
    let dir = w.dir.path().to_path_buf();
    let at = Instant::now();
    drop(w.c);
    drop(w.db);
    assert!(at.elapsed() < Duration::from_secs(3), "{:?}", at.elapsed());
    assert!(pointer_of(&*mem).is_none());
    assert!(ts.calls_of("abort_incomplete", "/pack/") >= 1);
    assert_eq!(
        mem.abort_incomplete(&keys().pack_root(), Duration::ZERO)
            .unwrap(),
        0,
        "no upload is left"
    );
    let left: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with(WORK_DIR_PREFIX))
        .collect();
    assert!(left.is_empty(), "{left:?}");
    let spans = cap.spans();
    let s = spans.last_named("buckwal.pack");
    assert_eq!(s.field("buckwal.outcome"), Some("failed"));
    assert_eq!(s.field("buckwal.pack.failed_step"), Some("closed"));
    // A close is not a fault: no WARN.
    assert!(!spans.events.iter().any(|e| e.level == tracing::Level::WARN));
}

#[test]
fn close_before_the_pointer_write_discards_the_pack() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    w.end_generation();
    let ts = TestStore::new(mem.clone());
    let stop = Arc::new(AtomicBool::new(false));
    *ts.on_pack_stat.lock().unwrap() = Some(stop.clone());
    let t = tempdir();
    let r = pack_run_in(&packer_cfg(&t, "p"), ts.clone(), clock(), &stop, 0, None).unwrap();
    assert!(
        matches!(&r.outcome, PackOutcome::Failed { step, .. } if step == "closed"),
        "{r:?}"
    );
    assert!(pointer_of(&*mem).is_none());
    assert!(packs(&*mem).is_empty(), "{:?}", packs(&*mem));
}

#[test]
fn back_off_doubles_from_the_interval_up_to_max_age() {
    let m = Duration::from_secs(60);
    let max = Duration::from_secs(3600);
    let waits: Vec<u64> = (0..9).map(|n| next_wait(m, n, max).as_secs()).collect();
    assert_eq!(waits, [60, 60, 120, 240, 480, 960, 1920, 3600, 3600]);
    assert_eq!(next_wait(m, 200, max).as_secs(), 3600, "no overflow");
    // A max age below the interval: the interval.
    assert_eq!(next_wait(m, 5, Duration::from_secs(10)).as_secs(), 60);
}

#[test]
fn failed_runs_back_off_and_a_success_resets_the_wait() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    w.end_generation();
    let ts = TestStore::new(mem.clone());
    ts.set_upload(Upload::Fail);
    let t = tempdir();
    let mut cfg = packer_cfg(&t, "p");
    let interval = Duration::from_millis(40);
    cfg.pack_check_interval = interval;
    cfg.pack_max_age = Duration::from_millis(320);
    let max = cfg.pack_max_age;
    let cap = Capture::new();
    let (stop, h) =
        tracing::subscriber::with_default(cap.subscriber(), || spawn_loop(cfg, ts.clone()));
    assert!(eventually(20, || ts.uploads_started.load(SeqCst) >= 6));
    // The loop counts each wait from the start of a check (each check here is a failed run).
    let times = check_starts(&t.path().join("p"));
    assert!(times.len() >= 6, "{times:?}");
    for k in 1..6 {
        let gap = times[k] - times[k - 1];
        let want = next_wait(interval, k as u64, max);
        assert!(gap >= want, "gap {k}: {gap:?} < {want:?}");
    }
    // A success resets the wait: the next failure counts from 0 again.
    ts.set_upload(Upload::Pass);
    assert!(eventually(20, || pointer_of(&*mem).is_some()));
    ts.set_upload(Upload::Fail);
    w.insert(4..=5);
    w.end_generation();
    let started = ts.uploads_started.load(SeqCst);
    assert!(eventually(20, || ts.uploads_started.load(SeqCst) > started + 1));
    stop.store(true, SeqCst);
    h.join().unwrap();
    let spans = cap.spans();
    let after: Vec<String> = spans
        .named("buckwal.pack")
        .into_iter()
        .skip_while(|s| s.field("buckwal.outcome") != Some("packed"))
        .filter(|s| s.field("buckwal.outcome") == Some("failed"))
        .map(|s| s.field("buckwal.pack.failures").unwrap().to_string())
        .collect();
    assert_eq!(&after[..2], ["0", "1"], "{after:?}");
    // The back-off is logged when it starts, not at each doubling.
    let backoffs = spans
        .events
        .iter()
        .filter(|e| e.field("message") == Some("pack runs back off"))
        .count();
    assert!((1..=2).contains(&backoffs), "{backoffs}");
}

#[test]
fn run_without_free_disk_fails_at_disk_and_writes_nothing() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    w.end_generation();
    let t = tempdir();
    crate::pack::pack_with_store(&packer_cfg(&t, "other"), mem.clone()).unwrap();
    // A packer with no kept copy: it must download the pack, so it needs the pack's size
    // plus 10% (a kept copy that it can use needs only room for the log).
    let cfg = packer_cfg(&t, "p");
    let p = pointer_of(&*mem).unwrap();
    w.insert(4..=5);
    w.end_generation();
    let ts = TestStore::new(mem.clone());
    let before = packs(&*mem);
    // Just below the pack's size plus 10%.
    let free = crate::pack::run::FreeDiskForTest::set(p.size + p.size / 10 - 1);
    let r = pack_run_in(&cfg, ts.clone(), clock(), &AtomicBool::new(false), 0, None).unwrap();
    let PackOutcome::Failed { step, .. } = &r.outcome else {
        panic!("{r:?}")
    };
    assert_eq!(step, "disk");
    assert!(
        r.error.as_deref().is_some_and(|e| e.contains("free")),
        "{r:?}"
    );
    assert_eq!(ts.calls_of("get_range", ""), 0, "no download");
    assert_eq!(ts.calls_of("get", "/log/"), 0, "no log read");
    assert_eq!(ts.calls_of("put", ""), 0);
    assert_eq!(ts.calls_of("put_multipart", ""), 0);
    assert_eq!(packs(&*mem), before);
    let left: Vec<_> = std::fs::read_dir(&cfg.dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with(WORK_DIR_PREFIX))
        .collect();
    assert!(left.is_empty(), "{left:?}");
    drop(free);
    // With enough room the run packs.
    let _free = crate::pack::run::FreeDiskForTest::set(p.size + p.size / 10);
    let r = pack_run_in(&cfg, ts.clone(), clock(), &AtomicBool::new(false), 0, None).unwrap();
    assert_eq!(r.outcome, PackOutcome::Packed);
}

#[test]
fn free_disk_of_a_real_directory_is_known() {
    let t = tempdir();
    assert!(crate::pack::run::free_disk(t.path()).unwrap() > 0);
}

#[test]
fn run_counts_only_its_own_bytes() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    w.end_generation();
    let ts = TestStore::new(mem.clone());
    // 4 MiB up and 4 MiB down by "another thread" during the run's upload.
    ts.unrelated_bytes.store(4 << 20, SeqCst);
    let t = tempdir();
    let r = pack_run_in(
        &packer_cfg(&t, "p"),
        ts.clone(),
        clock(),
        &AtomicBool::new(false),
        0,
        None,
    )
    .unwrap();
    assert_eq!(r.outcome, PackOutcome::Packed);
    let p = pointer_of(&*mem).unwrap();
    assert!(
        r.bytes_up >= p.size && r.bytes_up < p.size + (1 << 20),
        "{r:?}"
    );
    assert!(r.bytes_down < 1 << 20, "{r:?}");
}

#[test]
fn pack_span_failed_with_step_error() {
    // No database in the bucket: the run fails before any step.
    let mem = Arc::new(MemStore::new());
    let t = tempdir();
    let cap = Capture::new();
    let r = tracing::subscriber::with_default(cap.subscriber(), || {
        crate::pack::pack_with_store(&packer_cfg(&t, "p"), mem.clone())
    });
    assert!(r.is_err());
    let spans = cap.spans();
    let s = spans.last_named("buckwal.pack");
    assert_eq!(s.field("buckwal.outcome"), Some("failed"));
    assert_eq!(s.field("buckwal.pack.failed_step"), Some("error"));
    assert!(s.field("buckwal.error").is_some());
}

#[test]
fn mvcc_seal_writer_and_replica_start_no_packer() {
    // An MVCC bucket of format 2 seals: no packs.
    let mvcc = {
        let _legacy = crate::format::LegacySeal::on();
        writer_on(Arc::new(MemStore::new()), |d| {
            let mut c = fast_packer(d);
            c.journal_mode = crate::JournalMode::Mvcc;
            c
        })
    };
    assert_eq!(mvcc.db.shared.packer_threads(), (false, false));
    // A replica (of a format 1 bucket: replicas of format 5 come with task 13) only reads.
    let mem = Arc::new(MemStore::new());
    let w = {
        let _legacy = crate::format::LegacySeal::on();
        writer_on(mem.clone(), |d| {
            let mut c = config(d);
            c.pack = false;
            c
        })
    };
    w.insert(1..=3);
    let ts = TestStore::new(mem.clone());
    let t = tempdir();
    let mut rc = fast_packer(&t.path().join("r"));
    rc.pack_gauge_interval = Duration::from_millis(50);
    rc.replica_poll_interval = Duration::from_millis(200);
    let replica = crate::replica::open_replica_with_store(rc, ts.clone()).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(ts.calls_of("get_aged", "pack-latest"), 0);
    assert_eq!(ts.calls_of("put_multipart", ""), 0);
    drop(replica);
}

// ---- Fix round 2 ----

/// When the loop of the packer whose directory is `dir` started each check.
fn check_starts(dir: &Path) -> Vec<Instant> {
    CHECK_STARTS
        .lock()
        .unwrap()
        .iter()
        .filter(|(d, _)| d == dir)
        .map(|(_, t)| *t)
        .collect()
}

#[test]
fn a_failed_check_leaves_the_back_off_count() {
    use Checked::*;
    assert_eq!(failed_runs_after(0, RunFailed), 1);
    assert_eq!(failed_runs_after(2, RunFailed), 3);
    // A failed check reads only the head and the pointer: run, check, run still backs off.
    assert_eq!(failed_runs_after(2, CheckFailed), 2);
    assert_eq!(
        failed_runs_after(failed_runs_after(2, CheckFailed), RunFailed),
        3
    );
    assert_eq!(failed_runs_after(5, Succeeded), 0);
    assert_eq!(failed_runs_after(5, NothingDue), 0);
}

#[test]
fn first_pack_disk_check_uses_the_estimate() {
    let mem = Arc::new(MemStore::new());
    let w = plain_writer(mem.clone());
    w.insert(1..=3);
    w.end_generation();
    let t = tempdir();
    let cfg = packer_cfg(&t, "p");
    let ts = TestStore::new(mem.clone());
    let _free = crate::pack::run::FreeDiskForTest::set(1 << 20);
    // No pack yet: the estimate (the writer's local database file) is the size to check.
    let r = pack_run_in(
        &cfg,
        ts.clone(),
        clock(),
        &AtomicBool::new(false),
        0,
        Some(10 << 20),
    )
    .unwrap();
    assert!(
        matches!(&r.outcome, PackOutcome::Failed { step, .. } if step == "disk"),
        "{r:?}"
    );
    assert_eq!(ts.calls_of("get", "/log/"), 0, "no log read");
    assert!(pointer_of(&*mem).is_none() && packs(&*mem).is_empty());
    // Without an estimate (`buckwal pack`, `buckwal packer`) the first pack is not checked.
    let r = pack_run_in(&cfg, ts.clone(), clock(), &AtomicBool::new(false), 0, None).unwrap();
    assert_eq!(r.outcome, PackOutcome::Packed);
}

#[test]
fn writer_estimate_is_the_local_database_size() {
    let t = tempdir();
    assert_eq!(first_pack_estimate(Some(&t.path().join("app.db"))), None);
    std::fs::write(t.path().join("app.db"), vec![0u8; 8192]).unwrap();
    assert_eq!(
        first_pack_estimate(Some(&t.path().join("app.db"))),
        Some(8192)
    );
    assert_eq!(first_pack_estimate(None), None);
}

// ---- The writer's thread and the pack directory ----

/// The `buckwal.pack` spans that packed, as (source, reason).
fn packed_sources(cap: &Capture) -> Vec<(String, String)> {
    cap.spans()
        .named("buckwal.pack")
        .into_iter()
        .filter(|s| s.field("buckwal.outcome") == Some("packed"))
        .map(|s| {
            (
                s.field("buckwal.pack.source").unwrap_or("-").to_string(),
                s.field("buckwal.pack.reason").unwrap_or("-").to_string(),
            )
        })
        .collect()
}

fn names_in(d: &Path) -> Vec<String> {
    std::fs::read_dir(d)
        .map(|es| {
            es.map(|e| e.unwrap().file_name().to_string_lossy().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// A writer whose pack directory (`BUCKWAL_PACK_DIR`) does not exist at open: the open makes
/// it. Its thread's first run downloads the pack another packer made (`downloaded`, `none`),
/// the next run uses the kept copy (`kept`). The kept copy, its lock and the working files are
/// in the pack directory, and nothing of them is in the database's directory.
#[test]
fn empty_pack_dir_at_open_first_run_downloads_then_the_kept_copy_is_used() {
    let mem = Arc::new(MemStore::new());
    let a = plain_writer(mem.clone());
    a.insert(1..=20);
    a.end_generation();
    let t = tempdir();
    crate::pack::pack_with_store(&packer_cfg(&t, "other"), mem.clone()).unwrap();
    a.insert(21..=30);
    a.end_generation();
    drop(a.c);
    drop(a.db);
    let pack_dir = t.path().join("fast/pack");
    let cap = Capture::new();
    let dir = tempdir();
    let mut cfg = fast_packer(dir.path());
    cfg.pack_dir = Some(pack_dir.clone());
    let db = tracing::subscriber::with_default(cap.subscriber(), || {
        open_with_store(cfg, mem.clone()).unwrap()
    });
    let c = db.connect().unwrap();
    let b = Writer { db, c, dir };
    assert!(pack_dir.is_dir(), "the open makes the pack directory");
    assert!(eventually(10, || packed_sources(&cap).len() == 1));
    assert_eq!(
        packed_sources(&cap)[0],
        ("downloaded".to_string(), "none".to_string())
    );
    b.insert(31..=40);
    b.end_generation();
    assert!(eventually(10, || packed_sources(&cap).len() == 2));
    assert_eq!(
        packed_sources(&cap)[1],
        ("kept".to_string(), "none".to_string())
    );
    let copy = names_in(&pack_dir.join("pack-copy"));
    for f in ["app.db", "state.json", "lock"] {
        assert!(copy.iter().any(|n| n == f), "{f}: {copy:?}");
    }
    let in_db_dir = names_in(b.dir.path());
    assert!(
        in_db_dir
            .iter()
            .all(|n| n != "pack-copy" && !n.starts_with(WORK_DIR_PREFIX)),
        "{in_db_dir:?}"
    );
    // The pack and a restore equal the writer's database.
    let p = pointer_of(&*mem).unwrap();
    let want: Vec<i64> = (1..=40).collect();
    drop(b.c);
    drop(b.db);
    let r = tempdir();
    let rep = crate::restore::restore_with_store(
        &config(&r.path().join("r")),
        mem.clone(),
        &r.path().join("r"),
    )
    .unwrap();
    assert_eq!(rep.pack_key.as_deref(), Some(p.pack_key.as_str()));
    let db = open_with_store(config(&r.path().join("r")), mem.clone()).unwrap();
    let c = db.connect().unwrap();
    let got: Vec<i64> = c
        .query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect();
    assert_eq!(got, want);
}

/// Without `pack_dir`, the writer's thread keeps its copy in the database's directory.
#[test]
fn writer_thread_keeps_its_copy_in_the_database_directory_by_default() {
    let mem = Arc::new(MemStore::new());
    let w = writer_on(mem.clone(), fast_packer);
    w.insert(1..=20);
    w.end_generation();
    assert!(eventually(10, || pointer_of(&*mem).is_some()));
    assert!(eventually(10, || names_in(&w.dir.path().join("pack-copy"))
        .iter()
        .any(|n| n == "state.json")));
}

/// A writer whose `pack_dir` is a file does not open.
#[test]
fn writer_with_a_pack_dir_that_is_a_file_does_not_open() {
    let mem = Arc::new(MemStore::new());
    let t = tempdir();
    std::fs::write(t.path().join("file"), b"x").unwrap();
    let mut cfg = config(&t.path().join("db"));
    cfg.pack_dir = Some(t.path().join("file"));
    match open_with_store(cfg, mem.clone()) {
        Err(crate::Error::Config(m)) => assert!(m.contains("BUCKWAL_PACK_DIR"), "{m}"),
        r => panic!("{:?}", r.map(|_| ())),
    }
    assert_eq!(mem.stats().puts, 0, "nothing was written to the bucket");
}
