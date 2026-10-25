use super::*;
use crate::config::Event;
use crate::config::JournalMode;
use crate::store::{CondPut, MemStore, Object, ObjectStat, StatsSnap, StoreError};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::Instant;

fn keys() -> Keys {
    Keys::new("db")
}

fn head(generation: u64, manifest: Option<u64>) -> Head {
    Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation,
        manifest,
        writer_id: "w".into(),
        epoch: 0,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
        database_id: None,
    }
}

/// Keys under each root that no parser accepts: the trimmer keeps them.
fn junk(k: &Keys) -> [String; 3] {
    [
        format!("{}junk", k.log_root()),
        format!("{}0000000001/x", k.page_root()),
        format!("{}x", k.manifest_root()),
    ]
}

/// Generations 1-3 by hand: two logs in 1, one each in 2 and 3, manifests 1 and 2, pages
/// 1/1, 1/2 and 2/1. Manifest 2 maps page 1 to generation 2 and page 2 to generation 1.
/// Generation 3 is being sealed: `pages/3/1`, `pages/3/5` and `manifest/3` are uploaded but
/// the head does not reference them yet. Plus one unparseable key under each root.
fn three_generations() -> (MemStore, Manifest) {
    let k = keys();
    let mem = MemStore::new();
    let m2 = Manifest {
        page_size: 4096,
        page_count: 2,
        pages: BTreeMap::from([(1, 2), (2, 1)]),
    };
    for key in [
        k.log(1, 1),
        k.log(1, 2),
        k.log(2, 1),
        k.log(3, 1),
        k.page(1, 1),
        k.page(1, 2),
        k.page(2, 1),
        k.page(3, 1),
        k.page(3, 5),
        k.manifest(3),
    ]
    .into_iter()
    .chain(junk(&k))
    {
        mem.put(&key, b"x").unwrap();
    }
    mem.put(&k.manifest(1), b"m1").unwrap();
    mem.put(&k.manifest(2), &m2.encode()).unwrap();
    mem.put(&k.head(), &head(3, Some(2)).encode()).unwrap();
    (mem, m2)
}

fn all_keys(mem: &MemStore) -> Vec<String> {
    mem.list("").unwrap()
}

#[test]
fn trim_deletes_only_unreferenced_older_generations() {
    let k = keys();
    let (mem, m2) = three_generations();
    let deleted = trim_once(&mem, &k, &head(3, Some(2)), Some(&m2)).unwrap();
    assert_eq!(deleted, 5);
    // Kept: the current log, manifest 2 and what it references, the in-progress seal of
    // generation 3 (never touched: generation >= M) and keys no parser accepts.
    let mut want = vec![
        k.head(),
        k.log(3, 1),
        k.manifest(2),
        k.page(1, 2),
        k.page(2, 1),
        k.page(3, 1),
        k.page(3, 5),
        k.manifest(3),
    ];
    want.extend(junk(&k));
    want.sort();
    assert_eq!(all_keys(&mem), want);
    // Nothing more to do.
    assert_eq!(
        trim_once(&mem, &k, &head(3, Some(2)), Some(&m2)).unwrap(),
        0
    );
}

#[test]
fn trim_without_manifest_deletes_only_older_logs() {
    let k = keys();
    let (mem, _) = three_generations();
    let before = all_keys(&mem);
    let deleted = trim_once(&mem, &k, &head(2, None), None).unwrap();
    assert_eq!(deleted, 2);
    let mut want: Vec<String> = before
        .into_iter()
        .filter(|key| !key.starts_with(&k.log_prefix(1)))
        .collect();
    want.sort();
    assert_eq!(all_keys(&mem), want);
}

/// A trimmer over `store` with a hook that records each `Trimmed`.
fn trimmer(
    store: Arc<dyn ObjectStore>,
    min_age: Duration,
) -> (
    mpsc::Sender<TrimSnapshot>,
    JoinHandle<()>,
    Arc<Mutex<Vec<u64>>>,
) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let hook: EventHook = {
        let seen = seen.clone();
        Arc::new(move |e: &Event| {
            if let Event::Trimmed { deleted } = e {
                seen.lock().unwrap().push(*deleted);
            }
        })
    };
    let (tx, rx) = mpsc::channel();
    let t = spawn_trimmer(store, keys(), min_age, rx, Some(hook));
    (tx, t, seen)
}

fn wake(tx: &mpsc::Sender<TrimSnapshot>) {
    tx.send(TrimSnapshot {
        head: head(3, Some(2)),
        manifest: None,
    })
    .unwrap();
}

/// Wait until `seen` holds `n` entries.
fn wait_for(seen: &Mutex<Vec<u64>>, n: usize) {
    let start = Instant::now();
    while seen.lock().unwrap().len() < n {
        assert!(start.elapsed() < Duration::from_secs(5), "never trimmed");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// The trimmer acts once manifest 2 is old enough by the bucket's clock, on the next wake
/// (a seal), and exits when every sender is gone.
#[test]
fn trimmer_waits_for_the_bucket_clock_and_exits_on_disconnect() {
    let k = keys();
    let (mem, _) = three_generations();
    let mem: Arc<MemStore> = Arc::new(mem);
    let min_age = Duration::from_secs(60);
    let (tx, t, seen) = trimmer(mem.clone(), min_age);
    // The pass at start and one on a wake: every manifest is young.
    wake(&tx);
    std::thread::sleep(Duration::from_millis(50));
    assert!(seen.lock().unwrap().is_empty(), "trimmed a young manifest");
    assert!(mem.get(&k.log(1, 1)).unwrap().is_some());
    mem.advance_clock(min_age);
    wake(&tx);
    wait_for(&seen, 1);
    assert_eq!(*seen.lock().unwrap(), vec![5]);
    assert!(mem.get(&k.log(1, 1)).unwrap().is_none());
    // Nothing new: the same manifest is not trimmed against again.
    wake(&tx);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(*seen.lock().unwrap(), vec![5]);
    drop(tx);
    t.join().unwrap();
}

/// The pass at start trims without any wake; with every sender already gone it still runs,
/// then the thread exits.
#[test]
fn trimmer_trims_at_start() {
    let k = keys();
    let (mem, _) = three_generations();
    let mem: Arc<MemStore> = Arc::new(mem);
    let min_age = Duration::from_secs(60);
    mem.advance_clock(min_age);
    let (tx, t, seen) = trimmer(mem.clone(), min_age);
    drop(tx);
    t.join().unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![5]);
    assert!(mem.get(&k.log(1, 1)).unwrap().is_none());
}

/// No wake needed once a manifest comes of age: the trimmer waits for it itself. (MemStore
/// ages with the real clock too.)
#[test]
fn trimmer_wakes_when_the_next_manifest_comes_of_age() {
    let (mem, _) = three_generations();
    let mem: Arc<MemStore> = Arc::new(mem);
    let start = Instant::now();
    let min_age = Duration::from_millis(300);
    let (tx, t, seen) = trimmer(mem.clone(), min_age);
    wait_for(&seen, 1);
    assert!(start.elapsed() >= min_age, "{:?}", start.elapsed());
    assert_eq!(*seen.lock().unwrap(), vec![5]);
    drop(tx);
    t.join().unwrap();
}

/// A store whose LISTs fail while `fail` is set.
struct FailingList {
    inner: MemStore,
    fail: AtomicBool,
}

impl ObjectStore for FailingList {
    fn put(&self, key: &str, body: &[u8]) -> std::result::Result<String, StoreError> {
        self.inner.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> std::result::Result<CondPut, StoreError> {
        self.inner.put_if_absent(key, body)
    }
    fn put_if_match(
        &self,
        key: &str,
        body: &[u8],
        etag: &str,
    ) -> std::result::Result<CondPut, StoreError> {
        self.inner.put_if_match(key, body, etag)
    }
    fn get(&self, key: &str) -> std::result::Result<Option<Object>, StoreError> {
        self.inner.get(key)
    }
    fn stat(&self, key: &str) -> std::result::Result<Option<ObjectStat>, StoreError> {
        self.inner.stat(key)
    }
    fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StoreError> {
        if self.fail.load(SeqCst) {
            return Err(StoreError::Permanent("injected".into()));
        }
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
        self.inner.delete(key)
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

/// A failed pass neither panics the thread nor stops it: the next wake retries.
#[test]
fn trimmer_retries_a_failed_pass_on_the_next_wake() {
    let k = keys();
    let (mem, _) = three_generations();
    let min_age = Duration::from_secs(60);
    mem.advance_clock(min_age);
    let store = Arc::new(FailingList {
        inner: mem,
        fail: AtomicBool::new(true),
    });
    let (tx, t, seen) = trimmer(store.clone(), min_age);
    wake(&tx);
    std::thread::sleep(Duration::from_millis(50));
    assert!(seen.lock().unwrap().is_empty());
    assert!(store.get(&k.log(1, 1)).unwrap().is_some());
    store.fail.store(false, SeqCst);
    wake(&tx);
    wait_for(&seen, 1);
    assert_eq!(*seen.lock().unwrap(), vec![5]);
    assert!(!t.is_finished());
    drop(tx);
    t.join().unwrap();
}

fn head_v2(generation: u64, manifest: Option<u64>) -> Head {
    Head {
        journal_mode: JournalMode::Mvcc,
        format: 2,
        ..head(generation, manifest)
    }
}

/// A format-2 head (`"format":2`, `"journal_mode":"mvcc"`) decodes in the trimmer's and GC's
/// paths: `gc_once` reads it from the bucket, and `trim_once` trims exactly as it does for a
/// format-1 head.
#[test]
fn trim_reads_format_2_head() {
    let k = keys();
    let (mem, m2) = three_generations();
    mem.put(&k.head(), &head_v2(3, Some(2)).encode()).unwrap();
    let body = String::from_utf8(mem.get(&k.head()).unwrap().unwrap().body).unwrap();
    assert!(body.contains("\"format\":2") && body.contains("\"journal_mode\":\"mvcc\""));
    let decoded = Head::decode(body.as_bytes()).unwrap();
    assert_eq!(decoded, head_v2(3, Some(2)));
    // A kind-3 record of the current generation and of an old one.
    let kind3 = |tag: u8| {
        crate::format::encode_record(&crate::format::LogRecord::Extents {
            file: crate::format::LogFile::Log,
            writer_id: "w".into(),
            syncs: vec![vec![crate::format::Op::Write {
                off: 0,
                bytes: vec![tag; 8],
            }]],
        })
    };
    mem.put(&k.log(1, 3), &kind3(1)).unwrap();
    mem.put(&k.log(3, 2), &kind3(3)).unwrap();

    // GC against an old enough manifest 2, through the head in the bucket.
    mem.advance_clock(Duration::from_secs(3600));
    let pass = gc_once(&mem, &k, Duration::from_secs(60), None).unwrap();
    assert_eq!(pass.outcome, GcOutcome::Trimmed, "{pass:?}");
    assert_eq!(pass.generation, 3);
    assert_eq!(pass.trimmed_against, Some(2));
    // Logs 1/* (kind 1 and kind 3) went; the current generation's records stay.
    assert!(mem.list(&k.log_prefix(1)).unwrap().is_empty());
    assert_eq!(
        mem.list(&k.log_prefix(3)).unwrap(),
        [k.log(3, 1), k.log(3, 2)]
    );
    assert!(mem.get(&k.manifest(2)).unwrap().is_some());
    assert!(mem.get(&k.manifest(1)).unwrap().is_none());
    // And trim_once with the v2 head directly: nothing more to delete.
    assert_eq!(
        trim_once(&mem, &k, &head_v2(3, Some(2)), Some(&m2)).unwrap(),
        0
    );
}
