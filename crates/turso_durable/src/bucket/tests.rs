use super::*;
use crate::config::Event;
use crate::format::decode_record;
use crate::store::{FaultStore, MemStore};
use std::time::Duration;

const PREFIX: &str = "db";

fn fast() -> RetryPolicy {
    RetryPolicy {
        deadline: Duration::from_secs(2),
        first_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(4),
    }
}

fn keys() -> Keys {
    Keys::new(PREFIX)
}

struct Fixture {
    mem: Arc<MemStore>,
    fault: Arc<FaultStore>,
}

impl Fixture {
    fn new() -> Self {
        let mem = Arc::new(MemStore::new());
        let fault = Arc::new(FaultStore::new(mem.clone()));
        Fixture { mem, fault }
    }
    fn store(&self) -> Arc<dyn ObjectStore> {
        self.fault.clone()
    }
    fn create(&self, retry: RetryPolicy, concurrency: usize) -> Bucket {
        Bucket::create(self.store(), keys(), retry, concurrency, "w1".into()).unwrap()
    }
    fn attach(&self, writer_id: &str) -> Result<Bucket> {
        Bucket::attach(self.store(), keys(), fast(), 4, writer_id)
    }
    fn body(&self, key: &str) -> Option<Vec<u8>> {
        self.mem.get(key).unwrap().map(|o| o.body)
    }
    fn record(&self, gen: u64, seq: u64) -> Option<LogRecord> {
        self.body(&keys().log(gen, seq))
            .map(|b| decode_record(&b).unwrap())
    }
    fn stored_head(&self) -> Head {
        Head::decode(&self.body(&keys().head()).unwrap()).unwrap()
    }
    fn log_keys(&self, gen: u64) -> Vec<String> {
        self.mem.list(&keys().log_prefix(gen)).unwrap()
    }
}

/// One sync of one write whose bytes are `tag`.
fn syncs(tag: u8) -> Vec<Vec<Op>> {
    vec![vec![Op::Write {
        off: tag as u64,
        bytes: vec![tag; 4],
    }]]
}

fn ours(tag: u8) -> LogRecord {
    LogRecord::Extents {
        writer_id: "w1".into(),
        syncs: syncs(tag),
    }
}

fn pages(n: u32) -> Vec<(u32, Vec<u8>)> {
    (1..=n).map(|p| (p, vec![p as u8; 16])).collect()
}

#[test]
fn create_then_attach_round_trip() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    let want = Head {
        format: 1,
        generation: 1,
        manifest: None,
        writer_id: "w1".into(),
    };
    assert_eq!(b.head(), &want);
    assert_eq!(f.stored_head(), want);
    assert_eq!((b.next_seq(), b.manifest()), (1, None));
    b.append_log(syncs(1)).unwrap();
    b.append_log(syncs(2)).unwrap();

    let a = f.attach("w1").unwrap();
    assert_eq!((a.head(), a.next_seq(), a.manifest()), (&want, 3, None));

    b.seal_generation(pages(2), 4096, 2).unwrap();
    let mut a = f.attach("w1").unwrap();
    assert_eq!(a.head().generation, 2);
    assert_eq!(a.head().manifest, Some(1));
    assert_eq!(a.manifest(), b.manifest());
    assert_eq!(a.next_seq(), 1);
    // The attached bucket carries the head's ETag: it can seal.
    assert_eq!(a.seal_generation(vec![], 4096, 2), Ok(3));

    assert!(matches!(f.attach("w2"), Err(Error::Fenced(_))));
    let empty = Fixture::new();
    assert_eq!(
        empty.attach("w1").err(),
        Some(Error::NotFound("head".into()))
    );
}

#[test]
fn attach_ignores_unparseable_log_keys_and_needs_manifest() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.append_log(syncs(1)).unwrap();
    f.mem
        .put(&format!("{}junk", keys().log_prefix(1)), b"x")
        .unwrap();
    assert_eq!(f.attach("w1").unwrap().next_seq(), 2);

    b.seal_generation(pages(1), 4096, 1).unwrap();
    f.mem.delete(&keys().manifest(1)).unwrap();
    assert_eq!(
        f.attach("w1").err(),
        Some(Error::NotFound(keys().manifest(1)))
    );
}

#[test]
fn create_twice_is_config_error() {
    let f = Fixture::new();
    f.create(fast(), 4);
    let again = Bucket::create(f.store(), keys(), fast(), 4, "w1".into());
    assert!(matches!(again, Err(Error::Config(_))), "{:?}", again.err());
    // After a failed attempt the stored head is compared; another writer's is still an error.
    f.fault.fail_next_puts(1);
    let again = Bucket::create(f.store(), keys(), fast(), 4, "w2".into());
    assert!(matches!(again, Err(Error::Config(_))), "{:?}", again.err());
}

#[test]
fn create_accepts_its_own_ambiguous_head() {
    let f = Fixture::new();
    f.fault.store_then_fail_next_puts(1);
    let mut b = f.create(fast(), 4);
    assert_eq!(f.stored_head().writer_id, "w1");
    // The adopted ETag is the stored one: a seal's conditional head write succeeds.
    assert_eq!(b.seal_generation(vec![], 4096, 0), Ok(2));
}

#[test]
fn append_log_is_linear_and_conditional() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    let before = f.fault.events().len();
    for tag in 1..=3u8 {
        assert_eq!(
            b.append_log(syncs(tag)),
            Ok(LogPos {
                generation: 1,
                seq: tag as u64
            })
        );
    }
    let k = keys();
    assert_eq!(
        f.fault.events()[before..],
        [1, 2, 3].map(|s| format!("put_if_absent {}", k.log(1, s)))
    );
    for tag in 1..=3u8 {
        assert_eq!(f.record(1, tag as u64), Some(ours(tag)));
    }
    assert_eq!(b.next_seq(), 4);
    assert_eq!(b.counters().log_puts.load(Relaxed), 3);
}

#[test]
fn ambiguous_put_is_accepted_on_retry() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    f.fault.store_then_fail_next_puts(1);
    assert_eq!(b.append_log(syncs(1)).map(|p| p.seq), Ok(1));
    assert_eq!(f.log_keys(1), vec![keys().log(1, 1)]);
    assert_eq!(f.record(1, 1), Some(ours(1)));
    assert!(b.counters().retries.load(Relaxed) >= 1);
    assert_eq!(b.append_log(syncs(2)).map(|p| p.seq), Ok(2));
}

#[test]
fn foreign_object_fences_and_stops() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    let foreign = encode_record(&LogRecord::Fence {
        writer_id: "w2".into(),
    });
    f.mem.put(&keys().log(1, 2), &foreign).unwrap();
    assert_eq!(b.append_log(syncs(1)).map(|p| p.seq), Ok(1));
    assert!(matches!(b.append_log(syncs(2)), Err(Error::Fenced(_))));

    let n = f.fault.events().len();
    assert!(matches!(b.append_log(syncs(3)), Err(Error::Fenced(_))));
    assert!(matches!(
        b.seal_generation(pages(1), 4096, 1),
        Err(Error::Fenced(_))
    ));
    assert_eq!(f.fault.events().len(), n, "a fenced bucket writes nothing");
    assert_eq!(f.body(&keys().log(1, 2)), Some(foreign));
}

fn empty(writer_id: &str) -> LogRecord {
    LogRecord::Extents {
        writer_id: writer_id.into(),
        syncs: vec![],
    }
}

/// No object of a seal of generation `gen` (pages or manifest) exists.
fn no_seal_objects(f: &Fixture, gen: u64) {
    let k = keys();
    let pages: Vec<String> = f.mem.list(&k.page_root()).unwrap();
    let pages: Vec<&String> = pages
        .iter()
        .filter(|p| matches!(k.parse_page(p), Some((g, _)) if g == gen))
        .collect();
    assert!(pages.is_empty(), "{pages:?}");
    assert_eq!(f.body(&k.manifest(gen)), None);
}

/// B1: another writer's record at this writer's next seq (a claimer's claim record) fences a
/// seal that has nothing to ship before any page or manifest is written.
#[test]
fn seal_start_record_collision_fences_before_any_upload() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.append_log(syncs(1)).unwrap();
    let head = f.stored_head();
    let claim = encode_record(&empty("w2"));
    f.mem.put(&keys().log(1, 2), &claim).unwrap();
    let before = f.fault.events().len();
    assert!(matches!(
        b.seal_generation(pages(4), 4096, 4),
        Err(Error::Fenced(_))
    ));
    assert_eq!(
        f.fault.events()[before..],
        [
            format!("put_if_absent {}", keys().log(1, 2)),
            format!("get {}", keys().log(1, 2))
        ]
    );
    no_seal_objects(&f, 1);
    assert_eq!(f.stored_head(), head);
    assert_eq!(f.body(&keys().log(1, 2)), Some(claim));
    assert!(matches!(b.append_log(syncs(2)), Err(Error::Fenced(_))));
}

/// B1 once `log/G` is trimmed: the record lands, but the head moved (another writer sealed
/// G, or claimed it), so the seal is fenced before any page or manifest is written.
#[test]
fn seal_start_with_moved_head_fences_before_any_upload() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    let other = Head {
        format: 1,
        generation: 2,
        manifest: Some(1),
        writer_id: "w2".into(),
    };
    f.mem.put(&keys().head(), &other.encode()).unwrap();
    assert!(matches!(
        b.seal_generation(pages(4), 4096, 4),
        Err(Error::Fenced(_))
    ));
    no_seal_objects(&f, 1);
    assert_eq!(f.stored_head(), other);
    assert_eq!(f.record(1, 1), Some(empty("w1")));
    assert!(matches!(b.append_log(syncs(1)), Err(Error::Fenced(_))));
}

/// A head PUT of ours that lands late (in flight at a crash, after the restarted writer acked
/// more records into G) is not another writer's: the next seal passes the head check,
/// re-uploads G's pages and manifest, and adopts that head.
#[test]
fn seal_start_accepts_our_own_late_head() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.append_log(syncs(1)).unwrap();
    let late = Head {
        format: 1,
        generation: 2,
        manifest: Some(1),
        writer_id: "w1".into(),
    };
    f.mem.put(&keys().head(), &late.encode()).unwrap();
    assert_eq!(b.seal_generation(pages(2), 4096, 2), Ok(2));
    assert_eq!(f.body(&keys().page(1, 2)), Some(vec![2u8; 16]));
    assert!(f.body(&keys().manifest(1)).is_some());
    assert_eq!(b.head(), &late);
    assert_eq!(b.seal_generation(vec![], 4096, 2), Ok(3));
}

/// The idle self-check: an append after `idle_threshold` without a confirmed write GETs the
/// head first; one before it does not.
#[test]
fn append_after_idle_threshold_checks_the_head_first() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.set_idle_threshold(Duration::from_millis(100));
    let k = keys();

    let before = f.fault.events().len();
    b.append_log(syncs(1)).unwrap();
    assert_eq!(
        f.fault.events()[before..],
        [format!("put_if_absent {}", k.log(1, 1))],
        "inside the threshold: no head GET"
    );

    std::thread::sleep(Duration::from_millis(150));
    let before = f.fault.events().len();
    b.append_log(syncs(2)).unwrap();
    assert_eq!(
        f.fault.events()[before..],
        [
            format!("get {}", k.head()),
            format!("put_if_absent {}", k.log(1, 2))
        ],
        "idle past the threshold: the head is checked first"
    );

    // The append just confirmed a write: the next one goes straight to the log again.
    let before = f.fault.events().len();
    b.append_log(syncs(3)).unwrap();
    assert_eq!(
        f.fault.events()[before..],
        [format!("put_if_absent {}", k.log(1, 3))]
    );
}

/// Idle past the threshold with the head moved by another writer (it claimed and sealed
/// this generation, and its trimmer deleted the log): the append is fenced and writes
/// nothing, even though its seq is free.
#[test]
fn idle_append_with_moved_head_is_fenced_and_writes_nothing() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.set_idle_threshold(Duration::ZERO);
    b.append_log(syncs(1)).unwrap();
    let other = Head {
        format: 1,
        generation: 2,
        manifest: Some(1),
        writer_id: "w2".into(),
    };
    f.mem.put(&keys().head(), &other.encode()).unwrap();
    f.mem.delete(&keys().log(1, 1)).unwrap();
    let before = f.fault.events().len();
    assert!(matches!(b.append_log(syncs(2)), Err(Error::Fenced(_))));
    assert_eq!(
        f.fault.events()[before..],
        [format!("get {}", keys().head())]
    );
    assert!(f.log_keys(1).is_empty());
    let n = f.fault.events().len();
    assert!(matches!(b.append_log(syncs(3)), Err(Error::Fenced(_))));
    assert_eq!(f.fault.events().len(), n, "fenced for good");
}

/// Idle past the threshold, single writer: the head is unchanged (or is our own late head
/// for this generation's seal), so the append proceeds.
#[test]
fn idle_append_with_our_head_proceeds() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.set_idle_threshold(Duration::ZERO);
    assert_eq!(b.append_log(syncs(1)).map(|p| p.seq), Ok(1));
    assert_eq!(b.seal_generation(pages(1), 4096, 1), Ok(2));
    assert_eq!(
        b.append_log(syncs(2)).map(|p| (p.generation, p.seq)),
        Ok((2, 1))
    );
    let late = Head {
        format: 1,
        generation: 3,
        manifest: Some(2),
        writer_id: "w1".into(),
    };
    f.mem.put(&keys().head(), &late.encode()).unwrap();
    assert_eq!(
        b.append_log(syncs(3)).map(|p| (p.generation, p.seq)),
        Ok((2, 2))
    );
    assert_eq!(f.record(2, 2), Some(ours(3)));
}

/// An ambiguous seal-start record fails the seal before any upload and stays in doubt; the
/// next seal settles it at the same seq, then puts a fresh seal-start record after it.
#[test]
fn ambiguous_seal_start_record_is_settled_then_sealed() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    };
    let f = Fixture::new();
    let mut b = f.create(short, 4);
    b.append_log(syncs(1)).unwrap();
    f.fault.only_keys_containing("/log/");
    f.fault.store_then_fail_next_puts(1);
    f.fault.fail_puts_after(0);
    assert!(matches!(
        b.seal_generation(pages(2), 4096, 2),
        Err(Error::Timeout(_))
    ));
    assert_eq!(f.record(1, 2), Some(empty("w1")), "landed, reply lost");
    no_seal_objects(&f, 1);
    assert_eq!(b.next_seq(), 2, "in doubt, not advanced");

    f.fault.fail_puts_after(u64::MAX);
    let before = f.fault.events().len();
    assert_eq!(b.seal_generation(pages(2), 4096, 2), Ok(2));
    let ev = f.fault.events()[before..].to_vec();
    assert_eq!(
        ev[..4],
        [
            format!("put_if_absent {}", keys().log(1, 2)),
            format!("get {}", keys().log(1, 2)),
            format!("put_if_absent {}", keys().log(1, 3)),
            format!("get {}", keys().head()),
        ],
        "{ev:?}"
    );
    assert_eq!(f.record(1, 3), Some(empty("w1")));
    assert_eq!(
        f.log_keys(1),
        [keys().log(1, 1), keys().log(1, 2), keys().log(1, 3)]
    );
    assert_eq!(f.body(&keys().page(1, 2)), Some(vec![2u8; 16]));
}

#[test]
fn deadline_failure_leaves_record_in_doubt_then_settles() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(200),
        ..fast()
    };

    // Nothing landed: the settle re-puts A at seq 1, then B goes to seq 2.
    let f = Fixture::new();
    let mut b = f.create(short.clone(), 4);
    f.fault.fail_next_puts(1000);
    assert!(matches!(b.append_log(syncs(b'A')), Err(Error::Timeout(_))));
    assert_eq!(f.record(1, 1), None);
    f.fault.fail_next_puts(0);
    assert_eq!(b.append_log(syncs(b'B')).map(|p| p.seq), Ok(2));
    assert_eq!(f.record(1, 1), Some(ours(b'A')));
    assert_eq!(f.record(1, 2), Some(ours(b'B')));

    // A landed but every answer was an error: the settle finds it and B still goes to seq 2.
    let f = Fixture::new();
    let mut b = f.create(short, 4);
    f.fault.store_then_fail_next_puts(1);
    f.fault.fail_puts_after(0);
    assert!(matches!(b.append_log(syncs(b'A')), Err(Error::Timeout(_))));
    assert_eq!(f.record(1, 1), Some(ours(b'A')));
    f.fault.fail_puts_after(u64::MAX);
    assert_eq!(b.append_log(syncs(b'B')).map(|p| p.seq), Ok(2));
    assert_eq!(f.log_keys(1), vec![keys().log(1, 1), keys().log(1, 2)]);
    assert_eq!(f.record(1, 2), Some(ours(b'B')));
}

#[test]
fn settle_failure_keeps_record_in_doubt() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    };
    let f = Fixture::new();
    let mut b = f.create(short, 4);
    f.fault.fail_next_puts(100_000);
    assert!(matches!(b.append_log(syncs(b'A')), Err(Error::Timeout(_))));
    // The settle fails too: B is not written anywhere and A is still owed seq 1.
    assert!(matches!(b.append_log(syncs(b'B')), Err(Error::Timeout(_))));
    assert!(matches!(
        b.seal_generation(pages(1), 4096, 1),
        Err(Error::Timeout(_))
    ));
    f.fault.fail_next_puts(0);
    assert_eq!(b.append_log(syncs(b'C')).map(|p| p.seq), Ok(2));
    assert_eq!(f.record(1, 1), Some(ours(b'A')));
    assert_eq!(f.record(1, 2), Some(ours(b'C')));
    assert_eq!(f.log_keys(1).len(), 2);
}

#[test]
fn permanent_error_is_upload_failed() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    f.fault.fail_next_puts_permanently(1);
    assert!(matches!(
        b.append_log(syncs(1)),
        Err(Error::UploadFailed(_))
    ));
    assert_eq!(b.append_log(syncs(2)).map(|p| p.seq), Ok(2));
    assert_eq!(f.record(1, 1), Some(ours(1)));

    f.fault.only_keys_containing("/pages/");
    f.fault.fail_next_puts_permanently(1);
    assert!(matches!(
        b.seal_generation(pages(1), 4096, 1),
        Err(Error::UploadFailed(_))
    ));
}

#[test]
fn seal_uploads_pages_manifest_head_in_order() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.append_log(syncs(1)).unwrap();
    let before = f.fault.events().len();
    assert_eq!(b.seal_generation(pages(8), 4096, 8), Ok(2));

    let ev = f.fault.events()[before..].to_vec();
    let k = keys();
    let is_page = |e: &String| e.starts_with(&format!("put {PREFIX}/pages/"));
    let manifest_at = ev
        .iter()
        .position(|e| *e == format!("put {}", k.manifest(1)))
        .unwrap();
    let head_at = ev
        .iter()
        .position(|e| *e == format!("put_if_match {}", k.head()))
        .unwrap();
    assert_eq!(ev.iter().filter(|e| is_page(e)).count(), 8);
    // The seal-start gate comes first: an empty record at the next seq, then the head check.
    assert_eq!(
        ev[..2],
        [
            format!("put_if_absent {}", k.log(1, 2)),
            format!("get {}", k.head())
        ],
        "{ev:?}"
    );
    assert_eq!(
        f.record(1, 2),
        Some(LogRecord::Extents {
            writer_id: "w1".into(),
            syncs: vec![]
        })
    );
    assert!(ev[2..manifest_at].iter().all(is_page), "{ev:?}");
    assert!(manifest_at < head_at, "{ev:?}");
    for p in 1..=8u32 {
        assert_eq!(f.body(&k.page(1, p)), Some(vec![p as u8; 16]));
    }

    let head = f.stored_head();
    assert_eq!((head.generation, head.manifest), (2, Some(1)));
    assert_eq!(b.head(), &head);
    let m = Manifest::decode(&f.body(&k.manifest(1)).unwrap()).unwrap();
    assert_eq!((m.page_size, m.page_count), (4096, 8));
    assert_eq!(m.pages, (1..=8).map(|p| (p, 1)).collect());
    assert_eq!(b.manifest(), Some(&m));
    assert_eq!(b.counters().page_puts.load(Relaxed), 8);
    assert_eq!(b.counters().seals.load(Relaxed), 1);

    assert_eq!(
        b.append_log(syncs(2)),
        Ok(LogPos {
            generation: 2,
            seq: 1
        })
    );
    assert_eq!(f.record(2, 1), Some(ours(2)));

    // The next seal keeps clean pages at their old generation and drops pages past the end.
    assert_eq!(b.seal_generation(vec![(2, vec![9; 16])], 4096, 6), Ok(3));
    let m = b.manifest().unwrap();
    assert_eq!(m.page_count, 6);
    assert_eq!(
        m.pages,
        [(1, 1), (2, 2), (3, 1), (4, 1), (5, 1), (6, 1)].into()
    );
    assert_eq!(f.stored_head().manifest, Some(2));
}

#[test]
fn seal_settles_in_doubt_record_first() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    };
    let f = Fixture::new();
    let mut b = f.create(short, 4);
    f.fault.fail_next_puts(100_000);
    assert!(b.append_log(syncs(b'A')).is_err());
    f.fault.fail_next_puts(0);
    let before = f.fault.events().len();
    assert_eq!(b.seal_generation(pages(1), 4096, 1), Ok(2));
    assert_eq!(
        f.fault.events()[before],
        format!("put_if_absent {}", keys().log(1, 1))
    );
    assert_eq!(f.record(1, 1), Some(ours(b'A')));
}

#[test]
fn seal_respects_concurrency() {
    for (concurrency, lo, hi) in [(16, 100, 400), (1, 1600, 4000)] {
        let f = Fixture::new();
        let mut b = f.create(fast(), concurrency);
        f.fault.only_keys_containing("/pages/");
        f.fault.delay_puts(Duration::from_millis(50));
        let t = Instant::now();
        b.seal_generation(pages(32), 4096, 32).unwrap();
        let ms = t.elapsed().as_millis();
        assert!(
            (lo..hi).contains(&ms),
            "concurrency {concurrency}: {ms} ms not in {lo}..{hi}"
        );
    }
}

#[test]
fn seal_failure_keeps_generation() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(150),
        ..fast()
    };
    let f = Fixture::new();
    let mut b = f.create(short, 4);
    let head_before = f.stored_head();
    f.fault.only_keys_containing("/pages/");
    f.fault.fail_puts_after(0);
    let t = Instant::now();
    assert!(matches!(
        b.seal_generation(pages(8), 4096, 8),
        Err(Error::Timeout(_))
    ));
    assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
    assert_eq!(f.stored_head(), head_before);
    assert_eq!(b.head(), &head_before);
    assert_eq!(f.body(&keys().manifest(1)), None);
    assert_eq!(b.manifest(), None);
    assert_eq!(b.counters().seals.load(Relaxed), 0);
    // The failed seal's seal-start record holds seq 1.
    assert_eq!(f.log_keys(1), [keys().log(1, 1)]);
    assert_eq!(
        b.append_log(syncs(1)),
        Ok(LogPos {
            generation: 1,
            seq: 2
        })
    );

    // Pages of the unpublished generation may be overwritten by the next attempt.
    f.fault.fail_puts_after(u64::MAX);
    assert_eq!(b.seal_generation(pages(8), 4096, 8), Ok(2));
}

#[test]
fn seal_deadline_counts_from_the_start_of_the_call() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(300),
        ..fast()
    };
    let f = Fixture::new();
    let mut b = f.create(short, 1);
    // Every put takes 50 ms; the 8 pages (one at a time) use 400 ms, past the 300 ms
    // deadline, then the manifest put fails transiently. One deadline for the whole call
    // allows the manifest a single attempt (~450 ms in all); a fresh 300 ms deadline per
    // object would keep retrying it (~750 ms).
    f.fault.delay_puts(Duration::from_millis(50));
    f.fault.fail_puts_after(8);
    let t = Instant::now();
    let r = b.seal_generation(pages(8), 4096, 8);
    assert!(matches!(r, Err(Error::Timeout(_))), "{r:?}");
    let el = t.elapsed();
    assert!(el < Duration::from_millis(620), "{el:?}");
    assert_eq!(b.head().generation, 1);
    assert_eq!(f.body(&keys().manifest(1)), None);
}

#[test]
fn ambiguous_head_write_is_adopted() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    f.fault.only_keys_containing("/head");
    f.fault.store_then_fail_next_puts(1);
    assert_eq!(b.seal_generation(pages(1), 4096, 1), Ok(2));
    assert_eq!(f.stored_head().generation, 2);
    // The adopted ETag is current: the following seal's conditional write succeeds.
    assert_eq!(b.seal_generation(vec![], 4096, 1), Ok(3));
}

#[test]
fn head_write_in_doubt_blocks_appends_until_resolved() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    };

    // The head write landed but every reply was an error: generation 2 is published, so
    // nothing may go to log/1 any more. The next seal adopts the stored head.
    let f = Fixture::new();
    let mut b = f.create(short.clone(), 4);
    f.fault.only_keys_containing("/head");
    f.fault.store_then_fail_next_puts(1);
    f.fault.fail_puts_after(0);
    assert!(matches!(
        b.seal_generation(pages(1), 4096, 1),
        Err(Error::Timeout(_))
    ));
    assert_eq!(f.stored_head().generation, 2);
    assert_eq!(b.head().generation, 1);
    assert!(matches!(
        b.append_log(syncs(1)),
        Err(Error::UploadFailed(_))
    ));
    assert_eq!(
        f.log_keys(1),
        [keys().log(1, 1)],
        "no record after the seal-start record of a published seal"
    );
    f.fault.fail_puts_after(u64::MAX);
    assert_eq!(b.seal_generation(pages(1), 4096, 1), Ok(2));
    assert_eq!(
        b.append_log(syncs(1)),
        Ok(LogPos {
            generation: 2,
            seq: 1
        })
    );
    assert_eq!(b.seal_generation(vec![], 4096, 1), Ok(3));

    // The head write never landed: the next append checks, then carries on in generation 1,
    // after the failed seal's seal-start record.
    let f = Fixture::new();
    let mut b = f.create(short, 4);
    f.fault.only_keys_containing("/head");
    f.fault.fail_next_puts(100_000);
    assert!(b.seal_generation(pages(1), 4096, 1).is_err());
    f.fault.fail_next_puts(0);
    assert_eq!(
        b.append_log(syncs(1)),
        Ok(LogPos {
            generation: 1,
            seq: 2
        })
    );
    assert_eq!(b.seal_generation(pages(1), 4096, 1), Ok(2));

    // Someone else's head: fenced.
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    f.fault.only_keys_containing("/head");
    f.fault.fail_next_puts_permanently(1);
    assert!(matches!(
        b.seal_generation(pages(1), 4096, 1),
        Err(Error::UploadFailed(_))
    ));
    let other = Head {
        format: 1,
        generation: 1,
        manifest: None,
        writer_id: "w2".into(),
    };
    f.mem.put(&keys().head(), &other.encode()).unwrap();
    assert!(matches!(b.append_log(syncs(1)), Err(Error::Fenced(_))));
    assert_eq!(
        f.log_keys(1),
        [keys().log(1, 1)],
        "only the seal-start record"
    );
}

#[test]
fn reseal_after_uncertain_head_write_adopts_without_reupload() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    };
    let k = keys();

    // The head write landed, every reply was an error: generation 2 is published and
    // references pages/1/* and manifest/1 as they are now.
    let f = Fixture::new();
    let mut b = f.create(short.clone(), 4);
    let seen: Arc<Mutex<Vec<Event>>> = Arc::default();
    {
        let seen = seen.clone();
        b.set_event_hook(Some(Arc::new(move |e: &Event| {
            seen.lock().unwrap().push(e.clone())
        })));
    }
    f.fault.only_keys_containing("/head");
    f.fault.store_then_fail_next_puts(1);
    f.fault.fail_puts_after(0);
    assert!(matches!(
        b.seal_generation(pages(1), 4096, 1),
        Err(Error::Timeout(_))
    ));
    let published_page = f.body(&k.page(1, 1)).unwrap();
    let published_manifest = f.body(&k.manifest(1)).unwrap();
    f.fault.fail_puts_after(u64::MAX);
    seen.lock().unwrap().clear();

    // The retried seal is given a different page set; it must not touch the published objects.
    let before = f.fault.events().len();
    let other_pages = vec![(1, vec![7u8; 16]), (2, vec![8u8; 16])];
    assert_eq!(b.seal_generation(other_pages, 4096, 2), Ok(2));
    let ev = f.fault.events()[before..].to_vec();
    assert_eq!(
        ev,
        vec![format!("get {}", k.head())],
        "adopted without uploads"
    );
    assert_eq!(f.body(&k.page(1, 1)), Some(published_page));
    assert_eq!(f.body(&k.page(1, 2)), None);
    assert_eq!(f.body(&k.manifest(1)), Some(published_manifest.clone()));
    assert_eq!(
        b.manifest(),
        Some(&Manifest::decode(&published_manifest).unwrap())
    );
    assert_eq!(b.head(), &f.stored_head());
    assert_eq!(b.counters().seals.load(Relaxed), 1);
    assert!(matches!(
        seen.lock().unwrap().as_slice(),
        [Event::SealFinished { generation: 1, .. }]
    ));
    assert_eq!(
        b.append_log(syncs(1)),
        Ok(LogPos {
            generation: 2,
            seq: 1
        })
    );
    // The adopted ETag is current.
    assert_eq!(b.seal_generation(vec![], 4096, 2), Ok(3));

    // The head write never landed: the retried seal uploads its (new) pages normally.
    let f = Fixture::new();
    let mut b = f.create(short.clone(), 4);
    f.fault.only_keys_containing("/head");
    f.fault.fail_next_puts(100_000);
    assert!(b.seal_generation(pages(1), 4096, 1).is_err());
    f.fault.fail_next_puts(0);
    let other_pages = vec![(1, vec![7u8; 16]), (2, vec![8u8; 16])];
    let before = f.fault.events().len();
    assert_eq!(b.seal_generation(other_pages, 4096, 2), Ok(2));
    // The head GET finds the ETag unchanged; the seal then starts like any other, with a
    // seal-start record after the failed attempt's.
    assert_eq!(
        f.fault.events()[before..before + 3],
        [
            format!("get {}", k.head()),
            format!("put_if_absent {}", k.log(1, 2)),
            format!("get {}", k.head()),
        ]
    );
    assert_eq!(f.log_keys(1), [k.log(1, 1), k.log(1, 2)]);
    assert_eq!(f.body(&k.page(1, 1)), Some(vec![7u8; 16]));
    assert_eq!(f.body(&k.page(1, 2)), Some(vec![8u8; 16]));
    assert_eq!(b.manifest().unwrap().page_count, 2);

    // Someone else's head: the retried seal is fenced and uploads nothing.
    let f = Fixture::new();
    let mut b = f.create(short, 4);
    f.fault.only_keys_containing("/head");
    f.fault.fail_next_puts(100_000);
    assert!(b.seal_generation(pages(1), 4096, 1).is_err());
    f.fault.fail_next_puts(0);
    let other = Head {
        format: 1,
        generation: 1,
        manifest: None,
        writer_id: "w2".into(),
    };
    f.mem.put(&k.head(), &other.encode()).unwrap();
    let before = f.fault.events().len();
    assert!(matches!(
        b.seal_generation(pages(1), 4096, 1),
        Err(Error::Fenced(_))
    ));
    assert_eq!(f.fault.events()[before..], [format!("get {}", k.head())]);
}

#[test]
fn append_log_has_one_deadline_per_call() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(200),
        ..fast()
    };

    // Every put fails: the settle of the in-doubt record uses up the deadline.
    let f = Fixture::new();
    let mut b = f.create(short.clone(), 4);
    f.fault.fail_next_puts(1_000_000);
    assert!(matches!(b.append_log(syncs(b'A')), Err(Error::Timeout(_))));
    let t = Instant::now();
    assert!(matches!(b.append_log(syncs(b'B')), Err(Error::Timeout(_))));
    assert!(
        t.elapsed() < Duration::from_millis(400),
        "{:?}",
        t.elapsed()
    );

    // Every put takes 150 ms. The settle of A lands (150 ms); B's put then fails. With one
    // deadline for the call B gets one attempt (~300 ms in all); with a fresh 200 ms
    // deadline per step B would be tried twice (~450 ms).
    let f = Fixture::new();
    let mut b = f.create(short, 4);
    f.fault.fail_next_puts(1_000_000);
    assert!(matches!(b.append_log(syncs(b'A')), Err(Error::Timeout(_))));
    f.fault.fail_next_puts(0);
    f.fault.delay_puts(Duration::from_millis(150));
    f.fault.fail_puts_after(1);
    let t = Instant::now();
    assert!(matches!(b.append_log(syncs(b'B')), Err(Error::Timeout(_))));
    let el = t.elapsed();
    assert!(el < Duration::from_millis(390), "{el:?}");
    assert_eq!(f.record(1, 1), Some(ours(b'A')));
    assert_eq!(f.record(1, 2), None);
}

#[test]
fn head_conflict_fences() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    let other = Head {
        format: 1,
        generation: 1,
        manifest: None,
        writer_id: "w2".into(),
    };
    f.mem.put(&keys().head(), &other.encode()).unwrap();
    assert!(matches!(
        b.seal_generation(pages(1), 4096, 1),
        Err(Error::Fenced(_))
    ));
    no_seal_objects(&f, 1);
    assert_eq!(f.stored_head(), other);
    assert_eq!(b.head().generation, 1);
    assert!(matches!(b.append_log(syncs(1)), Err(Error::Fenced(_))));
}

#[test]
fn claim_rewrites_head_with_new_writer_only_if_unchanged() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.append_log(syncs(1)).unwrap();
    let before = f.mem.get(&keys().head()).unwrap().unwrap();
    let old = f.stored_head();

    // An ambiguous first attempt (stored, then an error): the retry finds its own head.
    f.fault.store_then_fail_next_puts(1);
    let etag = claim(&*f.store(), &keys(), &fast(), &before.etag, "w2").unwrap();
    let now = f.mem.get(&keys().head()).unwrap().unwrap();
    assert_eq!(now.etag, etag);
    assert_eq!(
        f.stored_head(),
        Head {
            writer_id: "w2".into(),
            ..old
        }
    );
    // The old writer can no longer attach; the new one can, after the old one's record.
    assert!(matches!(f.attach("w1"), Err(Error::Fenced(_))));
    assert_eq!(f.attach("w2").unwrap().next_seq(), 2);

    // The head moved since `before`: refused, and nothing is written.
    let puts = f.mem.stats().puts;
    let r = claim(&*f.store(), &keys(), &fast(), &before.etag, "w3");
    assert!(
        matches!(&r, Err(Error::Fenced(m)) if m.contains("restore again")),
        "{r:?}"
    );
    assert_eq!(f.mem.stats().puts, puts);
    let r = claim(&*f.store(), &keys(), &fast(), "", "w3");
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
    f.mem.delete(&keys().head()).unwrap();
    let r = claim(&*f.store(), &keys(), &fast(), &etag, "w3");
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
}
