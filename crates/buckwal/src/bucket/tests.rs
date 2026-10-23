use super::*;
use crate::config::Event;
use crate::config::JournalMode;
use crate::format::decode_record;
use crate::format::LogFile;
use crate::store::fault::HoldStore;
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
    /// An MVCC bucket (format 2) as `create_sealed` leaves it: generation 2.
    fn create_mvcc(&self, retry: RetryPolicy, concurrency: usize) -> Bucket {
        let image = crate::create::Image {
            page_size: 4096,
            pages: vec![(1, vec![0u8; 4096])],
        };
        Bucket::create_sealed(self.store(), keys(), retry, concurrency, "w1".into(), image).unwrap()
    }
    fn attach(&self, writer_id: &str) -> Result<Bucket> {
        Bucket::attach(self.store(), keys(), fast(), 4, writer_id, 0)
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
fn op(tag: u8) -> Vec<Op> {
    vec![Op::Write {
        off: tag as u64,
        bytes: vec![tag; 4],
    }]
}

/// A record of that one sync.
fn syncs(tag: u8) -> Vec<Vec<Op>> {
    vec![op(tag)]
}

/// The syncs of our record at `pos`, read straight from `mem`.
fn stored_ops(mem: &MemStore, pos: LogPos) -> Vec<Vec<Op>> {
    let body = mem.get(&keys().log(pos.generation, pos.seq)).unwrap();
    match decode_record(&body.expect("a record at pos").body).unwrap() {
        LogRecord::Extents {
            writer_id, syncs, ..
        } if writer_id == "w1" => syncs,
        r => panic!("not our record: {r:?}"),
    }
}

/// A new database on a `FaultStore` over a `MemStore`, with a 100 ms retry deadline.
fn fault_bucket_with_short_deadline() -> (Arc<MemStore>, Arc<FaultStore>, Bucket) {
    let f = Fixture::new();
    let short = RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    };
    let b = f.create(short, 4);
    (f.mem, f.fault, b)
}

fn ours(tag: u8) -> LogRecord {
    LogRecord::Extents {
        file: LogFile::Wal,
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
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 1,
        manifest: None,
        writer_id: "w1".into(),
        epoch: 1,
        lease_ms: Some(30_000),
        request_timeout_ms: Some(10_000),
        next_generation_floor: None,
    };
    assert_eq!(b.head(), &want);
    assert_eq!(f.stored_head(), want);
    assert_eq!((b.next_seq(), b.manifest()), (1, None));
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    b.append_log(LogFile::Wal, syncs(2)).unwrap();

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
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
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
            b.append_log(LogFile::Wal, syncs(tag))
                .map(|a| a.pos)
                .map_err(|f| f.error),
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
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error)
            .map(|p| p.seq),
        Ok(1)
    );
    assert_eq!(f.log_keys(1), vec![keys().log(1, 1)]);
    assert_eq!(f.record(1, 1), Some(ours(1)));
    assert!(b.counters().retries.load(Relaxed) >= 1);
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(2))
            .map(|a| a.pos)
            .map_err(|f| f.error)
            .map(|p| p.seq),
        Ok(2)
    );
}

#[test]
fn foreign_object_fences_and_stops() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    let foreign = encode_record(&LogRecord::Fence {
        writer_id: "w2".into(),
        epoch: 1,
    });
    f.mem.put(&keys().log(1, 2), &foreign).unwrap();
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error)
            .map(|p| p.seq),
        Ok(1)
    );
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(2))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Fenced(_))
    ));

    let n = f.fault.events().len();
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(3))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Fenced(_))
    ));
    assert!(matches!(
        b.seal_generation(pages(1), 4096, 1),
        Err(Error::Fenced(_))
    ));
    assert_eq!(f.fault.events().len(), n, "a fenced bucket writes nothing");
    assert_eq!(f.body(&keys().log(1, 2)), Some(foreign));
}

fn empty(writer_id: &str) -> LogRecord {
    LogRecord::Extents {
        file: LogFile::Wal,
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
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
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
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(2))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Fenced(_))
    ));
}

/// B1 once `log/G` is trimmed: the record lands, but the head moved (another writer sealed
/// G, or claimed it), so the seal is fenced before any page or manifest is written.
#[test]
fn seal_start_with_moved_head_fences_before_any_upload() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    let other = Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 2,
        manifest: Some(1),
        writer_id: "w2".into(),
        epoch: 0,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
    };
    f.mem.put(&keys().head(), &other.encode()).unwrap();
    assert!(matches!(
        b.seal_generation(pages(4), 4096, 4),
        Err(Error::Fenced(_))
    ));
    no_seal_objects(&f, 1);
    assert_eq!(f.stored_head(), other);
    assert_eq!(f.record(1, 1), Some(empty("w1")));
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Fenced(_))
    ));
}

/// A head PUT of ours that lands late (in flight at a crash, after the restarted writer acked
/// more records into G) is not another writer's: the next seal passes the head check,
/// re-uploads G's pages and manifest, and adopts that head.
#[test]
fn seal_start_accepts_our_own_late_head() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    let late = Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 2,
        manifest: Some(1),
        writer_id: "w1".into(),
        epoch: 1,
        lease_ms: Some(30_000),
        request_timeout_ms: Some(10_000),
        next_generation_floor: None,
    };
    f.mem.put(&keys().head(), &late.encode()).unwrap();
    assert_eq!(b.seal_generation(pages(2), 4096, 2), Ok(2));
    assert_eq!(f.body(&keys().page(1, 2)), Some(vec![2u8; 16]));
    assert!(f.body(&keys().manifest(1)).is_some());
    assert_eq!(b.head(), &late);
    assert_eq!(b.seal_generation(vec![], 4096, 2), Ok(3));
}

/// The lease: an append once `lease` has passed since the last confirmation GETs the head
/// first. The stamp is moved by hand (`age_lease`), so a slow machine cannot turn "within"
/// into "after".
#[test]
fn append_after_the_lease_checks_the_head() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.set_lease(Duration::from_millis(100));
    let k = keys();
    b.age_lease(Duration::from_millis(150), Duration::ZERO);
    let before = f.fault.events().len();
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    assert_eq!(
        f.fault.events()[before..],
        [
            format!("get {}", k.head()),
            format!("put_if_absent {}", k.log(1, 1))
        ],
        "past the lease: the head is checked first"
    );
}

/// Inside the lease no head GET is sent; a confirmed append renews it.
#[test]
fn append_within_the_lease_sends_no_get() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.set_lease(Duration::from_secs(10));
    let k = keys();

    let before = f.fault.events().len();
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    assert_eq!(
        f.fault.events()[before..],
        [format!("put_if_absent {}", k.log(1, 1))],
        "inside the lease: no head GET"
    );

    b.age_lease(Duration::from_secs(9), Duration::from_secs(9));
    let before = f.fault.events().len();
    b.append_log(LogFile::Wal, syncs(2)).unwrap();
    assert_eq!(
        f.fault.events()[before..],
        [format!("put_if_absent {}", k.log(1, 2))],
        "9 s of a 10 s lease on both clocks: no head GET"
    );

    b.age_lease(Duration::from_secs(11), Duration::ZERO);
    let before = f.fault.events().len();
    b.append_log(LogFile::Wal, syncs(3)).unwrap();
    assert_eq!(
        f.fault.events()[before..],
        [
            format!("get {}", k.head()),
            format!("put_if_absent {}", k.log(1, 3))
        ],
        "past the lease: the head is checked first"
    );

    // The append just confirmed a write: the next one goes straight to the log again.
    let before = f.fault.events().len();
    b.append_log(LogFile::Wal, syncs(4)).unwrap();
    assert_eq!(
        f.fault.events()[before..],
        [format!("put_if_absent {}", k.log(1, 4))]
    );
}

/// A writer paused between its lease check and its record's PUT (SIGSTOP, a suspended VM),
/// the PUT delayed 300 ms past a 100 ms lease and a 50 ms retry deadline here. Meanwhile
/// another writer took the head (and could have sealed and trimmed `log/G`, freeing this
/// writer's next seq): a landed record proves nothing then. The head is checked after it,
/// and the writer is fenced instead of acking it.
#[test]
fn a_record_landing_past_the_lease_checks_the_head() {
    let f = Fixture::new();
    let retry = RetryPolicy {
        deadline: Duration::from_millis(50),
        ..fast()
    };
    let mut b = f.create(retry, 4);
    b.set_lease(Duration::from_millis(100));
    let other = Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 1,
        manifest: None,
        writer_id: "w2".into(),
        epoch: 2,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
    };
    f.mem.put(&keys().head(), &other.encode()).unwrap();
    f.fault.only_keys_containing("/log/");
    f.fault.delay_puts(Duration::from_millis(300));
    let before = f.fault.events().len();
    let r = b
        .append_log(LogFile::Wal, syncs(1))
        .map(|a| a.pos)
        .map_err(|f| f.error);
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
    assert_eq!(
        f.fault.events()[before..],
        [
            format!("put_if_absent {}", keys().log(1, 1)),
            format!("get {}", keys().head())
        ],
        "inside the lease at the check, past it when the record landed: the head after it"
    );
    let r = b
        .append_log(LogFile::Wal, syncs(2))
        .map(|a| a.pos)
        .map_err(|f| f.error);
    assert!(r.is_err(), "fenced for good: {r:?}");
}

/// The same late landing with the head still ours: the record counts, the head GET renews
/// the lease, and the next append goes straight to the log.
#[test]
fn a_record_landing_past_the_lease_under_our_head_counts() {
    let f = Fixture::new();
    let retry = RetryPolicy {
        deadline: Duration::from_millis(50),
        ..fast()
    };
    let mut b = f.create(retry, 4);
    b.set_lease(Duration::from_millis(100));
    f.fault.only_keys_containing("/log/");
    f.fault.delay_puts(Duration::from_millis(300));
    let before = f.fault.events().len();
    let pos = b
        .append_log(LogFile::Wal, syncs(1))
        .map(|a| a.pos)
        .map_err(|f| f.error);
    assert_eq!(pos.map(|p| p.seq), Ok(1));
    assert_eq!(
        f.fault.events()[before..],
        [
            format!("put_if_absent {}", keys().log(1, 1)),
            format!("get {}", keys().head())
        ]
    );
    f.fault.delay_puts(Duration::ZERO);
    let before = f.fault.events().len();
    let pos = b
        .append_log(LogFile::Wal, syncs(2))
        .map(|a| a.pos)
        .map_err(|f| f.error);
    assert_eq!(pos.map(|p| p.seq), Ok(2));
    assert_eq!(
        f.fault.events()[before..],
        [format!("put_if_absent {}", keys().log(1, 2))],
        "the head GET renewed the lease"
    );
}

/// A machine that slept: `Instant` stood still but the wall clock moved past the lease. The
/// append checks the head, finds another writer's, and is fenced without writing.
#[test]
fn a_writer_that_slept_checks_the_head() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.set_lease(Duration::from_millis(100));
    let other = Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 1,
        manifest: None,
        writer_id: "w2".into(),
        epoch: 1,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
    };
    f.mem.put(&keys().head(), &other.encode()).unwrap();
    b.age_lease(Duration::ZERO, Duration::from_secs(1));
    let before = f.fault.events().len();
    let r = b
        .append_log(LogFile::Wal, syncs(1))
        .map(|a| a.pos)
        .map_err(|f| f.error);
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
    let ev = f.fault.events()[before..].to_vec();
    assert_eq!(ev, [format!("get {}", keys().head())]);
    assert!(!ev.iter().any(|e| e.starts_with("put_if_absent")), "{ev:?}");
    assert!(f.log_keys(1).is_empty());
}

/// The head GETs, page PUTs and manifest PUTs among `events`, as "get head", "page" and
/// "manifest".
fn seal_events(events: &[String]) -> Vec<&'static str> {
    let k = keys();
    let head = format!("get {}", k.head());
    let (pages, manifest) = (k.page_root(), k.manifest_root());
    events
        .iter()
        .filter_map(|e| {
            let (verb, key) = e.split_once(' ')?;
            if *e == head {
                Some("get head")
            } else if !verb.starts_with("put") {
                None
            } else if key.starts_with(&pages) {
                Some("page")
            } else if key.starts_with(&manifest) {
                Some("manifest")
            } else {
                None
            }
        })
        .collect()
}

/// A seal whose page PUTs outlast the lease re-reads the head between them (renewing it) and
/// resumes the pages not yet uploaded. With 150 ms per page PUT and a 200 ms lease, no round
/// after a head check can send a third PUT: it would go 300 ms after the check.
#[test]
fn seal_renews_the_lease_between_page_puts() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 1);
    b.set_lease(Duration::from_millis(200));
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    f.fault.only_keys_containing("/pages/");
    f.fault.delay_puts(Duration::from_millis(150));
    let before = f.fault.events().len();
    assert_eq!(b.seal_generation(pages(4), 4096, 4), Ok(2));
    let ev = seal_events(&f.fault.events()[before..]);
    let first = ev.iter().position(|e| *e == "page").unwrap();
    let last = ev.iter().rposition(|e| *e == "page").unwrap();
    assert!(
        ev[first..last].contains(&"get head"),
        "a head check between page PUTs: {ev:?}"
    );
    assert_eq!(ev.iter().filter(|e| **e == "page").count(), 4, "{ev:?}");
    assert_eq!(ev.last(), Some(&"manifest"), "{ev:?}");
    for round in ev.split(|e| *e == "get head") {
        assert!(
            round.iter().filter(|e| **e == "page").count() <= 2,
            "at most two 150 ms PUTs inside a 200 ms lease: {ev:?}"
        );
    }
    for p in 1..=4 {
        assert_eq!(f.body(&keys().page(1, p)), Some(vec![p as u8; 16]));
    }
    assert_eq!(f.stored_head().generation, 2);
}

/// The order of a seal's last requests when the lease lapses before its head PUT: with a
/// 200 ms lease and the manifest PUT taking 300 ms, the seal GETs the head after the manifest
/// PUT and only then PUTs the head. It pins request order only: the check does not stop a
/// late head (`Bucket::seal`).
#[test]
fn a_lapsed_lease_before_the_head_put_gets_the_head_first() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 1);
    b.set_lease(Duration::from_millis(200));
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    f.fault.only_keys_containing("/manifest/");
    f.fault.delay_puts(Duration::from_millis(300));
    let before = f.fault.events().len();
    assert_eq!(b.seal_generation(pages(2), 4096, 2), Ok(2));
    let k = keys();
    let ev = f.fault.events()[before..].to_vec();
    let manifest = ev
        .iter()
        .rposition(|e| e.ends_with(&k.manifest(1)) && e.starts_with("put"))
        .unwrap();
    let head_put = ev
        .iter()
        .rposition(|e| e.starts_with("put") && e.ends_with(&format!(" {}", k.head())))
        .unwrap();
    assert!(
        ev[manifest + 1..head_put].contains(&format!("get {}", k.head())),
        "no head GET between the manifest PUT and the head PUT: {:?}",
        &ev[manifest..]
    );
    assert_eq!(f.stored_head().generation, 2);
}

/// A zero lease (tests) would refuse every attempt: instead each worker sends one PUT
/// attempt after each head check, so with one worker every page and the manifest follow
/// their own head GET, and the head PUT follows one too (`check_lease`).
#[test]
fn a_zero_lease_seal_checks_the_head_before_each_put() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 1);
    b.set_lease(Duration::ZERO);
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    let before = f.fault.events().len();
    assert_eq!(b.seal_generation(pages(3), 4096, 3), Ok(2));
    assert_eq!(
        seal_events(&f.fault.events()[before..]),
        [
            "get head", "page", "get head", "page", "get head", "page", "get head", "manifest",
            "get head"
        ]
    );
    assert_eq!(f.stored_head().generation, 2);
}

/// A seal that keeps uploading is not cut off by the retry deadline: with 150 ms page PUTs,
/// one worker and a 300 ms deadline, the seal outlasts its deadline while every round
/// uploads pages, and the manifest's first lapse (pages went out since the last head check)
/// still renews the lease instead of ending the seal with `Timeout`. Under a 200 ms lease and
/// under a zero lease.
#[test]
fn a_long_seal_that_keeps_uploading_outlasts_the_deadline() {
    let mut got = Vec::new();
    for (lease, n) in [(Duration::from_millis(200), 4), (Duration::ZERO, 3)] {
        let f = Fixture::new();
        let short = RetryPolicy {
            deadline: Duration::from_millis(300),
            ..fast()
        };
        let mut b = f.create(short, 1);
        b.set_lease(lease);
        b.append_log(LogFile::Wal, syncs(1)).unwrap();
        f.fault.only_keys_containing("/pages/");
        f.fault.delay_puts(Duration::from_millis(150));
        let before = f.fault.events().len();
        let r = b.seal_generation(pages(n), 4096, n);
        let ev = seal_events(&f.fault.events()[before..]);
        // The manifest is the last PUT before the head's (a zero lease GETs the head before
        // that PUT too).
        let manifest_last = ev.iter().rev().find(|e| **e != "get head") == Some(&"manifest");
        got.push((lease, r, manifest_last, f.stored_head().generation, ev));
    }
    for (lease, r, manifest_last, generation, ev) in &got {
        assert_eq!(
            (r, manifest_last, generation),
            (&Ok(2), &true, &2),
            "lease {lease:?}: {ev:?}; all: {got:?}"
        );
    }
}

/// Head GETs slower than the lease leave it lapsed as soon as it is renewed, so no PUT can
/// go out: once the deadline has passed, the seal ends with `Timeout` instead of reading the
/// head forever, and it uploaded nothing.
#[test]
fn a_seal_whose_head_checks_outlast_the_lease_times_out() {
    let f = Fixture::new();
    let short = RetryPolicy {
        deadline: Duration::from_millis(300),
        ..fast()
    };
    let mut b = f.create(short, 1);
    b.set_lease(Duration::from_millis(50));
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    f.fault.only_keys_containing("/head");
    f.fault.delay_gets(Duration::from_millis(100));
    let before = f.fault.events().len();
    let t = Instant::now();
    let r = b.seal_generation(pages(2), 4096, 2);
    assert!(matches!(r, Err(Error::Timeout(_))), "{r:?}");
    assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
    let ev = seal_events(&f.fault.events()[before..]);
    assert!(!ev.contains(&"page") && !ev.contains(&"manifest"), "{ev:?}");
    assert!(
        ev.iter().filter(|e| **e == "get head").count() >= 2,
        "{ev:?}"
    );
    assert_eq!(f.stored_head().generation, 1);
    assert!(!b.degraded(), "nothing owed: the next seal starts over");
}

/// Another writer moves the head while a seal's page PUTs are in flight: the head check at
/// the lapse fences the seal, which sends no page PUT after it and no manifest.
#[test]
fn seal_stops_page_puts_when_the_head_moves_mid_seal() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 1);
    b.set_lease(Duration::from_millis(200));
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    f.fault.only_keys_containing("/pages/");
    f.fault.delay_puts(Duration::from_millis(150));
    let before = f.fault.events().len();
    let r = std::thread::scope(|s| {
        s.spawn(|| {
            while !seal_events(&f.fault.events()[before..]).contains(&"page") {
                std::thread::sleep(Duration::from_millis(1));
            }
            let current = f.mem.get(&keys().head()).unwrap().unwrap();
            let other = Head {
                writer_id: "w2".into(),
                epoch: 1,
                ..Head::decode(&current.body).unwrap()
            };
            let r = f
                .mem
                .put_if_match(&keys().head(), &other.encode(), &current.etag)
                .unwrap();
            assert!(matches!(r, CondPut::Created { .. }), "{r:?}");
        });
        b.seal_generation(pages(4), 4096, 4)
    });
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
    let ev = seal_events(&f.fault.events()[before..]);
    let check = ev.iter().rposition(|e| *e == "get head").unwrap();
    assert!(
        ev[..check].contains(&"page"),
        "the fencing check came between page PUTs: {ev:?}"
    );
    assert!(
        !ev[check..].contains(&"page"),
        "no page PUT after the fencing check: {ev:?}"
    );
    assert!(!ev.contains(&"manifest"), "no manifest PUT: {ev:?}");
    assert!(f.body(&keys().manifest(1)).is_none());
    assert!(b.degraded(), "fenced for good");
}

/// A failed or ambiguous append confirms nothing: it does not renew the lease, so once the
/// lease has passed since the last confirmed write, the next append still GETs the head
/// first (then settles the in-doubt record).
#[test]
fn failed_or_ambiguous_append_does_not_renew_the_lease() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    };
    let k = keys();
    for ambiguous in [false, true] {
        let f = Fixture::new();
        let mut b = f.create(short.clone(), 4);
        b.set_lease(Duration::from_secs(10));
        b.append_log(LogFile::Wal, syncs(1)).unwrap();
        // 9 s after the last confirmed write: no check, and this append fails.
        b.age_lease(Duration::from_secs(9), Duration::from_secs(9));
        if ambiguous {
            f.fault.store_then_fail_next_puts(1);
        }
        f.fault.fail_puts_after(0);
        let before = f.fault.events().len();
        assert!(matches!(
            b.append_log(LogFile::Wal, syncs(2))
                .map(|a| a.pos)
                .map_err(|f| f.error),
            Err(Error::Timeout(_))
        ));
        assert!(
            !f.fault.events()[before..].contains(&format!("get {}", k.head())),
            "ambiguous={ambiguous}: no check inside the lease"
        );
        assert_eq!(f.record(1, 2).is_some(), ambiguous);
        f.fault.fail_puts_after(u64::MAX);

        // 2 s later: 11 s since the last confirmed write. Had the failed append renewed
        // the lease, it would be 2 s and nothing would be checked.
        b.age_lease(Duration::from_secs(2), Duration::from_secs(2));
        let before = f.fault.events().len();
        assert_eq!(
            b.append_log(LogFile::Wal, syncs(3))
                .map(|a| a.pos)
                .map_err(|f| f.error)
                .map(|p| p.seq),
            Ok(3)
        );
        let ev = f.fault.events()[before..].to_vec();
        assert_eq!(
            ev[0],
            format!("get {}", k.head()),
            "ambiguous={ambiguous}: {ev:?}"
        );
        assert_eq!(f.record(1, 2), Some(ours(2)));
        assert_eq!(f.record(1, 3), Some(ours(3)));
    }
}

/// Past the lease with the head moved by another writer (it claimed and sealed
/// this generation, and its trimmer deleted the log): the append is fenced and writes
/// nothing, even though its seq is free.
#[test]
fn lapsed_append_with_moved_head_is_fenced_and_writes_nothing() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.set_lease(Duration::ZERO);
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    let other = Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 2,
        manifest: Some(1),
        writer_id: "w2".into(),
        epoch: 0,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
    };
    f.mem.put(&keys().head(), &other.encode()).unwrap();
    f.mem.delete(&keys().log(1, 1)).unwrap();
    let before = f.fault.events().len();
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(2))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Fenced(_))
    ));
    assert_eq!(
        f.fault.events()[before..],
        [format!("get {}", keys().head())]
    );
    assert!(f.log_keys(1).is_empty());
    let n = f.fault.events().len();
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(3))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Fenced(_))
    ));
    assert_eq!(f.fault.events().len(), n, "fenced for good");
}

/// Past the lease, single writer: the head is unchanged (or is our own late head
/// for this generation's seal), so the append proceeds.
#[test]
fn lapsed_append_with_our_head_proceeds() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.set_lease(Duration::ZERO);
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error)
            .map(|p| p.seq),
        Ok(1)
    );
    assert_eq!(b.seal_generation(pages(1), 4096, 1), Ok(2));
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(2))
            .map(|a| a.pos)
            .map_err(|f| f.error)
            .map(|p| (p.generation, p.seq)),
        Ok((2, 1))
    );
    let late = Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 3,
        manifest: Some(2),
        writer_id: "w1".into(),
        epoch: 1,
        lease_ms: Some(0), // the writer's `set_lease(ZERO)` above

        request_timeout_ms: Some(10_000),
        next_generation_floor: None,
    };
    f.mem.put(&keys().head(), &late.encode()).unwrap();
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(3))
            .map(|a| a.pos)
            .map_err(|f| f.error)
            .map(|p| (p.generation, p.seq)),
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
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
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
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(b'A'))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Timeout(_))
    ));
    assert_eq!(f.record(1, 1), None);
    f.fault.fail_next_puts(0);
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(b'B'))
            .map(|a| a.pos)
            .map_err(|f| f.error)
            .map(|p| p.seq),
        Ok(2)
    );
    assert_eq!(f.record(1, 1), Some(ours(b'A')));
    assert_eq!(f.record(1, 2), Some(ours(b'B')));

    // A landed but every answer was an error: the settle finds it and B still goes to seq 2.
    let f = Fixture::new();
    let mut b = f.create(short, 4);
    f.fault.store_then_fail_next_puts(1);
    f.fault.fail_puts_after(0);
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(b'A'))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Timeout(_))
    ));
    assert_eq!(f.record(1, 1), Some(ours(b'A')));
    f.fault.fail_puts_after(u64::MAX);
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(b'B'))
            .map(|a| a.pos)
            .map_err(|f| f.error)
            .map(|p| p.seq),
        Ok(2)
    );
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
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(b'A'))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Timeout(_))
    ));
    // The settle fails too: B is not written anywhere and A is still owed seq 1.
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(b'B'))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Timeout(_))
    ));
    assert!(matches!(
        b.seal_generation(pages(1), 4096, 1),
        Err(Error::Timeout(_))
    ));
    f.fault.fail_next_puts(0);
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(b'C'))
            .map(|a| a.pos)
            .map_err(|f| f.error)
            .map(|p| p.seq),
        Ok(2)
    );
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
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::UploadFailed(_))
    ));
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(2))
            .map(|a| a.pos)
            .map_err(|f| f.error)
            .map(|p| p.seq),
        Ok(2)
    );
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
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    let before = f.fault.events().len();
    assert_eq!(b.seal_generation(pages(8), 4096, 8), Ok(2));

    let ev = f.fault.events()[before..].to_vec();
    let k = keys();
    // Every page and the manifest are new keys: one `put_if_absent` each.
    let is_page = |e: &String| e.starts_with(&format!("put_if_absent {PREFIX}/pages/"));
    let manifest_at = ev
        .iter()
        .position(|e| *e == format!("put_if_absent {}", k.manifest(1)))
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
            file: LogFile::Wal,
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
        b.append_log(LogFile::Wal, syncs(2))
            .map(|a| a.pos)
            .map_err(|f| f.error),
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
    assert!(b.append_log(LogFile::Wal, syncs(b'A')).is_err());
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
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error),
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
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error),
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
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error),
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
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error),
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
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 1,
        manifest: None,
        writer_id: "w2".into(),
        epoch: 0,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
    };
    f.mem.put(&keys().head(), &other.encode()).unwrap();
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Fenced(_))
    ));
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
    let cap = crate::telemetry::testing::Capture::new();
    let sealed = tracing::subscriber::with_default(cap.subscriber(), || {
        tracing::info_span!(target: "buckwal", "seal", buckwal.seq = tracing::field::Empty)
            .in_scope(|| b.seal_generation(other_pages, 4096, 2))
    });
    assert_eq!(sealed, Ok(2));
    // Its seq is the failed attempt's seal-start record, log/1/1.
    assert_eq!(
        cap.spans().last_named("seal").field("buckwal.seq"),
        Some("1")
    );
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
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error),
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
    // The head GET finds the ETag unchanged, and the epoch is raised on it (a late PUT of
    // the failed head write gets a 412); the seal then starts like any other, with a
    // seal-start record after the failed attempt's.
    assert_eq!(
        f.fault.events()[before..before + 4],
        [
            format!("get {}", k.head()),
            format!("put_if_match {}", k.head()),
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
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 1,
        manifest: None,
        writer_id: "w2".into(),
        epoch: 0,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
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
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(b'A'))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Timeout(_))
    ));
    let t = Instant::now();
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(b'B'))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Timeout(_))
    ));
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
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(b'A'))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Timeout(_))
    ));
    f.fault.fail_next_puts(0);
    f.fault.delay_puts(Duration::from_millis(150));
    f.fault.fail_puts_after(1);
    let t = Instant::now();
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(b'B'))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Timeout(_))
    ));
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
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 1,
        manifest: None,
        writer_id: "w2".into(),
        epoch: 0,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
    };
    f.mem.put(&keys().head(), &other.encode()).unwrap();
    assert!(matches!(
        b.seal_generation(pages(1), 4096, 1),
        Err(Error::Fenced(_))
    ));
    no_seal_objects(&f, 1);
    assert_eq!(f.stored_head(), other);
    assert_eq!(b.head().generation, 1);
    assert!(matches!(
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Err(Error::Fenced(_))
    ));
}

#[test]
fn take_over_raises_the_epoch_with_the_new_writer_in_the_same_generation() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    let old = f.stored_head();

    // An ambiguous first attempt (stored, then an error): the retry finds its own head.
    f.fault.store_then_fail_next_puts(1);
    let (h, etag) = take_over(&*f.store(), &keys(), &fast(), 1, "w2").unwrap();
    let want = Head {
        writer_id: "w2".into(),
        epoch: old.epoch + 1,
        // One above the highest generation the old writer could seal into (2): its first seal
        // moves the head to 3.
        next_generation_floor: Some(3),
        ..old.clone()
    };
    assert_eq!(h, want);
    assert_eq!(f.stored_head(), want);
    assert_eq!(f.mem.get(&keys().head()).unwrap().unwrap().etag, etag);

    // A resumed claim: the head already names the id; returned as is, nothing written.
    let puts = f.mem.stats().puts;
    let again = take_over(&*f.store(), &keys(), &fast(), 1, "w2").unwrap();
    assert_eq!(again, (want.clone(), etag.clone()));
    assert_eq!(f.mem.stats().puts, puts);

    // The old writer can no longer attach; the new one can, after the old one's record.
    assert!(matches!(f.attach("w1"), Err(Error::Fenced(_))));
    assert_eq!(f.attach("w2").unwrap().next_seq(), 2);

    // Another takeover of the same generation wins the next epoch (whatever the ETag).
    let (h3, _) = take_over(&*f.store(), &keys(), &fast(), 1, "w3").unwrap();
    assert_eq!((h3.writer_id.as_str(), h3.epoch), ("w3", old.epoch + 2));

    // Another generation, or no head: "restore again", and nothing is written.
    let puts = f.mem.stats().puts;
    let r = take_over(&*f.store(), &keys(), &fast(), 2, "w4");
    assert!(
        matches!(&r, Err(Error::Fenced(m)) if m.contains("restore again")),
        "{r:?}"
    );
    assert_eq!(f.mem.stats().puts, puts);
    f.mem.delete(&keys().head()).unwrap();
    let r = take_over(&*f.store(), &keys(), &fast(), 1, "w4");
    assert!(
        matches!(&r, Err(Error::Fenced(m)) if m.contains("restore again")),
        "{r:?}"
    );
    assert_eq!(f.mem.stats().puts, puts);
}

/// A store over a `MemStore` with hooks: `before_head_cas` runs once, before the first
/// `put_if_match` of the head is passed on; `before_log_put` runs before each `put_if_absent`
/// of a log key is passed on. It counts the `put_if_absent` calls of log keys.
type HeadHook = Box<dyn FnOnce(&MemStore) + Send>;
type LogHook = Box<dyn Fn(&str, &MemStore) + Send + Sync>;

#[derive(Default)]
struct Interpose {
    mem: Arc<MemStore>,
    before_head_cas: Mutex<Option<HeadHook>>,
    before_log_put: Option<LogHook>,
    log_puts: AtomicU64,
}

impl ObjectStore for Interpose {
    fn put(&self, key: &str, body: &[u8]) -> std::result::Result<String, StoreError> {
        self.mem.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> std::result::Result<CondPut, StoreError> {
        if keys().parse_log(key).is_some() {
            self.log_puts.fetch_add(1, Relaxed);
            if let Some(h) = &self.before_log_put {
                h(key, &self.mem);
            }
        }
        self.mem.put_if_absent(key, body)
    }
    fn put_if_match(
        &self,
        key: &str,
        body: &[u8],
        etag: &str,
    ) -> std::result::Result<CondPut, StoreError> {
        if key == keys().head() {
            if let Some(h) = self.before_head_cas.lock().unwrap().take() {
                h(&self.mem);
            }
        }
        self.mem.put_if_match(key, body, etag)
    }
    fn get(&self, key: &str) -> std::result::Result<Option<Object>, StoreError> {
        self.mem.get(key)
    }
    fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StoreError> {
        self.mem.list(prefix)
    }
    fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
        self.mem.delete(key)
    }
    fn stat(
        &self,
        key: &str,
    ) -> std::result::Result<Option<crate::store::ObjectStat>, crate::store::StoreError> {
        self.mem.stat(key)
    }
    fn stats(&self) -> crate::store::StatsSnap {
        self.mem.stats()
    }
}

/// `head` with its epoch raised and another writer: a concurrent takeover that landed.
fn rival_takeover(mem: &MemStore, writer_id: &str, generation: Option<u64>) {
    let o = mem.get(&keys().head()).unwrap().unwrap();
    let h = Head::decode(&o.body).unwrap();
    let rival = Head {
        writer_id: writer_id.into(),
        epoch: h.epoch + 1,
        generation: generation.unwrap_or(h.generation),
        ..h
    };
    let r = mem.put_if_match(&keys().head(), &rival.encode(), &o.etag);
    assert!(matches!(r, Ok(CondPut::Created { .. })), "{r:?}");
}

/// Two takeovers race: the other one's head lands between our GET and our `put_if_match`.
/// Same generation: ours is retried on the new ETag and wins the next epoch. Another
/// generation: "restore again".
#[test]
fn take_over_after_a_rival_retries_on_the_same_generation_only() {
    let mem = Arc::new(MemStore::new());
    Bucket::create(mem.clone(), keys(), fast(), 4, "w1".into()).unwrap();
    let store = Interpose {
        mem: mem.clone(),
        ..Interpose::default()
    };
    *store.before_head_cas.lock().unwrap() =
        Some(Box::new(|m: &MemStore| rival_takeover(m, "rival", None)));
    let (h, _) = take_over(&store, &keys(), &fast(), 1, "w2").unwrap();
    assert_eq!((h.writer_id.as_str(), h.epoch, h.generation), ("w2", 3, 1));

    *store.before_head_cas.lock().unwrap() = Some(Box::new(|m: &MemStore| {
        rival_takeover(m, "rival2", Some(2))
    }));
    let r = take_over(&store, &keys(), &fast(), 1, "w3");
    assert!(
        matches!(&r, Err(Error::Fenced(m)) if m.contains("restore again")),
        "{r:?}"
    );
    let stored = Head::decode(&mem.get(&keys().head()).unwrap().unwrap().body).unwrap();
    assert_eq!(stored.writer_id, "rival2");
}

fn engine_cfg() -> crate::config::EngineConfig {
    crate::config::Config::new("/tmp/x", "http://127.0.0.1:1", "b", "k", "s").engine(false)
}

fn fence(writer_id: &str, epoch: u64) -> LogRecord {
    LogRecord::Fence {
        writer_id: writer_id.into(),
        epoch,
    }
}

/// w1 at seq 1, then a takeover by w2 (epoch 2) attached at seq 2.
fn taken_over(f: &Fixture) -> (Bucket, Bucket) {
    let mut a = f.create(fast(), 4);
    a.append_log(LogFile::Wal, syncs(1)).unwrap();
    take_over(&*f.store(), &keys(), &fast(), 1, "w2").unwrap();
    let b = Bucket::attach(f.store(), keys(), fast(), 4, "w2", 2).unwrap();
    assert_eq!(b.next_seq(), 2);
    (a, b)
}

#[test]
fn append_fence_steps_over_an_old_writer_record() {
    // The old writer's record lands at the new writer's next seq first: the fence steps over
    // it, and the old writer's next append collides with the fence.
    let f = Fixture::new();
    let (mut a, mut b) = taken_over(&f);
    a.append_log(LogFile::Wal, syncs(2)).unwrap();
    let out = b.append_fence().unwrap();
    assert_eq!(
        (out.pos, out.seal_may_be_in_flight),
        (
            LogPos {
                generation: 1,
                seq: 3
            },
            false
        )
    );
    assert_eq!(f.record(1, 3), Some(fence("w2", 2)));
    assert_eq!(b.next_seq(), 4);
    let r = a.append_log(LogFile::Wal, syncs(3)).map_err(|f| f.error);
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
    // The restore image: w1's two records, then the fence.
    let n = crate::restore::generation_image(&*f.store(), &keys(), b.head(), &engine_cfg())
        .unwrap()
        .records;
    assert_eq!(n, 3);

    // A losing concurrent takeover's fence (a lower epoch) is stepped over too.
    let f = Fixture::new();
    let (_a, mut b) = taken_over(&f);
    f.mem
        .put(&keys().log(1, 2), &encode_record(&fence("w0", 1)))
        .unwrap();
    assert_eq!(b.append_fence().unwrap().pos.seq, 3);
    assert_eq!(f.record(1, 3), Some(fence("w2", 2)));

    // Another writer's fence at an equal or higher epoch fences this writer.
    for epoch in [2, 3] {
        let f = Fixture::new();
        let (_a, mut b) = taken_over(&f);
        f.mem
            .put(&keys().log(1, 2), &encode_record(&fence("w3", epoch)))
            .unwrap();
        let r = b.append_fence();
        assert!(matches!(r, Err(Error::Fenced(_))), "epoch {epoch}: {r:?}");
        assert_eq!(f.record(1, 3), None);
        assert!(
            b.append_log(LogFile::Wal, syncs(4)).is_err(),
            "fenced for good"
        );
    }

    // Our own fence already there (an earlier attempt landed): landed, no step.
    let f = Fixture::new();
    let (_a, mut b) = taken_over(&f);
    f.mem
        .put(&keys().log(1, 2), &encode_record(&fence("w2", 2)))
        .unwrap();
    assert_eq!(b.append_fence().unwrap().pos.seq, 2);
    assert_eq!(f.log_keys(1).len(), 2);
}

#[test]
fn append_fence_after_an_empty_old_writer_record_reports_it() {
    // The old writer's seal-start record (no syncs) is the last before the fence.
    let f = Fixture::new();
    let (_a, mut b) = taken_over(&f);
    f.mem
        .put(&keys().log(1, 2), &encode_record(&empty("w1")))
        .unwrap();
    let out = b.append_fence().unwrap();
    assert_eq!((out.pos.seq, out.seal_may_be_in_flight), (3, true));
    // Right after the old writer's commit record: false.
    let f = Fixture::new();
    let (_a, mut b) = taken_over(&f);
    let out = b.append_fence().unwrap();
    assert_eq!((out.pos.seq, out.seal_may_be_in_flight), (2, false));
}

/// C1: the quiet looks at every other writer's last record before the fence, not only the
/// record just before it. w1 is left mid-seal (its last record is its seal-start record);
/// w2 takes over (its fence follows that record), and w3 takes over after it. Each case's
/// records before w3's fence are listed; `true` means some other writer's last record there
/// is a seal-start record (or a record cannot be read).
#[test]
fn a_chained_fence_reports_a_seal_in_flight_further_back() {
    // [w1 commit, w1 seal-start, F(w2)] then w2's `b_commits` commits, then F(w3).
    for b_commits in [0u8, 2] {
        let f = Fixture::new();
        let (_a, mut b) = taken_over(&f);
        f.mem
            .put(&keys().log(1, 2), &encode_record(&empty("w1")))
            .unwrap();
        assert!(
            b.append_fence().unwrap().seal_may_be_in_flight,
            "w2's fence"
        );
        for t in 0..b_commits {
            b.append_log(LogFile::Wal, syncs(10 + t)).unwrap();
        }
        take_over(&*f.store(), &keys(), &fast(), 1, "w3").unwrap();
        let mut c = Bucket::attach(f.store(), keys(), fast(), 4, "w3", 3).unwrap();
        let out = c.append_fence().unwrap();
        assert_eq!(out.pos.seq, 4 + u64::from(b_commits));
        assert!(
            out.seal_may_be_in_flight,
            "w2 committed {b_commits}: w1's seal-start record is w1's last, before w3's fence"
        );
    }

    // Concurrent takeovers: w3's fence steps over w2's (a lower epoch), which landed after
    // w1's seal-start record.
    let f = Fixture::new();
    let (_a, _b) = taken_over(&f);
    f.mem
        .put(&keys().log(1, 2), &encode_record(&empty("w1")))
        .unwrap();
    take_over(&*f.store(), &keys(), &fast(), 1, "w3").unwrap();
    let mut c = Bucket::attach(f.store(), keys(), fast(), 4, "w3", 3).unwrap();
    f.mem
        .put(&keys().log(1, 3), &encode_record(&fence("w2", 2)))
        .unwrap();
    let out = c.append_fence().unwrap();
    assert_eq!(out.pos.seq, 4);
    assert!(out.seal_may_be_in_flight, "stepped over w2's fence");

    // No seal in flight: [w1 commit, F(w2), w2 commit, F(w3)].
    let f = Fixture::new();
    let (_a, mut b) = taken_over(&f);
    assert!(!b.append_fence().unwrap().seal_may_be_in_flight);
    b.append_log(LogFile::Wal, syncs(10)).unwrap();
    take_over(&*f.store(), &keys(), &fast(), 1, "w3").unwrap();
    let mut c = Bucket::attach(f.store(), keys(), fast(), 4, "w3", 3).unwrap();
    assert!(!c.append_fence().unwrap().seal_may_be_in_flight);

    // A record before the fence that cannot be read counts as a seal in flight.
    let f = Fixture::new();
    let (_a, mut b) = taken_over(&f);
    f.mem.put(&keys().log(1, 1), b"not a record").unwrap();
    assert!(b.append_fence().unwrap().seal_may_be_in_flight);
}

#[test]
fn append_fence_gives_up_after_the_step_bound() {
    // An old writer that always wins: before each fence PUT, its record lands at that key.
    let mem = Arc::new(MemStore::new());
    let mut a = Bucket::create(mem.clone(), keys(), fast(), 4, "w1".into()).unwrap();
    a.append_log(LogFile::Wal, syncs(1)).unwrap();
    take_over(&*mem, &keys(), &fast(), 1, "w2").unwrap();
    let store = Arc::new(Interpose {
        mem: mem.clone(),
        before_log_put: Some(Box::new(|key: &str, m: &MemStore| {
            m.put_if_absent(key, &encode_record(&ours(9))).unwrap();
        })),
        ..Interpose::default()
    });
    let mut b = Bucket::attach(store.clone(), keys(), fast(), 4, "w2", 2).unwrap();
    let r = b.append_fence();
    assert!(
        matches!(&r, Err(Error::Timeout(m)) if m.contains(RESUME)),
        "{r:?}"
    );
    assert_eq!(
        store.log_puts.load(Relaxed),
        65,
        "64 steps after the first PUT"
    );
    let fences = mem
        .list(&keys().log_prefix(1))
        .unwrap()
        .iter()
        .filter(|k| {
            matches!(
                decode_record(&mem.get(k).unwrap().unwrap().body).unwrap(),
                LogRecord::Fence { .. }
            )
        })
        .count();
    assert_eq!(fences, 0, "no fence anywhere in the log");
}

#[test]
fn append_refused_before_any_put_hands_the_syncs_back() {
    // A record left in doubt (every attempt failed); then its settle fails: the new record is
    // never sent.
    let (mem, fault, mut b) = fault_bucket_with_short_deadline();
    fault.fail_puts_after(0);
    let first = b.append_log(LogFile::Wal, vec![op(1)]).unwrap_err();
    assert!(
        first.unsent.is_none(),
        "an attempted PUT is in doubt, not unsent"
    );
    assert!(b.degraded());
    let second = b.append_log(LogFile::Wal, vec![op(2)]).unwrap_err();
    assert_eq!(second.unsent, Some(vec![op(2)]));
    fault.fail_puts_after(u64::MAX);
    b.catch_up().unwrap(); // settles the in-doubt record at its own seq
    assert!(!b.degraded());
    let a = b.append_log(LogFile::Wal, vec![op(2)]).unwrap();
    assert_eq!(a.pos.seq, 2);
    assert_eq!(
        stored_ops(
            &mem,
            LogPos {
                generation: 1,
                seq: 1
            }
        ),
        vec![op(1)]
    );
}

#[test]
fn catch_up_writes_nothing_when_nothing_is_owed() {
    let (mem, _fault, mut b) = fault_bucket_with_short_deadline();
    let before = mem.stats();
    b.catch_up().unwrap();
    assert_eq!(mem.stats().since(&before).puts, 0);
}

#[test]
fn fenced_append_reports_no_unsent_syncs_and_degrades() {
    // A foreign record at the next seq.
    let (mem, _fault, mut b) = fault_bucket_with_short_deadline();
    mem.put_if_absent(&keys().log(1, 1), b"someone else")
        .unwrap();
    let f = b.append_log(LogFile::Wal, vec![op(1)]).unwrap_err();
    assert!(matches!(f.error, Error::Fenced(_)) && f.unsent.is_none());
    assert!(b.degraded());
    assert!(matches!(b.catch_up(), Err(Error::Fenced(_))));
}

#[test]
fn appended_reports_bytes_and_retries() {
    let (_mem, fault, mut b) = fault_bucket_with_short_deadline();
    fault.fail_next_puts(2);
    let a = b.append_log(LogFile::Wal, vec![op(1)]).unwrap();
    assert_eq!(a.retries, 2);
    assert!(a.bytes > 0);
}

#[test]
fn refused_before_the_record_is_built_hands_the_syncs_back() {
    // A seal's head write landed but every reply was an error: the append is refused in
    // `check_head`, and `catch_up` leaves the published head for the next seal to adopt.
    let (mem, fault, mut b) = fault_bucket_with_short_deadline();
    fault.only_keys_containing("/head");
    fault.store_then_fail_next_puts(1);
    fault.fail_puts_after(0);
    assert!(b.seal_generation(pages(1), 4096, 1).is_err());
    let f = b.append_log(LogFile::Wal, vec![op(1)]).unwrap_err();
    assert!(matches!(f.error, Error::UploadFailed(_)), "{f:?}");
    assert_eq!(f.unsent, Some(vec![op(1)]));
    let before = mem.stats();
    b.catch_up().unwrap();
    assert_eq!(mem.stats().since(&before).puts, 0);
    assert!(b.degraded(), "only a seal resolves a published head");
    fault.fail_puts_after(u64::MAX);
    assert_eq!(b.seal_generation(pages(1), 4096, 1), Ok(2));
    assert!(!b.degraded());

    // The lease check finds a moved head: fenced before the record is built.
    let (mem, _fault, mut b) = fault_bucket_with_short_deadline();
    b.set_lease(Duration::ZERO);
    let other = Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 1,
        manifest: None,
        writer_id: "w2".into(),
        epoch: 0,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
    };
    mem.put(&keys().head(), &other.encode()).unwrap();
    let f = b.append_log(LogFile::Wal, vec![op(1)]).unwrap_err();
    assert!(matches!(f.error, Error::Fenced(_)), "{f:?}");
    assert_eq!(f.unsent, Some(vec![op(1)]));
    assert!(b.degraded());
    // Already fenced: nothing is sent at all.
    let before = mem.stats();
    let f = b.append_log(LogFile::Wal, vec![op(2)]).unwrap_err();
    assert!(matches!(f.error, Error::Fenced(_)), "{f:?}");
    assert_eq!(f.unsent, Some(vec![op(2)]));
    assert_eq!(mem.stats().since(&before).requests(), 0);
    assert!(mem.list(&keys().log_prefix(1)).unwrap().is_empty());
}

#[test]
fn create_starts_at_epoch_1() {
    let f = Fixture::new();
    let b = f.create(fast(), 4);
    assert_eq!(b.head().epoch, 1);
    assert_eq!(f.stored_head().epoch, 1);
}

/// A seal's head write that never landed may still be in flight (If-Match on the pre-seal
/// ETag). The next append finds the head unchanged and raises the epoch on that ETag before it
/// carries on, so the late PUT can no longer land.
#[test]
fn ambiguous_head_write_not_landed_raises_the_epoch() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    };
    let f = Fixture::new();
    let mut b = f.create(short, 4);
    let old = f.stored_head();
    let pre_seal = f.mem.get(&keys().head()).unwrap().unwrap().etag;
    f.fault.only_keys_containing("/head");
    f.fault.fail_next_puts(100_000);
    assert!(b.seal_generation(pages(1), 4096, 1).is_err());
    f.fault.fail_next_puts(0);
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Ok(LogPos {
            generation: 1,
            seq: 2
        })
    );
    let raised = Head {
        epoch: old.epoch + 1,
        ..old.clone()
    };
    assert_eq!(f.stored_head(), raised);
    assert_eq!(b.head(), &raised);
    // The head the failed seal meant to publish, arriving late on the pre-seal ETag.
    let late = Head {
        generation: 2,
        manifest: Some(1),
        ..old
    };
    let r = f
        .mem
        .put_if_match(&keys().head(), &late.encode(), &pre_seal);
    assert!(matches!(r, Ok(CondPut::PreconditionFailed)), "{r:?}");
    assert_eq!(f.stored_head(), raised);
    // The writer holds the raised head's ETag: its next seal publishes.
    assert_eq!(b.seal_generation(pages(1), 4096, 1), Ok(2));
    assert_eq!(f.stored_head().epoch, raised.epoch);
}

/// A head whose epoch is below the directory's is older than one this directory already saw:
/// attaching to it is refused. Equal or above is accepted.
#[test]
fn attach_refuses_a_head_epoch_below_the_directorys() {
    let f = Fixture::new();
    f.create(fast(), 4);
    let r = Bucket::attach(f.store(), keys(), fast(), 4, "w1", 2);
    assert!(
        matches!(&r, Err(Error::Fenced(m)) if m.contains("epoch 1") && m.contains("epoch 2")),
        "{:?}",
        r.err()
    );
    assert!(Bucket::attach(f.store(), keys(), fast(), 4, "w1", 1).is_ok());
    assert!(Bucket::attach(f.store(), keys(), fast(), 4, "w1", 0).is_ok());
}

/// `raise_epoch` rewrites the head with the epoch one higher and nothing else changed, adopts
/// its ETag (the next seal's conditional write succeeds), and accepts its own write whose
/// reply was lost. A head another writer moved fences.
#[test]
fn raise_epoch_keeps_writer_generation_and_manifest() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    b.seal_generation(pages(1), 4096, 1).unwrap();
    let before = f.stored_head();
    assert_eq!(b.raise_epoch(), Ok(Some(before.epoch + 1)));
    let want = Head {
        epoch: before.epoch + 1,
        ..before.clone()
    };
    assert_eq!((f.stored_head(), b.head()), (want.clone(), &want));

    f.fault.only_keys_containing("/head");
    f.fault.store_then_fail_next_puts(1);
    assert_eq!(
        b.raise_epoch(),
        Ok(Some(want.epoch + 1)),
        "landed, reply lost"
    );
    assert_eq!(f.stored_head().epoch, want.epoch + 1);
    f.fault.only_keys_containing("");
    assert_eq!(b.seal_generation(vec![], 4096, 1), Ok(3));
    assert_eq!(
        f.stored_head().epoch,
        want.epoch + 1,
        "a seal keeps the epoch"
    );

    let other = Head {
        writer_id: "w2".into(),
        ..f.stored_head()
    };
    f.mem.put(&keys().head(), &other.encode()).unwrap();
    assert!(matches!(b.raise_epoch(), Err(Error::Fenced(_))));
    assert_eq!(f.stored_head(), other);
    assert!(matches!(b.raise_epoch(), Err(Error::Fenced(_))), "for good");
}

/// The raise after an unlanded seal head write was itself ambiguous: it landed, every reply
/// was an error. The next append finds the raised head and adopts it instead of fencing.
#[test]
fn check_head_adopts_its_own_raise_whose_reply_was_lost() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    };
    let f = Fixture::new();
    let mut b = f.create(short, 4);
    let old = f.stored_head();
    f.fault.only_keys_containing("/head");
    f.fault.fail_next_puts(100_000);
    assert!(b.seal_generation(pages(1), 4096, 1).is_err());
    f.fault.fail_next_puts(0);
    f.fault.store_then_fail_next_puts(1);
    f.fault.fail_puts_after(0);
    let r = b
        .append_log(LogFile::Wal, syncs(1))
        .map(|a| a.pos)
        .map_err(|f| f.error);
    assert!(matches!(r, Err(Error::Timeout(_))), "{r:?}");
    let raised = Head {
        epoch: old.epoch + 1,
        ..old
    };
    assert_eq!(f.stored_head(), raised, "the raise landed");
    assert_eq!(b.head().epoch, raised.epoch - 1, "its reply was lost");
    f.fault.fail_puts_after(u64::MAX);
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(1))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Ok(LogPos {
            generation: 1,
            seq: 2
        })
    );
    assert_eq!(b.head(), &raised);
    assert_eq!(b.seal_generation(pages(1), 4096, 1), Ok(2));
}

/// The pending seal's late head PUT lands between `check_head`'s GET and its raise: the raise
/// gets a 412, finds the pending head, and reports it published (nothing fenced).
#[test]
fn raise_finding_the_pending_seal_head_reports_it_published() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    };
    let f = Fixture::new();
    let mut b = f.create(short, 4);
    f.fault.only_keys_containing("/head");
    f.fault.fail_next_puts(100_000);
    assert!(b.seal_generation(pages(1), 4096, 1).is_err());
    f.fault.fail_next_puts(0);
    let pending = b.pending_seal.as_ref().unwrap().head.clone();
    f.mem.put(&keys().head(), &pending.encode()).unwrap();
    let r = b.raise_epoch_at(Instant::now(), Some(&pending.encode()));
    assert!(matches!(r, Ok(Raise::Published(_))), "not published");
    assert!(!b.fenced);
    assert_eq!(f.stored_head(), pending);
    // The next seal adopts it.
    assert_eq!(b.seal_generation(pages(1), 4096, 1), Ok(2));
    assert_eq!(b.head(), &pending);
}

/// What a takeover `Timeout` must say: the retry is an open of the directory.
const RESUME: &str = "open this directory to resume the takeover (`open`, or `buckwal sql \
     --claim --dir DIR 'SELECT 1'`)";

/// The head moved under the takeover's head write and the deadline has passed: `Timeout`,
/// with the same advice as the fence's.
#[test]
fn take_over_timeout_says_to_open_the_directory() {
    let mem = Arc::new(MemStore::new());
    Bucket::create(mem.clone(), keys(), fast(), 4, "w1".into()).unwrap();
    let store = Interpose {
        mem: mem.clone(),
        ..Interpose::default()
    };
    *store.before_head_cas.lock().unwrap() =
        Some(Box::new(|m: &MemStore| rival_takeover(m, "rival", None)));
    let once = RetryPolicy {
        deadline: Duration::ZERO,
        ..fast()
    };
    let r = take_over(&store, &keys(), &once, 1, "w2");
    assert!(
        matches!(&r, Err(Error::Timeout(m)) if m.contains("the head kept changing") && m.contains(RESUME)),
        "{r:?}"
    );
}

/// `n` pages, each body 16 bytes of `fill`.
fn pages_of(n: u32, fill: u8) -> Vec<(u32, Vec<u8>)> {
    (1..=n).map(|p| (p, vec![fill; 16])).collect()
}

/// The objects of a seal of `gen` in `mem`: `pages/<gen>/*` and `manifest/<gen>`, key → body.
fn sealed(mem: &MemStore, gen: u64) -> BTreeMap<String, Vec<u8>> {
    let k = keys();
    let mut all = mem.list(&format!("{}{gen:010}/", k.page_root())).unwrap();
    all.push(k.manifest(gen));
    all.into_iter()
        .filter_map(|key| mem.get(&key).unwrap().map(|o| (key, o.body)))
        .collect()
}

/// The page and manifest requests among `events` (PUTs of any flavour and stats), in order.
fn object_events(events: &[String]) -> Vec<String> {
    let k = keys();
    let (pages, manifest) = (k.page_root(), k.manifest_root());
    events
        .iter()
        .filter(|e| {
            e.split_once(' ')
                .is_some_and(|(_, key)| key.starts_with(&pages) || key.starts_with(&manifest))
        })
        .cloned()
        .collect()
}

/// Old writer A (lease 200 ms, one PUT at a time) seals generation 1, and its next PUT of a
/// key containing `held` is held after its lease check, as if A were paused (SIGSTOP) right
/// before the send. Meanwhile B takes over (`take_over`, then `append_fence`, which lands
/// right after A's seal-start record), waits out its quiet and seals itself, with other page
/// bodies and page count, publishing its head. B's first seal skips a generation: head 3,
/// objects under generation 2, none under A's generation 1. Then A's PUT goes on: it can only
/// land on a key under generation 1, which nothing references, and leaves B's `pages/2/*` and
/// `manifest/2` byte-identical; A's seal ends `Fenced`.
fn stale_writers_late_put_is_refused(held: &str) {
    let f = Fixture::new();
    let hold = Arc::new(HoldStore::new(f.mem.clone()));
    let mut a = Bucket::create(hold.clone(), keys(), fast(), 1, "w1".into()).unwrap();
    a.set_lease(Duration::from_millis(200));
    a.append_log(LogFile::Wal, syncs(1)).unwrap();
    let (held_rx, go) = hold.hold_next_put(held);
    // Nothing is asserted inside the scope: a failure would leave A's PUT held for ever.
    let (a_result, a, key, published, after, sealed_b, fence_in_flight) = std::thread::scope(|s| {
        let a_seal = s.spawn(move || {
            let r = a.seal_generation(pages_of(2, 0xA), 4096, 2);
            (r, a)
        });
        let key = held_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("A's PUT is held");
        take_over(&*f.store(), &keys(), &fast(), 1, "w2").unwrap();
        let mut b = Bucket::attach(f.store(), keys(), fast(), 4, "w2", 2).unwrap();
        let fo = b.append_fence().unwrap();
        let quiet = Duration::from_millis(300);
        b.set_seal_not_before(1, Instant::now() + quiet);
        b.append_log(LogFile::Wal, syncs(2)).unwrap();
        let sealed_b = b.seal_generation(pages_of(3, 0xB), 4096, 3);
        let published = sealed(&f.mem, 2);
        go.send(()).unwrap();
        let (r, a) = a_seal.join().unwrap();
        let after = sealed(&f.mem, 2);
        (
            r,
            a,
            key,
            published,
            after,
            sealed_b,
            fo.seal_may_be_in_flight,
        )
    });
    assert!(fence_in_flight, "B's fence follows A's seal-start record");
    assert_eq!(sealed_b, Ok(3), "the head goes to 3");
    assert!(
        after == published,
        "A's late PUT of {key} overwrote B's seal of generation 2: {:?}",
        hold.outcomes()
    );
    let outcomes = hold.outcomes();
    assert!(
        matches!(&outcomes[..], [o] if o.contains(": stored")),
        "A's held PUT lands on its own, unreferenced key: {outcomes:?}"
    );
    assert!(key.contains("/1/") || key.contains("0000000001"), "{key}");
    assert!(matches!(a_result, Err(Error::Fenced(_))), "{a_result:?}");
    assert!(a.degraded(), "A is fenced for good");
    for p in 1..=3 {
        assert_eq!(f.body(&keys().page(2, p)), Some(vec![0xB; 16]), "page {p}");
    }
    let m = Manifest::decode(&f.body(&keys().manifest(2)).unwrap()).unwrap();
    assert_eq!(m.page_count, 3);
    assert_eq!(
        m.pages.values().collect::<BTreeSet<_>>(),
        BTreeSet::from([&2])
    );
    let head = f.stored_head();
    assert_eq!(
        (head.writer_id.as_str(), head.generation, head.manifest),
        ("w2", 3, Some(2))
    );
}

/// A stale writer's page PUT, held between its lease check and its send until a new writer
/// has sealed and published the generation, cannot overwrite the published page.
#[test]
fn a_stale_writers_late_page_put_cannot_overwrite_a_published_seal() {
    stale_writers_late_put_is_refused("/pages/");
}

/// The same for the stale writer's manifest PUT, held after its pages went out.
#[test]
fn a_stale_writers_late_manifest_put_cannot_overwrite_a_published_seal() {
    stale_writers_late_put_is_refused("/manifest/");
}

/// A seal fails after its page PUTs (the manifest PUT fails permanently once); the retry, with
/// other page bodies, overwrites its own pages with `put_if_match` on the ETags it got back,
/// with no stat, and the stored bodies are the new ones.
#[test]
fn a_retried_seal_overwrites_its_own_pages() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 1);
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    f.fault.only_keys_containing("/manifest/");
    f.fault.fail_next_puts_permanently(1);
    let r = b.seal_generation(pages_of(2, 1), 4096, 2);
    assert!(matches!(r, Err(Error::UploadFailed(_))), "{r:?}");
    let before = f.fault.events().len();
    assert_eq!(b.seal_generation(pages_of(2, 2), 4096, 2), Ok(2));
    let k = keys();
    assert_eq!(
        object_events(&f.fault.events()[before..]),
        [
            format!("put_if_match {}", k.page(1, 1)),
            format!("put_if_match {}", k.page(1, 2)),
            format!("put_if_absent {}", k.manifest(1)),
        ]
    );
    for p in 1..=2 {
        assert_eq!(f.body(&k.page(1, p)), Some(vec![2; 16]));
    }
    assert_eq!(f.stored_head().generation, 2);
}

/// `pages/1/1` was put by another writer beforehand: the seal's `put_if_absent` gets a 412,
/// it stats the key and overwrites it with `put_if_match` on that ETag.
#[test]
fn a_seal_overwrites_a_foreign_leftover_page() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 1);
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    let k = keys();
    f.mem
        .put(&k.page(1, 1), b"another writer's leftover")
        .unwrap();
    let before = f.fault.events().len();
    assert_eq!(b.seal_generation(pages_of(2, 5), 4096, 2), Ok(2));
    assert_eq!(
        object_events(&f.fault.events()[before..]),
        [
            format!("put_if_absent {}", k.page(1, 1)),
            format!("stat {}", k.page(1, 1)),
            format!("put_if_match {}", k.page(1, 1)),
            format!("put_if_absent {}", k.page(1, 2)),
            format!("put_if_absent {}", k.manifest(1)),
        ]
    );
    for p in 1..=2 {
        assert_eq!(f.body(&k.page(1, p)), Some(vec![5; 16]));
    }
    assert_eq!(f.stored_head().generation, 2);
}

/// A page PUT that is stored but whose reply is lost: the retry's `put_if_absent` gets a 412
/// on our own object, whose ETag we never learnt. The seal stats it and puts the same bytes
/// with `put_if_match` on that ETag. It is not fenced, and the stored body is ours.
#[test]
fn a_lost_reply_on_a_conditional_page_put_is_rewritten_without_fencing() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 1);
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    f.fault.only_keys_containing("/pages/");
    f.fault.store_then_fail_next_puts(1);
    let before = f.fault.events().len();
    assert_eq!(b.seal_generation(pages_of(1, 7), 4096, 1), Ok(2));
    let k = keys();
    assert_eq!(
        object_events(&f.fault.events()[before..]),
        [
            format!("put_if_absent {}", k.page(1, 1)),
            format!("put_if_absent {}", k.page(1, 1)),
            format!("stat {}", k.page(1, 1)),
            format!("put_if_match {}", k.page(1, 1)),
            format!("put_if_absent {}", k.manifest(1)),
        ]
    );
    assert!(!b.degraded());
    assert_eq!(f.body(&k.page(1, 1)), Some(vec![7; 16]));
    assert_eq!(b.counters().page_puts.load(Relaxed), 1);
    let head = f.stored_head();
    assert_eq!((head.writer_id.as_str(), head.generation), ("w1", 2));
}

/// A seal fails after its page PUTs. The retry's `put_if_match` of `pages/1/1` on its own
/// ETag is held, and meanwhile someone else writes that key (a crashed process's late PUT,
/// say), and in the second case also takes the head over. The PUT then gets a 412, and the
/// seal GETs the head first. Still ours: it stats the key and overwrites it on the ETag read.
/// Moved: it is fenced and the other object stays.
#[test]
fn a_seal_finding_its_own_page_overwritten_checks_the_head_first() {
    let k = keys();
    for moved in [false, true] {
        let f = Fixture::new();
        let hold = Arc::new(HoldStore::new(f.store()));
        let mut b = Bucket::create(hold.clone(), keys(), fast(), 1, "w1".into()).unwrap();
        b.append_log(LogFile::Wal, syncs(1)).unwrap();
        f.fault.only_keys_containing("/manifest/");
        f.fault.fail_next_puts_permanently(1);
        let r = b.seal_generation(pages_of(2, 1), 4096, 2);
        assert!(matches!(r, Err(Error::UploadFailed(_))), "{r:?}");
        let (held_rx, go) = hold.hold_next_put("/pages/");
        let before = f.fault.events().len();
        let r = std::thread::scope(|s| {
            let seal = s.spawn(|| b.seal_generation(pages_of(2, 2), 4096, 2));
            let key = held_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!(key, k.page(1, 1));
            f.mem.put(&key, b"someone else").unwrap();
            if moved {
                rival_takeover(&f.mem, "w2", None);
            }
            go.send(()).unwrap();
            seal.join().unwrap()
        });
        assert_eq!(
            hold.outcomes(),
            [format!("put_if_match {}: 412", k.page(1, 1))]
        );
        let ev: Vec<String> = f.fault.events()[before..]
            .iter()
            .filter(|e| !e.contains("/log/"))
            .cloned()
            .collect();
        let head_check = format!("get {}", k.head());
        if moved {
            assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
            assert_eq!(f.body(&k.page(1, 1)), Some(b"someone else".to_vec()));
            assert_eq!(
                ev,
                [
                    head_check.clone(),
                    format!("put_if_match {}", k.page(1, 1)),
                    head_check,
                ]
            );
            continue;
        }
        assert_eq!(r, Ok(2));
        assert_eq!(
            ev,
            [
                head_check.clone(),
                format!("put_if_match {}", k.page(1, 1)),
                head_check,
                format!("stat {}", k.page(1, 1)),
                format!("put_if_match {}", k.page(1, 1)),
                format!("put_if_match {}", k.page(1, 2)),
                format!("put_if_absent {}", k.manifest(1)),
                format!("put_if_match {}", k.head()),
            ]
        );
        for p in 1..=2 {
            assert_eq!(f.body(&k.page(1, p)), Some(vec![2; 16]));
        }
    }
}

/// The residual at `Bucket::seal_put_step`, closed by the skipped generation. On a store whose
/// ETags depend only on the body, as a single-part PUT's MD5 ETag does on S3 and RustFS, old
/// writer A's late `put_if_match` lands over whatever sits at its key when A was paused longer
/// than the takeover quiet between its lease check and the send, and the object there is
/// byte-identical to the one A conditioned on.
///
/// A's first seal attempt uploads page 1 as bytes X and fails at its manifest, so A holds X's
/// ETag. A's second attempt, with other bytes, sends `put_if_match` on it and is held. B takes
/// over and seals with page 1 = X. B's seal moves the head to generation 3 and writes under
/// generation 2, not under A's generation 1, so A's late PUT, released, lands on a key nothing
/// references: B's objects are untouched, and the manifest B published maps its pages to
/// generation 2, so a restore holds B's rows.
#[test]
fn late_put_cannot_replace_new_writers_page() {
    let mem = Arc::new(MemStore::with_content_etags());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let f = Fixture { mem, fault };
    let k = keys();
    let hold = Arc::new(HoldStore::new(f.store()));
    let mut a = Bucket::create(hold.clone(), keys(), fast(), 1, "w1".into()).unwrap();
    a.set_lease(Duration::from_millis(200));
    a.append_log(LogFile::Wal, syncs(1)).unwrap();
    f.fault.only_keys_containing("/manifest/");
    f.fault.fail_next_puts_permanently(1);
    let r = a.seal_generation(pages_of(1, 0xA), 4096, 1);
    assert!(matches!(r, Err(Error::UploadFailed(_))), "{r:?}");
    let (held_rx, go) = hold.hold_next_put("/pages/");
    std::thread::scope(|s| {
        let a_seal = s.spawn(move || a.seal_generation(pages_of(1, 0xC), 4096, 1));
        let key = held_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(key, k.page(1, 1));
        take_over(&*f.store(), &keys(), &fast(), 1, "w2").unwrap();
        let mut b = Bucket::attach(f.store(), keys(), fast(), 4, "w2", 2).unwrap();
        assert!(b.append_fence().unwrap().seal_may_be_in_flight);
        let quiet = Duration::from_millis(300);
        b.set_seal_not_before(1, Instant::now() + quiet);
        b.append_log(LogFile::Wal, syncs(2)).unwrap();
        let b_pages = vec![(1, vec![0xA; 16]), (2, vec![0xB; 16])];
        // Nothing is asserted until A's PUT is released (a failure would leave it held).
        let sealed = b.seal_generation(b_pages, 4096, 2);
        go.send(()).unwrap();
        let r = a_seal.join().unwrap();
        assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
        assert_eq!(sealed, Ok(3), "the head skips 2");
    });
    assert_eq!(
        hold.outcomes(),
        [format!("put_if_match {}: stored", k.page(1, 1))],
        "A's late PUT matched its own content-equal ETag, on its own key"
    );
    assert_eq!(
        f.body(&k.page(1, 1)),
        Some(vec![0xC; 16]),
        "A's key, unreferenced"
    );
    assert_eq!(
        f.body(&k.page(2, 1)),
        Some(vec![0xA; 16]),
        "B's page 1 untouched"
    );
    assert_eq!(
        f.body(&k.page(2, 2)),
        Some(vec![0xB; 16]),
        "B's page 2 untouched"
    );
    let h = f.stored_head();
    assert_eq!(
        (h.writer_id.as_str(), h.generation, h.manifest),
        ("w2", 3, Some(2))
    );
    assert_eq!(h.next_generation_floor, Some(3), "the head keeps the floor");
    let m = Manifest::decode(&f.body(&k.manifest(2)).unwrap()).unwrap();
    assert_eq!(
        m.pages,
        BTreeMap::from([(1, 2), (2, 2)]),
        "B's manifest maps B's pages"
    );
}

/// A store over a `MemStore` that answers every PUT of `key` with a 412 and changes that key's
/// object (so its ETag) each time: a seal's chain for it never ends on its own.
struct ForeverPreconditionFailed {
    mem: Arc<MemStore>,
    key: String,
    n: AtomicU64,
}

impl ForeverPreconditionFailed {
    fn refuse(&self) -> std::result::Result<CondPut, StoreError> {
        let n = self.n.fetch_add(1, Relaxed);
        self.mem.put(&self.key, &n.to_le_bytes())?;
        Ok(CondPut::PreconditionFailed)
    }
}

impl ObjectStore for ForeverPreconditionFailed {
    fn put(&self, key: &str, body: &[u8]) -> std::result::Result<String, StoreError> {
        self.mem.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> std::result::Result<CondPut, StoreError> {
        if key == self.key {
            return self.refuse();
        }
        self.mem.put_if_absent(key, body)
    }
    fn put_if_match(
        &self,
        key: &str,
        body: &[u8],
        etag: &str,
    ) -> std::result::Result<CondPut, StoreError> {
        if key == self.key {
            return self.refuse();
        }
        self.mem.put_if_match(key, body, etag)
    }
    fn get(&self, key: &str) -> std::result::Result<Option<Object>, StoreError> {
        self.mem.get(key)
    }
    fn stat(&self, key: &str) -> std::result::Result<Option<crate::store::ObjectStat>, StoreError> {
        self.mem.stat(key)
    }
    fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StoreError> {
        self.mem.list(prefix)
    }
    fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
        self.mem.delete(key)
    }
    fn stats(&self) -> crate::store::StatsSnap {
        self.mem.stats()
    }
}

/// A store that answers every PUT of one page with a 412 (and keeps changing its ETag): the
/// 412s and stats are answers, but no round stores anything. The seal must still end with
/// `Timeout` within about the retry deadline plus a few leases, without fencing, and keep
/// the page uploaded before it. A watchdog fails the test instead of letting it hang.
#[test]
fn a_seal_against_a_store_that_answers_412_forever_times_out() {
    let mem = Arc::new(MemStore::new());
    let store = Arc::new(ForeverPreconditionFailed {
        mem: mem.clone(),
        key: keys().page(1, 2),
        n: AtomicU64::new(0),
    });
    let deadline = Duration::from_millis(300);
    let lease = Duration::from_millis(50);
    let retry = RetryPolicy { deadline, ..fast() };
    let mut b = Bucket::create(store, keys(), retry, 1, "w1".into()).unwrap();
    b.set_lease(lease);
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let t = Instant::now();
        let r = b.seal_generation(pages_of(2, 9), 4096, 2);
        let _ = tx.send((r, t.elapsed(), b.degraded()));
    });
    let Ok((r, elapsed, degraded)) = rx.recv_timeout(Duration::from_secs(10)) else {
        panic!("the seal did not end within 10 s: unbounded against a store answering 412");
    };
    assert!(
        matches!(&r, Err(Error::Timeout(m)) if m.contains("no object stored")),
        "{r:?}"
    );
    let bound = deadline + 4 * lease + Duration::from_millis(500);
    assert!(elapsed < bound, "{elapsed:?} is past {bound:?}");
    assert!(!degraded, "not fenced, nothing owed");
    let k = keys();
    assert_eq!(mem.get(&k.page(1, 1)).unwrap().unwrap().body, vec![9; 16]);
    assert!(mem.get(&k.manifest(1)).unwrap().is_none());
    let head = Head::decode(&mem.get(&k.head()).unwrap().unwrap().body).unwrap();
    assert_eq!(head.generation, 1);
}

/// `append_log` writes the record kind its `LogFile` names: 1 for the `-wal`, 3 for the
/// `.db-log`, at consecutive seqs of one log.
#[test]
fn append_log_writes_the_kind_of_its_file() {
    let f = Fixture::new();
    let mut b = f.create_mvcc(fast(), 4);
    let a = b.append_log(LogFile::Log, syncs(1)).unwrap();
    let w = b.append_log(LogFile::Wal, syncs(2)).unwrap();
    assert_eq!((a.pos.seq, w.pos.seq), (1, 2));
    assert!(matches!(
        f.record(2, 1),
        Some(LogRecord::Extents { file: LogFile::Log, syncs: s, .. }) if s == syncs(1)
    ));
    assert!(matches!(
        f.record(2, 2),
        Some(LogRecord::Extents { file: LogFile::Wal, syncs: s, .. }) if s == syncs(2)
    ));
}

/// `settle_all`: Ok only with nothing left to settle. A record in doubt is settled at its
/// seq (`false`: no generation change); a published pending seal is adopted (`true`), and
/// the next append goes to the new generation.
#[test]
fn settle_all_settles_in_doubt_and_adopts_a_published_seal() {
    let short = RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    };
    let f = Fixture::new();
    let mut b = f.create_mvcc(short, 4);
    f.fault.only_keys_containing("/log/");
    f.fault.store_then_fail_next_puts(1);
    f.fault.fail_puts_after(0);
    assert!(b.append_log(LogFile::Log, syncs(1)).is_err());
    assert!(b.degraded());
    assert!(b.settle_all().is_err(), "still failing");
    f.fault.fail_puts_after(u64::MAX);
    assert_eq!(b.settle_all(), Ok(false));
    assert!(!b.degraded());
    assert_eq!(
        f.log_keys(2),
        [keys().log(2, 1)],
        "settled once, at its seq"
    );

    f.fault.only_keys_containing("/head");
    f.fault.store_then_fail_next_puts(1);
    f.fault.fail_puts_after(0);
    assert!(b.seal_generation(pages(1), 4096, 1).is_err());
    assert_eq!(f.stored_head().generation, 3);
    assert_eq!(
        b.settle_all(),
        Ok(true),
        "a published seal needs no PUT to adopt"
    );
    assert_eq!(b.head().generation, 3);
    assert!(!b.degraded());
    f.fault.fail_puts_after(u64::MAX);
    assert_eq!(
        b.append_log(LogFile::Log, syncs(2))
            .map(|a| a.pos)
            .map_err(|f| f.error),
        Ok(LogPos {
            generation: 3,
            seq: 1
        })
    );
}

/// A kind-3 record belongs to an MVCC bucket: a format-1 (WAL) bucket refuses it before any
/// request, and hands its syncs back.
#[test]
fn kind3_record_is_refused_in_a_format1_bucket() {
    let f = Fixture::new();
    let mut b = f.create(fast(), 4);
    let requests = f.fault.events().len();
    match b.append_log(LogFile::Log, syncs(1)) {
        Err(AppendFailure { error, unsent }) => {
            assert_eq!(
                error,
                Error::Unsupported("MVCC: a .db-log record in a WAL bucket")
            );
            assert_eq!(unsent, Some(syncs(1)));
        }
        Ok(a) => panic!("appended at {:?}", a.pos),
    }
    assert_eq!(f.fault.events().len(), requests, "a request was sent");
    assert_eq!(
        b.append_log(LogFile::Wal, syncs(2)).map(|a| a.pos.seq).ok(),
        Some(1)
    );
}
