use super::*;
use crate::config::Event;
use crate::format::encode_record;
use crate::format::LogFile;
use crate::format::{decode_record, Keys, LogRecord};
use crate::store::{
    CondPut, FaultStore, MemStore, Object, ObjectStore, RetryPolicy, StatsSnap, StoreError,
};
use std::time::Duration;

fn fast() -> RetryPolicy {
    RetryPolicy {
        deadline: Duration::from_secs(2),
        first_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(4),
    }
}

/// An MVCC bucket (format 2, generation 2, as `create_sealed` leaves it): kind-3 records go
/// only there.
fn mvcc_bucket_with(store: Arc<dyn ObjectStore>, retry: RetryPolicy) -> Bucket {
    let image = crate::create::Image {
        page_size: 8,
        pages: vec![(1, vec![0u8; 8])],
    };
    Bucket::create_sealed(store, Keys::new("db"), retry, 4, "w1".into(), image).unwrap()
}

fn bucket(store: Arc<dyn ObjectStore>) -> Bucket {
    bucket_with(store, fast())
}

/// A WAL bucket of format 1, which seals (`bucket::legacy`): the seal jobs are tested on it.
fn bucket_with(store: Arc<dyn ObjectStore>, retry: RetryPolicy) -> Bucket {
    let _legacy = crate::format::LegacySeal::on();
    Bucket::create(store, Keys::new("db"), retry, 4, "w1".into()).unwrap()
}

/// A WAL bucket of format 5, which switches generations.
fn packed_bucket(store: Arc<dyn ObjectStore>) -> Bucket {
    Bucket::create(store, Keys::new("db"), fast(), 4, "w1".into()).unwrap()
}

/// A 100 ms retry deadline, so failing uploads fail quickly.
fn short() -> RetryPolicy {
    RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    }
}

fn window(max_wait_ms: u64, max_commits: u32) -> Batching {
    Batching::Window {
        max_wait: Duration::from_millis(max_wait_ms),
        max_commits,
        max_bytes: 1 << 20,
    }
}

fn recorder() -> (EventHook, Arc<Mutex<Vec<Event>>>) {
    let seen: Arc<Mutex<Vec<Event>>> = Arc::default();
    let s = seen.clone();
    (
        Arc::new(move |e: &Event| s.lock().unwrap().push(e.clone())),
        seen,
    )
}

fn pos(generation: u64, seq: u64) -> LogPos {
    LogPos { generation, seq }
}

fn op(n: u64) -> Vec<Op> {
    vec![Op::Write {
        off: n * 8,
        bytes: n.to_le_bytes().to_vec(),
    }]
}

fn stored_ops(mem: &MemStore, pos: LogPos) -> Vec<Vec<Op>> {
    let body = mem
        .get(&Keys::new("db").log(pos.generation, pos.seq))
        .unwrap()
        .unwrap()
        .body;
    match decode_record(&body).unwrap() {
        LogRecord::Extents {
            writer_id, syncs, ..
        } => {
            assert_eq!(writer_id, "w1");
            syncs
        }
        r => panic!("unexpected record {r:?}"),
    }
}

#[test]
fn uploader_preserves_fifo_order() {
    let mem = Arc::new(MemStore::new());
    let up = Uploader::spawn(bucket(mem.clone()), None, None, Arc::default());
    let results: Vec<(u64, LogPos)> = (0..4u64)
        .map(|t| {
            let up = up.clone();
            std::thread::spawn(move || {
                (0..25u64)
                    .map(|i| {
                        let n = t * 100 + i;
                        (n, up.submit_log(op(n), Batching::Off).wait().unwrap())
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();

    let mut seqs: Vec<u64> = results.iter().map(|(_, p)| p.seq).collect();
    seqs.sort();
    assert_eq!(seqs, (1..=100).collect::<Vec<_>>());
    for (n, pos) in &results {
        assert_eq!(pos.generation, 1);
        assert_eq!(stored_ops(&mem, *pos), vec![op(*n)]);
    }
    assert_eq!(mem.list(&Keys::new("db").log_prefix(1)).unwrap().len(), 100);
    assert_eq!(up.counters().log_puts, 100);
}

#[test]
fn seal_is_ordered_after_queued_logs_and_reports() {
    let mem = Arc::new(MemStore::new());
    let seen: Arc<Mutex<Vec<Event>>> = Arc::default();
    let hook: EventHook = {
        let seen = seen.clone();
        Arc::new(move |e: &Event| seen.lock().unwrap().push(e.clone()))
    };
    let (trim_tx, trim_rx) = mpsc::channel();
    let up = Uploader::spawn(
        bucket(mem.clone()),
        Some(trim_tx),
        Some(hook),
        Arc::default(),
    );
    assert_eq!(up.generation(), 1);

    let tickets: Vec<Ticket> = (1..=3)
        .map(|n| up.submit_log(op(n), Batching::Off))
        .collect();
    let pages = vec![(1, vec![1u8; 32]), (2, vec![2u8; 32])];
    assert_eq!(up.seal_generation(pages, 32, 2), Ok(2));
    assert_eq!(up.generation(), 2);
    for (i, t) in tickets.into_iter().enumerate() {
        assert_eq!(
            t.wait(),
            Ok(LogPos {
                generation: 1,
                seq: i as u64 + 1
            })
        );
    }
    assert_eq!(
        up.submit_log(op(4), Batching::Off).wait(),
        Ok(LogPos {
            generation: 2,
            seq: 1
        })
    );
    assert_eq!(
        up.counters(),
        UploaderCounters {
            // The three records, the seal-start record, and one in generation 2.
            log_puts: 5,
            page_puts: 2,
            seals: 1,
            retries: 0
        }
    );

    let snap = trim_rx.try_recv().expect("a trim snapshot after the seal");
    assert_eq!((snap.head.generation, snap.head.manifest), (2, Some(1)));
    assert_eq!(snap.manifest.unwrap().page_count, 2);

    let seen = seen.lock().unwrap().clone();
    let kinds: Vec<String> = seen
        .iter()
        .map(|e| match e {
            Event::LogUploaded {
                generation,
                seq,
                commits,
                ..
            } => format!("log {generation}/{seq} {commits:?}"),
            Event::SealStarted { generation, pages } => format!("seal {generation} {pages}"),
            Event::SealFinished { generation, .. } => format!("sealed {generation}"),
            e => panic!("unexpected event {e:?}"),
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "log 1/1 [1]",
            "log 1/2 [2]",
            "log 1/3 [3]",
            // The seal-start record carries no commit.
            "log 1/4 []",
            "seal 1 2",
            "sealed 1",
            "log 2/1 [4]"
        ]
    );
    let Event::LogUploaded { bytes, .. } = &seen[0] else {
        unreachable!()
    };
    assert!(*bytes > 0);
}

#[test]
fn disconnected_trim_receiver_is_ignored() {
    let (trim_tx, trim_rx) = mpsc::channel();
    drop(trim_rx);
    let up = Uploader::spawn(
        bucket(Arc::new(MemStore::new())),
        Some(trim_tx),
        None,
        Arc::default(),
    );
    assert_eq!(up.seal_generation(vec![], 4096, 0), Ok(2));
    assert_eq!(up.seal_generation(vec![], 4096, 0), Ok(3));
}

#[test]
fn counters_count_retries() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let up = Uploader::spawn(bucket(fault.clone()), None, None, Arc::default());
    fault.fail_next_puts(2);
    assert!(up.submit_log(op(1), Batching::Off).wait().is_ok());
    let c = up.counters();
    assert_eq!((c.log_puts, c.retries), (1, 2));
}

#[test]
fn failed_seal_keeps_generation() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let up = Uploader::spawn(bucket(fault.clone()), None, None, Arc::default());
    fault.fail_next_puts_permanently(1);
    assert!(matches!(
        up.seal_generation(vec![(1, vec![0; 8])], 8, 1),
        Err(Error::UploadFailed(_))
    ));
    assert_eq!(up.generation(), 1);
    assert_eq!(up.counters().seals, 0);
}

#[test]
fn shutdown_drains_queue_then_refuses() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    fault.delay_puts(Duration::from_millis(5));
    let up = Uploader::spawn(bucket(fault.clone()), None, None, Arc::default());
    let tickets: Vec<Ticket> = (1..=10)
        .map(|n| up.submit_log(op(n), Batching::Off))
        .collect();
    up.shutdown();
    // Every queued job ran before the thread stopped.
    assert_eq!(mem.list(&Keys::new("db").log_prefix(1)).unwrap().len(), 10);
    for t in tickets {
        assert!(t.wait().is_ok());
    }
    let stopped = Error::UploadFailed("uploader stopped".into());
    assert_eq!(
        up.submit_log(op(11), Batching::Off).wait(),
        Err(stopped.clone())
    );
    assert_eq!(up.seal_generation(vec![], 4096, 0), Err(stopped));
    up.shutdown(); // idempotent
    assert_eq!(up.generation(), 1);
}

#[test]
fn dropping_the_last_handle_stops_the_thread() {
    let mem = Arc::new(MemStore::new());
    let up = Uploader::spawn(bucket(mem.clone()), None, None, Arc::default());
    let t = up.submit_log(op(1), Batching::Off);
    assert!(
        Arc::strong_count(&mem) > 1,
        "the thread's bucket holds the store"
    );
    drop(up);
    // Drop drained the queue and joined the thread, which dropped the bucket and its store.
    assert!(t.wait().is_ok());
    assert_eq!(Arc::strong_count(&mem), 1);
}

#[test]
fn tickets_from_one_thread_get_seqs_in_submission_order() {
    let mem = Arc::new(MemStore::new());
    let up = Uploader::spawn(bucket(mem.clone()), None, None, Arc::default());
    let tickets: Vec<Ticket> = (1..=50)
        .map(|n| up.submit_log(op(n), Batching::Off))
        .collect();
    for (i, t) in tickets.into_iter().enumerate() {
        let n = i as u64 + 1;
        let pos = t.wait().unwrap();
        assert_eq!(
            pos,
            LogPos {
                generation: 1,
                seq: n
            }
        );
        assert_eq!(stored_ops(&mem, pos), vec![op(n)]);
    }
}

#[test]
fn last_handle_dropped_on_the_uploader_thread_does_not_join_itself() {
    let mem = Arc::new(MemStore::new());
    let slot: Arc<Mutex<Option<Arc<Uploader>>>> = Arc::default();
    let hook: EventHook = {
        let slot = slot.clone();
        Arc::new(move |_: &Event| {
            let last = slot.lock().unwrap().take();
            drop(last); // the final Arc, on the uploader thread
        })
    };
    let up = Uploader::spawn(bucket(mem.clone()), None, Some(hook), Arc::default());
    *slot.lock().unwrap() = Some(up.clone());
    let t = up.submit_log(op(1), Batching::Off);
    drop(up);
    assert!(t.wait().is_ok(), "the uploader thread must not panic");
    // The thread exits on its own once its queue is closed and drained.
    let deadline = Instant::now() + Duration::from_secs(2);
    while Arc::strong_count(&mem) > 1 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(Arc::strong_count(&mem), 1);
}

#[test]
fn window_commits_share_one_put_off_commit_flushes_them() {
    let mem = Arc::new(MemStore::new());
    let (hook, seen) = recorder();
    let u = Uploader::spawn(bucket(mem.clone()), None, Some(hook), Arc::default());
    let w = Batching::Window {
        max_wait: Duration::from_secs(10),
        max_commits: 64,
        max_bytes: 1 << 20,
    };
    let t1 = u.submit_log(op(1), w);
    let t2 = u.submit_log(op(2), w);
    let t3 = u.submit_log(op(3), Batching::Off);
    assert_eq!((t1.commit, t2.commit, t3.commit), (1, 2, 3));
    let p = t3.wait().unwrap();
    assert_eq!(t1.wait().unwrap(), p);
    assert_eq!(t2.wait().unwrap(), p);
    assert_eq!(stored_ops(&mem, p), vec![op(1), op(2), op(3)]);
    let s = u.stats().snapshot();
    assert_eq!((s.log_puts, s.commits_uploaded), (1, 3));
    assert!(s.log_put_bytes > 0);
    assert_eq!(s.commits_per_log_put, 3.0);
    let seen = seen.lock().unwrap().clone();
    assert!(
        matches!(
            seen.as_slice(),
            [
                Event::BatchWaiting { commits: 1 },
                Event::BatchWaiting { commits: 2 },
                Event::LogUploaded { generation: 1, seq: 1, commits, .. },
            ] if commits == &[1, 2, 3]
        ),
        "{seen:?}"
    );
}

#[test]
fn window_flushes_after_max_wait() {
    let mem = Arc::new(MemStore::new());
    let u = Uploader::spawn(bucket(mem.clone()), None, None, Arc::default());
    let w = Batching::Window {
        max_wait: Duration::from_millis(30),
        max_commits: 64,
        max_bytes: 1 << 20,
    };
    let t = u.submit_log(op(1), w);
    let submitted = t.submitted;
    let p = t.wait().unwrap();
    let waited = submitted.elapsed();
    assert!(waited >= Duration::from_millis(30), "{waited:?}");
    assert!(
        waited < Duration::from_millis(30) + Duration::from_secs(1),
        "{waited:?}"
    );
    assert_eq!(stored_ops(&mem, p), vec![op(1)]);
}

#[test]
fn failed_window_flush_fails_every_waiter_then_lands_in_order() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let u = Uploader::spawn(
        bucket_with(fault.clone(), short()),
        None,
        None,
        Arc::default(),
    );
    fault.fail_puts_after(0);
    let tickets: Vec<Ticket> = (1..=3)
        .map(|n| u.submit_log(op(n), window(10_000, 3)))
        .collect();
    for t in tickets {
        assert!(t.wait().is_err());
    }
    assert!(u.degraded());

    fault.fail_puts_after(u64::MAX);
    let p = u.submit_log(op(4), Batching::Off).wait().unwrap();
    assert!(
        p.seq <= 2,
        "one record, or the settled one and a new one: {p:?}"
    );
    let log: Vec<Vec<Op>> = (1..=p.seq)
        .flat_map(|s| stored_ops(&mem, pos(1, s)))
        .collect();
    assert_eq!(log, vec![op(1), op(2), op(3), op(4)]);
    assert!(!u.degraded());
}

#[test]
fn failed_off_flush_drops_the_off_sync_but_keeps_window_syncs() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let u = Uploader::spawn(
        bucket_with(fault.clone(), short()),
        None,
        None,
        Arc::default(),
    );
    fault.fail_puts_after(0);
    // In doubt at log/1/1.
    assert!(u.submit_log(op(0), Batching::Off).wait().is_err());
    let t1 = u.submit_log(op(1), window(10_000, 64));
    // [op1, op2] is never built: settling op(0) fails first, so both are handed back.
    let t2 = u.submit_log(op(2), Batching::Off);
    assert!(t1.wait().is_err());
    assert!(t2.wait().is_err());
    assert!(u.degraded());

    fault.fail_puts_after(u64::MAX);
    u.submit_log(op(3), Batching::Off).wait().unwrap();
    // An idle retry may land op(1) on its own before op(3) arrives, or op(3) carries it.
    assert_eq!(
        logged_ops(&mem, 1),
        vec![op(0), op(1), op(3)],
        "op(1) owed, op(2) dropped"
    );
    assert!(!u.degraded());
}

/// Every sync in generation `generation`'s log, in seq order.
fn logged_ops(mem: &MemStore, generation: u64) -> Vec<Vec<Op>> {
    let mut keys = mem.list(&Keys::new("db").log_prefix(generation)).unwrap();
    keys.sort();
    keys.iter()
        .flat_map(|k| {
            let seq = k.rsplit('/').next().unwrap().parse().unwrap();
            stored_ops(mem, pos(generation, seq))
        })
        .collect()
}

#[test]
fn owed_retries_are_not_held_back_by_a_long_window() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let u = Uploader::spawn(
        bucket_with(fault.clone(), short()),
        None,
        None,
        Arc::default(),
    );
    fault.fail_puts_after(0);
    // op(0) in doubt; settling it fails first, so op(1) is handed back and owed.
    assert!(u.submit_log(op(0), Batching::Off).wait().is_err());
    let t1 = u.submit_log(op(1), window(10_000, 64));
    assert!(u.submit_log(op(2), Batching::Off).wait().is_err());
    assert!(t1.wait().is_err());
    // A Window sync with a long max_wait joins the batch while op(1) is owed.
    let t3 = u.submit_log(op(3), window(60_000, 64));
    fault.fail_puts_after(u64::MAX);
    let started = Instant::now();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || tx.send(t3.wait()));
    let r = rx
        .recv_timeout(defaults::MAX_BACKOFF * 3)
        .expect("the owed retry did not wait for the window");
    assert!(r.is_ok(), "{r:?}");
    assert!(started.elapsed() < defaults::MAX_BACKOFF * 3);
    assert_eq!(logged_ops(&mem, 1), vec![op(0), op(1), op(3)]);
    assert!(!u.degraded());
}

#[test]
fn seal_flushes_waiting_batch_first_and_fails_if_it_cannot() {
    let keys = Keys::new("db");
    let pages = || vec![(1, vec![1u8; 32])];
    let is_page_put = |e: &String| e.starts_with("put") && e.contains("pages/");

    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let u = Uploader::spawn(bucket(fault.clone()), None, None, Arc::default());
    let t = u.submit_log(op(1), window(10_000, 64));
    let started = Instant::now();
    assert_eq!(u.seal_generation(pages(), 32, 1), Ok(2));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the seal did not wait out max_wait"
    );
    assert_eq!(t.wait(), Ok(pos(1, 1)));
    assert_eq!(stored_ops(&mem, pos(1, 1)), vec![op(1)]);
    let ev = fault.events();
    let log = ev
        .iter()
        .position(|e| *e == format!("put_if_absent {}", keys.log(1, 1)))
        .expect("the log record");
    let page = ev.iter().position(is_page_put).expect("a page put");
    assert!(log < page, "{ev:?}");

    // Log PUTs fail: the seal fails before any page is uploaded.
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let u = Uploader::spawn(
        bucket_with(fault.clone(), short()),
        None,
        None,
        Arc::default(),
    );
    fault.only_keys_containing("/log/");
    fault.fail_puts_after(0);
    let t = u.submit_log(op(1), window(10_000, 64));
    assert!(u.seal_generation(pages(), 32, 1).is_err());
    assert!(t.wait().is_err());
    assert_eq!(u.generation(), 1);
    let ev = fault.events();
    assert!(!ev.iter().any(is_page_put), "{ev:?}");
}

#[test]
fn seal_refuses_while_window_syncs_are_owed() {
    // A seal's head write landed but every reply failed: the head may be published, so the
    // next append is refused with its syncs handed back, and only a seal may adopt the head.
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let u = Uploader::spawn(
        bucket_with(fault.clone(), short()),
        None,
        None,
        Arc::default(),
    );
    fault.only_keys_containing("/head");
    fault.store_then_fail_next_puts(1);
    fault.fail_puts_after(0);
    assert!(u.seal_generation(vec![], 4096, 0).is_err());
    assert!(u.degraded());
    fault.fail_puts_after(u64::MAX);

    // A Window sync owed in generation 1 would fall outside the published manifest if the
    // seal adopted the head now, so the seal fails with the flush's error instead.
    let t = u.submit_log(op(1), window(10_000, 64));
    let sealed = u.seal_generation(vec![], 4096, 0);
    assert!(matches!(sealed, Err(Error::UploadFailed(_))), "{sealed:?}");
    assert!(t.wait().is_err());
    assert_eq!(u.generation(), 1, "the published head was not adopted");
    assert!(u.degraded());
}

#[test]
fn shutdown_flushes_waiting_batch() {
    let mem = Arc::new(MemStore::new());
    let u = Uploader::spawn(bucket(mem.clone()), None, None, Arc::default());
    let t = u.submit_log(op(1), window(10_000, 64));
    let started = Instant::now();
    u.shutdown();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown did not wait out max_wait"
    );
    let p = t.wait().unwrap();
    assert_eq!(stored_ops(&mem, p), vec![op(1)]);
}

#[test]
fn fence_fails_every_waiting_window_commit_and_later_ones() {
    let mem = Arc::new(MemStore::new());
    let u = Uploader::spawn(bucket(mem.clone()), None, None, Arc::default());
    let foreign = encode_record(&LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: "w2".into(),
        syncs: vec![],
        next_generation: None,
    });
    mem.put(&Keys::new("db").log(1, 1), &foreign).unwrap();
    let tickets: Vec<Ticket> = (1..=3)
        .map(|n| u.submit_log(op(n), window(10_000, 3)))
        .collect();
    for t in tickets {
        assert!(matches!(t.wait(), Err(Error::Fenced(_))));
    }
    assert!(u.degraded());
    // Refused before any PUT (its syncs come back unsent), yet fenced: nothing is owed.
    assert!(matches!(
        u.submit_log(op(4), window(10_000, 1)).wait(),
        Err(Error::Fenced(_))
    ));
    assert!(matches!(
        u.submit_log(op(5), Batching::Off).wait(),
        Err(Error::Fenced(_))
    ));
    assert!(matches!(
        u.seal_generation(vec![], 4096, 0),
        Err(Error::Fenced(_))
    ));
}

/// A MemStore whose head GETs fail transiently while `failing` is set, and that counts the
/// log PUTs it is sent.
#[derive(Default)]
struct HeadGetsFail {
    inner: MemStore,
    failing: AtomicBool,
    log_puts: AtomicU64,
    head_gets: AtomicU64,
}

impl ObjectStore for HeadGetsFail {
    fn put(&self, key: &str, body: &[u8]) -> std::result::Result<String, StoreError> {
        self.inner.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> std::result::Result<CondPut, StoreError> {
        if key.contains("/log/") {
            self.log_puts.fetch_add(1, SeqCst);
        }
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
        if key.ends_with("/head") {
            self.head_gets.fetch_add(1, SeqCst);
        }
        if key.ends_with("/head") && self.failing.load(SeqCst) {
            return Err(StoreError::Transient("injected head GET failure".into()));
        }
        self.inner.get(key)
    }
    fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StoreError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
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
fn uploader_is_degraded_while_window_syncs_are_owed() {
    // With a lease of 0 every append GETs the head first (`check_lease`). While
    // those GETs fail, the record is never built: no PUT is sent, so the bucket has nothing
    // to settle (not fenced, nothing in doubt, no seal pending) and is not degraded itself.
    // Only the owed Window sync makes the uploader degraded.
    let store = Arc::new(HeadGetsFail::default());
    let mut b = bucket_with(store.clone(), short());
    b.set_lease(Duration::ZERO);
    let u = Uploader::spawn(b, None, None, Arc::default());
    store.failing.store(true, SeqCst);
    let t = u.submit_log(op(1), window(10, 64));
    assert!(matches!(t.wait(), Err(Error::Timeout(_))));
    assert_eq!(store.log_puts.load(SeqCst), 0, "no record was built");
    assert!(u.degraded(), "a Window sync is owed");
    // It stays degraded while the owed sync keeps failing (retried every MAX_BACKOFF).
    std::thread::sleep(defaults::MAX_BACKOFF + Duration::from_millis(500));
    assert!(u.degraded(), "the owed sync still has not landed");
    assert_eq!(store.log_puts.load(SeqCst), 0);

    store.failing.store(false, SeqCst);
    let until = Instant::now() + defaults::MAX_BACKOFF * 3;
    while u.degraded() {
        assert!(Instant::now() < until, "the owed sync never landed");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(logged_ops(&store.inner, 1), vec![op(1)]);
}

/// Wait (at most `within`) until the uploader no longer reports the bucket failing.
fn wait_not_failing(u: &Uploader, within: Duration) {
    let until = Instant::now() + within;
    while let Some(why) = u.bucket_failing() {
        assert!(Instant::now() < until, "still failing: {why}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// An Off sync that fails before its record is built (the lease check's head GET fails)
/// leaves nothing owed and nothing in doubt: not degraded, but "last bucket operation
/// failed". On an idle database the uploader probes the bucket (one head GET per wake)
/// until one succeeds, so maintenance is not skipped forever after the bucket recovers.
#[test]
fn last_failed_is_probed_until_the_bucket_answers() {
    let store = Arc::new(HeadGetsFail::default());
    let mut b = bucket_with(store.clone(), short());
    b.set_lease(Duration::ZERO);
    let u = Uploader::spawn(b, None, None, Arc::default());
    store.failing.store(true, SeqCst);
    assert!(matches!(
        u.submit_log(op(1), Batching::Off).wait(),
        Err(Error::Timeout(_))
    ));
    assert!(!u.degraded(), "nothing owed, nothing in doubt");
    assert_eq!(u.bucket_failing(), Some("last bucket operation failed"));
    let gets = store.head_gets.load(SeqCst);
    // While the head GETs keep failing, the probes (head GETs) fail too.
    std::thread::sleep(defaults::MAX_BACKOFF + Duration::from_millis(500));
    assert_eq!(u.bucket_failing(), Some("last bucket operation failed"));
    assert!(
        store.head_gets.load(SeqCst) > gets,
        "no probe was sent while failing"
    );

    store.failing.store(false, SeqCst);
    wait_not_failing(&u, defaults::MAX_BACKOFF * 3);
    assert_eq!(store.log_puts.load(SeqCst), 0, "a probe writes nothing");
}

/// A failed seal (a page PUT refused) on an idle bucket: nothing owed, not degraded, and
/// the lease check not due. The uploader still sends a head GET before it clears the
/// failure; it does not take "nothing to settle" for a working bucket.
#[test]
fn a_failed_seal_on_an_idle_bucket_clears_only_after_a_head_get() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let u = Uploader::spawn(bucket(fault.clone()), None, None, Arc::default());
    fault.only_keys_containing("/pages/");
    fault.fail_next_puts_permanently(1);
    assert!(u.seal_generation(vec![(1, vec![0; 8])], 8, 1).is_err());
    assert!(!u.degraded(), "nothing in doubt: only a page PUT failed");
    assert_eq!(u.bucket_failing(), Some("last bucket operation failed"));
    let failed_at = fault.events().len();
    wait_not_failing(&u, defaults::MAX_BACKOFF * 3);
    let head = Keys::new("db").head();
    let after: Vec<String> = fault.events().split_off(failed_at);
    assert!(
        after.iter().any(|e| *e == format!("get {head}")),
        "cleared without a request: {after:?}"
    );
    assert!(
        !after.iter().any(|e| e.starts_with("put")),
        "a probe writes nothing: {after:?}"
    );
}

/// A kind-3 submission that was never sent comes back to its caller (`LogFailure::unsent`)
/// instead of being dropped like an `Off` WAL sync, and is neither owed nor degraded. One
/// in doubt comes back with `None` (the bucket settles it), and `settle_log` is Ok only
/// once it has.
#[test]
fn log_file_unsent_ops_come_back_to_the_caller() {
    let store = Arc::new(HeadGetsFail::default());
    let mut b = mvcc_bucket_with(store.clone(), short());
    b.set_lease(Duration::ZERO);
    let u = Uploader::spawn(b, None, None, Arc::default());
    store.failing.store(true, SeqCst);
    let f = u
        .submit_log_file(LogFile::Log, op(1), 1)
        .wait()
        .unwrap_err();
    assert!(matches!(f.error, Error::Timeout(_)), "{f:?}");
    assert_eq!(f.unsent, Some(op(1)), "handed back, not dropped");
    assert!(!u.degraded(), "nothing owed, nothing in doubt");
    assert!(u.settle_log().is_err(), "the head GET still fails");
    store.failing.store(false, SeqCst);
    assert_eq!(u.settle_log(), Ok(2));
    let p = u.submit_log_file(LogFile::Log, op(1), 1).wait().unwrap();
    assert_eq!(p, pos(2, 1));
    let body = store
        .inner
        .get(&Keys::new("db").log(2, 1))
        .unwrap()
        .unwrap()
        .body;
    assert!(matches!(
        decode_record(&body).unwrap(),
        LogRecord::Extents { file: LogFile::Log, syncs, .. } if syncs == vec![op(1)]
    ));

    // In doubt: every PUT fails, so the record may be anywhere; `None`, degraded, and
    // `settle_log` fails until the bucket settles it.
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let u = Uploader::spawn(
        mvcc_bucket_with(fault.clone(), short()),
        None,
        None,
        Arc::default(),
    );
    fault.only_keys_containing("/log/");
    fault.fail_puts_after(0);
    let f = u
        .submit_log_file(LogFile::Log, op(2), 1)
        .wait()
        .unwrap_err();
    assert_eq!(
        f.unsent, None,
        "in doubt: never sent again in another record"
    );
    assert!(u.degraded());
    assert!(u.settle_log().is_err());
    fault.fail_puts_after(u64::MAX);
    assert_eq!(u.settle_log(), Ok(2));
    assert!(!u.degraded());
    assert_eq!(stored_ops(&mem, pos(2, 1)), vec![op(2)]);
}

/// `settle_log` adopts a seal whose head PUT stored but whose reply was lost: the uploader
/// moves to the new generation, and the next append goes there.
#[test]
fn settle_log_adopts_a_published_pending_seal() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let (trim_tx, trim_rx) = mpsc::channel();
    let u = Uploader::spawn(
        mvcc_bucket_with(fault.clone(), short()),
        Some(trim_tx),
        None,
        Arc::default(),
    );
    fault.only_keys_containing("/head");
    fault.store_then_fail_next_puts(1);
    fault.fail_puts_after(0);
    assert!(u.seal_generation(vec![(1, vec![1; 8])], 8, 1).is_err());
    assert!(u.degraded());
    fault.fail_puts_after(u64::MAX);
    assert_eq!(u.settle_log(), Ok(3));
    assert_eq!(u.generation(), 3);
    assert!(!u.degraded());
    assert_eq!(
        trim_rx.try_recv().unwrap().head.generation,
        3,
        "the trimmer is told"
    );
    let p = u.submit_log_file(LogFile::Log, op(3), 1).wait().unwrap();
    assert_eq!(p, pos(3, 1));
}

/// A generation switch runs behind every log record queued before it, wakes the trimmer, and
/// reports `GenerationSwitched`; it uploads no page. Its `buckwal.generation_switch` span says
/// `switched`.
#[test]
fn switch_is_ordered_after_queued_logs_and_reports() {
    let mem = Arc::new(MemStore::new());
    let seen: Arc<Mutex<Vec<Event>>> = Arc::default();
    let hook: EventHook = {
        let seen = seen.clone();
        Arc::new(move |e: &Event| seen.lock().unwrap().push(e.clone()))
    };
    let (trim_tx, trim_rx) = mpsc::channel();
    // The uploader thread reports to the subscriber of the thread that spawned it.
    let cap = crate::telemetry::testing::Capture::new();
    let up = tracing::subscriber::with_default(cap.subscriber(), || {
        Uploader::spawn(
            packed_bucket(mem.clone()),
            Some(trim_tx),
            Some(hook),
            Arc::default(),
        )
    });
    let tickets: Vec<Ticket> = (1..=3)
        .map(|n| up.submit_log(op(n), Batching::Off))
        .collect();
    let switched = up.switch_generation();
    assert_eq!(switched, Ok(2));
    assert_eq!(up.generation(), 2);
    for (i, t) in tickets.into_iter().enumerate() {
        assert_eq!(
            t.wait(),
            Ok(LogPos {
                generation: 1,
                seq: i as u64 + 1
            })
        );
    }
    assert_eq!(
        up.submit_log(op(4), Batching::Off).wait(),
        Ok(LogPos {
            generation: 2,
            seq: 1
        })
    );
    assert_eq!(
        up.counters(),
        UploaderCounters {
            // The three records, the seal-start record, and one in generation 2.
            log_puts: 5,
            page_puts: 0,
            seals: 1,
            retries: 0
        }
    );
    let snap = trim_rx
        .try_recv()
        .expect("a trim snapshot after the switch");
    assert_eq!((snap.head.generation, snap.head.manifest), (2, None));
    assert!(snap.manifest.is_none());
    let spans = cap.spans();
    let span = spans.last_named("buckwal.generation_switch");
    assert_eq!(span.field("buckwal.generation"), Some("1"));
    assert_eq!(span.field("buckwal.next_generation"), Some("2"));
    assert_eq!(span.field("buckwal.seq"), Some("4"));
    assert_eq!(span.field("buckwal.outcome"), Some("switched"));
    assert!(span.field("buckwal.elapsed_ms").is_some());
    let kinds: Vec<String> = seen
        .lock()
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            Event::LogUploaded {
                generation, seq, ..
            } => Some(format!("log {generation}/{seq}")),
            Event::GenerationSwitched { from, to, .. } => Some(format!("switched {from}->{to}")),
            Event::SealStarted { .. } | Event::SealFinished { .. } => Some("seal".into()),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "log 1/1",
            "log 1/2",
            "log 1/3",
            "log 1/4",
            "switched 1->2",
            "log 2/1"
        ]
    );
}

/// A switch whose head move another writer beat ends `fenced` on its span.
#[test]
fn a_fenced_switch_says_fenced_on_its_span() {
    let mem = Arc::new(MemStore::new());
    let cap = crate::telemetry::testing::Capture::new();
    let up = tracing::subscriber::with_default(cap.subscriber(), || {
        Uploader::spawn(packed_bucket(mem.clone()), None, None, Arc::default())
    });
    up.submit_log(op(1), Batching::Off).wait().unwrap();
    let k = Keys::new("db");
    crate::bucket::take_over(&*mem, &k, &fast(), 1, "w2").unwrap();
    let r = up.switch_generation();
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
    let spans = cap.spans();
    let span = spans.last_named("buckwal.generation_switch");
    assert_eq!(span.field("buckwal.outcome"), Some("fenced"));
    assert_eq!(up.generation(), 1);
}

/// A switch whose head move fails (the store refuses every PUT) ends `upload_failed`.
#[test]
fn a_failed_switch_says_upload_failed_on_its_span() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let b = Bucket::create(fault.clone(), Keys::new("db"), short(), 4, "w1".into()).unwrap();
    let cap = crate::telemetry::testing::Capture::new();
    let up = tracing::subscriber::with_default(cap.subscriber(), || {
        Uploader::spawn(b, None, None, Arc::default())
    });
    fault.only_keys_containing("/head");
    fault.fail_puts_after(0);
    let r = up.switch_generation();
    assert!(r.is_err() && !matches!(r, Err(Error::Fenced(_))), "{r:?}");
    let spans = cap.spans();
    let span = spans.last_named("buckwal.generation_switch");
    assert_eq!(span.field("buckwal.outcome"), Some("upload_failed"));
    assert_eq!(up.generation(), 1);
}
