use super::*;
use crate::config::Event;
use crate::format::{decode_record, Keys, LogRecord};
use crate::store::{FaultStore, MemStore, ObjectStore, RetryPolicy};
use std::time::Duration;

fn fast() -> RetryPolicy {
    RetryPolicy {
        deadline: Duration::from_secs(2),
        first_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(4),
    }
}

fn bucket(store: Arc<dyn ObjectStore>) -> Bucket {
    Bucket::create(store, Keys::new("db"), fast(), 4, "w1".into()).unwrap()
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
        LogRecord::Extents { writer_id, syncs } => {
            assert_eq!(writer_id, "w1");
            syncs
        }
        r => panic!("unexpected record {r:?}"),
    }
}

#[test]
fn uploader_preserves_fifo_order() {
    let mem = Arc::new(MemStore::new());
    let up = Uploader::spawn(bucket(mem.clone()), None, None);
    let results: Vec<(u64, LogPos)> = (0..4u64)
        .map(|t| {
            let up = up.clone();
            std::thread::spawn(move || {
                (0..25u64)
                    .map(|i| {
                        let n = t * 100 + i;
                        (n, up.submit_log(op(n)).wait().unwrap())
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
    let up = Uploader::spawn(bucket(mem.clone()), Some(trim_tx), Some(hook));
    assert_eq!(up.generation(), 1);

    let tickets: Vec<Ticket> = (1..=3).map(|n| up.submit_log(op(n))).collect();
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
        up.submit_log(op(4)).wait(),
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
    assert!(snap.at <= Instant::now());

    let seen = seen.lock().unwrap().clone();
    let kinds: Vec<String> = seen
        .iter()
        .map(|e| match e {
            Event::LogUploaded {
                generation, seq, ..
            } => format!("log {generation}/{seq}"),
            Event::SealStarted { generation, pages } => format!("seal {generation} {pages}"),
            Event::SealFinished { generation, .. } => format!("sealed {generation}"),
            Event::Trimmed { .. } => "trimmed".into(),
        })
        .collect();
    assert_eq!(
        kinds,
        ["log 1/1", "log 1/2", "log 1/3", "log 1/4", "seal 1 2", "sealed 1", "log 2/1"]
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
    let up = Uploader::spawn(bucket(Arc::new(MemStore::new())), Some(trim_tx), None);
    assert_eq!(up.seal_generation(vec![], 4096, 0), Ok(2));
    assert_eq!(up.seal_generation(vec![], 4096, 0), Ok(3));
}

#[test]
fn counters_count_retries() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let up = Uploader::spawn(bucket(fault.clone()), None, None);
    fault.fail_next_puts(2);
    assert!(up.submit_log(op(1)).wait().is_ok());
    let c = up.counters();
    assert_eq!((c.log_puts, c.retries), (1, 2));
}

#[test]
fn failed_seal_keeps_generation() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let up = Uploader::spawn(bucket(fault.clone()), None, None);
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
    let up = Uploader::spawn(bucket(fault.clone()), None, None);
    let tickets: Vec<Ticket> = (1..=10).map(|n| up.submit_log(op(n))).collect();
    up.shutdown();
    // Every queued job ran before the thread stopped.
    assert_eq!(mem.list(&Keys::new("db").log_prefix(1)).unwrap().len(), 10);
    for t in tickets {
        assert!(t.wait().is_ok());
    }
    let stopped = Error::UploadFailed("uploader stopped".into());
    assert_eq!(up.submit_log(op(11)).wait(), Err(stopped.clone()));
    assert_eq!(up.seal_generation(vec![], 4096, 0), Err(stopped));
    up.shutdown(); // idempotent
    assert_eq!(up.generation(), 1);
}

#[test]
fn dropping_the_last_handle_stops_the_thread() {
    let mem = Arc::new(MemStore::new());
    let up = Uploader::spawn(bucket(mem.clone()), None, None);
    let t = up.submit_log(op(1));
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
    let up = Uploader::spawn(bucket(mem.clone()), None, None);
    let tickets: Vec<Ticket> = (1..=50).map(|n| up.submit_log(op(n))).collect();
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
    let up = Uploader::spawn(bucket(mem.clone()), None, Some(hook));
    *slot.lock().unwrap() = Some(up.clone());
    let t = up.submit_log(op(1));
    drop(up);
    assert!(t.wait().is_ok(), "the uploader thread must not panic");
    // The thread exits on its own once its queue is closed and drained.
    let deadline = Instant::now() + Duration::from_secs(2);
    while Arc::strong_count(&mem) > 1 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(Arc::strong_count(&mem), 1);
}
