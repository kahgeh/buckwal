//! The generation switch of a format 5 bucket (spec "The writer"): the seal-start record of the
//! old generation, which names the next one, then the head move with `If-Match`. No page and
//! no manifest goes up.
use super::*;
use crate::format::FORMAT_V5;

/// The record a switch of `writer_id` to `next` writes.
fn switch_record(writer_id: &str, next: u64) -> LogRecord {
    LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: writer_id.into(),
        syncs: vec![],
        next_generation: Some(next),
    }
}

fn no_pages_and_no_manifests(f: &Fixture) {
    let k = keys();
    assert_eq!(f.mem.list(&k.page_root()).unwrap(), Vec::<String>::new());
    assert_eq!(
        f.mem.list(&k.manifest_root()).unwrap(),
        Vec::<String>::new()
    );
}

fn pos(f: std::result::Result<Appended, AppendFailure>) -> Result<LogPos> {
    f.map(|a| a.pos).map_err(|f| f.error)
}

#[test]
fn switch_uploads_no_pages_and_no_manifest() {
    let f = Fixture::new();
    let mut b = f.create_packed(fast(), 4);
    assert_eq!(f.stored_head().format, FORMAT_V5);
    let seen: Arc<Mutex<Vec<Event>>> = Arc::default();
    {
        let seen = seen.clone();
        b.set_event_hook(Some(Arc::new(move |e: &Event| {
            seen.lock().unwrap().push(e.clone())
        })));
    }
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    let before = f.fault.events().len();
    assert_eq!(b.switch_generation(), Ok(2));
    let k = keys();
    assert_eq!(
        f.fault.events()[before..],
        [
            format!("put_if_absent {}", k.log(1, 2)),
            format!("put_if_match {}", k.head()),
        ]
    );
    no_pages_and_no_manifests(&f);
    let head = f.stored_head();
    assert_eq!(
        (head.format, head.generation, head.manifest),
        (FORMAT_V5, 2, None)
    );
    assert_eq!(b.head(), &head);
    assert_eq!(b.manifest(), None);
    assert_eq!(b.counters().page_puts.load(Relaxed), 0);
    assert_eq!(b.counters().seals.load(Relaxed), 1);
    let events: Vec<String> = seen
        .lock()
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            Event::GenerationSwitched { from, to, .. } => Some(format!("{from}->{to}")),
            Event::SealStarted { .. } | Event::SealFinished { .. } => Some("seal".into()),
            _ => None,
        })
        .collect();
    assert_eq!(events, ["1->2"]);
}

/// The three steps in order: the seal-start record (naming `seal_target()`), the head move, then
/// the next record under the new generation. After a takeover the target is the floor, and the
/// generation before it is skipped (no log).
#[test]
fn switch_writes_seal_start_then_head_then_log_record() {
    let f = Fixture::new();
    let mut b = f.create_packed(fast(), 4);
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    assert_eq!(b.seal_target(), 2);
    let before = f.fault.events().len();
    assert_eq!(b.switch_generation(), Ok(2));
    assert_eq!(
        pos(b.append_log(LogFile::Wal, syncs(2))),
        Ok(LogPos {
            generation: 2,
            seq: 1
        })
    );
    let k = keys();
    assert_eq!(
        f.fault.events()[before..],
        [
            format!("put_if_absent {}", k.log(1, 2)),
            format!("put_if_match {}", k.head()),
            format!("put_if_absent {}", k.log(2, 1)),
        ]
    );
    assert_eq!(f.record(1, 2), Some(switch_record("w1", 2)));
    assert_eq!(f.record(2, 1), Some(ours(2)));

    // After a takeover: the new writer stays in generation 1, after its fence; its switch
    // names the floor (3), and generation 2 has no log.
    let f = Fixture::new();
    let (_a, mut b) = taken_over_with(&f, f.create_packed(fast(), 4));
    b.append_fence().unwrap();
    b.append_log(LogFile::Wal, syncs(3)).unwrap();
    assert_eq!(b.head().generation, 1);
    assert_eq!(b.seal_target(), 3);
    assert_eq!(b.switch_generation(), Ok(3));
    assert_eq!(f.record(1, 4), Some(switch_record("w2", 3)));
    assert_eq!(f.stored_head().generation, 3);
    assert_eq!(f.stored_head().next_generation_floor, Some(3));
    assert!(f.log_keys(2).is_empty());
    assert_eq!(
        pos(b.append_log(LogFile::Wal, syncs(4))),
        Ok(LogPos {
            generation: 3,
            seq: 1
        })
    );
    no_pages_and_no_manifests(&f);
}

/// The pending switch covers the head move only. A head PUT whose reply is lost but which
/// landed is found in the same call (412, then the GET finds our head). One whose every reply
/// is lost leaves the switch pending: appends are refused, and the next switch adopts the
/// published head with one GET and no upload.
#[test]
fn switch_with_lost_head_reply_adopts() {
    let f = Fixture::new();
    let mut b = f.create_packed(fast(), 4);
    f.fault.only_keys_containing("/head");
    f.fault.store_then_fail_next_puts(1);
    assert_eq!(b.switch_generation(), Ok(2));
    assert_eq!(f.stored_head().generation, 2);
    assert_eq!(b.head(), &f.stored_head());
    assert_eq!(b.switch_generation(), Ok(3));

    let short = RetryPolicy {
        deadline: Duration::from_millis(100),
        ..fast()
    };
    let f = Fixture::new();
    let mut b = f.create_packed(short, 4);
    b.append_log(LogFile::Wal, syncs(1)).unwrap();
    f.fault.only_keys_containing("/head");
    f.fault.store_then_fail_next_puts(1);
    f.fault.fail_puts_after(0);
    assert!(matches!(b.switch_generation(), Err(Error::Timeout(_))));
    assert_eq!(f.stored_head().generation, 2);
    assert_eq!(b.head().generation, 1);
    assert!(b.degraded());
    assert!(matches!(
        pos(b.append_log(LogFile::Wal, syncs(2))),
        Err(Error::UploadFailed(_))
    ));
    assert_eq!(f.log_keys(1), [keys().log(1, 1), keys().log(1, 2)]);
    f.fault.fail_puts_after(u64::MAX);
    let before = f.fault.events().len();
    assert_eq!(b.switch_generation(), Ok(2));
    assert_eq!(
        f.fault.events()[before..],
        [format!("get {}", keys().head())],
        "adopted without an upload"
    );
    assert_eq!(b.head(), &f.stored_head());
    assert!(!b.degraded());
    assert_eq!(
        pos(b.append_log(LogFile::Wal, syncs(2))),
        Ok(LogPos {
            generation: 2,
            seq: 1
        })
    );
    no_pages_and_no_manifests(&f);
}

/// A takeover wrote the head, but its fence is not there yet: the old writer's seal-start
/// record lands, its head move gets a 412, and it is fenced before its next record. The fence
/// then steps over the seal-start record.
#[test]
fn fenced_writer_stops_at_switch_before_next_record() {
    let f = Fixture::new();
    let (mut a, mut b) = taken_over_with(&f, f.create_packed(fast(), 4));
    let head = f.stored_head();
    let k = keys();
    let before = f.fault.events().len();
    assert!(matches!(a.switch_generation(), Err(Error::Fenced(_))));
    assert_eq!(
        f.fault.events()[before..],
        [
            format!("put_if_absent {}", k.log(1, 2)),
            format!("put_if_match {}", k.head()),
            format!("get {}", k.head()),
        ]
    );
    assert_eq!(f.stored_head(), head);
    assert_eq!(f.record(1, 2), Some(switch_record("w1", 2)));
    assert!(matches!(
        pos(a.append_log(LogFile::Wal, syncs(2))),
        Err(Error::Fenced(_))
    ));
    assert!(
        f.log_keys(2).is_empty(),
        "no record under the next generation"
    );
    let fenced = b.append_fence().unwrap();
    assert_eq!(fenced.pos.seq, 3);
    assert_eq!(f.log_keys(1).len(), 3);
}

/// The fence is already there: the old writer's seal-start record collides with it, and the
/// writer stops before it writes the head.
#[test]
fn switch_after_fence_record_collision_stops_writer() {
    let f = Fixture::new();
    let (mut a, mut b) = taken_over_with(&f, f.create_packed(fast(), 4));
    b.append_fence().unwrap();
    let head = f.stored_head();
    let k = keys();
    let before = f.fault.events().len();
    assert!(matches!(a.switch_generation(), Err(Error::Fenced(_))));
    assert_eq!(
        f.fault.events()[before..],
        [
            format!("put_if_absent {}", k.log(1, 2)),
            format!("get {}", k.log(1, 2)),
        ]
    );
    assert_eq!(f.stored_head(), head);
    assert_eq!(f.record(1, 2), Some(fence("w2", 2)));
    assert!(matches!(a.switch_generation(), Err(Error::Fenced(_))));
    assert!(matches!(
        pos(a.append_log(LogFile::Wal, syncs(2))),
        Err(Error::Fenced(_))
    ));
    assert_eq!(f.log_keys(1).len(), 2);
    assert!(f.log_keys(2).is_empty());
}

/// The legacy seal refuses a format 5 bucket: it would upload pages and a manifest.
#[test]
fn legacy_seal_refuses_a_format_5_bucket() {
    let f = Fixture::new();
    let mut b = f.create_packed(fast(), 4);
    assert!(matches!(
        b.seal_generation(pages(1), 4096, 1),
        Err(Error::Unsupported(_))
    ));
    no_pages_and_no_manifests(&f);
    assert!(f.log_keys(1).is_empty());
}
