//! Task 6: the takeover, skipped generation and integrity signals the spec's Observability
//! section promises, read from the spans and records a `Capture` sees.
use super::mvcc::{mvcc_enc, truncate_checkpoint};
use super::*;
use crate::telemetry::testing::{Capture, Spans};

/// `f` under a capture; returns its result and what was captured.
fn captured<T>(f: impl FnOnce() -> T) -> (T, Spans) {
    let cap = Capture::new();
    let out = tracing::subscriber::with_default(cap.subscriber(), f);
    (out, cap.spans())
}

#[test]
fn takeover_span_carries_the_quiet_and_the_next_generation_floor() {
    let store = mem();
    let _a = writer_left_in_a_seal(&store, 5);
    let g = head(&*store).generation;
    let bt = tempdir();
    let (rep, spans) = captured(|| {
        crate::takeover_with_store(quiet_config(&fresh(&bt), None), store.clone()).unwrap()
    });
    assert_eq!(rep.quiet, QUIET);
    let t = spans.last_named("buckwal.takeover");
    assert_eq!(t.field("buckwal.quiet_ms"), Some("800"), "{t:?}");
    assert_eq!(t.field("buckwal.quiet_old_ms"), Some("800"));
    assert_eq!(t.field("buckwal.quiet_own_ms"), Some("800"));
    // max(G + 1, no earlier floor) + 1.
    let floor = (g + 2).to_string();
    assert_eq!(
        t.field("buckwal.next_generation_floor"),
        Some(floor.as_str())
    );
}

#[test]
fn a_head_without_timeouts_has_no_quiet_old_ms() {
    let store = mem();
    let _a = writer_left_in_a_seal(&store, 5);
    // The head of a build without the timeouts.
    let key = keys().head();
    let body = String::from_utf8(store.get(&key).unwrap().unwrap().body).unwrap();
    let stripped = body
        .replace(",\"lease_ms\":300", "")
        .replace(",\"request_timeout_ms\":500", "");
    assert_ne!(stripped, body, "{body}");
    store.put(&key, stripped.as_bytes()).unwrap();
    let bt = tempdir();
    let (_, spans) = captured(|| {
        crate::takeover_with_store(quiet_config(&fresh(&bt), None), store.clone()).unwrap()
    });
    let t = spans.last_named("buckwal.takeover");
    assert_eq!(t.field("buckwal.quiet_old_ms"), None, "{t:?}");
    assert_eq!(t.field("buckwal.quiet_own_ms"), Some("800"));
    assert_eq!(t.field("buckwal.quiet_ms"), Some("800"));
}

#[test]
fn a_takeover_after_a_commit_reports_a_zero_quiet() {
    let store = mem();
    let a = tempdir();
    let db_a = open_with_store(quiet_config(a.path(), None), store.clone()).unwrap();
    let ca = db_a.connect().unwrap();
    create(&ca);
    insert(&ca, 1).unwrap();
    let bt = tempdir();
    let (_, spans) = captured(|| {
        crate::takeover_with_store(quiet_config(&fresh(&bt), None), store.clone()).unwrap()
    });
    let t = spans.last_named("buckwal.takeover");
    assert_eq!(t.field("buckwal.quiet_ms"), Some("0"), "{t:?}");
    assert_eq!(t.field("buckwal.quiet_old_ms"), Some("800"));
    crash(db_a, vec![ca]);
}

/// The first seal after a takeover names the skipped generation (the number with no log, under
/// which it wrote its pages and manifest); the next seal does not.
#[test]
fn only_the_first_seal_after_a_takeover_has_a_skipped_generation() {
    let store = mem();
    let _a = writer_left_in_a_seal(&store, 5);
    let g = head(&*store).generation;
    let bt = tempdir();
    let b_dir = fresh(&bt);
    crate::takeover_with_store(quiet_config(&b_dir, None), store.clone()).unwrap();
    let ((), spans) = captured(|| {
        let db = open_with_store(quiet_config(&b_dir, None), store.clone()).unwrap();
        let c = db.connect().unwrap();
        insert(&c, 1001).unwrap();
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        insert(&c, 1002).unwrap();
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        c.close().unwrap();
        db.close().unwrap();
    });
    let seals = spans.named("buckwal.checkpoint_upload");
    assert!(seals.len() >= 2, "{seals:?}");
    let skipped = (g + 1).to_string();
    assert_eq!(
        seals[0].field("buckwal.skipped_generation"),
        Some(skipped.as_str()),
        "{:?}",
        seals[0]
    );
    for s in &seals[1..] {
        assert_eq!(s.field("buckwal.skipped_generation"), None, "{s:?}");
    }
    // The wait record carries its length.
    let waits: Vec<_> = spans
        .events
        .iter()
        .filter(|e| e.field("message") == Some("seal waits out the takeover quiet"))
        .collect();
    assert_eq!(waits.len(), 1, "{waits:?}");
    let ms: u64 = waits[0].field("ms").unwrap().parse().unwrap();
    assert!(ms > 0 && ms <= 800, "{ms}");
}

#[test]
fn the_mvcc_seal_span_names_the_skipped_generation_too() {
    let store = mem();
    let k = tempdir();
    let a = tempdir();
    let db_a = open_with_store(mvcc_enc(a.path(), &k, &key_hex()), store.clone()).unwrap();
    let ca = db_a.connect().unwrap();
    create(&ca);
    insert(&ca, 1).unwrap();
    truncate_checkpoint(&ca).unwrap();
    let g = head(&*store).generation;
    let bt = tempdir();
    let b_dir = fresh(&bt);
    crate::takeover_with_store(mvcc_enc(&b_dir, &k, &key_hex()), store.clone()).unwrap();
    let ((), spans) = captured(|| {
        let db = open_with_store(mvcc_enc(&b_dir, &k, &key_hex()), store.clone()).unwrap();
        let c = db.connect().unwrap();
        insert(&c, 1001).unwrap();
        truncate_checkpoint(&c).unwrap();
        insert(&c, 1002).unwrap();
        truncate_checkpoint(&c).unwrap();
        c.close().unwrap();
        db.close().unwrap();
    });
    let seals = spans.named("buckwal.checkpoint_upload");
    assert!(seals.len() >= 2, "{seals:?}");
    let skipped = (g + 1).to_string();
    assert_eq!(
        seals[0].field("buckwal.skipped_generation"),
        Some(skipped.as_str()),
        "{:?}",
        seals[0]
    );
    for s in &seals[1..] {
        assert_eq!(s.field("buckwal.skipped_generation"), None, "{s:?}");
    }
    crash(db_a, vec![ca]);
}

/// `SealStarted` and `SealFinished` report the same number, the generation sealed, also for
/// the seal that skips one.
#[test]
fn seal_events_report_the_generation_sealed_after_a_skip_too() {
    let store = mem();
    let _a = writer_left_in_a_seal(&store, 5);
    let g = head(&*store).generation;
    let bt = tempdir();
    let b_dir = fresh(&bt);
    crate::takeover_with_store(quiet_config(&b_dir, None), store.clone()).unwrap();
    let (hook, seen) = recorder();
    let db = open_with_store(quiet_config(&b_dir, Some(hook)), store.clone()).unwrap();
    let c = db.connect().unwrap();
    insert(&c, 1001).unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    assert_eq!(head(&*store).generation, g + 2);
    let ev = seen.lock().unwrap().clone();
    let started: Vec<u64> = ev
        .iter()
        .filter_map(|e| match e {
            Event::SealStarted { generation, .. } => Some(*generation),
            _ => None,
        })
        .collect();
    let finished: Vec<u64> = ev
        .iter()
        .filter_map(|e| match e {
            Event::SealFinished { generation, .. } => Some(*generation),
            _ => None,
        })
        .collect();
    assert_eq!(started, [g], "{ev:?}");
    assert_eq!(finished, [g], "{ev:?}");
    c.close().unwrap();
    db.close().unwrap();
}
