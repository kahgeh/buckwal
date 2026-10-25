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
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
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

/// The MVCC generation switch (`LogDurableStorage::truncate`) after a takeover: its
/// `buckwal.generation_switch` span names the generation G and the next one, G + 2 (a skipped
/// generation, as the WAL switch); the next switch moves by one.
#[test]
fn the_mvcc_switch_span_names_the_skipped_generation_too() {
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
    let switches = spans.named("buckwal.generation_switch");
    assert!(switches.len() >= 2, "{switches:?}");
    let field = |i: usize, f: &str| switches[i].field(f).map(str::to_string);
    assert_eq!(field(0, "buckwal.generation"), Some(g.to_string()));
    assert_eq!(
        field(0, "buckwal.next_generation"),
        Some((g + 2).to_string()),
        "the first switch after a takeover skips generation {}",
        g + 1
    );
    assert_eq!(field(1, "buckwal.generation"), Some((g + 2).to_string()));
    assert_eq!(
        field(1, "buckwal.next_generation"),
        Some((g + 3).to_string())
    );
    assert_eq!(field(0, "buckwal.outcome").as_deref(), Some("switched"));
    assert!(
        spans.named("buckwal.checkpoint_upload").is_empty(),
        "no seal"
    );
    crash(db_a, vec![ca]);
}

/// `SealStarted` and `SealFinished` report the same number, the generation sealed, also for
/// the seal that skips one.
#[test]
fn seal_events_report_the_generation_sealed_after_a_skip_too() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
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

/// Format 5: a writer that took over waits out the takeover quiet before its first switch (the
/// old writer's last record is a seal-start record), then switches over the skipped generation:
/// `SealQuiet`, then `GenerationSwitched` from G to G+2, and the span names G+2.
#[test]
fn the_first_switch_after_a_takeover_waits_the_quiet_and_skips_a_generation() {
    let store = mem();
    let _a = writer_left_in_a_seal(&store, 5);
    let g = head(&*store).generation;
    assert_eq!(head(&*store).format, crate::format::FORMAT_V5);
    let bt = tempdir();
    let b_dir = fresh(&bt);
    crate::takeover_with_store(quiet_config(&b_dir, None), store.clone()).unwrap();
    let (hook, seen) = recorder();
    let ((), spans) = captured(|| {
        let db = open_with_store(quiet_config(&b_dir, Some(hook)), store.clone()).unwrap();
        let c = db.connect().unwrap();
        insert(&c, 1001).unwrap();
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        assert_eq!(head(&*store).generation, g + 2);
        c.close().unwrap();
        db.close().unwrap();
    });
    let ev = seen.lock().unwrap().clone();
    let kinds: Vec<String> = ev
        .iter()
        .filter_map(|e| match e {
            Event::SealQuiet { generation, .. } => Some(format!("quiet {generation}")),
            Event::GenerationSwitched { from, to, .. } => Some(format!("switched {from}->{to}")),
            Event::SealStarted { .. } | Event::SealFinished { .. } => Some("seal".into()),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds[..2],
        [format!("quiet {g}"), format!("switched {g}->{}", g + 2)]
    );
    assert!(!kinds.contains(&"seal".to_string()), "{kinds:?}");
    let switches = spans.named("buckwal.generation_switch");
    let next = (g + 2).to_string();
    assert_eq!(
        switches[0].field("buckwal.next_generation"),
        Some(next.as_str()),
        "{:?}",
        switches[0]
    );
    assert_eq!(switches[0].field("buckwal.outcome"), Some("switched"));
    assert!(spans.named("buckwal.checkpoint_upload").is_empty());
}

/// A takeover that fails before its head write still carries what it knew by then: its own
/// quiet. One that fails after the head write also carries the epoch the write set.
#[test]
fn a_failed_takeover_keeps_the_attributes_it_reached() {
    for (key, want_epoch) in [("head", false), ("log/", true)] {
        let mem = Arc::new(crate::store::MemStore::new());
        let a = tempdir();
        let db_a = open_with_store(quiet_config(a.path(), None), mem.clone()).unwrap();
        let ca = db_a.connect().unwrap();
        create(&ca);
        insert(&ca, 1).unwrap();
        let fault = Arc::new(crate::store::FaultStore::new(mem.clone()));
        fault.only_keys_containing(key);
        fault.fail_next_puts_permanently(1);
        let bt = tempdir();
        let (r, spans) =
            captured(|| crate::takeover_with_store(quiet_config(&fresh(&bt), None), fault.clone()));
        assert!(r.is_err(), "{key}");
        let t = spans.last_named("buckwal.takeover");
        assert_eq!(t.field("buckwal.outcome"), Some("error"), "{key}: {t:?}");
        assert_eq!(t.field("buckwal.quiet_own_ms"), Some("800"), "{key}: {t:?}");
        assert_eq!(
            t.field("buckwal.epoch").is_some(),
            want_epoch,
            "{key}: {t:?}"
        );
        crash(db_a, vec![ca]);
    }
}
