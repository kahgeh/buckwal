//! A format 5 writer: the generation switch at each WAL restart, and a restore that reads the
//! log only (no pack yet; `pack_restore` restores from a pack).
use super::*;
use crate::format::FORMAT_V5;

/// Every key the bucket holds.
fn every_key(store: &MemStore) -> Vec<String> {
    store.list(PREFIX).unwrap()
}

fn no_pages_and_no_manifests(store: &MemStore) {
    let k = keys();
    let bad: Vec<String> = every_key(store)
        .into_iter()
        .filter(|key| key.starts_with(&k.page_root()) || key.starts_with(&k.manifest_root()))
        .collect();
    assert!(bad.is_empty(), "{bad:?}");
}

/// The last record of `log/<g>`.
fn last_record(store: &MemStore, g: u64) -> LogRecord {
    let seq = *log_seqs(store, g).iter().max().expect("a log");
    decode_record(&store.get(&keys().log(g, seq)).unwrap().unwrap().body).unwrap()
}

fn switch_record_names(store: &MemStore, g: u64) -> Option<u64> {
    match last_record(store, g) {
        LogRecord::Extents {
            syncs,
            next_generation,
            ..
        } if syncs.is_empty() => next_generation,
        r => panic!("log/{g} does not end with a seal-start record: {r:?}"),
    }
}

/// The commit whose WAL header write restarts the WAL (a PASSIVE checkpoint backfilled the whole
/// WAL first) waits for three small PUTs: the seal-start record, the head move and its own
/// record. No GET, no list, no page. The engine's restart syncs the new WAL header alone
/// before the commit's frames, and that sync ships nothing: the held header goes in the
/// commit's record (`log/2/1`, header first, then the frames). The trimmer is off: its GC pass after the
/// switch reads the head on its own thread, not on the commit's path.
#[test]
fn commit_that_restarts_wal_makes_three_small_puts() {
    let store = mem();
    let t = tempdir();
    let cfg = config(t.path());
    let _no_gc = crate::trim::NoWriterGc::on();
    let db = open_with_store(cfg, store.clone()).unwrap();
    assert_eq!(head(&*store).format, FORMAT_V5);
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=20 {
        insert(&c, id).unwrap();
    }
    c.checkpoint(CheckpointMode::Passive).unwrap();
    assert_eq!(db.generation(), 1);
    let before = store.stats();
    insert(&c, 21).unwrap();
    let d = store.stats().since(&before);
    assert_eq!(db.generation(), 2);
    assert_eq!((d.puts, d.gets, d.lists, d.deletes), (3, 0, 0, 0), "{d:?}");
    assert!(d.bytes_up < 64 * 1024, "{d:?}");
    assert_eq!(switch_record_names(&store, 1), Some(2));
    // One record: the new WAL header, then the commit's frames.
    assert_eq!(log_seqs(&*store, 2), [1]);
    let header_first = match decode_record(&store.get(&keys().log(2, 1)).unwrap().unwrap().body)
        .unwrap()
    {
        LogRecord::Extents { syncs, .. } => {
            let ops: Vec<_> = syncs.iter().flatten().collect();
            ops.len() > 1
                && matches!(ops[0], crate::format::Op::Write { off: 0, bytes } if bytes.len() == 32)
        }
        r => panic!("{r:?}"),
    };
    assert!(
        header_first,
        "log/2/1 does not hold the WAL header, then frames"
    );
    let h = head(&*store);
    assert_eq!((h.format, h.generation, h.manifest), (FORMAT_V5, 2, None));
    no_pages_and_no_manifests(&store);
    assert!(!t.path().join(crate::files::DIRTY_FILE).exists());
    c.close().unwrap();
    db.close().unwrap();
}

/// A log-only restore of a format 5 bucket (no pack yet) after several switches and a takeover:
/// generation 1 on from an empty file, each ended generation applied with a local checkpoint,
/// the skipped generation passed over, the head's generation as the WAL. It equals the writer's
/// database.
#[test]
fn log_only_restore_after_switches_and_a_takeover_equals_the_writer() {
    let store = mem();
    let a = tempdir();
    let db_a = open_mem(&store, a.path());
    let ca = db_a.connect().unwrap();
    create(&ca);
    for id in 1..=30 {
        insert(&ca, id).unwrap();
    }
    ca.checkpoint(CheckpointMode::Truncate).unwrap();
    ca.execute("UPDATE t SET v = 'x' WHERE id <= 5", &[])
        .unwrap();
    ca.execute("DELETE FROM t WHERE id = 7", &[]).unwrap();
    ca.checkpoint(CheckpointMode::Truncate).unwrap();
    for id in 31..=40 {
        insert(&ca, id).unwrap();
    }
    assert_eq!(db_a.generation(), 3);
    assert_eq!(switch_record_names(&store, 1), Some(2));
    assert_eq!(switch_record_names(&store, 2), Some(3));

    // A takeover: a log-only restore, then the claim. The new writer stays in generation 3,
    // after its fence, and its switch skips generation 4.
    let b = tempdir();
    let rep = restore_with_store(&config(&fresh(&b)), store.clone(), &fresh(&b)).unwrap();
    assert_eq!(rep.generation, 3);
    let db_b = open_mem(&store, &fresh(&b));
    let cb = db_b.connect().unwrap();
    assert_eq!(ids(&cb), ids(&ca));
    crash(db_a, vec![ca]);
    assert_eq!(db_b.generation(), 3);
    for id in 41..=50 {
        insert(&cb, id).unwrap();
    }
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    assert_eq!(db_b.generation(), 5);
    assert_eq!(switch_record_names(&store, 3), Some(5));
    assert!(log_seqs(&*store, 4).is_empty(), "generation 4 is skipped");
    cb.execute("UPDATE t SET v = 'y' WHERE id > 45", &[])
        .unwrap();
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    for id in 51..=55 {
        insert(&cb, id).unwrap();
    }
    let want: Vec<(i64, String)> = cb
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| (r[0].as_int().unwrap(), text(&r[1])))
        .collect();
    no_pages_and_no_manifests(&store);

    let r = tempdir();
    let rep = restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r)).unwrap();
    assert_eq!(rep.generation, 6);
    let v = open_read_only_with_store(config(&fresh(&r)), store.clone()).unwrap();
    let c = v.connect().unwrap();
    integrity_ok(&c);
    let got: Vec<(i64, String)> = c
        .query("SELECT id, v FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| (r[0].as_int().unwrap(), text(&r[1])))
        .collect();
    assert_eq!(got, want);
    drop(c);
    drop(v);
    // GC finds no manifest in a format 5 bucket and deletes nothing: without a pack, the log is
    // the only copy.
    let g = crate::gc::gc_with_store(&config(&fresh(&r)), store.clone()).unwrap();
    assert_eq!((g.deleted, g.outcome), (0, crate::gc::GcOutcome::Nothing));
    assert!(!log_seqs(&*store, 1).is_empty());
    crash(db_b, vec![cb]);
}

/// An encrypted format 5 bucket: its head carries a database id and a key check, its log holds
/// no plaintext, and a log-only restore with the key equals the writer (the ended generations
/// are applied with the key). Without the key the restore is refused before it writes anything.
#[test]
fn encrypted_log_only_restore_after_switches_equals_the_writer() {
    let store = mem();
    let k = tempdir();
    let a = tempdir();
    let db = open_with_store(enc_config(a.path(), &k, &key_hex()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=20 {
        insert(&c, id).unwrap();
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    for id in 21..=30 {
        insert(&c, id).unwrap();
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert(&c, 31).unwrap();
    assert_eq!(db.generation(), 3);
    let h = head(&*store);
    assert_eq!((h.format, h.manifest), (FORMAT_V5, None));
    assert!(h.database_id.is_some());
    no_pages_and_no_manifests(&store);
    for key in every_key(&store) {
        let body = store.get(&key).unwrap().unwrap().body;
        assert!(
            !body.windows(5).any(|w| w == b"row 1"),
            "plaintext in {key}"
        );
    }
    let want = ids(&c);

    let r = tempdir();
    let got = restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r));
    assert!(matches!(got, Err(Error::Encryption(_))), "{got:?}");
    assert!(!fresh(&r).join("app.db").exists());
    let r = tempdir();
    let cfg = enc_config(&fresh(&r), &k, &key_hex());
    let rep = restore_with_store(&cfg, store.clone(), &fresh(&r)).unwrap();
    assert_eq!(rep.generation, 3);
    let v = open_read_only_with_store(cfg, store.clone()).unwrap();
    let vc = v.connect().unwrap();
    integrity_ok(&vc);
    assert_eq!(ids(&vc), want);
    drop(vc);
    drop(v);
    crash(db, vec![c]);
}
