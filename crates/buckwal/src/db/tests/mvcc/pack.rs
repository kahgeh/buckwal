//! Task 8: an MVCC database of format 5. A TRUNCATE checkpoint (`LogDurableStorage::truncate`)
//! is a generation switch, a pack run applies its ended generations, and a restore reads the
//! pack and the chain of generations after it.
use super::*;
use crate::format::FORMAT_V5;
use crate::pack::pointer::PackPointer;
use crate::pack::run::{PackOutcome, PackReport};
use crate::restore::RestoreReport;

/// The pack pointer, decoded without its code (an encrypted bucket's pointer carries one),
/// its keys under the prefix (the object holds them relative to it).
fn pointer(store: &dyn ObjectStore) -> Option<PackPointer> {
    let body = store.get(&keys().pack_pointer()).unwrap()?.body;
    let p = PackPointer::decode_fields(&body).unwrap().0;
    Some(PackPointer {
        pack_key: format!("{PREFIX}/{}", p.pack_key),
        codes_key: format!("{PREFIX}/{}", p.codes_key),
        ..p
    })
}

/// The writes `store` saw from event `from` on: every PUT flavour, in order.
fn writes(store: &FaultStore, from: usize) -> Vec<String> {
    store.events()[from..]
        .iter()
        .filter(|e| e.starts_with("put"))
        .cloned()
        .collect()
}

/// One pack run against `store`, from a directory of its own (`cfg_for` makes its config).
fn pack_run(store: Arc<dyn ObjectStore>, cfg_for: &dyn Fn(&Path) -> Config) -> PackReport {
    let t = tempdir();
    crate::pack::run::pack_with_store(&cfg_for(&t.path().join("packer")), store)
        .unwrap_or_else(|e| panic!("pack run: {e}"))
}

/// A restore of `store` into a new directory, then a read-only open of it: the report and the
/// ids of `t`.
fn restored_ids(
    store: Arc<dyn ObjectStore>,
    cfg_for: &dyn Fn(&Path) -> Config,
) -> (RestoreReport, Vec<i64>) {
    let t = tempdir();
    let dir = fresh(&t);
    let rep = restore_with_store(&cfg_for(&dir), store.clone(), &dir)
        .unwrap_or_else(|e| panic!("restore: {e}"));
    let db = open_read_only_with_store(cfg_for(&dir), store).unwrap();
    let c = db.connect().unwrap();
    let rows = ids(&c);
    c.close().unwrap();
    db.close().unwrap();
    (rep, rows)
}

/// Update row 2 and delete row 3 (`insert_c` makes them), each in its own transaction.
fn change_rows(c: &Connection) {
    for sql in [
        "UPDATE t SET v = 'changed' WHERE id = 2",
        "DELETE FROM t WHERE id = 3",
    ] {
        c.execute("BEGIN CONCURRENT", &[]).unwrap();
        c.execute(sql, &[]).unwrap();
        c.execute("COMMIT", &[]).unwrap();
    }
}

/// The values of `t`, in id order.
fn values(c: &Connection) -> Vec<(i64, String)> {
    c.query("SELECT id, v FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| (r[0].as_int().unwrap(), text(&r[1])))
        .collect()
}

#[test]
fn mvcc_truncate_is_a_generation_switch() {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 3);
    let h = head(&*mem);
    assert_eq!(
        (h.format, h.generation, h.manifest, h.journal_mode),
        (FORMAT_V5, 2, None, JournalMode::Mvcc),
        "a new MVCC database is format 5"
    );
    let from = store.events().len();
    truncate_checkpoint(&c).unwrap();
    assert_eq!(db.generation(), 3);
    let h = head(&*mem);
    assert_eq!(
        (h.format, h.generation, h.manifest, h.journal_mode),
        (FORMAT_V5, 3, None, JournalMode::Mvcc),
        "the head keeps its journal mode"
    );
    // The checkpoint's `-wal` frames went up as records of generation 2, then the seal-start
    // record naming generation 3, then the head move with If-Match. No page and no manifest.
    let w = writes(&store, from);
    assert!(
        !w.iter()
            .any(|e| e.contains("/pages/") || e.contains("/manifest/")),
        "{w:?}"
    );
    let last = *log_seqs(&*mem, 2).iter().max().unwrap();
    assert_eq!(
        w[w.len() - 2..],
        [
            format!("put_if_absent {}", keys().log(2, last)),
            format!("put_if_match {}", keys().head()),
        ],
        "{w:?}"
    );
    match record(&mem, 2, last) {
        LogRecord::Extents {
            file: LogFile::Wal,
            syncs,
            next_generation,
            ..
        } => {
            assert!(syncs.is_empty(), "a seal-start record holds no ops");
            assert_eq!(next_generation, Some(3), "it names the next generation");
        }
        r => panic!("{r:?}"),
    }
    assert!(mem.list(&keys().page_root()).unwrap().is_empty());
    assert!(mem.list(&keys().manifest_root()).unwrap().is_empty());
    assert!(log_seqs(&*mem, 3).is_empty(), "nothing shipped into 3 yet");
    assert_eq!(read(&dir, "app.db-log"), Vec::<u8>::new());
    assert_eq!(read(&dir, "app.db-wal"), Vec::<u8>::new());
    assert_eq!(state(&dir).generation, 3);
    insert_c(&c, 4).unwrap();
    let b = bucket_image(&*mem);
    assert_eq!(b.kinds, [3]);
    assert_eq!(b.log, read(&dir, "app.db-log"));
    assert_eq!(ids(&c), vec![1, 2, 3, 4]);
    c.close().unwrap();
    db.close().unwrap();
    // Every acked commit is in the bucket: the log of generations 2 and 3 and the first pack.
    let (rep, rows) = restored_ids(mem.clone(), &mvcc);
    assert_eq!(rep.generation, 3);
    assert_eq!(rows, vec![1, 2, 3, 4]);
}

/// The writer of `mvcc_pack_run_restores_equal_to_writer_db` (with `cfg`): rows 1 to 5, a
/// switch, rows 6 to 8 with an update and a delete, a switch, rows 9 and 10 in the head's
/// generation. Head 2 at creation (the first pack's pack generation is 2 too), then 3, then 4.
fn writer_with_two_switches(store: Arc<dyn ObjectStore>, cfg: Config) -> (Database, Connection) {
    let (db, c) = mvcc_with_rows(store, cfg, 5);
    truncate_checkpoint(&c).unwrap();
    for id in 6..=8 {
        insert_c(&c, id).unwrap();
    }
    change_rows(&c);
    truncate_checkpoint(&c).unwrap();
    insert_c(&c, 9).unwrap();
    insert_c(&c, 10).unwrap();
    assert_eq!(db.generation(), 4);
    (db, c)
}

#[test]
fn mvcc_pack_run_restores_equal_to_writer_db() {
    let mem = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = writer_with_two_switches(mem.clone(), mvcc(&dir));
    let first = pointer(&*mem).expect("the first pack");
    assert_eq!(first.pack_generation, 2);
    let r = pack_run(mem.clone(), &mvcc);
    assert_eq!(r.outcome, PackOutcome::Packed, "{r:?}");
    // Generations 2 and 3 ended (their terminal records name 3 and 4); 4 is the head's.
    assert_eq!(r.pack_generation, 4);
    let p = pointer(&*mem).unwrap();
    assert_eq!(Some(p.pack_key.clone()), r.pack_key);
    assert!(
        p.pack_key.starts_with("t/pack/0000000004/"),
        "{}",
        p.pack_key
    );
    // Nothing ended since.
    assert_eq!(
        pack_run(mem.clone(), &mvcc).outcome,
        PackOutcome::NothingToDo
    );
    let want = ids(&c);
    assert_eq!(want, [1, 2, 4, 5, 6, 7, 8, 9, 10]);
    let (rep, rows) = restored_ids(mem.clone(), &mvcc);
    assert_eq!(rep.pack_key, Some(p.pack_key.clone()));
    assert_eq!(rep.generation, 4);
    assert_eq!(rows, want);
    // The values too: the update is in the pack.
    let t2 = tempdir();
    let d2 = fresh(&t2);
    restore_with_store(&mvcc(&d2), mem.clone(), &d2).unwrap();
    let ro = open_read_only_with_store(mvcc(&d2), mem.clone()).unwrap();
    let rc = ro.connect().unwrap();
    assert_eq!(values(&rc), values(&c));
    rc.close().unwrap();
    ro.close().unwrap();
    c.close().unwrap();
    db.close().unwrap();
}

/// A writer killed after a pack run and more commits: a restore from the pack and the log
/// since it holds every acked commit, and the restored copy takes the database over and goes
/// on (its next switch, after the takeover, lands on G+2).
#[test]
fn mvcc_killed_writer_restores_from_pack_and_log_equal_to_writer() {
    for encrypted in [false, true] {
        let mem = mem();
        let k = tempdir();
        let cfg_for = |d: &Path| {
            if encrypted {
                mvcc_enc(d, &k, &key_hex())
            } else {
                mvcc(d)
            }
        };
        let t = tempdir();
        let dir = fresh(&t);
        let (db, c) = writer_with_two_switches(mem.clone(), cfg_for(&dir));
        let r = pack_run(mem.clone(), &cfg_for);
        assert_eq!(
            (r.outcome.clone(), r.pack_generation),
            (PackOutcome::Packed, 4),
            "encrypted={encrypted}"
        );
        for id in 11..=13 {
            insert_c(&c, id).unwrap();
        }
        truncate_checkpoint(&c).unwrap();
        insert_c(&c, 14).unwrap();
        let want = ids(&c);
        let marker = MARKER.as_bytes();
        crash(db, vec![c]);
        if encrypted {
            for key in mem.list("").unwrap() {
                let body = mem.get(&key).unwrap().unwrap().body;
                assert!(
                    !body.windows(marker.len()).any(|w| w == marker),
                    "plaintext in {key}"
                );
            }
        }
        // A second machine: the pack of generation 4, the log of 4, then 5 as the WAL.
        let (rep, rows) = restored_ids(mem.clone(), &cfg_for);
        assert_eq!(rep.generation, 5, "encrypted={encrypted}");
        assert_eq!(rep.pack_key, r.pack_key, "encrypted={encrypted}");
        assert_eq!(rows, want, "encrypted={encrypted}");
        // A restore and a writer open: the takeover, then a commit and a switch.
        let t2 = tempdir();
        let d2 = fresh(&t2);
        restore_with_store(&cfg_for(&d2), mem.clone(), &d2).unwrap();
        let db = open_with_store(cfg_for(&d2), mem.clone()).unwrap();
        let c = db.connect().unwrap();
        assert_eq!(ids(&c), want, "encrypted={encrypted}");
        insert_c(&c, 15).unwrap();
        truncate_checkpoint(&c).unwrap();
        let h = head(&*mem);
        assert_eq!(
            (h.format, h.generation, h.journal_mode),
            (FORMAT_V5, 7, JournalMode::Mvcc),
            "encrypted={encrypted}: the first switch after a takeover skips generation 6"
        );
        insert_c(&c, 16).unwrap();
        let want = ids(&c);
        c.close().unwrap();
        db.close().unwrap();
        // The packer follows the chain over the skipped generation.
        let r = pack_run(mem.clone(), &cfg_for);
        assert_eq!(
            (r.outcome, r.pack_generation),
            (PackOutcome::Packed, 7),
            "encrypted={encrypted}"
        );
        let (rep, rows) = restored_ids(mem.clone(), &cfg_for);
        assert_eq!(rep.generation, 7);
        assert_eq!(rows, want, "encrypted={encrypted}");
    }
}

#[test]
fn mvcc_window_batching_still_refused() {
    let w = Batching::Window {
        max_wait: Duration::from_millis(10),
        max_commits: 4,
        max_bytes: 1024,
    };
    // At open, before anything is written to the bucket.
    let store = mem();
    let t = tempdir();
    let mut c = mvcc(&fresh(&t));
    c.batching = w;
    assert_eq!(
        unsupported(open_with_store(c, store.clone())),
        "MVCC: Window batching"
    );
    assert!(store.list("").unwrap().is_empty(), "nothing written");
    // On an open format 5 database.
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 1);
    assert_eq!(head(&*store).format, FORMAT_V5);
    assert_eq!(unsupported(db.set_batching(w)), "MVCC: Window batching");
    assert_eq!(
        unsupported(c.set_batching(Some(w))),
        "MVCC: Window batching"
    );
    assert_eq!(db.batching(), Batching::Off);
    insert_c(&c, 2).unwrap();
    c.close().unwrap();
    db.close().unwrap();
}

/// An MVCC bucket made before format 5 (format 2; `LegacySeal` makes one) keeps working as it
/// did: its TRUNCATE checkpoint still seals (pages, manifest, head), it restores and reopens,
/// and a pack run refuses it (`Unsupported`, nothing written). No new feature reaches it.
#[test]
fn mvcc_bucket_of_format_2_keeps_sealing() {
    let _legacy = crate::format::LegacySeal::on();
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 3);
    let h = head(&*store);
    assert_eq!((h.format, h.generation, h.manifest), (2, 2, Some(1)));
    truncate_checkpoint(&c).unwrap();
    let h = head(&*store);
    assert_eq!(
        (h.format, h.generation, h.manifest),
        (2, 3, Some(2)),
        "a seal"
    );
    assert!(!store.list(&keys().page_root()).unwrap().is_empty());
    assert!(store.get(&keys().pack_pointer()).unwrap().is_none());
    insert_c(&c, 4).unwrap();
    let objects = store.list("").unwrap();
    let t2 = tempdir();
    let r = crate::pack::run::pack_with_store(&mvcc(&t2.path().join("packer")), store.clone());
    assert!(matches!(r, Err(Error::Unsupported(_))), "{r:?}");
    assert_eq!(
        store.list("").unwrap(),
        objects,
        "the pack run wrote nothing"
    );
    c.close().unwrap();
    db.close().unwrap();
    let (rep, rows) = restored_ids(store.clone(), &mvcc);
    assert_eq!((rep.generation, rep.pack_key), (3, None));
    assert_eq!(rows, vec![1, 2, 3, 4]);
    // The writer reopens and seals again.
    let db = open_mvcc(store.clone(), mvcc(&dir));
    let c = db.connect().unwrap();
    insert_c(&c, 5).unwrap();
    truncate_checkpoint(&c).unwrap();
    assert_eq!(head(&*store).manifest, Some(3));
    assert_eq!(ids(&c), vec![1, 2, 3, 4, 5]);
    c.close().unwrap();
    db.close().unwrap();
}

/// A text of about 3.5 KB: one row fills most of a 4 KiB page.
fn big(id: i64) -> String {
    format!("{id:06}{}", "x".repeat(3500))
}

/// `BEGIN CONCURRENT` inserts of rows `ids` with `big` texts.
fn insert_big(c: &Connection, ids: std::ops::RangeInclusive<i64>) {
    for id in ids {
        c.execute("BEGIN CONCURRENT", &[]).unwrap();
        c.execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            &[Value::from_i64(id), Value::build_text(big(id))],
        )
        .unwrap();
        c.execute("COMMIT", &[]).unwrap();
    }
}

/// Every row of `t`: its id, the first 12 bytes of its text and its length.
fn big_rows(c: &Connection) -> Vec<(i64, String, i64)> {
    c.query(
        "SELECT id, substr(v, 1, 12), length(v) FROM t ORDER BY id",
        &[],
    )
    .unwrap()
    .iter()
    .map(|r| (r[0].as_int().unwrap(), text(&r[1]), r[2].as_int().unwrap()))
    .collect()
}

/// An MVCC pack of several 1 MiB blocks, with the background fetch off: a restore reads the
/// pack's blocks as the engine needs them and applies the generation that ended after the
/// pack over them. The copy opens read-only with every row, then as a writer (a takeover)
/// that commits and switches.
#[test]
fn mvcc_restore_of_a_pack_of_several_blocks_equals_writer_db() {
    let _off = crate::io::block_store::NoBackgroundFetch::on();
    let mem = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(mem.clone(), mvcc(&dir), 0);
    insert_big(&c, 1..=450);
    truncate_checkpoint(&c).unwrap();
    let r = pack_run(mem.clone(), &mvcc);
    assert_eq!((r.outcome, r.pack_generation), (PackOutcome::Packed, 3));
    let p = pointer(&*mem).unwrap();
    assert!(p.size > 1 << 20, "a pack of several blocks: {p:?}");
    // A generation after the pack that changes a few rows, then rows in the head's.
    c.execute("BEGIN CONCURRENT", &[]).unwrap();
    c.execute("UPDATE t SET v = 'changed' WHERE id IN (5, 400)", &[])
        .unwrap();
    c.execute("COMMIT", &[]).unwrap();
    truncate_checkpoint(&c).unwrap();
    insert_big(&c, 451..=455);
    let want = big_rows(&c);
    c.close().unwrap();
    db.close().unwrap();

    let t2 = tempdir();
    let d2 = fresh(&t2);
    let rep = restore_with_store(&mvcc(&d2), mem.clone(), &d2).unwrap();
    assert_eq!(rep.pack_key, Some(p.pack_key.clone()));
    assert_eq!(rep.generation, 4);
    // Observed (turso 0.8.3): the MVCC engine reads every page when it opens, so the local
    // checkpoint of generation 3 brings the whole pack down (1855488 of 1855488 bytes). A WAL
    // restore fetches only the blocks its generations touch.
    assert!(
        rep.bytes_fetched > 0 && rep.bytes_fetched <= p.size,
        "{rep:?}"
    );
    let ro = open_read_only_with_store(mvcc(&d2), mem.clone()).unwrap();
    let rc = ro.connect().unwrap();
    assert_eq!(big_rows(&rc), want);
    rc.close().unwrap();
    ro.close().unwrap();
    let db = open_with_store(mvcc(&d2), mem.clone()).unwrap();
    let c = db.connect().unwrap();
    assert_eq!(big_rows(&c), want);
    insert_big(&c, 456..=456);
    truncate_checkpoint(&c).unwrap();
    let want = big_rows(&c);
    c.close().unwrap();
    db.close().unwrap();
    let (_, ids) = restored_ids(mem.clone(), &mvcc);
    assert_eq!(ids, want.iter().map(|r| r.0).collect::<Vec<_>>());
}
