//! `buckwal upgrade` (spec Choice D): a bucket of format 1 to 4 becomes a format 5 bucket in
//! place. The upgrade takes the database over (the old writer is fenced), builds the first pack
//! from the pages, the manifest and the log, writes the pack and its codes, the pointer and,
//! last, the head at format 5 with `If-Match`.
use super::*;
use crate::format::{FORMAT, FORMAT_V2, FORMAT_V3, FORMAT_V4, FORMAT_V5};
use crate::pack::PackPointer;
use crate::upgrade::{upgrade_with_store, UpgradeReport};

/// The bucket formats an upgrade starts from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Legacy {
    /// Plaintext WAL: format 1, no database id.
    F1,
    /// Encrypted, made before format 4: no database id.
    F3,
    /// Encrypted with a database id.
    F4,
}

/// Text that each row holds, to look for in the bucket's objects.
const NEEDLE: &str = "UPGRADE-NEEDLE";

fn cfg_of(kind: Legacy, dir: &Path, k: &TempDir) -> Config {
    match kind {
        Legacy::F1 => config(dir),
        Legacy::F3 | Legacy::F4 => enc_config(dir, k, &key_hex()),
    }
}

fn row_text(id: i64) -> String {
    format!("{NEEDLE} {id} {}", "x".repeat(400))
}

fn put_rows(c: &Connection, ids: std::ops::RangeInclusive<i64>) {
    for id in ids {
        c.execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            &[Value::from_i64(id), Value::build_text(row_text(id))],
        )
        .unwrap();
    }
}

fn all_rows(c: &Connection) -> Vec<(i64, String)> {
    c.query("SELECT id, v FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| (r[0].as_int().unwrap(), text(&r[1])))
        .collect()
}

fn want(ids: std::ops::RangeInclusive<i64>) -> Vec<(i64, String)> {
    ids.map(|id| (id, row_text(id))).collect()
}

/// A bucket of `kind`, as a build before format 5 makes it: rows 1..=20, a seal (pages and a
/// manifest), rows 21..=30 that stay in the log of the head's generation. The writer stays
/// open (returned with its directory).
fn legacy_writer(
    kind: Legacy,
    store: &Arc<MemStore>,
    k: &TempDir,
) -> (TempDir, Database, Connection) {
    let t = tempdir();
    let db = {
        let _seal = crate::format::LegacySeal::on();
        let _f3 = (kind == Legacy::F3).then(crate::format::LegacyFormat3::on);
        open_with_store(cfg_of(kind, &fresh(&t), k), store.clone()).unwrap()
    };
    let c = db.connect().unwrap();
    create(&c);
    put_rows(&c, 1..=20);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    put_rows(&c, 21..=30);
    let h = head(&**store);
    let format = match kind {
        Legacy::F1 => FORMAT,
        Legacy::F3 => FORMAT_V3,
        Legacy::F4 => FORMAT_V4,
    };
    assert_eq!(h.format, format, "{kind:?}");
    assert!(h.manifest.is_some(), "{kind:?}: sealed once");
    (t, db, c)
}

/// `legacy_writer`, closed.
fn legacy_bucket(kind: Legacy, store: &Arc<MemStore>, k: &TempDir) {
    let (_t, db, c) = legacy_writer(kind, store, k);
    c.close().unwrap();
    db.close().unwrap();
}

/// Upgrade into a new directory; the directory is returned with the result.
fn upgrade_into(
    kind: Legacy,
    store: Arc<dyn ObjectStore>,
    k: &TempDir,
) -> (TempDir, Result<UpgradeReport>) {
    let t = tempdir();
    let r = upgrade_with_store(&cfg_of(kind, &fresh(&t), k), store, &fresh(&t));
    (t, r)
}

/// Keys that read the bucket's head (so a pointer of this database decodes).
fn adopted_keys(kind: Legacy, store: &dyn ObjectStore, k: &TempDir) -> Keys {
    let t = tempdir();
    let cfg = cfg_of(kind, t.path(), k);
    let enc = crate::encryption::load(&cfg).unwrap();
    let keys = cfg.keys_for(enc.as_deref());
    keys.decode_head_open(&store.get(&keys.head()).unwrap().unwrap().body)
        .unwrap();
    keys
}

fn pointer_of(kind: Legacy, store: &dyn ObjectStore, k: &TempDir) -> PackPointer {
    let keys = adopted_keys(kind, store, k);
    keys.decode_pointer(&store.get(&keys.pack_pointer()).unwrap().unwrap().body)
        .unwrap()
}

/// Restore into a new directory, then read every row with a read-only open of it.
fn restored(
    kind: Legacy,
    store: &Arc<MemStore>,
    k: &TempDir,
) -> (crate::RestoreReport, Vec<(i64, String)>) {
    let t = tempdir();
    let dir = fresh(&t);
    let rep = restore_with_store(&cfg_of(kind, &dir, k), store.clone(), &dir).unwrap();
    let db = open_read_only_with_store(cfg_of(kind, &dir, k), store.clone()).unwrap();
    let c = db.connect().unwrap();
    let rows = all_rows(&c);
    c.query("PRAGMA integrity_check", &[]).unwrap();
    (rep, rows)
}

fn has(bytes: &[u8], needle: &[u8]) -> bool {
    bytes.windows(needle.len()).any(|w| w == needle)
}

fn objects_under(store: &MemStore, prefix: &str) -> Vec<String> {
    store.list(prefix).unwrap()
}

/// Every check after a good upgrade of a `kind` bucket that held rows 1..=30.
fn assert_upgraded(kind: Legacy, store: &Arc<MemStore>, k: &TempDir, r: &UpgradeReport) {
    let h = head(&**store);
    assert_eq!(h.format, FORMAT_V5, "{kind:?}");
    assert_eq!(h.manifest, None, "{kind:?}");
    assert_eq!(h.generation, r.generation, "{kind:?}");
    assert_eq!(h.database_id.as_deref(), Some(r.database_id.as_str()));
    let p = pointer_of(kind, &**store, k);
    // The first pack holds the last generation of the old format whole: the chain from it
    // ends at once at the head's generation.
    assert_eq!(p.pack_generation, h.generation, "{kind:?}");
    assert_eq!(p.pack_key, r.pack_key, "{kind:?}");
    assert_eq!(p.database_id, h.database_id, "{kind:?}");
    assert!(r.generation > r.from_generation, "{r:?}");
    let (rep, rows) = restored(kind, store, k);
    assert_eq!(rep.generation, h.generation);
    assert_eq!(rep.pack_key.as_deref(), Some(r.pack_key.as_str()));
    assert_eq!(rows, want(1..=30), "{kind:?}");
}

#[test]
fn upgrade_format_1_bucket_restores_equal() {
    let store = mem();
    let k = tempdir();
    legacy_bucket(Legacy::F1, &store, &k);
    let before = head(&*store);
    let (t, r) = upgrade_into(Legacy::F1, store.clone(), &k);
    let r = r.unwrap();
    assert_eq!(r.from_format, FORMAT);
    assert_eq!(r.from_generation, before.generation);
    assert_upgraded(Legacy::F1, &store, &k, &r);
    // A format 1 bucket gets a database id at the upgrade.
    assert_eq!(r.database_id.len(), 32);
    // The upgrade's own directory holds the restored copy: the first writer open continues
    // the database there.
    let db = open_with_store(config(&fresh(&t)), store.clone()).unwrap();
    let c = db.connect().unwrap();
    assert_eq!(all_rows(&c), want(1..=30));
    put_rows(&c, 31..=32);
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(restored(Legacy::F1, &store, &k).1, want(1..=32));
}

#[test]
fn upgrade_format_4_bucket_restores_equal() {
    let store = mem();
    let k = tempdir();
    legacy_bucket(Legacy::F4, &store, &k);
    let before = head(&*store);
    let (t, r) = upgrade_into(Legacy::F4, store.clone(), &k);
    let r = r.unwrap();
    assert_eq!(r.from_format, FORMAT_V4);
    assert_upgraded(Legacy::F4, &store, &k, &r);
    // A format 4 bucket keeps its database id.
    assert_eq!(head(&*store).database_id, before.database_id);
    // The pack holds the pages as the engine encrypted them, and no plaintext.
    let pack = store.get(&r.pack_key).unwrap().unwrap().body;
    assert!(!has(&pack, NEEDLE.as_bytes()), "plaintext in the pack");
    assert!(!has(&pack, b"CREATE TABLE"), "plaintext in the pack");
    assert_eq!(
        std::fs::read(fresh(&t).join("app.db")).unwrap(),
        pack,
        "the upgrade's copy is the pack"
    );
    for key in objects_under(&store, "") {
        let body = store.get(&key).unwrap().unwrap().body;
        assert!(!has(&body, NEEDLE.as_bytes()), "plaintext in {key}");
    }
}

#[test]
fn upgrade_format_3_bucket_gets_a_new_database_id() {
    let store = mem();
    let k = tempdir();
    legacy_bucket(Legacy::F3, &store, &k);
    assert_eq!(head(&*store).database_id, None);
    let (_t, r) = upgrade_into(Legacy::F3, store.clone(), &k);
    let r = r.unwrap();
    assert_eq!(r.from_format, FORMAT_V3);
    assert_upgraded(Legacy::F3, &store, &k, &r);
    assert_eq!(head(&*store).database_id.unwrap().len(), 32);
    let pack = store.get(&r.pack_key).unwrap().unwrap().body;
    assert!(!has(&pack, NEEDLE.as_bytes()), "plaintext in the pack");
}

#[test]
fn upgrade_format_2_mvcc_bucket_restores_equal() {
    let store = mem();
    let t = tempdir();
    let mvcc_rows = |c: &Connection, ids: std::ops::RangeInclusive<i64>| {
        for id in ids {
            c.execute("BEGIN CONCURRENT", &[]).unwrap();
            c.execute(
                "INSERT INTO t(id, v) VALUES (?, ?)",
                &[Value::from_i64(id), Value::build_text(row_text(id))],
            )
            .unwrap();
            c.execute("COMMIT", &[]).unwrap();
        }
    };
    {
        let db = {
            let _seal = crate::format::LegacySeal::on();
            open_with_store(mvcc_config(&fresh(&t)), store.clone()).unwrap()
        };
        let c = db.connect().unwrap();
        create(&c);
        mvcc_rows(&c, 1..=20);
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        mvcc_rows(&c, 21..=30);
        c.close().unwrap();
        db.close().unwrap();
    }
    assert_eq!(head(&*store).format, FORMAT_V2);
    let u = tempdir();
    let r = upgrade_with_store(&mvcc_config(&fresh(&u)), store.clone(), &fresh(&u)).unwrap();
    let h = head(&*store);
    assert_eq!((h.format, h.journal_mode), (FORMAT_V5, JournalMode::Mvcc));
    assert_eq!(h.generation, r.generation);
    let r2 = tempdir();
    let dir = fresh(&r2);
    let rep = restore_with_store(&mvcc_config(&dir), store.clone(), &dir).unwrap();
    assert_eq!(rep.pack_key.as_deref(), Some(r.pack_key.as_str()));
    let db = open_with_store(mvcc_config(&dir), store.clone()).unwrap();
    let c = db.connect().unwrap();
    assert_eq!(all_rows(&c), want(1..=30));
    mvcc_rows(&c, 31..=31);
    assert_eq!(all_rows(&c), want(1..=31));
}

/// A store whose head PUTs fail after the first one (the upgrade's takeover): the upgrade
/// stops before its head write, as a crash there does.
fn head_write_fails(store: &Arc<MemStore>) -> Arc<FaultStore> {
    let f = Arc::new(FaultStore::new(store.clone()));
    f.only_keys_containing(&keys().head());
    f.fail_puts_after(1);
    f
}

/// `cfg_of` with a short retry deadline, so a failing head write ends soon.
fn short(kind: Legacy, dir: &Path, k: &TempDir) -> Config {
    let mut c = cfg_of(kind, dir, k);
    c.retry_deadline = Duration::from_millis(300);
    c
}

#[test]
fn upgrade_crash_before_head_write_leaves_working_format_4_bucket() {
    for kind in [Legacy::F4, Legacy::F3] {
        let store = mem();
        let k = tempdir();
        legacy_bucket(kind, &store, &k);
        let format = head(&*store).format;
        let t = tempdir();
        let r = upgrade_with_store(
            &short(kind, &fresh(&t), &k),
            head_write_fails(&store),
            &fresh(&t),
        );
        assert!(r.is_err(), "{kind:?}: {r:?}");
        // The head is still of the old format, and its database works as before: the pack and
        // the pointer are there, and older code does not read them.
        let h = head(&*store);
        assert_eq!(h.format, format, "{kind:?}");
        assert!(store.get(&keys().pack_pointer()).unwrap().is_some());
        assert!(!objects_under(&store, &keys().pack_root()).is_empty());
        // The failed upgrade left no database in its directory.
        assert!(!fresh(&t).join("app.db").exists(), "{kind:?}");
        let (rep, rows) = restored(kind, &store, &k);
        assert_eq!(rep.pack_key, None, "{kind:?}");
        assert_eq!(rows, want(1..=30), "{kind:?}");
        // A writer takes it over, commits and seals.
        let w = tempdir();
        let db = open_with_store(cfg_of(kind, &fresh(&w), &k), store.clone()).unwrap();
        let c = db.connect().unwrap();
        put_rows(&c, 31..=35);
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        put_rows(&c, 36..=36);
        assert_eq!(head(&*store).format, format, "{kind:?}");
        c.close().unwrap();
        db.close().unwrap();
        assert_eq!(restored(kind, &store, &k).1, want(1..=36), "{kind:?}");
    }
}

#[test]
fn upgrade_twice_starts_over_and_gc_deletes_orphan_pack() {
    for kind in [Legacy::F4, Legacy::F3] {
        let store = mem();
        let k = tempdir();
        legacy_bucket(kind, &store, &k);
        let t = tempdir();
        let dir = fresh(&t);
        let r = upgrade_with_store(&short(kind, &dir, &k), head_write_fails(&store), &dir);
        assert!(r.is_err(), "{kind:?}: {r:?}");
        let first_packs = objects_under(&store, &keys().pack_root());
        assert_eq!(first_packs.len(), 2, "{kind:?}: a pack and its codes");
        let leftover = store.get(&keys().pack_pointer()).unwrap().unwrap();
        // Again, into the same directory: it starts over and finishes.
        let r = upgrade_with_store(&cfg_of(kind, &dir, &k), store.clone(), &dir).unwrap();
        assert_upgraded(kind, &store, &k, &r);
        assert!(!first_packs.contains(&r.pack_key), "{kind:?}: a new pack");
        let now = store.get(&keys().pack_pointer()).unwrap().unwrap();
        assert_ne!(
            now.body, leftover.body,
            "{kind:?}: the pointer was replaced"
        );
        // GC, once the orphan grace and the pack keep time have passed, deletes the first
        // pack and every page and manifest, and keeps the pointer's pack.
        store.advance_clock(crate::config::defaults::PACK_ORPHAN_GRACE + Duration::from_secs(60));
        let gc = crate::gc_with_store(&cfg_of(kind, t.path(), &k), store.clone()).unwrap();
        assert_eq!(gc.outcome, crate::GcOutcome::Trimmed, "{kind:?}: {gc:?}");
        let packs = objects_under(&store, &keys().pack_root());
        for key in &first_packs {
            assert!(!packs.contains(key), "{kind:?}: {key} is still there");
        }
        assert!(packs.contains(&r.pack_key), "{kind:?}");
        assert!(objects_under(&store, &keys().page_root()).is_empty());
        assert!(objects_under(&store, &keys().manifest_root()).is_empty());
        assert_eq!(restored(kind, &store, &k).1, want(1..=30), "{kind:?}");
    }
}

#[test]
fn upgrade_then_restore_then_commit_then_pack_run() {
    let store = mem();
    let k = tempdir();
    legacy_bucket(Legacy::F1, &store, &k);
    let (_u, r) = upgrade_into(Legacy::F1, store.clone(), &k);
    let r = r.unwrap();
    // The restore chain from the first pack ends at the head's generation: the pack
    // generation is the head's.
    let h = head(&*store);
    let p = pointer_of(Legacy::F1, &*store, &k);
    assert_eq!(p.pack_generation, h.generation);
    assert_eq!(r.generation, h.generation);
    let t = tempdir();
    let dir = fresh(&t);
    let rep = restore_with_store(&config(&dir), store.clone(), &dir).unwrap();
    assert_eq!(rep.generation, h.generation);
    assert_eq!(rep.pack_key.as_deref(), Some(p.pack_key.as_str()));
    let db = open_with_store(config(&dir), store.clone()).unwrap();
    let c = db.connect().unwrap();
    assert_eq!(all_rows(&c), want(1..=30));
    put_rows(&c, 31..=40);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    put_rows(&c, 41..=41);
    assert!(
        db.generation() > h.generation,
        "the switch ended the head's generation"
    );
    let w = tempdir();
    let mut pcfg = config(&fresh(&w));
    pcfg.dir = w.path().join("packer");
    let pr = crate::pack::pack_with_store(&pcfg, store.clone()).unwrap();
    assert_eq!(pr.outcome, crate::PackOutcome::Packed, "{pr:?}");
    assert_eq!(pr.pack_generation, db.generation());
    c.close().unwrap();
    db.close().unwrap();
    let (rep, rows) = restored(Legacy::F1, &store, &k);
    assert_eq!(rows, want(1..=41));
    assert_ne!(rep.pack_key.as_deref(), Some(p.pack_key.as_str()));
}

#[test]
fn upgrade_fences_the_old_writer() {
    for kind in [Legacy::F1, Legacy::F4] {
        let store = mem();
        let k = tempdir();
        let (_t, old_db, old) = legacy_writer(kind, &store, &k);
        let (_u, r) = upgrade_into(kind, store.clone(), &k);
        let r = r.unwrap();
        let late = c_insert(&old, 99);
        assert!(matches!(late, Err(Error::Fenced(_))), "{kind:?}: {late:?}");
        assert_upgraded(kind, &store, &k, &r);
        crash(old_db, vec![old]);
    }
}

fn c_insert(c: &Connection, id: i64) -> Result<u64> {
    c.execute(
        "INSERT INTO t(id, v) VALUES (?, ?)",
        &[Value::from_i64(id), Value::build_text(row_text(id))],
    )
}

#[test]
fn old_build_refuses_format_5() {
    let store = mem();
    let k = tempdir();
    legacy_bucket(Legacy::F1, &store, &k);
    upgrade_into(Legacy::F1, store.clone(), &k).1.unwrap();
    let body = store.get(&keys().head()).unwrap().unwrap().body;
    // The formats the builds before format 5 accept (3ac8ff3 and older).
    assert_eq!(
        Head::decode_accepting(&body, &[FORMAT, FORMAT_V2, FORMAT_V3, FORMAT_V4]),
        Err(Error::Corrupt("head: unsupported format 5".into()))
    );
}

#[test]
fn gc_deletes_pages_and_manifest_after_upgrade_and_trim_grace() {
    for kind in [Legacy::F1, Legacy::F4] {
        let store = mem();
        let k = tempdir();
        legacy_bucket(kind, &store, &k);
        let (_u, r) = upgrade_into(kind, store.clone(), &k);
        let r = r.unwrap();
        let t = tempdir();
        let cfg = cfg_of(kind, t.path(), &k);
        assert!(!objects_under(&store, &keys().page_root()).is_empty());
        // Before the trim grace: nothing goes.
        let gc = crate::gc_with_store(&cfg, store.clone()).unwrap();
        assert_eq!(gc.outcome, crate::GcOutcome::TooYoung, "{kind:?}: {gc:?}");
        assert!(!objects_under(&store, &keys().page_root()).is_empty());
        assert!(!objects_under(&store, &keys().manifest_root()).is_empty());
        let old_log = keys().log_prefix(r.from_generation);
        assert!(!objects_under(&store, &old_log).is_empty());
        // After it: every page and manifest goes, and the log below the pack generation.
        let grace = cfg.engine(crate::io::IoMode::Writer).gc_min_age();
        store.advance_clock(grace + Duration::from_secs(1));
        let gc = crate::gc_with_store(&cfg, store.clone()).unwrap();
        assert_eq!(gc.outcome, crate::GcOutcome::Trimmed, "{kind:?}: {gc:?}");
        assert!(objects_under(&store, &keys().page_root()).is_empty());
        assert!(objects_under(&store, &keys().manifest_root()).is_empty());
        assert!(objects_under(&store, &old_log).is_empty());
        assert!(store.get(&r.pack_key).unwrap().is_some());
        assert_eq!(restored(kind, &store, &k).1, want(1..=30), "{kind:?}");
    }
}

#[test]
fn upgrade_of_a_format_5_bucket_is_refused() {
    let store = mem();
    let k = tempdir();
    legacy_bucket(Legacy::F1, &store, &k);
    upgrade_into(Legacy::F1, store.clone(), &k).1.unwrap();
    let before = objects_under(&store, "");
    let (_t, r) = upgrade_into(Legacy::F1, store.clone(), &k);
    assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
    assert_eq!(objects_under(&store, ""), before, "nothing written");
}

#[test]
fn upgrade_into_a_directory_with_a_database_is_refused() {
    let store = mem();
    let k = tempdir();
    let (t, db, c) = legacy_writer(Legacy::F1, &store, &k);
    c.close().unwrap();
    db.close().unwrap();
    let before = head(&*store);
    let r = upgrade_with_store(&config(&fresh(&t)), store.clone(), &fresh(&t));
    assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
    assert_eq!(head(&*store), before, "nothing written");
}
