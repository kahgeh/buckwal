use super::*;
use crate::config::{Event, EventHook};
use crate::conn::{CheckpointMode, Connection};
use crate::db::{open_read_only_with_store, open_with_store};
use crate::engine::tests::{head, keys, PREFIX};
use crate::format::Manifest;
use crate::restore::restore_with_store;
use crate::store::{FaultStore, MemStore};
use crate::Value;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Mutex;
use tempfile::TempDir;

/// Defaults (min age 641 s), the test prefix, and no GC of the writer's own: only `gc` trims.
fn config(dir: &Path) -> Config {
    let mut c = Config::new(dir, "http://127.0.0.1:1", "bucket", "key", "secret");
    c.prefix = PREFIX.to_string();
    c.gc_after_seal = false;
    c
}

fn min_age() -> Duration {
    config(Path::new("/x"))
        .engine(crate::io::IoMode::Writer)
        .gc_min_age()
}

fn mem() -> Arc<MemStore> {
    Arc::new(MemStore::new())
}

fn tempdir() -> TempDir {
    tempfile::tempdir().unwrap()
}

fn insert(c: &Connection, id: i64) {
    c.execute(
        "INSERT INTO t(id, v) VALUES (?, ?)",
        &[Value::from_i64(id), Value::build_text(format!("row {id}"))],
    )
    .unwrap();
}

/// Rows `from..=to`, then a TRUNCATE checkpoint: one seal.
fn seal_rows(c: &Connection, from: i64, to: i64) {
    for id in from..=to {
        insert(c, id);
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
}

/// A writer that creates `t`, commits and seals `seals` times (10 rows each) and closes.
fn sealed_writer(store: &Arc<MemStore>, cfg: Config, seals: i64) -> i64 {
    let db = open_with_store(cfg, store.clone()).unwrap();
    let c = db.connect().unwrap();
    c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    for s in 0..seals {
        seal_rows(&c, s * 10 + 1, s * 10 + 10);
    }
    c.close().unwrap();
    db.close().unwrap();
    seals * 10
}

fn all_keys(store: &MemStore) -> BTreeSet<String> {
    store.list("").unwrap().into_iter().collect()
}

/// What `trim_once` against the current head would delete, from the key list: logs below
/// the head's generation, manifests below its manifest, and pages below its manifest that
/// the manifest does not map.
fn reachable_no_more(store: &MemStore) -> BTreeSet<String> {
    let k = keys();
    let h = head(store);
    let m_gen = h.manifest.expect("a sealed head");
    let m = Manifest::decode(&store.get(&k.manifest(m_gen)).unwrap().unwrap().body).unwrap();
    all_keys(store)
        .into_iter()
        .filter(
            |key| match (k.parse_log(key), k.parse_manifest(key), k.parse_page(key)) {
                (Some((g, _)), _, _) => g < h.generation,
                (_, Some(g), _) => g < m_gen,
                (_, _, Some((g, p))) => g < m_gen && m.pages.get(&p) != Some(&g),
                _ => false,
            },
        )
        .collect()
}

/// Every row restores, and the database is intact.
fn restores_rows(store: &Arc<MemStore>, rows: i64) {
    let v = tempdir();
    let db = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    let ids: Vec<i64> = c
        .query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect();
    assert_eq!(ids, (1..=rows).collect::<Vec<_>>());
    let ok = c.query("PRAGMA integrity_check", &[]).unwrap();
    assert_eq!(ok[0][0].to_text(), Some("ok"));
}

fn recorder() -> (EventHook, Arc<Mutex<Vec<u64>>>) {
    let seen: Arc<Mutex<Vec<u64>>> = Arc::default();
    let s = seen.clone();
    let hook: EventHook = Arc::new(move |e: &Event| {
        if let Event::Trimmed { deleted } = e {
            s.lock().unwrap().push(*deleted);
        }
    });
    (hook, seen)
}

#[test]
fn gc_deletes_nothing_while_the_manifest_is_young() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    let store = mem();
    let a = tempdir();
    sealed_writer(&store, config(a.path()), 2);
    let before = all_keys(&store);
    assert!(!reachable_no_more(&store).is_empty(), "something to trim");

    let rep = gc_with_store(&config(a.path()), store.clone()).unwrap();
    assert_eq!(rep.outcome, GcOutcome::TooYoung, "{rep:?}");
    assert_eq!((rep.deleted, rep.trimmed_against), (0, None), "{rep:?}");
    assert_eq!(rep.generation, head(&*store).generation);
    let pass = gc_once(&*store, &keys(), min_age(), None).unwrap();
    assert_eq!(pass.outcome, GcOutcome::TooYoung);
    let due = pass.next_due.expect("a manifest comes of age");
    assert!(due <= min_age() && due > Duration::ZERO, "{due:?}");
    assert_eq!(all_keys(&store), before);
}

#[test]
fn gc_deletes_what_an_old_enough_head_no_longer_reaches() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    let store = mem();
    let a = tempdir();
    let rows = sealed_writer(&store, config(a.path()), 2);
    store.advance_clock(min_age() + Duration::from_secs(1));
    let doomed = reachable_no_more(&store);
    assert!(!doomed.is_empty());
    let before = all_keys(&store);
    let h = head(&*store);

    let rep = gc_with_store(&config(a.path()), store.clone()).unwrap();
    assert_eq!(rep.outcome, GcOutcome::Trimmed, "{rep:?}");
    assert_eq!(rep.trimmed_against, h.manifest);
    assert_eq!(rep.generation, h.generation);
    assert!(rep.age.unwrap() >= min_age(), "{rep:?}");
    assert_eq!(rep.deleted, doomed.len() as u64, "{doomed:?}");
    let want: BTreeSet<String> = before.difference(&doomed).cloned().collect();
    assert_eq!(all_keys(&store), want);
    restores_rows(&store, rows);

    let again = gc_with_store(&config(a.path()), store.clone()).unwrap();
    assert_eq!((again.outcome, again.deleted), (GcOutcome::Trimmed, 0));
}

#[test]
fn gc_uses_the_newest_old_enough_manifest() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    let store = mem();
    let a = tempdir();
    let k = keys();
    let db = open_with_store(config(a.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    seal_rows(&c, 1, 10);
    assert_eq!(head(&*store).manifest, Some(1));
    store.advance_clock(min_age() + Duration::from_secs(1));
    seal_rows(&c, 11, 20);
    assert_eq!(head(&*store).manifest, Some(2));
    c.close().unwrap();
    db.close().unwrap();

    let rep = gc_with_store(&config(a.path()), store.clone()).unwrap();
    assert_eq!(rep.outcome, GcOutcome::Trimmed, "{rep:?}");
    assert_eq!(rep.trimmed_against, Some(1), "{rep:?}");
    assert!(store.list(&k.log_prefix(1)).unwrap().is_empty(), "log/1");
    assert!(!store.list(&k.log_prefix(2)).unwrap().is_empty(), "log/2");
    assert!(store.get(&k.manifest(1)).unwrap().is_some(), "manifest/1");
    restores_rows(&store, 20);
}

#[test]
fn gc_with_no_head_or_no_manifest_does_nothing() {
    let store = mem();
    let a = tempdir();
    let rep = gc_with_store(&config(a.path()), store.clone()).unwrap();
    assert_eq!((rep.outcome, rep.generation), (GcOutcome::Nothing, 0));
    // A new database: generation 1, no manifest yet.
    let db = open_with_store(config(a.path()), store.clone()).unwrap();
    db.close().unwrap();
    store.advance_clock(min_age() + Duration::from_secs(1));
    let rep = gc_with_store(&config(a.path()), store.clone()).unwrap();
    assert_eq!(
        (rep.outcome, rep.deleted),
        (GcOutcome::Nothing, 0),
        "{rep:?}"
    );
    assert_eq!(rep.generation, 1);
}

/// The pass the trimmer already made against manifest `last` is not made again.
#[test]
fn gc_once_skips_the_manifest_it_last_trimmed_against() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    let store = mem();
    let a = tempdir();
    sealed_writer(&store, config(a.path()), 2);
    store.advance_clock(min_age() + Duration::from_secs(1));
    let m = head(&*store).manifest;
    let fault = FaultStore::new(store.clone());
    let pass = gc_once(&fault, &keys(), min_age(), m).unwrap();
    assert_eq!((pass.outcome, pass.deleted), (GcOutcome::Nothing, 0));
    // The head, the list and the manifest's stat: nothing else.
    let ev = fault.events();
    assert_eq!(ev.len(), 3, "{ev:?}");
    assert!(ev[2].starts_with("stat "), "{ev:?}");
}

#[test]
fn short_lived_writer_trims_at_open() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    for gc_after_seal in [true, false] {
        let store = mem();
        let a = tempdir();
        let mut cfg = config(a.path());
        cfg.gc_after_seal = gc_after_seal;
        sealed_writer(&store, cfg.clone(), 2);
        store.advance_clock(min_age() + Duration::from_secs(1));
        let doomed = reachable_no_more(&store);
        assert!(!doomed.is_empty());

        let (hook, seen) = recorder();
        cfg.on_event = Some(hook);
        let db = open_with_store(cfg, store.clone()).unwrap();
        db.close().unwrap();
        let now = all_keys(&store);
        let seen = seen.lock().unwrap().clone();
        if gc_after_seal {
            assert!(matches!(seen[..], [n] if n > 0), "{seen:?}");
            assert!(now.is_disjoint(&doomed), "{:?}", now.intersection(&doomed));
        } else {
            assert!(seen.is_empty(), "{seen:?}");
            assert!(now.is_superset(&doomed));
        }
    }
}

/// A restore of the current head and a GC of an old enough manifest, interleaved: GC deletes
/// nothing the restore reads.
#[test]
fn gc_during_restore_keeps_what_the_restore_reads() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    let store = mem();
    let a = tempdir();
    let rows = sealed_writer(&store, config(a.path()), 3);
    store.advance_clock(min_age() + Duration::from_secs(1));
    let fault = Arc::new(FaultStore::new(store.clone()));
    fault.delay_gets(Duration::from_millis(20));
    let r = tempdir();
    let (restored, collected) = std::thread::scope(|s| {
        let restore = s.spawn(|| {
            restore_with_store(&config(r.path()), fault.clone(), r.path()).map(|rep| rep.generation)
        });
        let gc = s.spawn(|| gc_with_store(&config(a.path()), fault.clone()));
        (restore.join().unwrap(), gc.join().unwrap())
    });
    assert_eq!(restored.unwrap(), head(&*store).generation);
    let rep = collected.unwrap();
    assert_eq!(rep.outcome, GcOutcome::Trimmed, "{rep:?}");
    assert!(rep.deleted > 0, "{rep:?}");
    let db = open_read_only_with_store(config(r.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    let ok = c.query("PRAGMA integrity_check", &[]).unwrap();
    assert_eq!(ok[0][0].to_text(), Some("ok"));
    let n = c.query("SELECT count(*) FROM t", &[]).unwrap()[0][0].as_int();
    assert_eq!(n, Some(rows));
}

/// `gc` needs no directory: `config.dir` is never touched.
#[test]
fn gc_ignores_the_directory() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    let store = mem();
    let a = tempdir();
    sealed_writer(&store, config(a.path()), 2);
    let missing = a.path().join("never").join("made");
    let rep = gc_with_store(&config(&missing), store.clone()).unwrap();
    assert_eq!(rep.outcome, GcOutcome::TooYoung);
    assert!(!missing.exists());
    // The config is still validated.
    let mut bad = config(&missing);
    bad.bucket.clear();
    assert!(matches!(
        gc_with_store(&bad, store.clone()),
        Err(Error::Config(_))
    ));
}

fn mvcc_config(dir: &Path) -> Config {
    let mut c = config(dir);
    c.journal_mode = crate::config::JournalMode::Mvcc;
    c.retry_deadline = Duration::from_millis(300);
    c.maintenance_log_objects = 0;
    c.maintenance_wal_bytes = 0;
    c
}

/// The kind byte of a log object (1: kind 1, 2: fence, 3: kind 3).
fn log_kind(store: &MemStore, key: &str) -> u8 {
    store.get(key).unwrap().unwrap().body[8]
}

fn restored_mvcc_rows(store: &Arc<MemStore>) -> Vec<i64> {
    let v = tempdir();
    let db = open_read_only_with_store(mvcc_config(v.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    let ok = c.query("PRAGMA integrity_check", &[]).unwrap();
    assert_eq!(ok[0][0].to_text(), Some("ok"));
    c.query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect()
}

/// Three MVCC seals, then five unsealed commits (kind 3 in the current generation), the
/// writer crashed. After the manifest comes of age GC keeps exactly the head's manifest, its
/// pages and the current generation's log (kind 3 included), deletes every older generation's
/// kind-1 and kind-3 objects, and a restore still has every row.
#[test]
fn gc_and_trim_across_mvcc_generations_keep_only_sealed_manifest_pages_and_log() {
    // A seal test of formats 2 and 4 (`bucket::legacy`): MVCC buckets made before format 5.
    let _legacy = crate::format::LegacySeal::on();
    let store = mem();
    let a = tempdir();
    let k = keys();
    let db = open_with_store(mvcc_config(a.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    // The first seal writes many pages; the later ones touch few, so the head's manifest
    // maps some pages to an older generation (kept) next to newer ones.
    for id in 1..=300 {
        c.execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            &[Value::from_i64(id), Value::build_text("x".repeat(200))],
        )
        .unwrap();
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    seal_rows(&c, 301, 302);
    seal_rows(&c, 303, 304);
    for id in 305..=309 {
        insert(&c, id);
    }
    let h = head(&*store);
    assert_eq!(h.journal_mode, crate::config::JournalMode::Mvcc);
    assert!(h.manifest.unwrap() >= 3, "{h:?}");
    // Old generations hold kind-3 (and kind-1) objects now doomed.
    let old_logs: Vec<String> = all_keys(&store)
        .into_iter()
        .filter(|key| matches!(k.parse_log(key), Some((g, _)) if g < h.generation))
        .collect();
    assert!(
        old_logs.iter().any(|key| log_kind(&store, key) == 3),
        "an old generation shipped kind-3 records"
    );
    let current: Vec<String> = store.list(&k.log_prefix(h.generation)).unwrap();
    assert!(
        current.iter().any(|key| log_kind(&store, key) == 3),
        "the current generation holds kind-3 records: {current:?}"
    );
    store.advance_clock(min_age() + Duration::from_secs(1));
    let doomed = reachable_no_more(&store);
    assert!(old_logs.iter().all(|l| doomed.contains(l)));
    let before = all_keys(&store);

    let rep = gc_with_store(&mvcc_config(a.path()), store.clone()).unwrap();
    assert_eq!(rep.outcome, GcOutcome::Trimmed, "{rep:?}");
    assert_eq!(rep.trimmed_against, h.manifest);
    assert_eq!(rep.deleted, doomed.len() as u64);
    let now = all_keys(&store);
    assert_eq!(now, before.difference(&doomed).cloned().collect());
    // The current generation's log, kind 3 included, and the head's manifest and pages stay.
    for key in &current {
        assert!(now.contains(key), "{key} was trimmed");
    }
    let m = Manifest::decode(
        &store
            .get(&k.manifest(h.manifest.unwrap()))
            .unwrap()
            .unwrap()
            .body,
    )
    .unwrap();
    for (p, g) in &m.pages {
        assert!(now.contains(&k.page(*g, *p)), "page {p} of {g}");
    }
    assert!(
        m.pages.values().any(|g| *g < h.manifest.unwrap()),
        "the head's manifest maps a page to an older generation: {:?}",
        m.pages
    );
    // Only one manifest older than the head's is gone, none newer; no old-generation log.
    assert!(now
        .iter()
        .all(|key| !matches!(k.parse_log(key), Some((g, _)) if g < h.generation)));
    assert_eq!(
        now.iter().filter_map(|key| k.parse_manifest(key)).min(),
        h.manifest
    );
    // A restore after the trim has every row.
    assert_eq!(restored_mvcc_rows(&store), (1..=309).collect::<Vec<_>>());
    std::mem::forget(c);
    std::mem::forget(db);
}

// ---- GC and the skipped generation (item 1) ----

fn insert_kb(c: &Connection, id: i64) {
    c.execute(
        "INSERT INTO t(id, v) VALUES (?, ?)",
        &[Value::from_i64(id), Value::build_text("x".repeat(1024))],
    )
    .unwrap();
}

/// Writer A creates `t` with 1..=60 rows of 1 KB and seals (generation 1 into manifest 1, head
/// 2), commits 61..=65 in generation 2 and crashes. Returns the writer's directory.
fn crashed_after_one_seal(store: &Arc<MemStore>) -> TempDir {
    let a = tempdir();
    let db = open_with_store(config(a.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    for id in 1..=60 {
        insert_kb(&c, id);
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    for id in 61..=65 {
        insert_kb(&c, id);
    }
    std::mem::forget(c);
    std::mem::forget(db);
    a
}

/// B takes A's directory's bucket over and opens (no GC of its own).
fn takeover_and_open(store: &Arc<MemStore>, holder: &TempDir) -> (crate::db::Database, Connection) {
    let dir = holder.path().join("db");
    crate::takeover_with_store(config(&dir), store.clone()).unwrap();
    let db = open_with_store(config(&dir), store.clone()).unwrap();
    let c = db.connect().unwrap();
    (db, c)
}

fn rows_of(store: &Arc<MemStore>) -> Vec<i64> {
    let v = tempdir();
    let db = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    c.query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect()
}

/// A's seal of generation 2 got as far as its manifest and crashed (`manifest/2`, mapping every
/// page to generation 2, old enough to trim against). B takes over, and its skip seal's
/// `manifest/4`... is younger than the trim grace. GC must not trim against A's manifest: it
/// would delete `pages/1/*` that B's manifest still maps. It trims nothing, and a restore from
/// B's head still works.
#[test]
fn gc_ignores_fenced_writers_leftover_manifest() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    let store = mem();
    let _a = crashed_after_one_seal(&store);
    let k = keys();
    let m1 = Manifest::decode(&store.get(&k.manifest(1)).unwrap().unwrap().body).unwrap();
    assert!(m1.pages.len() > 3, "a database of several pages: {m1:?}");
    let leftover = Manifest {
        pages: m1.pages.keys().map(|p| (*p, 2)).collect(),
        ..m1.clone()
    };
    store.put(&k.manifest(2), &leftover.encode()).unwrap();
    store.advance_clock(min_age() + Duration::from_secs(1));

    let tb = tempdir();
    let (db, c) = takeover_and_open(&store, &tb);
    for id in 1001..=1003 {
        insert_kb(&c, id);
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    let h = head(&*store);
    assert_eq!((h.generation, h.manifest), (4, Some(3)), "{h:?}");
    let before = all_keys(&store);

    let pass = gc_once(&*store, &k, min_age(), None).unwrap();
    assert_eq!(pass.outcome, GcOutcome::TooYoung, "{pass:?}");
    assert_eq!((pass.deleted, pass.trimmed_against), (0, None), "{pass:?}");
    assert_eq!(all_keys(&store), before, "GC deleted something");
    let mut want: Vec<i64> = (1..=65).collect();
    want.extend(1001..=1003);
    assert_eq!(rows_of(&store), want);
    std::mem::forget(c);
    std::mem::forget(db);
}

/// Between a takeover and its skip seal the head carries a floor above its manifest, so GC
/// has no manifest it may trust and trims nothing, however old they are. Once the skip seal
/// lands and its manifest is old enough, GC trims against it and removes A's leftover.
#[test]
fn gc_pause_between_takeover_and_skip_seal_ends() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    let store = mem();
    let _a = crashed_after_one_seal(&store);
    let k = keys();
    store.put(&k.page(2, 1), b"A's page").unwrap();
    store.advance_clock(min_age() + Duration::from_secs(1));
    let tb = tempdir();
    let (db, c) = takeover_and_open(&store, &tb);
    let h = head(&*store);
    assert_eq!(
        (h.generation, h.manifest, h.next_generation_floor),
        (2, Some(1), Some(4))
    );
    let before = all_keys(&store);
    let pass = gc_once(&*store, &k, min_age(), None).unwrap();
    assert_eq!(
        (pass.outcome, pass.deleted),
        (GcOutcome::Nothing, 0),
        "{pass:?}"
    );
    assert_eq!(all_keys(&store), before, "paused: nothing trimmed");

    for id in 1001..=1003 {
        insert_kb(&c, id);
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    store.advance_clock(min_age() + Duration::from_secs(1));
    let pass = gc_once(&*store, &k, min_age(), None).unwrap();
    assert_eq!(pass.outcome, GcOutcome::Trimmed, "{pass:?}");
    assert_eq!(pass.trimmed_against, Some(3), "{pass:?}");
    assert!(
        store.get(&k.page(2, 1)).unwrap().is_none(),
        "A's leftover is gone"
    );
    assert!(store.get(&k.manifest(1)).unwrap().is_none(), "manifest/1");
    let mut want: Vec<i64> = (1..=65).collect();
    want.extend(1001..=1003);
    assert_eq!(rows_of(&store), want);
    std::mem::forget(c);
    std::mem::forget(db);
}

/// A format 5 bucket with no pack: its head names no manifest, so a GC pass (the trimmer's, or
/// `gc`) deletes nothing. The log is the only copy of the database until a pack exists.
#[test]
fn gc_of_a_format_5_bucket_without_a_pack_deletes_no_log() {
    let store = mem();
    let w = tempdir();
    let mut cfg = config(w.path());
    // The writer's own trimmer runs too, after each switch.
    cfg.gc_after_seal = true;
    let rows = sealed_writer(&store, cfg, 3);
    let h = head(&*store);
    assert_eq!((h.format, h.manifest), (crate::format::FORMAT_V5, None));
    let before = all_keys(&store);
    let k = keys();
    let r = gc_with_store(&config(Path::new("/x")), store.clone()).unwrap();
    assert_eq!((r.outcome, r.deleted), (GcOutcome::Nothing, 0));
    // A pass with no minimum age, as the trimmer of a writer whose grace has passed.
    let pass = gc_once(&*store, &k.for_open(), Duration::ZERO, None).unwrap();
    assert_eq!((pass.outcome, pass.deleted), (GcOutcome::Nothing, 0));
    assert_eq!(all_keys(&store), before);
    for g in 1..h.generation {
        assert!(
            before
                .iter()
                .any(|key| k.parse_log(key).map(|(lg, _)| lg) == Some(g)),
            "log/{g} is gone"
        );
    }
    restores_rows(&store, rows);
}
