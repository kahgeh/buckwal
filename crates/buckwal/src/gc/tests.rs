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

/// Defaults (min age 641 s) and the test prefix. A writer runs its own GC thread too; it
/// deletes nothing until the test moves the bucket's clock.
fn config(dir: &Path) -> Config {
    let mut c = Config::new(dir, "http://127.0.0.1:1", "bucket", "key", "secret");
    c.prefix = PREFIX.to_string();
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
    let store = mem();
    let a = tempdir();
    let mut cfg = config(a.path());
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
    assert!(matches!(seen[..], [n] if n > 0), "{seen:?}");
    assert!(now.is_disjoint(&doomed), "{:?}", now.intersection(&doomed));
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

// ---- Format 5: packs, the pack pointer, the log and multipart uploads (spec "GC") ----

fn orphan_grace() -> Duration {
    config(Path::new("/x")).pack_orphan_grace
}

/// A pass of `gc` over `store`.
fn gc5(store: &Arc<MemStore>) -> GcReport {
    gc_with_store(&config(Path::new("/x")), store.clone()).unwrap()
}

fn exists(store: &MemStore, key: &str) -> bool {
    store.get(key).unwrap().is_some()
}

/// The generations that have log objects.
fn log_gens(store: &MemStore) -> BTreeSet<u64> {
    let k = keys();
    all_keys(store)
        .iter()
        .filter_map(|key| k.parse_log(key).map(|(g, _)| g))
        .collect()
}

/// The keys under `pack/`.
fn pack_keys(store: &MemStore) -> BTreeSet<String> {
    store
        .list(&keys().pack_root())
        .unwrap()
        .into_iter()
        .collect()
}

/// A pack `pack/<generation>/<run>` and its codes, by hand: GC reads only their keys and ages.
fn put_pack(store: &MemStore, generation: u64, run: &str) -> (String, String) {
    let k = keys();
    let (p, c) = (k.pack(generation, run), k.pack_codes(generation, run));
    store.put(&p, b"pack").unwrap();
    store.put(&c, b"codes").unwrap();
    (p, c)
}

fn pointer_body(generation: u64, run: &str, database_id: Option<String>) -> Vec<u8> {
    pointer_body_sized(generation, run, database_id, 4)
}

fn pointer_body_sized(
    generation: u64,
    run: &str,
    database_id: Option<String>,
    size: u64,
) -> Vec<u8> {
    let k = keys();
    k.encode_pointer(&crate::pack::PackPointer {
        format: crate::pack::pointer::POINTER_FORMAT,
        pack_key: k.pack(generation, run),
        codes_key: k.pack_codes(generation, run),
        write_id: format!("{generation}{run}"),
        pack_generation: generation,
        size,
        page_size: 4096,
        part_size: 16 << 20,
        block_size: 1 << 20,
        database_id,
    })
}

/// Move this database's pack pointer to `pack/<generation>/<run>`.
fn point(store: &MemStore, generation: u64, run: &str) {
    let id = head(store).database_id;
    assert!(id.is_some(), "a format 5 head has a database_id");
    store
        .put(&keys().pack_pointer(), &pointer_body(generation, run, id))
        .unwrap();
}

/// A format 5 writer made `switches` generation switches (10 rows each) and closed: head
/// generation `switches + 1`, log in every generation below it.
fn switched(store: &Arc<MemStore>, switches: i64) -> i64 {
    let a = tempdir();
    let rows = sealed_writer(store, config(a.path()), switches);
    let h = head(&**store);
    assert_eq!(
        (h.format, h.manifest, h.generation),
        (crate::format::FORMAT_V5, None, switches as u64 + 1)
    );
    for g in 1..=switches as u64 {
        assert!(log_gens(store).contains(&g), "log/{g}");
    }
    rows
}

/// A source whose every read fails: its multipart upload stays incomplete in the store.
struct FailingSource;

impl crate::store::PartSource for FailingSource {
    fn len(&self) -> u64 {
        10
    }
    fn read_at(&self, _: u64, _: &mut [u8]) -> std::io::Result<()> {
        Err(std::io::Error::other("crashed"))
    }
}

/// A pack run that crashed during its upload: an incomplete multipart upload under `pack/`.
fn crashed_upload(store: &MemStore) {
    let key = keys().pack(9, "crashed");
    assert!(store.put_multipart(&key, &mut FailingSource, 4, 1).is_err());
    assert!(!exists(store, &key));
}

/// A pack `a` that the pointer named, then a newer pack `b` that it names now. `a` goes, with
/// its codes, only once the trim grace has passed since `b` was uploaded; `b` stays.
#[test]
fn gc_deletes_older_pack_and_codes_after_trim_grace() {
    let store = mem();
    switched(&store, 3);
    let (old_p, old_c) = put_pack(&store, 2, "a");
    point(&store, 2, "a");
    store.advance_clock(2 * min_age());
    let (new_p, new_c) = put_pack(&store, 3, "b");
    point(&store, 3, "b");

    // `a` is old, but `b` is young: `a` stays. `a` held generation 1 long enough, so log/1
    // goes; log/2 stays (only `b` holds it, and `b` is young).
    let r = gc5(&store);
    assert_eq!((r.packs_deleted, r.orphan_packs_deleted), (0, 0), "{r:?}");
    for key in [&old_p, &old_c, &new_p, &new_c] {
        assert!(exists(&store, key), "{key}");
    }
    assert_eq!(log_gens(&store).first(), Some(&2), "{r:?}");

    store.advance_clock(min_age());
    let r = gc5(&store);
    assert_eq!(r.outcome, GcOutcome::Trimmed, "{r:?}");
    assert_eq!((r.packs_deleted, r.orphan_packs_deleted), (1, 0), "{r:?}");
    assert!(!exists(&store, &old_p) && !exists(&store, &old_c));
    assert!(exists(&store, &new_p) && exists(&store, &new_c));
    assert_eq!(pack_keys(&store), BTreeSet::from([new_c, new_p]));
    assert_eq!(log_gens(&store).first(), Some(&3), "log below 3 only");
}

/// The pack the pointer names stays, with its codes, however old it is; another pack of the
/// same generation that the pointer never named is an orphan.
#[test]
fn gc_keeps_the_pack_the_pointer_names() {
    let store = mem();
    switched(&store, 3);
    let (p, c) = put_pack(&store, 3, "named");
    let (op, oc) = put_pack(&store, 3, "other");
    point(&store, 3, "named");
    store.advance_clock(orphan_grace() + min_age());
    for _ in 0..2 {
        let r = gc5(&store);
        assert_eq!(r.outcome, GcOutcome::Trimmed, "{r:?}");
        assert!(exists(&store, &p) && exists(&store, &c), "{r:?}");
    }
    assert!(!exists(&store, &op) && !exists(&store, &oc));
    assert_eq!(pack_keys(&store), BTreeSet::from([c, p]));
}

/// A complete pack of a later pack generation whose run has not moved the pointer yet stays
/// past the trim grace: it is an orphan only after the orphan grace.
#[test]
fn gc_keeps_complete_pack_waiting_for_pointer_move() {
    let store = mem();
    switched(&store, 3);
    put_pack(&store, 2, "named");
    point(&store, 2, "named");
    let (p, c) = put_pack(&store, 4, "waiting");
    store.advance_clock(3 * min_age());
    assert!(3 * min_age() < orphan_grace());
    let r = gc5(&store);
    assert_eq!(r.orphan_packs_deleted, 0, "{r:?}");
    assert!(exists(&store, &p) && exists(&store, &c));
    // The run moves the pointer now: the pack is the newest, and stays.
    point(&store, 4, "waiting");
    let r = gc5(&store);
    assert_eq!(r.orphan_packs_deleted, 0, "{r:?}");
    assert!(exists(&store, &p) && exists(&store, &c));
}

/// An orphan pack (complete, never named) goes with its codes once its own upload is older
/// than the orphan grace, with a pointer and without one. A codes object whose pack is gone
/// goes too.
#[test]
fn gc_deletes_orphan_after_orphan_grace() {
    for with_pointer in [true, false] {
        let store = mem();
        switched(&store, 3);
        if with_pointer {
            put_pack(&store, 2, "named");
            point(&store, 2, "named");
        }
        let (p, c) = put_pack(&store, 4, "orphan");
        let (lp, lc) = put_pack(&store, 4, "leftover");
        store.delete(&lp).unwrap();
        store.advance_clock(orphan_grace() - Duration::from_secs(60));
        let r = gc5(&store);
        assert_eq!(r.orphan_packs_deleted, 0, "{r:?}");
        assert!(exists(&store, &p) && exists(&store, &c) && exists(&store, &lc));
        store.advance_clock(Duration::from_secs(120));
        let r = gc5(&store);
        assert_eq!(r.orphan_packs_deleted, 1, "{with_pointer}: {r:?}");
        assert!(!exists(&store, &p) && !exists(&store, &c), "{with_pointer}");
        assert!(!exists(&store, &lc), "{with_pointer}: the leftover codes");
        if with_pointer {
            assert!(exists(&store, &keys().pack(2, "named")));
        } else {
            assert!(pack_keys(&store).is_empty());
            assert_eq!(r.outcome, GcOutcome::Nothing, "{r:?}");
        }
    }
}

/// A part source that, at its second part, moves the bucket's clock past the trim grace and
/// runs a GC pass: an upload that takes longer than the trim grace while GC runs.
struct SlowSource {
    store: Arc<MemStore>,
    body: Vec<u8>,
    during: Mutex<Option<GcReport>>,
}

impl crate::store::PartSource for SlowSource {
    fn len(&self) -> u64 {
        self.body.len() as u64
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
        if offset > 0 && self.during.lock().unwrap().is_none() {
            self.store.advance_clock(3 * min_age());
            *self.during.lock().unwrap() = Some(gc5(&self.store));
        }
        let from = offset as usize;
        buf.copy_from_slice(&self.body[from..from + buf.len()]);
        Ok(())
    }
}

/// A pack upload that outlasts the trim grace while GC runs: GC aborts no upload during it,
/// and keeps the complete pack after it (its Last-Modified is the upload start, older than
/// the trim grace and younger than the orphan grace).
#[test]
fn slow_upload_outlasting_trim_grace_keeps_its_pack() {
    let store = mem();
    switched(&store, 3);
    put_pack(&store, 2, "named");
    point(&store, 2, "named");
    let k = keys();
    let (p, c) = (k.pack(4, "slow"), k.pack_codes(4, "slow"));
    let mut src = SlowSource {
        store: store.clone(),
        body: vec![7; 12],
        during: Mutex::new(None),
    };
    store.put_multipart(&p, &mut src, 4, 1).unwrap();
    let during = src.during.lock().unwrap().clone().expect("a pass ran");
    assert_eq!(during.uploads_aborted, 0, "{during:?}");
    store.put(&c, b"codes").unwrap();
    let age = store.stat(&p).unwrap().unwrap().age;
    assert!(
        age >= 3 * min_age(),
        "Last-Modified is the upload start: {age:?}"
    );
    let r = gc5(&store);
    assert_eq!((r.orphan_packs_deleted, r.uploads_aborted), (0, 0), "{r:?}");
    assert!(exists(&store, &p) && exists(&store, &c));
    assert_eq!(store.get(&p).unwrap().unwrap().body, vec![7; 12]);
}

/// The log of every generation below the pointer's pack generation goes, but only after a HEAD
/// of the pack and of its codes in the same pass; the log at or above it stays.
#[test]
fn gc_deletes_log_below_pack_generation_only_after_head_of_pack_and_codes() {
    let store = mem();
    switched(&store, 4);
    let (p, c) = put_pack(&store, 3, "named");
    point(&store, 3, "named");
    // Young: no log goes.
    let r = gc5(&store);
    assert_eq!((r.outcome, r.deleted), (GcOutcome::TooYoung, 0), "{r:?}");
    assert_eq!(log_gens(&store).first(), Some(&1));

    store.advance_clock(min_age());
    let fault = Arc::new(FaultStore::new(store.clone()));
    let r = gc_with_store(&config(Path::new("/x")), fault.clone()).unwrap();
    assert_eq!(r.outcome, GcOutcome::Trimmed, "{r:?}");
    assert!(r.deleted > 0, "{r:?}");
    let gens = log_gens(&store);
    assert_eq!(gens.first(), Some(&3), "{gens:?}");
    assert!(gens.contains(&4), "{gens:?}");
    let ev = fault.events();
    let at = |e: &str| ev.iter().position(|x| x == e);
    let head_pack = at(&format!("stat {p}")).expect("a HEAD of the pack");
    let head_codes = at(&format!("stat {c}")).expect("a HEAD of the codes");
    let first_log_delete = ev
        .iter()
        .position(|e| e.starts_with(&format!("delete {}", keys().log_root())))
        .expect("a log delete");
    assert!(
        head_pack < first_log_delete && head_codes < first_log_delete,
        "{ev:?}"
    );
}

/// The pointer names a pack, or a codes object, that is gone: the pass deletes no log (the
/// log is then the only copy), no pack and nothing else, and logs an error.
#[test]
fn gc_with_missing_named_pack_deletes_no_log_and_logs_error() {
    for missing in ["pack", "codes"] {
        let store = mem();
        switched(&store, 3);
        put_pack(&store, 2, "old");
        let (p, c) = put_pack(&store, 3, "named");
        point(&store, 3, "named");
        store
            .delete(if missing == "pack" { &p } else { &c })
            .unwrap();
        put_pack(&store, 4, "orphan");
        store.advance_clock(orphan_grace() + min_age());
        let before = all_keys(&store);
        let cap = crate::telemetry::testing::Capture::new();
        let r = tracing::subscriber::with_default(cap.subscriber(), || gc5(&store));
        assert_eq!(r.outcome, GcOutcome::PackMissing, "{missing}: {r:?}");
        assert_eq!(r.deleted, 0, "{missing}: {r:?}");
        assert_eq!(all_keys(&store), before, "{missing}");
        let spans = cap.spans();
        let errors: Vec<_> = spans
            .events
            .iter()
            .filter(|e| e.level == tracing::Level::ERROR)
            .collect();
        assert_eq!(errors.len(), 1, "{missing}: {errors:?}");
        let gone = if missing == "pack" { &p } else { &c };
        assert_eq!(errors[0].field("key"), Some(gone.as_str()), "{errors:?}");
        assert_eq!(
            spans.last_named("buckwal.gc").field("buckwal.outcome"),
            Some("pack_missing")
        );
    }
}

/// An incomplete multipart upload under `pack/` older than the trim grace but younger than the
/// orphan grace may be a live pack run: GC leaves it alone.
#[test]
fn gc_leaves_multipart_younger_than_orphan_grace() {
    let store = mem();
    switched(&store, 2);
    crashed_upload(&store);
    store.advance_clock(3 * min_age());
    assert!(3 * min_age() < orphan_grace());
    let r = gc5(&store);
    assert_eq!(r.uploads_aborted, 0, "{r:?}");
    assert_eq!(
        store
            .abort_incomplete(&keys().pack_root(), Duration::ZERO)
            .unwrap(),
        1,
        "the upload is still there"
    );
}

/// An incomplete multipart upload under `pack/` that started longer ago than the orphan grace
/// is aborted.
#[test]
fn gc_aborts_multipart_older_than_orphan_grace() {
    let store = mem();
    switched(&store, 2);
    crashed_upload(&store);
    store.advance_clock(orphan_grace() + Duration::from_secs(2));
    let r = gc5(&store);
    assert_eq!(r.uploads_aborted, 1, "{r:?}");
    assert_eq!(
        store
            .abort_incomplete(&keys().pack_root(), Duration::ZERO)
            .unwrap(),
        0
    );
}

/// A bucket upgraded from a seal format by hand (Choice D): a format 1 bucket with pages and
/// manifests, then a first pack, its pointer and a format 5 head. While the head is format 1,
/// GC keeps the seal rules and leaves the pack alone. Once the head is format 5 and the pack
/// is old enough, every `pages/` and `manifest/` object goes.
#[test]
fn gc_deletes_pages_and_manifest_in_upgraded_bucket() {
    let store = mem();
    let a = tempdir();
    {
        let _legacy = crate::format::LegacySeal::on();
        sealed_writer(&store, config(a.path()), 2);
    }
    let k = keys();
    let h = head(&*store);
    assert_eq!((h.format, h.generation, h.manifest), (1, 3, Some(2)));
    let seal_objects = |s: &MemStore| -> Vec<String> {
        all_keys(s)
            .into_iter()
            .filter(|key| key.starts_with(&k.page_root()) || key.starts_with(&k.manifest_root()))
            .collect()
    };
    // The upgrade's first pack and pointer, while the head is still format 1.
    let id = "ab".repeat(16);
    let (p, c) = put_pack(&store, 3, "upgrade");
    store
        .put(
            &k.pack_pointer(),
            &pointer_body(3, "upgrade", Some(id.clone())),
        )
        .unwrap();
    store.advance_clock(orphan_grace() + min_age());
    let r = gc5(&store);
    assert_eq!(r.outcome, GcOutcome::Trimmed, "the seal rules: {r:?}");
    assert_eq!((r.packs_deleted, r.orphan_packs_deleted), (0, 0), "{r:?}");
    assert!(exists(&store, &p) && exists(&store, &c));
    let kept = seal_objects(&store);
    assert!(
        kept.iter().any(|key| key.starts_with(&k.page_root())),
        "{kept:?}"
    );
    assert!(kept.contains(&k.manifest(2)), "{kept:?}");

    // The upgrade's head write: format 5, no manifest, the pointer's database_id.
    let h5 = Head {
        format: crate::format::FORMAT_V5,
        manifest: None,
        database_id: Some(id),
        ..h
    };
    store.put(&k.head(), &k.encode_head(&h5)).unwrap();
    let r = gc5(&store);
    assert_eq!(r.outcome, GcOutcome::Trimmed, "{r:?}");
    assert!(
        seal_objects(&store).is_empty(),
        "{:?}",
        seal_objects(&store)
    );
    assert_eq!(r.deleted, kept.len() as u64, "{r:?}");
    assert!(exists(&store, &p) && exists(&store, &c));
}

/// The same while the first pack is younger than the trim grace: the pages stay.
#[test]
fn gc_keeps_pages_of_an_upgraded_bucket_while_its_first_pack_is_young() {
    let store = mem();
    let a = tempdir();
    {
        let _legacy = crate::format::LegacySeal::on();
        sealed_writer(&store, config(a.path()), 2);
    }
    let k = keys();
    let h = head(&*store);
    let h5 = Head {
        format: crate::format::FORMAT_V5,
        manifest: None,
        database_id: Some("cd".repeat(16)),
        ..h
    };
    store.put(&k.head(), &k.encode_head(&h5)).unwrap();
    put_pack(&store, 3, "upgrade");
    point(&store, 3, "upgrade");
    let before = all_keys(&store);
    let r = gc5(&store);
    assert_eq!((r.outcome, r.deleted), (GcOutcome::TooYoung, 0), "{r:?}");
    assert_eq!(all_keys(&store), before);
}

/// With no packer, a format 5 bucket has no pointer: GC (`gc`, and the trimmer's pass) deletes
/// no log, however old it is. The log is the only copy of the database until a pack exists.
#[test]
fn gc_with_no_packer_deletes_no_log() {
    let store = mem();
    let rows = switched(&store, 3);
    store.advance_clock(orphan_grace() + min_age());
    let before = all_keys(&store);
    let r = gc5(&store);
    assert_eq!((r.outcome, r.deleted), (GcOutcome::Nothing, 0), "{r:?}");
    let pass = gc_once(&*store, &keys().for_open(), Duration::ZERO, None).unwrap();
    assert_eq!((pass.outcome, pass.deleted), (GcOutcome::Nothing, 0));
    let gens = log_gens(&store);
    for g in 1..=3 {
        assert!(gens.contains(&g), "log/{g} is gone: {gens:?}");
    }
    assert!(before.is_subset(&all_keys(&store)));
    restores_rows(&store, rows);
}

/// A pointer of another database (another `database_id`) is no pointer: GC deletes no log. The
/// pack it names is an orphan of this database and goes after the orphan grace.
#[test]
fn gc_with_foreign_pointer_deletes_no_log() {
    let store = mem();
    let rows = switched(&store, 3);
    let (p, c) = put_pack(&store, 3, "foreign");
    store
        .put(
            &keys().pack_pointer(),
            &pointer_body(3, "foreign", Some("ee".repeat(16))),
        )
        .unwrap();
    store.advance_clock(3 * min_age());
    let r = gc5(&store);
    assert_eq!((r.outcome, r.deleted), (GcOutcome::Nothing, 0), "{r:?}");
    assert!(exists(&store, &p) && exists(&store, &c));
    store.advance_clock(orphan_grace());
    let r = gc5(&store);
    assert_eq!(r.outcome, GcOutcome::Nothing, "{r:?}");
    assert_eq!(r.orphan_packs_deleted, 1, "{r:?}");
    let gens = log_gens(&store);
    for g in 1..=3 {
        assert!(gens.contains(&g), "log/{g} is gone: {gens:?}");
    }
    assert!(
        exists(&store, &keys().pack_pointer()),
        "GC never deletes the pointer"
    );
    restores_rows(&store, rows);
}

/// End to end: a writer, a pack run, two more switches, a second pack run, commits in the
/// current generation; after the trim grace GC leaves one pack (the newest) with its codes and
/// the log from its pack generation on, and a restore equals the writer.
#[test]
fn gc_after_two_packs_leaves_one_newest_pack_and_needed_log() {
    let store = mem();
    let a = tempdir();
    let packer = tempdir();
    let db = open_with_store(config(a.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    seal_rows(&c, 1, 10);
    seal_rows(&c, 11, 20);
    let first = crate::pack::pack_with_store(&config(packer.path()), store.clone()).unwrap();
    assert_eq!(
        (first.outcome.clone(), first.pack_generation),
        (crate::pack::PackOutcome::Packed, 3)
    );
    seal_rows(&c, 21, 30);
    seal_rows(&c, 31, 40);
    let second = crate::pack::pack_with_store(&config(packer.path()), store.clone()).unwrap();
    assert_eq!(
        (second.outcome.clone(), second.pack_generation),
        (crate::pack::PackOutcome::Packed, 5)
    );
    for id in 41..=45 {
        insert(&c, id);
    }
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(pack_keys(&store).len(), 4, "two packs and their codes");

    store.advance_clock(min_age());
    let r = gc5(&store);
    assert_eq!(r.outcome, GcOutcome::Trimmed, "{r:?}");
    assert_eq!((r.packs_deleted, r.orphan_packs_deleted), (1, 0), "{r:?}");
    let newest = second.pack_key.clone().unwrap();
    assert_eq!(
        pack_keys(&store),
        BTreeSet::from([format!("{newest}.codes"), newest])
    );
    let gens = log_gens(&store);
    assert_eq!(gens.first(), Some(&5), "{gens:?}");
    restores_rows(&store, 45);
    // A second pass has nothing more to delete.
    let again = gc5(&store);
    assert_eq!((again.outcome, again.deleted), (GcOutcome::Trimmed, 0));
}

/// The ages of `gc` with the test config.
fn ages() -> GcAges {
    GcAges::new(min_age(), orphan_grace())
}

/// A pass as the writer's trimmer makes it, with the trimmer's last mark and the pack its
/// background fetch reads.
fn trimmer_pass(store: &dyn ObjectStore, last: Option<&GcMark>, fetching: Option<&str>) -> GcPass {
    gc_pass(store, &keys().for_open(), ages(), last, fetching).unwrap()
}

/// The writer's trimmer, after a pass that left nothing to wait for, skips the trim while the
/// pointer keeps its ETag: no HEAD, no log listing. A pointer move ends it.
#[test]
fn gc_pass_skips_the_pointer_it_last_trimmed_against() {
    let store = mem();
    switched(&store, 3);
    put_pack(&store, 3, "named");
    point(&store, 3, "named");
    store.advance_clock(min_age());
    let first = trimmer_pass(&*store, None, None);
    assert_eq!(first.outcome, GcOutcome::Trimmed, "{first:?}");
    let etag = store.get(&keys().pack_pointer()).unwrap().unwrap().etag;
    assert_eq!(
        first.mark,
        Some(GcMark::Pointer {
            etag,
            fetching: None
        })
    );
    let fault = FaultStore::new(store.clone());
    let again = trimmer_pass(&fault, first.mark.as_ref(), None);
    assert_eq!((again.outcome, again.deleted), (GcOutcome::Nothing, 0));
    let ev = fault.events();
    assert!(!ev.iter().any(|e| e.starts_with("stat ")), "{ev:?}");
    let log_list = format!("list {}", keys().log_root());
    assert!(!ev.contains(&log_list), "{ev:?}");
    // The pointer moves: a whole pass again.
    put_pack(&store, 4, "next");
    point(&store, 4, "next");
    let fault = FaultStore::new(store.clone());
    let moved = trimmer_pass(&fault, first.mark.as_ref(), None);
    // Pack 3 is old enough, pack 4 is not yet: no mark until it is.
    assert_eq!(moved.outcome, GcOutcome::Trimmed, "{moved:?}");
    assert!(
        moved.next_due.is_some() && moved.mark.is_none(),
        "{moved:?}"
    );
    assert!(fault.events().contains(&log_list));
}

/// I1: an orphan pack and a dead multipart upload that appear after the trimmer's mark (a pack
/// run crashed, so the pointer kept its ETag) still go once the orphan grace has passed.
#[test]
fn gc_removes_orphan_and_upload_that_appear_after_the_trimmer_mark() {
    let store = mem();
    switched(&store, 3);
    put_pack(&store, 3, "named");
    point(&store, 3, "named");
    store.advance_clock(min_age());
    let first = trimmer_pass(&*store, None, None);
    assert!(first.mark.is_some(), "{first:?}");
    let (op, oc) = put_pack(&store, 4, "crashed-run");
    crashed_upload(&store);
    let young = trimmer_pass(&*store, first.mark.as_ref(), None);
    assert_eq!((young.orphan_packs_deleted, young.uploads_aborted), (0, 0));
    assert!(young.next_due.is_some(), "the orphan's due time: {young:?}");
    store.advance_clock(orphan_grace() + Duration::from_secs(2));
    let late = trimmer_pass(&*store, first.mark.as_ref(), None);
    assert_eq!(
        (late.orphan_packs_deleted, late.uploads_aborted),
        (1, 1),
        "{late:?}"
    );
    assert!(!exists(&store, &op) && !exists(&store, &oc));
    assert!(exists(&store, &keys().pack(3, "named")));
}

/// Packs made closer together than the pack keep time (4 min apart, keep time 641 s): the GC
/// base still moves forward, one keep time behind the newest pack, and GC deletes the packs
/// and the log below it.
#[test]
fn gc_base_moves_one_keep_time_behind_packs_closer_than_the_keep_time() {
    let store = mem();
    switched(&store, 6);
    let step = Duration::from_secs(240);
    for g in 2..=6 {
        put_pack(&store, g, "p");
        point(&store, g, "p");
        store.advance_clock(step);
    }
    // Codes ages now: 6 -> 4 min, 5 -> 8 min, 4 -> 12 min, 3 -> 16 min, 2 -> 20 min. The keep
    // time is 641 s (10.7 min): the GC base is 4.
    let r = gc5(&store);
    assert_eq!(r.outcome, GcOutcome::Trimmed, "{r:?}");
    assert_eq!(r.packs_deleted, 2, "{r:?}");
    let packs: BTreeSet<u64> = pack_keys(&store)
        .iter()
        .filter_map(|k| keys().parse_pack(k).map(|(g, _)| g))
        .collect();
    assert_eq!(packs, BTreeSet::from([4, 5, 6]));
    assert_eq!(log_gens(&store).first(), Some(&4));
    let age = r.age.expect("the GC base's age");
    assert!(age >= min_age() && age < min_age() + step, "{age:?}");
}

/// The pointer's pack and the pack below it both have codes younger than the pack keep time:
/// there is no GC base, and GC keeps both packs and the log they need.
#[test]
fn gc_keeps_young_pointer_pack_the_pack_below_and_their_log() {
    let store = mem();
    switched(&store, 4);
    put_pack(&store, 2, "a");
    point(&store, 2, "a");
    store.advance_clock(Duration::from_secs(300));
    put_pack(&store, 4, "b");
    point(&store, 4, "b");
    store.advance_clock(Duration::from_secs(300));
    let before = all_keys(&store);
    let r = trimmer_pass(&*store, None, None);
    assert_eq!((r.outcome, r.deleted), (GcOutcome::TooYoung, 0), "{r:?}");
    assert_eq!(all_keys(&store), before);
    assert!(r.next_due.is_some(), "{r:?}");
}

/// I2: a codes object whose pack is gone (a crash between the two deletes, or a run that never
/// finished) below the pointer never makes the GC base: only a complete pack does. GC keeps the
/// older complete pack and its log.
#[test]
fn gc_orphan_codes_below_the_pointer_never_make_the_gc_base() {
    let store = mem();
    switched(&store, 4);
    put_pack(&store, 2, "a");
    point(&store, 2, "a");
    store.advance_clock(Duration::from_secs(60));
    let (lp, lc) = put_pack(&store, 3, "leftover");
    store.delete(&lp).unwrap();
    store.advance_clock(min_age());
    put_pack(&store, 4, "b");
    point(&store, 4, "b");
    // Codes ages: 4 young, 3 (no pack) and 2 old: the GC base is 2.
    let r = gc5(&store);
    assert_eq!(r.outcome, GcOutcome::Trimmed, "{r:?}");
    assert_eq!(r.packs_deleted, 0, "{r:?}");
    assert!(exists(&store, &keys().pack(2, "a")), "{r:?}");
    assert_eq!(log_gens(&store).first(), Some(&2), "{r:?}");
    assert!(exists(&store, &lc), "the leftover codes stay");
}

/// Before it deletes log, GC HEADs the GC base's pack too: a GC base whose pack is gone between
/// the listing and the HEAD deletes no log and logs an error.
#[test]
fn gc_with_missing_gc_base_pack_deletes_no_log() {
    let store = mem();
    switched(&store, 4);
    let (ap, _) = put_pack(&store, 2, "a");
    point(&store, 2, "a");
    store.advance_clock(min_age());
    put_pack(&store, 4, "b");
    point(&store, 4, "b");
    // The listing still shows pack 2; its HEAD finds nothing.
    struct GoneOnHead {
        inner: Arc<MemStore>,
        gone: String,
    }
    impl ObjectStore for GoneOnHead {
        fn put(&self, k: &str, b: &[u8]) -> std::result::Result<String, crate::store::StoreError> {
            self.inner.put(k, b)
        }
        fn put_if_absent(
            &self,
            k: &str,
            b: &[u8],
        ) -> std::result::Result<crate::store::CondPut, crate::store::StoreError> {
            self.inner.put_if_absent(k, b)
        }
        fn put_if_match(
            &self,
            k: &str,
            b: &[u8],
            e: &str,
        ) -> std::result::Result<crate::store::CondPut, crate::store::StoreError> {
            self.inner.put_if_match(k, b, e)
        }
        fn get(
            &self,
            k: &str,
        ) -> std::result::Result<Option<crate::store::Object>, crate::store::StoreError> {
            self.inner.get(k)
        }
        fn stat(
            &self,
            k: &str,
        ) -> std::result::Result<Option<crate::store::ObjectStat>, crate::store::StoreError>
        {
            if k == self.gone {
                return Ok(None);
            }
            self.inner.stat(k)
        }
        fn list(&self, p: &str) -> std::result::Result<Vec<String>, crate::store::StoreError> {
            self.inner.list(p)
        }
        fn delete(&self, k: &str) -> std::result::Result<(), crate::store::StoreError> {
            self.inner.delete(k)
        }
        fn stats(&self) -> crate::store::StatsSnap {
            self.inner.stats()
        }
    }
    let s = GoneOnHead {
        inner: store.clone(),
        gone: ap.clone(),
    };
    let before = all_keys(&store);
    let cap = crate::telemetry::testing::Capture::new();
    let r = tracing::subscriber::with_default(cap.subscriber(), || {
        gc_pass(&s, &keys().for_open(), ages(), None, None).unwrap()
    });
    assert_eq!((r.outcome, r.deleted), (GcOutcome::PackMissing, 0), "{r:?}");
    assert_eq!(all_keys(&store), before);
    let errors: Vec<_> = cap
        .spans()
        .events
        .into_iter()
        .filter(|e| e.level == tracing::Level::ERROR)
        .collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].field("key"), Some(ap.as_str()));
}

/// The pack keep time grows with the newest pack: a pointer of a 10 GB pack keeps the pack
/// below it for 4 x 10 GB / 60 MB/s = 667 s, longer than the 641 s minimum age.
#[test]
fn gc_waits_the_keep_time_of_a_large_newest_pack() {
    let store = mem();
    switched(&store, 4);
    put_pack(&store, 2, "a");
    point(&store, 2, "a");
    put_pack(&store, 4, "big");
    let id = head(&*store).database_id;
    store
        .put(
            &keys().pack_pointer(),
            &pointer_body_sized(4, "big", id, 10_000_000_000),
        )
        .unwrap();
    store.advance_clock(Duration::from_secs(650));
    assert!(Duration::from_secs(650) > min_age());
    let r = gc5(&store);
    assert_eq!(
        (r.outcome, r.packs_deleted),
        (GcOutcome::TooYoung, 0),
        "{r:?}"
    );
    store.advance_clock(Duration::from_secs(20));
    let r = gc5(&store);
    assert_eq!(
        (r.outcome, r.packs_deleted),
        (GcOutcome::Trimmed, 1),
        "{r:?}"
    );
}

/// The writer's GC never deletes the pack that its own background fetch still reads, past the
/// pack keep time; the log below the GC base still goes. Once the fetch is done, the pack goes.
#[test]
fn writer_gc_keeps_the_pack_its_fetch_reads() {
    let store = mem();
    switched(&store, 4);
    let (ap, ac) = put_pack(&store, 2, "a");
    point(&store, 2, "a");
    put_pack(&store, 4, "b");
    point(&store, 4, "b");
    store.advance_clock(3 * min_age());
    let r = trimmer_pass(&*store, None, Some(&ap));
    assert_eq!(
        (r.outcome, r.packs_deleted),
        (GcOutcome::Trimmed, 0),
        "{r:?}"
    );
    assert!(exists(&store, &ap) && exists(&store, &ac));
    assert_eq!(log_gens(&store).first(), Some(&4));
    let mark = r.mark.clone();
    assert!(mark.is_some(), "{r:?}");
    // The fetch is done: the next pass is a whole one, and the pack goes.
    let r = trimmer_pass(&*store, mark.as_ref(), None);
    assert_eq!(r.packs_deleted, 1, "{r:?}");
    assert!(!exists(&store, &ap) && !exists(&store, &ac));
}

/// With no GC base yet (the pointer's pack is young), an orphan older than the orphan grace
/// still goes.
#[test]
fn gc_without_gc_base_still_deletes_orphans() {
    let store = mem();
    switched(&store, 3);
    let (op, oc) = put_pack(&store, 5, "orphan");
    store.advance_clock(orphan_grace() + Duration::from_secs(2));
    put_pack(&store, 3, "named");
    point(&store, 3, "named");
    let r = gc5(&store);
    assert_eq!(
        (r.outcome, r.orphan_packs_deleted),
        (GcOutcome::TooYoung, 1),
        "{r:?}"
    );
    assert!(!exists(&store, &op) && !exists(&store, &oc));
}
