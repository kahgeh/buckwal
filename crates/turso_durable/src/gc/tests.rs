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
    config(Path::new("/x")).engine(false).gc_min_age()
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
