use super::*;
use crate::config::{Event, EventHook};
use crate::error::Error;
use crate::format::{Head, Keys, LocalState, Manifest};
use crate::io::db_storage::seal_page_set;
use crate::io::stash;
use crate::io::wal_file::wal_frame_pages;
use crate::restore::restore_into;
use crate::store::{FaultStore, MemStore, ObjectStore, RetryPolicy};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use turso_core::{CheckpointMode, Connection, LimboError, Numeric, Value, IO};

const PREFIX: &str = "t";

fn keys() -> Keys {
    Keys::new(PREFIX)
}

fn cfg_deadline(deadline: Duration) -> EngineConfig {
    EngineConfig {
        retry: RetryPolicy {
            deadline,
            first_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(20),
        },
        upload_concurrency: 8,
        prefetch: false,
        events: None,
    }
}

fn cfg() -> EngineConfig {
    cfg_deadline(Duration::from_secs(5))
}

/// Create a database in `dir` (new writer id, generation 1) or attach to the one its local
/// state names, then open the engine and one connection.
fn writer_with(
    store: Arc<dyn ObjectStore>,
    dir: &Path,
    cfg: &EngineConfig,
) -> (Engine, Arc<Connection>) {
    let (bucket, state) = match LocalState::read(dir).unwrap() {
        Some(s) => {
            let b = Bucket::attach(
                store,
                keys(),
                cfg.retry.clone(),
                cfg.upload_concurrency,
                &s.writer_id,
            )
            .unwrap();
            (b, s)
        }
        None => {
            let mut id = [0u8; 16];
            turso_core::PlatformIO::new().unwrap().fill_bytes(&mut id);
            let writer_id = hex::encode(id);
            let b = Bucket::create(
                store,
                keys(),
                cfg.retry.clone(),
                cfg.upload_concurrency,
                writer_id.clone(),
            )
            .unwrap();
            let s = LocalState {
                format: 1,
                writer_id,
                generation: 1,
                complete: true,
            };
            s.write_atomic(dir).unwrap();
            (b, s)
        }
    };
    let e = open_engine(dir, bucket, &state, cfg, None).unwrap();
    let conn = e.db.connect().unwrap();
    (e, conn)
}

fn writer(store: Arc<dyn ObjectStore>, dir: &Path) -> (Engine, Arc<Connection>) {
    writer_with(store, dir, &cfg())
}

/// Model a process crash on the same disk: forget the engine (nothing runs on drop), then
/// copy its files to a new directory, which `Database::open`'s registry has never seen.
fn crash_copy(engine: Engine, dir: &Path) -> TempDir {
    std::mem::forget(engine);
    let to = tempfile::tempdir().unwrap();
    for f in ["app.db", "app.db-wal", "app.db-durable", "app.db-dirty"] {
        let from = dir.join(f);
        if from.exists() {
            std::fs::copy(&from, to.path().join(f)).unwrap();
        }
    }
    to
}

/// `crash_copy` for a writer whose connection must not run its close checkpoint either.
fn crash(engine: Engine, conn: Arc<Connection>, dir: &Path) -> TempDir {
    std::mem::forget(conn);
    crash_copy(engine, dir)
}

/// Restore the bucket into a new directory and open it.
fn restored(store: Arc<dyn ObjectStore>) -> (TempDir, Engine, Arc<Connection>) {
    let r = tempfile::tempdir().unwrap();
    restore_into(store.clone(), &keys(), r.path(), &cfg()).unwrap();
    let (e, c) = writer(store, r.path());
    (r, e, c)
}

fn run(conn: &Arc<Connection>, sql: &str) -> std::result::Result<Vec<Vec<Value>>, LimboError> {
    stash::clear();
    conn.prepare(sql)?.run_collect_rows()
}

fn exec(conn: &Arc<Connection>, sql: &str) {
    run(conn, sql).unwrap_or_else(|e| panic!("{sql}: {e} ({:?})", stash::take()));
}

fn int(v: &Value) -> i64 {
    match v {
        Value::Numeric(Numeric::Integer(i)) => *i,
        v => panic!("not an integer: {v:?}"),
    }
}

fn ids(conn: &Arc<Connection>) -> Vec<i64> {
    run(conn, "SELECT id FROM t ORDER BY id")
        .unwrap()
        .iter()
        .map(|r| int(&r[0]))
        .collect()
}

fn integrity(conn: &Arc<Connection>) -> String {
    match &run(conn, "PRAGMA integrity_check").unwrap()[0][0] {
        Value::Text(t) => t.as_str().to_string(),
        v => panic!("integrity_check returned {v:?}"),
    }
}

fn create_table(conn: &Arc<Connection>) {
    exec(conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)");
}

/// One autocommit insert of a 1000-byte row: one commit, one shipped log record.
fn commit_row(conn: &Arc<Connection>, id: i64) -> std::result::Result<(), LimboError> {
    run(
        conn,
        &format!("INSERT INTO t(id, v) VALUES ({id}, randomblob(1000))"),
    )
    .map(|_| ())
}

/// Seal the current generation now: a TRUNCATE checkpoint ends with `truncate(0)`.
fn seal_now(conn: &Arc<Connection>) {
    stash::clear();
    conn.checkpoint(CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    })
    .unwrap_or_else(|e| panic!("checkpoint: {e} ({:?})", stash::take()));
}

fn head(store: &dyn ObjectStore) -> Head {
    Head::decode(&store.get(&keys().head()).unwrap().unwrap().body).unwrap()
}

fn manifest(store: &dyn ObjectStore) -> (u64, Manifest) {
    let g = head(store).manifest.expect("sealed at least once");
    let body = store.get(&keys().manifest(g)).unwrap().unwrap().body;
    (g, Manifest::decode(&body).unwrap())
}

fn log_seqs(store: &dyn ObjectStore, gen: u64) -> Vec<u64> {
    store
        .list(&keys().log_prefix(gen))
        .unwrap()
        .iter()
        .filter_map(|k| keys().parse_log(k))
        .map(|(_, s)| s)
        .collect()
}

#[test]
fn every_ack_restores_across_checkpoints_and_pages_are_lazy() {
    let t = Instant::now();
    let mem = Arc::new(MemStore::new());
    let store: Arc<dyn ObjectStore> = mem.clone();
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(store.clone(), d.path());
    create_table(&conn);
    for id in 1..=3000 {
        commit_row(&conn, id).unwrap_or_else(|err| panic!("commit {id}: {err}"));
    }
    let seals = e.uploader.counters().seals;
    let _crashed = crash(e, conn, d.path());
    let h = head(&*mem);
    assert!(h.generation >= 2, "3000 commits never sealed: {h:?}");
    eprintln!(
        "every_ack: {seals} seals, head {h:?}, writer {:?}",
        t.elapsed()
    );

    let (_r, _e2, c2) = restored(store.clone());
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), (1..=3000).collect::<Vec<_>>());

    // A point read on a fresh restore fetches far fewer pages than the database has.
    let (_r3, e3, c3) = restored(store.clone());
    let rows = run(&c3, "SELECT id FROM t WHERE id = 1500").unwrap();
    assert_eq!(int(&rows[0][0]), 1500);
    let pages = manifest(&*mem).1.pages.len() as u64;
    let gets = e3.storage.lazy_gets();
    eprintln!(
        "every_ack: point read fetched {gets} of {pages} pages, total {:?}",
        t.elapsed()
    );
    assert!(
        gets > 0 && gets < pages / 2,
        "{gets} lazy GETs of {pages} pages"
    );
}

#[test]
fn failed_upload_then_commit_reusing_frames_restores_exactly() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer_with(
        fault.clone(),
        d.path(),
        &cfg_deadline(Duration::from_millis(200)),
    );
    create_table(&conn);
    for id in 1..=5 {
        commit_row(&conn, id).unwrap();
    }
    fault.fail_next_puts(u64::MAX / 2);
    assert!(commit_row(&conn, 6).is_err(), "commit 6 must fail");
    let err = stash::take();
    assert!(matches!(err, Some(Error::Timeout(_))), "{err:?}");
    fault.fail_next_puts(0);
    for id in 7..=10 {
        commit_row(&conn, id).unwrap_or_else(|err| panic!("commit {id}: {err}"));
    }
    // The local database never shows the failed commit either.
    assert_eq!(ids(&conn), vec![1, 2, 3, 4, 5, 7, 8, 9, 10]);
    let _crashed = crash(e, conn, d.path());

    let (_r, _e2, c2) = restored(mem.clone());
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), vec![1, 2, 3, 4, 5, 7, 8, 9, 10]);
}

#[test]
fn ambiguous_wal_put_is_acked_once() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(fault.clone(), d.path());
    create_table(&conn);
    commit_row(&conn, 1).unwrap();
    fault.store_then_fail_next_puts(1);
    commit_row(&conn, 2).expect("an ambiguous put that landed is acknowledged");
    commit_row(&conn, 3).unwrap();
    let _crashed = crash(e, conn, d.path());

    let seqs = log_seqs(&*mem, 1);
    assert_eq!(
        seqs,
        (1..=seqs.len() as u64).collect::<Vec<_>>(),
        "gap in log/1"
    );
    let (_r, _e2, c2) = restored(mem.clone());
    assert_eq!(ids(&c2), vec![1, 2, 3]);
}

#[test]
fn header_resend_after_failed_header_sync_stays_in_generation() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    // The first seal arms a failure for the next log upload: the new generation's header.
    let armed = Arc::new(AtomicBool::new(false));
    let hook: EventHook = {
        let (fault, armed) = (fault.clone(), armed.clone());
        Arc::new(move |ev: &Event| {
            if matches!(ev, Event::SealFinished { .. }) && !armed.swap(true, SeqCst) {
                fault.only_keys_containing("/log/");
                fault.fail_next_puts(u64::MAX / 2);
            }
        })
    };
    let mut cfg = cfg_deadline(Duration::from_millis(200));
    cfg.events = Some(hook);
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer_with(fault.clone(), d.path(), &cfg);
    create_table(&conn);
    let gen0 = e.uploader.generation();

    // Commit until the WAL restarts (auto PASSIVE checkpoint, then the next commit).
    let mut acked = vec![];
    let mut id = 1;
    loop {
        assert!(id < 5000, "the WAL never restarted");
        match commit_row(&conn, id) {
            Ok(()) => acked.push(id),
            Err(err) => {
                assert!(
                    armed.load(SeqCst),
                    "commit {id} failed before any seal: {err}"
                );
                let stashed = stash::take();
                assert!(matches!(stashed, Some(Error::Timeout(_))), "{stashed:?}");
                break;
            }
        }
        id += 1;
    }
    assert_eq!(e.uploader.counters().seals, 1);
    assert_eq!(e.uploader.generation(), gen0 + 1);

    fault.fail_next_puts(0);
    id += 1;
    commit_row(&conn, id).expect("the re-sent header is not a new generation");
    acked.push(id);
    assert_eq!(
        e.uploader.counters().seals,
        1,
        "the re-sent header sealed again"
    );
    assert_eq!(e.uploader.generation(), gen0 + 1);
    assert_eq!(head(&*mem).generation, gen0 + 1);
    let _crashed = crash(e, conn, d.path());

    let (_r, _e2, c2) = restored(mem.clone());
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), acked);
}

#[test]
fn seal_failure_refuses_restart_and_loses_nothing() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer_with(
        fault.clone(),
        d.path(),
        &cfg_deadline(Duration::from_millis(200)),
    );
    create_table(&conn);
    let gen0 = e.uploader.generation();
    fault.only_keys_containing("/pages/");
    fault.fail_puts_after(0);

    let mut acked = vec![];
    let mut id = 1;
    loop {
        assert!(id < 5000, "no commit ever tried to restart the WAL");
        match commit_row(&conn, id) {
            Ok(()) => acked.push(id),
            Err(_) => break,
        }
        id += 1;
    }
    let err = stash::take();
    assert!(
        matches!(&err, Some(Error::Timeout(m)) if m.contains("/pages/")),
        "the refused restart must stash the page upload's error: {err:?}"
    );
    assert_eq!(e.uploader.generation(), gen0);
    assert_eq!(e.uploader.counters().seals, 0);

    fault.fail_puts_after(u64::MAX);
    id += 1;
    commit_row(&conn, id).unwrap_or_else(|err| panic!("commit {id}: {err}"));
    acked.push(id);
    assert_eq!(e.uploader.generation(), gen0 + 1);
    let _crashed = crash(e, conn, d.path());

    let (_r, _e2, c2) = restored(mem.clone());
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), acked);
}

#[test]
fn missing_page_object_is_not_found() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    for id in 1..=300 {
        commit_row(&conn, id).unwrap();
    }
    seal_now(&conn);
    for id in 301..=310 {
        commit_row(&conn, id).unwrap();
    }
    let crashed = crash(e, conn, d.path());

    // A page the scan must read from the bucket: sealed, and not in the current WAL.
    let wal = wal_frame_pages(&std::fs::read(crashed.path().join("app.db-wal")).unwrap());
    let (_, m) = manifest(&*mem);
    let (&p, &g) = m
        .pages
        .iter()
        .find(|(p, _)| **p >= 3 && !wal.contains(p))
        .expect("a sealed page outside the WAL");
    let key = keys().page(g, p);
    mem.delete(&key).unwrap();

    let (_r, _e2, c2) = restored(mem.clone());
    assert!(run(&c2, "SELECT sum(length(v)) FROM t").is_err());
    assert_eq!(stash::take(), Some(Error::NotFound(key)));
}

#[test]
fn prefetch_fetches_every_sealed_page() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    for id in 1..=200 {
        commit_row(&conn, id).unwrap();
    }
    seal_now(&conn);
    let _crashed = crash(e, conn, d.path());

    let r = tempfile::tempdir().unwrap();
    restore_into(mem.clone(), &keys(), r.path(), &cfg()).unwrap();
    let mut cfg = cfg();
    cfg.prefetch = true;
    let (e2, c2) = writer_with(mem.clone(), r.path(), &cfg);
    let t = Instant::now();
    while !e2.storage.is_complete() {
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "prefetch never finished"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let pages = manifest(&*mem).1.pages.len() as u64;
    assert_eq!(e2.storage.lazy_gets(), pages);
    assert_eq!(ids(&c2), (1..=200).collect::<Vec<_>>());
    assert_eq!(
        e2.storage.lazy_gets(),
        pages,
        "a complete database fetches nothing"
    );
}

#[test]
fn seal_page_set_unions_memory_persisted_and_wal_frames() {
    let ps = 512usize;
    let mut wal = vec![0u8; 32];
    wal[8..12].copy_from_slice(&(ps as u32).to_be_bytes());
    for p in [3u32, 7, 3, 900] {
        let mut frame = vec![0u8; 24 + ps];
        frame[0..4].copy_from_slice(&p.to_be_bytes());
        wal.extend(frame);
    }
    // A trailing partial frame is ignored.
    let mut partial = vec![0u8; 24 + ps / 2];
    partial[0..4].copy_from_slice(&5u32.to_be_bytes());
    wal.extend(partial);
    let wal_pages = wal_frame_pages(&wal);
    assert_eq!(wal_pages, BTreeSet::from([3, 7, 900]));
    assert!(wal_frame_pages(&wal[..31]).is_empty());

    let mut persisted: Vec<u8> = [2u32, 50, 200]
        .iter()
        .flat_map(|p| p.to_le_bytes())
        .collect();
    persisted.extend([1, 0]); // a torn append is ignored
    let memory = BTreeSet::from([1, 9, 150]);
    assert_eq!(
        seal_page_set(&memory, &persisted, &wal_pages, 100),
        BTreeSet::from([1, 2, 3, 7, 9, 50])
    );
}

#[test]
fn db_log_path_is_refused() {
    let d = tempfile::tempdir().unwrap();
    let (e, _conn) = writer(Arc::new(MemStore::new()), d.path());
    let path = d.path().join("app.db-log");
    stash::clear();
    let r =
        e.io.open_file(path.to_str().unwrap(), turso_core::OpenFlags::Create, false);
    assert!(r.is_err());
    assert_eq!(stash::take(), Some(Error::Unsupported("MVCC")));
    assert!(!path.exists());
}

#[test]
fn restore_refuses_mvcc_page_one() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_row(&conn, 1).unwrap();
    seal_now(&conn);
    let _crashed = crash(e, conn, d.path());

    let (_, m) = manifest(&*mem);
    let key = keys().page(m.pages[&1], 1);
    let mut body = mem.get(&key).unwrap().unwrap().body;
    body[18] = 255;
    body[19] = 255;
    mem.put(&key, &body).unwrap();
    let r = tempfile::tempdir().unwrap();
    let res = restore_into(mem.clone(), &keys(), r.path(), &cfg());
    assert!(matches!(res, Err(Error::Unsupported("MVCC"))), "{res:?}");
}

#[test]
fn wal_log_gap_is_corrupt() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    for id in 1..=5 {
        commit_row(&conn, id).unwrap();
    }
    let _crashed = crash(e, conn, d.path());
    assert!(log_seqs(&*mem, 1).len() >= 3);
    mem.delete(&keys().log(1, 2)).unwrap();
    let r = tempfile::tempdir().unwrap();
    match restore_into(mem.clone(), &keys(), r.path(), &cfg()) {
        Err(Error::Corrupt(m)) => assert!(m.contains("gap"), "{m}"),
        other => panic!("expected Corrupt(gap), got {other:?}"),
    }
}
