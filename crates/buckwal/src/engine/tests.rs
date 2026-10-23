use super::*;
use crate::config::JournalMode;
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

pub(crate) const PREFIX: &str = "t";

pub(crate) fn keys() -> Keys {
    Keys::new(PREFIX)
}

pub(crate) fn cfg_deadline(deadline: Duration) -> EngineConfig {
    EngineConfig {
        retry: RetryPolicy {
            deadline,
            first_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(20),
        },
        upload_concurrency: 8,
        prefetch: false,
        lease: crate::config::defaults::LEASE,
        request_timeout: crate::config::defaults::REQUEST_TIMEOUT,
        trim_grace: crate::config::defaults::TRIM_GRACE,
        events: None,
        read_only: false,
        io_mode: crate::io::IoMode::Writer,
        encryption: None,
        journal_mode: crate::config::JournalMode::Wal,
    }
}

pub(crate) fn cfg() -> EngineConfig {
    cfg_deadline(Duration::from_secs(5))
}

/// Create a database in `dir` (new writer id, generation 1) or attach to the one its local
/// state names, then open the engine and one connection.
pub(crate) fn writer_with(
    store: Arc<dyn ObjectStore>,
    dir: &Path,
    cfg: &EngineConfig,
) -> (Engine, Arc<Connection>) {
    writer_trimmed(store, dir, cfg, None)
}

/// `writer_with`, handing seal snapshots to `trim_tx`.
pub(crate) fn writer_trimmed(
    store: Arc<dyn ObjectStore>,
    dir: &Path,
    cfg: &EngineConfig,
    trim_tx: Option<std::sync::mpsc::Sender<crate::uploader::TrimSnapshot>>,
) -> (Engine, Arc<Connection>) {
    let (bucket, state) = match LocalState::read(dir).unwrap() {
        Some(s) => {
            let b = Bucket::attach(
                store,
                keys(),
                cfg.retry.clone(),
                cfg.upload_concurrency,
                &s.writer_id,
                s.epoch,
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
                claimed: true,
                head_etag: String::new(),
                epoch: b.head().epoch,
                claim_id: String::new(),
                seal_not_before_ms: 0,
                seal_quiet_ms: 0,
                next_generation_floor: 0,
            };
            s.write_atomic(dir).unwrap();
            (b, s)
        }
    };
    let e = open_engine(dir, bucket, &state, cfg, trim_tx).unwrap();
    let conn = e.db.connect().unwrap();
    (e, conn)
}

pub(crate) fn writer(store: Arc<dyn ObjectStore>, dir: &Path) -> (Engine, Arc<Connection>) {
    writer_with(store, dir, &cfg())
}

/// Model a process crash on the same disk: forget the engine (nothing runs on drop), then
/// copy its files to a new directory, which `Database::open`'s registry has never seen.
pub(crate) fn crash_copy(engine: Engine, dir: &Path) -> TempDir {
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
pub(crate) fn crash(engine: Engine, conn: Arc<Connection>, dir: &Path) -> TempDir {
    std::mem::forget(conn);
    crash_copy(engine, dir)
}

/// Restore the bucket into a new directory and open it.
pub(crate) fn restored(store: Arc<dyn ObjectStore>) -> (TempDir, Engine, Arc<Connection>) {
    let r = tempfile::tempdir().unwrap();
    restore_into(store.clone(), &keys(), r.path(), &cfg()).unwrap();
    let (e, c) = writer(store, r.path());
    (r, e, c)
}

pub(crate) fn run(
    conn: &Arc<Connection>,
    sql: &str,
) -> std::result::Result<Vec<Vec<Value>>, LimboError> {
    stash::clear();
    conn.prepare(sql)?.run_collect_rows()
}

pub(crate) fn exec(conn: &Arc<Connection>, sql: &str) {
    run(conn, sql).unwrap_or_else(|e| panic!("{sql}: {e} ({:?})", stash::take()));
}

pub(crate) fn int(v: &Value) -> i64 {
    match v {
        Value::Numeric(Numeric::Integer(i)) => *i,
        v => panic!("not an integer: {v:?}"),
    }
}

pub(crate) fn ids(conn: &Arc<Connection>) -> Vec<i64> {
    run(conn, "SELECT id FROM t ORDER BY id")
        .unwrap()
        .iter()
        .map(|r| int(&r[0]))
        .collect()
}

pub(crate) fn integrity(conn: &Arc<Connection>) -> String {
    match &run(conn, "PRAGMA integrity_check").unwrap()[0][0] {
        Value::Text(t) => t.as_str().to_string(),
        v => panic!("integrity_check returned {v:?}"),
    }
}

pub(crate) fn create_table(conn: &Arc<Connection>) {
    exec(conn, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)");
}

/// One autocommit insert of a 1000-byte row: one commit, one shipped log record.
pub(crate) fn commit_row(conn: &Arc<Connection>, id: i64) -> std::result::Result<(), LimboError> {
    run(
        conn,
        &format!("INSERT INTO t(id, v) VALUES ({id}, randomblob(1000))"),
    )
    .map(|_| ())
}

/// Seal the current generation now: a TRUNCATE checkpoint ends with `truncate(0)`.
pub(crate) fn seal_now(conn: &Arc<Connection>) {
    stash::clear();
    conn.checkpoint(CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    })
    .unwrap_or_else(|e| panic!("checkpoint: {e} ({:?})", stash::take()));
}

/// A manifest or log record of a bucket that may be encrypted, as the plain codec reads it: the
/// 32-byte trailer (a SHA-256, or an integrity code) is replaced by the SHA-256 of the rest.
/// Tests that look inside a bucket use it instead of holding the key.
pub(crate) fn as_plain(body: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut out = body[..body.len() - 32].to_vec();
    let sha = Sha256::digest(&out);
    out.extend(sha);
    out
}

/// A page object's page: without the integrity code an encrypted bucket's carries.
pub(crate) fn page_of(body: &[u8], page_size: usize) -> &[u8] {
    assert!(body.len() == page_size || body.len() == page_size + 32);
    &body[..page_size]
}

pub(crate) fn head(store: &dyn ObjectStore) -> Head {
    Head::decode(&store.get(&keys().head()).unwrap().unwrap().body).unwrap()
}

pub(crate) fn manifest(store: &dyn ObjectStore) -> (u64, Manifest) {
    let g = head(store).manifest.expect("sealed at least once");
    let body = store.get(&keys().manifest(g)).unwrap().unwrap().body;
    (g, Manifest::decode(&as_plain(&body)).unwrap())
}

pub(crate) fn log_seqs(store: &dyn ObjectStore, gen: u64) -> Vec<u64> {
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

#[test]
fn removing_the_wal_is_refused() {
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(Arc::new(MemStore::new()), d.path());
    create_table(&conn);
    commit_row(&conn, 1).unwrap();
    let wal = d.path().join("app.db-wal");
    let path = wal.to_str().unwrap();
    stash::clear();
    assert!(e.io.remove_file(path).is_err());
    match stash::take() {
        Some(Error::Corrupt(m)) => assert_eq!(
            m,
            format!("refusing to delete {path}: it may hold acknowledged frames")
        ),
        other => panic!("expected Corrupt, got {other:?}"),
    }
    assert!(wal_len(d.path()) > 32, "the WAL was touched");
    // Every other path passes through.
    let other = d.path().join("scratch");
    std::fs::write(&other, b"x").unwrap();
    e.io.remove_file(other.to_str().unwrap()).unwrap();
    assert!(!other.exists());
}

#[test]
fn restore_with_manifest_missing_page_one_is_corrupt() {
    let mem = Arc::new(MemStore::new());
    let k = keys();
    let m = Manifest {
        page_size: 4096,
        page_count: 2,
        pages: [(2, 1)].into_iter().collect(),
    };
    mem.put(&k.manifest(1), &m.encode()).unwrap();
    mem.put(&k.page(1, 2), &[0u8; 4096]).unwrap();
    let h = Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 2,
        manifest: Some(1),
        writer_id: "ab".into(),
        epoch: 0,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
    };
    mem.put(&k.head(), &h.encode()).unwrap();
    let r = tempfile::tempdir().unwrap();
    match restore_into(mem.clone(), &k, r.path(), &cfg()) {
        Err(Error::Corrupt(m)) => assert!(m.contains("page 1"), "{m}"),
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

fn wal_len(dir: &Path) -> u64 {
    std::fs::metadata(dir.join("app.db-wal")).unwrap().len()
}

const PASSIVE: CheckpointMode = CheckpointMode::Passive {
    upper_bound_inclusive: None,
};
const TRUNCATE: CheckpointMode = CheckpointMode::Truncate {
    upper_bound_inclusive: None,
};

/// RESTART and TRUNCATE restart the log inside the checkpoint (D6).
fn restarts_log(mode: CheckpointMode) -> bool {
    matches!(
        mode,
        CheckpointMode::Restart | CheckpointMode::Truncate { .. }
    )
}

/// 400 commits with an explicit `mode` checkpoint every 50; a crash; a restore.
fn checkpoint_restores_every_ack(mode: CheckpointMode) {
    let t = Instant::now();
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    for id in 1..=400 {
        commit_row(&conn, id).unwrap_or_else(|err| panic!("commit {id}: {err}"));
        if id % 50 == 0 {
            stash::clear();
            conn.checkpoint(mode)
                .unwrap_or_else(|err| panic!("{mode:?} at {id}: {err} ({:?})", stash::take()));
        }
    }
    let seals = e.uploader.counters().seals;
    if restarts_log(mode) {
        assert!(seals >= 1, "{mode:?}: no seal");
    }
    let _crashed = crash(e, conn, d.path());

    let (_r, _e2, c2) = restored(mem.clone());
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), (1..=400).collect::<Vec<_>>());
    eprintln!(
        "{mode:?} restores_every_ack: {seals} seals, {:?}",
        t.elapsed()
    );
}

/// Page uploads fail from just before the checkpoint after commit 350: commits either succeed
/// or fail at the generation boundary. After the faults clear, 20 more commits succeed and a
/// restore has exactly the acknowledged ids.
fn checkpoint_with_page_upload_failure_loses_nothing(mode: CheckpointMode) {
    let t = Instant::now();
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer_with(
        fault.clone(),
        d.path(),
        &cfg_deadline(Duration::from_millis(30)),
    );
    create_table(&conn);
    let mut acked = vec![];
    let mut failed = 0;
    let mut seals_at_arm = 0;
    for id in 1..=400 {
        match commit_row(&conn, id) {
            Ok(()) => acked.push(id),
            Err(err) => {
                failed += 1;
                let stashed = stash::take();
                assert!(
                    matches!(&stashed, Some(Error::Timeout(m)) if m.contains("/pages/")),
                    "{mode:?} commit {id}: {err} ({stashed:?})"
                );
            }
        }
        if id == 350 {
            seals_at_arm = e.uploader.counters().seals;
            fault.only_keys_containing("/pages/");
            fault.fail_puts_after(0);
        }
        if id % 50 == 0 {
            stash::clear();
            // Under the faults a checkpoint may fail too; nothing may panic.
            let _ = conn.checkpoint(mode);
        }
    }
    assert!(failed >= 1, "{mode:?}: no commit met the failing boundary");
    assert_eq!(e.uploader.counters().seals, seals_at_arm, "{mode:?}");

    fault.fail_puts_after(u64::MAX);
    for id in 401..=420 {
        commit_row(&conn, id)
            .unwrap_or_else(|err| panic!("{mode:?} commit {id}: {err} ({:?})", stash::take()));
        acked.push(id);
    }
    assert!(e.uploader.counters().seals > seals_at_arm, "{mode:?}");
    assert_eq!(ids(&conn), acked);
    let _crashed = crash(e, conn, d.path());

    let (_r, _e2, c2) = restored(mem.clone());
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), acked);
    eprintln!(
        "{mode:?} page_upload_failure: {failed} failed commits, {:?}",
        t.elapsed()
    );
}

#[test]
fn checkpoint_mode_passive_restores_every_ack() {
    checkpoint_restores_every_ack(PASSIVE);
}

#[test]
fn checkpoint_mode_full_restores_every_ack() {
    checkpoint_restores_every_ack(CheckpointMode::Full);
}

#[test]
fn checkpoint_mode_restart_restores_every_ack() {
    checkpoint_restores_every_ack(CheckpointMode::Restart);
}

#[test]
fn checkpoint_mode_truncate_restores_every_ack() {
    checkpoint_restores_every_ack(TRUNCATE);
}

#[test]
fn checkpoint_mode_passive_with_page_upload_failure_loses_nothing() {
    checkpoint_with_page_upload_failure_loses_nothing(PASSIVE);
}

#[test]
fn checkpoint_mode_full_with_page_upload_failure_loses_nothing() {
    checkpoint_with_page_upload_failure_loses_nothing(CheckpointMode::Full);
}

#[test]
fn checkpoint_mode_restart_with_page_upload_failure_loses_nothing() {
    checkpoint_with_page_upload_failure_loses_nothing(CheckpointMode::Restart);
}

#[test]
fn checkpoint_mode_truncate_with_page_upload_failure_loses_nothing() {
    checkpoint_with_page_upload_failure_loses_nothing(TRUNCATE);
}

#[test]
fn close_runs_truncate_and_seals() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    for id in 1..=100 {
        commit_row(&conn, id).unwrap();
    }
    let before = head(&*mem);
    assert!(wal_len(d.path()) > 32);
    stash::clear();
    conn.close()
        .unwrap_or_else(|err| panic!("close: {err} ({:?})", stash::take()));
    let after = head(&*mem);
    assert!(
        after.generation > before.generation,
        "close did not seal: {before:?} -> {after:?}"
    );
    assert_eq!(wal_len(d.path()), 0);
    drop(conn);
    drop(e);

    let (_r, _e2, c2) = restored(mem.clone());
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), (1..=100).collect::<Vec<_>>());
}

#[test]
fn close_with_bucket_down_keeps_wal_and_loses_nothing() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer_with(
        fault.clone(),
        d.path(),
        &cfg_deadline(Duration::from_millis(200)),
    );
    create_table(&conn);
    for id in 1..=100 {
        commit_row(&conn, id).unwrap();
    }
    let before = head(&*mem);
    // Every put fails: pages, manifest and head.
    fault.fail_puts_after(0);
    stash::clear();
    conn.close()
        .expect("close ignores a failed shutdown checkpoint");
    assert!(
        wal_len(d.path()) > 32,
        "the local -wal must keep its frames"
    );
    assert_eq!(head(&*mem), before);
    assert_eq!(e.uploader.counters().seals, 0);
    let _crashed = crash(e, conn, d.path());

    fault.fail_puts_after(u64::MAX);
    let (_r, _e2, c2) = restored(mem.clone());
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), (1..=100).collect::<Vec<_>>());
}

/// Review Focus 5.
#[test]
fn sql_truncate_checkpoint_with_bucket_down_loses_nothing() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer_with(
        fault.clone(),
        d.path(),
        &cfg_deadline(Duration::from_millis(200)),
    );
    create_table(&conn);
    for id in 1..=50 {
        commit_row(&conn, id).unwrap();
    }
    let gen0 = e.uploader.generation();
    fault.only_keys_containing("/pages/");
    fault.fail_puts_after(0);
    let rows = run(&conn, "PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap_or_else(|err| panic!("the PRAGMA must return a row: {err}"));
    assert_eq!(rows.len(), 1, "{rows:?}");
    eprintln!("sql_truncate: row {:?}", rows[0]);
    assert_eq!(e.uploader.generation(), gen0);
    assert_eq!(e.uploader.counters().seals, 0);

    fault.fail_puts_after(u64::MAX);
    commit_row(&conn, 51)
        .unwrap_or_else(|err| panic!("commit after recovery: {err} ({:?})", stash::take()));
    assert_eq!(
        e.uploader.generation(),
        gen0 + 1,
        "the commit after the in-memory restart seals the old generation"
    );
    assert_eq!(ids(&conn), (1..=51).collect::<Vec<_>>());
    let _crashed = crash(e, conn, d.path());

    let (_r, _e2, c2) = restored(mem.clone());
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), (1..=51).collect::<Vec<_>>());
}

#[test]
fn restore_after_many_checkpoints_reads_only_current_generation() {
    let t = Instant::now();
    let mem = Arc::new(MemStore::new());
    let trimmed = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let hook: EventHook = {
        let trimmed = trimmed.clone();
        Arc::new(move |ev: &Event| {
            if let Event::Trimmed { deleted } = ev {
                trimmed.lock().unwrap().push(*deleted);
            }
        })
    };
    let mut cfg = cfg();
    cfg.events = Some(hook.clone());
    let (tx, rx) = std::sync::mpsc::channel();
    let _trimmer = crate::trim::spawn_trimmer(mem.clone(), keys(), Duration::ZERO, rx, Some(hook));
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer_trimmed(mem.clone(), d.path(), &cfg, Some(tx));
    create_table(&conn);
    let mut id = 0;
    for _ in 0..6 {
        for _ in 0..30 {
            id += 1;
            commit_row(&conn, id).unwrap();
        }
        seal_now(&conn);
    }
    assert_eq!(e.uploader.counters().seals, 6);
    let h = head(&*mem);
    let m_gen = h.manifest.unwrap();
    let k = keys();

    // Wait for the trimmer to act on the last seal: older logs, older manifests and every
    // page object manifest M no longer references are gone (`trim_once` deletes pages last).
    let (_, m) = manifest(&*mem);
    loop {
        let logs = mem.list(&k.log_root()).unwrap();
        let manifests = mem.list(&k.manifest_root()).unwrap();
        let stale_pages: Vec<String> = mem
            .list(&k.page_root())
            .unwrap()
            .into_iter()
            .filter(|key| {
                let (g, p) = k.parse_page(key).unwrap();
                g < m_gen && m.pages.get(&p) != Some(&g)
            })
            .collect();
        let done = trimmed.lock().unwrap().iter().any(|&n| n > 0)
            && logs
                .iter()
                .all(|key| k.parse_log(key).map(|(g, _)| g) == Some(h.generation))
            && manifests == vec![k.manifest(m_gen)]
            && stale_pages.is_empty();
        if done {
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "never trimmed: {logs:?} {manifests:?} unreferenced pages kept: {stale_pages:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    let r = tempfile::tempdir().unwrap();
    let rep = restore_into(
        mem.clone(),
        &k,
        r.path(),
        &cfg_deadline(Duration::from_secs(5)),
    )
    .unwrap();
    assert_eq!(rep.generation, h.generation);
    assert_eq!(rep.log_objects, log_seqs(&*mem, h.generation).len() as u64);
    // The head, the manifest, page 1 (the MVCC check) and the current generation's log.
    assert_eq!(rep.gets, 3 + rep.log_objects, "{rep:?}");
    assert_eq!(rep.lists, 1, "{rep:?}");
    let (_e2, c2) = writer(mem.clone(), r.path());
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), (1..=id).collect::<Vec<_>>());
    eprintln!(
        "restore_after_many: trimmed {:?}, {rep:?}, {:?}",
        trimmed.lock().unwrap(),
        t.elapsed()
    );
}

/// The maintenance trigger's WAL measure: the highest byte written since the generation's
/// header, which is the file's length as the engine appends, and back to 0 at a TRUNCATE
/// (or near 0 at a RESTART's new header).
#[test]
fn wal_high_water_follows_the_generation() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    for id in 1..=20 {
        commit_row(&conn, id).unwrap();
    }
    let high = e.wal_bytes.load(SeqCst);
    assert!(high > 20 * 1000, "{high}");
    assert_eq!(high, wal_len(d.path()));
    seal_now(&conn);
    assert_eq!(e.wal_bytes.load(SeqCst), 0);
    commit_row(&conn, 21).unwrap();
    let after = e.wal_bytes.load(SeqCst);
    assert!(after > 0 && after < high, "{after} vs {high}");
    assert_eq!(after, wal_len(d.path()));
    for id in 22..=40 {
        commit_row(&conn, id).unwrap();
    }
    let high = e.wal_bytes.load(SeqCst);
    stash::clear();
    conn.checkpoint(CheckpointMode::Restart).unwrap();
    commit_row(&conn, 41).unwrap();
    let restarted = e.wal_bytes.load(SeqCst);
    assert!(restarted < high / 4, "{restarted} vs {high}");
}

#[test]
fn passive_checkpoint_flag_never_set() {
    // The library builds `DatabaseOpts`; no call that turns the experimental flag on.
    let src = include_str!("../engine.rs");
    for line in src.lines() {
        let code = line.split("//").next().unwrap();
        assert!(!code.contains("passive_checkpoint"), "{line}");
    }
    let d = tempfile::tempdir().unwrap();
    let (e, _conn) = writer(Arc::new(MemStore::new()), d.path());
    assert!(!e.db.db.experimental_mvcc_passive_checkpoint_enabled());
}
