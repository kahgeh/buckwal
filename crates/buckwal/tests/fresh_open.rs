//! Black-box tests of what the library's writer open does to a fresh database: it allocates
//! page 1 on a connection of its own (turso_core 0.8.3 can allocate it twice, and panic, when
//! the first writes to a fresh database arrive on several connections at once), and commits
//! nothing, so no ack waits on a batching window.
use buckwal::store::{MemStore, ObjectStore};
use buckwal::{
    open_read_only_with_store, open_with_store, Batching, Config, Database, JournalMode,
    UploadStats,
};
use std::collections::BTreeSet;
use std::panic::{self, catch_unwind, AssertUnwindSafe};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const CONNECTIONS: usize = 32;
const ROUNDS: usize = 20;
const WRITER: &str = "fresh-open-writer";

fn config(dir: &Path, key: bool) -> Config {
    let mut c = Config::new(dir, "http://127.0.0.1:1", "b", "k", "s");
    // A long window, so a commit made by the open would hold it up.
    c.batching = "window 60s 1000 64MiB".parse().unwrap();
    if key {
        let file = dir.with_extension("key");
        std::fs::write(&file, "ab".repeat(32)).unwrap();
        c.encryption_key_file = Some(file);
    }
    c
}

fn commits(s: &UploadStats) -> (u64, u64, u64) {
    (s.commits_acked, s.commits_uploaded, s.log_puts)
}

/// Every key in the bucket and its writes (puts, deletes, bytes up) at one moment.
type Bucket = (BTreeSet<String>, (u64, u64, u64));

fn bucket(store: &MemStore) -> Bucket {
    let s = store.stats();
    (
        store.list("").unwrap().into_iter().collect(),
        (s.puts, s.deletes, s.bytes_up),
    )
}

fn page_count(db: &Database) -> i64 {
    let c = db.connect().unwrap();
    let rows = c.query("PRAGMA page_count", &[]).unwrap();
    rows[0][0].as_int().unwrap()
}

/// 32 connections of one `Database`, their first writes to a fresh database released at once.
/// Every round runs; a round counts as panicked when any writer thread's join is an `Err`.
#[test]
fn concurrent_first_writes_to_a_fresh_database_all_succeed() {
    // Panics are counted on the writer threads (named, so another test's failing assert is
    // not counted), whether or not the writer's catch_unwind sees them.
    static PANICS: AtomicUsize = AtomicUsize::new(0);
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        if thread::current().name() == Some(WRITER) {
            PANICS.fetch_add(1, SeqCst);
        }
        default_hook(info)
    }));
    let mut panicked_rounds = Vec::new();
    for round in 0..ROUNDS {
        let t = tempfile::tempdir().unwrap();
        let store = Arc::new(MemStore::new());
        let mut cfg = config(&t.path().join("db"), false);
        cfg.batching = Batching::Off; // a window would hold the 32 commits for up to 60 s
        let db = open_with_store(cfg, store).unwrap();
        PANICS.store(0, SeqCst);
        let barrier = Arc::new(Barrier::new(CONNECTIONS));
        let (tx, rx) = mpsc::channel();
        let handles: Vec<_> = (0..CONNECTIONS)
            .map(|i| {
                let c = db.connect().unwrap();
                let barrier = barrier.clone();
                let tx = tx.clone();
                thread::Builder::new()
                    .name(WRITER.into())
                    .spawn(move || {
                        barrier.wait();
                        let sql = format!("CREATE TABLE f{i}(x INTEGER)");
                        let r = catch_unwind(AssertUnwindSafe(|| c.execute(&sql, &[])));
                        let _ = tx.send(r.map(|r| r.map(|_| ())));
                        c
                    })
                    .unwrap()
            })
            .collect();
        drop(tx);
        // A thread that panicked inside the engine can leave the others waiting for good, so
        // a round waits at most 15 s for its 32 answers.
        let (mut panics, mut failures, mut answered) = (0, Vec::new(), 0);
        let deadline = Instant::now() + Duration::from_secs(15);
        while answered < CONNECTIONS {
            let left = deadline.saturating_duration_since(Instant::now());
            match rx.recv_timeout(left) {
                Ok(Err(_)) => panics += 1,
                Ok(Ok(Err(e))) => failures.push(e.to_string()),
                Ok(Ok(Ok(()))) => {}
                Err(_) => break,
            }
            answered += 1;
        }
        let hung = CONNECTIONS - answered;
        let panics = panics.max(PANICS.load(SeqCst));
        if panics > 0 || hung > 0 {
            // The engine is not usable; count the round, leak it (the directory under it goes
            // when the round ends) and run the next.
            panicked_rounds.push((round, panics, hung));
            std::mem::forget((handles, db));
            continue;
        }
        let conns: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(failures.is_empty(), "round {round}: {failures:?}");
        drop(conns);
        db.close()
            .unwrap_or_else(|e| panic!("round {round}: close: {e}"));
    }
    drop(panic::take_hook()); // the counting hook goes; the default hook is back
    println!(
        "rounds that panic: {} of {ROUNDS}: {panicked_rounds:?}",
        panicked_rounds.len()
    );
    assert!(
        panicked_rounds.is_empty(),
        "{} of {ROUNDS} rounds panic (round, panics, writers left hanging): {panicked_rounds:?}",
        panicked_rounds.len()
    );
}

/// A fresh database has page 1 when the open returns, and getting it made no commit, so
/// nothing waited out the long window: the stats are the zero they were before the open, and
/// the bucket, empty before the open, holds what creating the database wrote (every put one
/// key, no delete) and does not change afterwards.
#[test]
fn open_of_a_fresh_database_allocates_page_one_without_a_commit() {
    for key in [false, true] {
        let t = tempfile::tempdir().unwrap();
        let store = Arc::new(MemStore::new());
        let before = bucket(&store);
        assert_eq!(before.1, (0, 0, 0), "key={key}: an empty bucket");
        let started = Instant::now();
        let db = open_with_store(config(&t.path().join("db"), key), store.clone()).unwrap();
        assert!(started.elapsed() < Duration::from_secs(30), "key={key}");
        // Measured before the test's own connection (the last to close commits too).
        assert_eq!(commits(&db.upload_stats()), (0, 0, 0), "key={key}");
        let after = bucket(&store);
        assert_eq!(
            after.1 .0 as usize,
            after.0.len(),
            "key={key}: puts vs keys"
        );
        assert_eq!(after.1 .1, 0, "key={key}: deletes");
        thread::sleep(Duration::from_millis(300));
        assert_eq!(bucket(&store), after, "key={key}: nothing follows the open");
        assert_eq!(commits(&db.upload_stats()), (0, 0, 0), "key={key}");
        assert_eq!(page_count(&db), 1, "key={key}: page 1 is allocated");
        drop(db);
    }
}

/// Opening a database that is not fresh adds no commit, no ack and no bucket write: the
/// stats of the new open are the zero they were before it, and the bucket's keys and deletes
/// are those taken just before the open. Puts rise by exactly one, the `head` rewrite of the
/// epoch raise that an open of an existing database makes; bytes up are not compared.
#[test]
fn open_of_a_database_that_is_not_fresh_adds_no_commit() {
    for key in [false, true] {
        let t = tempfile::tempdir().unwrap();
        let store = Arc::new(MemStore::new());
        let dir = t.path().join("db");
        let db = open_with_store(config(&dir, key), store.clone()).unwrap();
        let c = db.connect().unwrap();
        c.set_batching(Some(Batching::Off)).unwrap();
        c.execute("CREATE TABLE t(x INTEGER)", &[]).unwrap();
        c.execute("INSERT INTO t(x) VALUES (1)", &[]).unwrap();
        drop(c);
        db.close().unwrap();

        let before = bucket(&store);
        let db = open_with_store(config(&dir, key), store.clone()).unwrap();
        // Measured before the test's own connection (the last to close commits too).
        assert_eq!(commits(&db.upload_stats()), (0, 0, 0), "key={key}");
        let after = bucket(&store);
        assert_eq!(after.0, before.0, "key={key}: bucket keys");
        // The open raises the epoch: one put, of `head`. Nothing else is written.
        assert_eq!(after.1 .0, before.1 .0 + 1, "key={key}: bucket puts");
        assert_eq!(after.1 .1, before.1 .1, "key={key}: bucket deletes");
        assert!(page_count(&db) > 0, "key={key}: not fresh");
        db.close().unwrap();
    }
}

/// A read-only open refuses every write, `BEGIN IMMEDIATE` included, so it allocates nothing.
#[test]
fn read_only_open_of_a_fresh_directory_writes_nothing() {
    let t = tempfile::tempdir().unwrap();
    let store = Arc::new(MemStore::new());
    open_with_store(config(&t.path().join("w"), false), store.clone())
        .unwrap()
        .close()
        .unwrap();
    let before = bucket(&store);
    let db = open_read_only_with_store(config(&t.path().join("r"), false), store.clone()).unwrap();
    // The writer closed without a write, so the bucket holds no page 1: the directory is fresh.
    assert_eq!(page_count(&db), 0);
    assert_eq!(commits(&db.upload_stats()), (0, 0, 0));
    assert_eq!(bucket(&store), before);
    db.close().unwrap();
}

/// An MVCC database is created, opened and given page 1 the same way, and takes writes.
#[test]
fn mvcc_fresh_open_allocates_page_one_and_takes_writes() {
    let t = tempfile::tempdir().unwrap();
    let store = Arc::new(MemStore::new());
    let mut c = config(&t.path().join("db"), false);
    c.batching = Batching::Off;
    c.journal_mode = JournalMode::Mvcc;
    let db = open_with_store(c, store).unwrap();
    // MVCC's file already holds its schema page next to page 1, so the count is not 1.
    assert!(page_count(&db) >= 1, "page 1 is allocated");
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x INTEGER)", &[]).unwrap();
    drop(conn);
    db.close().unwrap();
}
