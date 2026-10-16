use super::*;
use crate::engine::tests::{head, keys, log_seqs, PREFIX};
use crate::format::{decode_record, Head, LocalState, LogRecord};
use crate::reconcile::Reconciled;
use crate::restore::restore_with_store;
use crate::store::{FaultStore, MemStore};
use crate::{CheckpointMode, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn config(dir: &Path) -> Config {
    let mut c = Config::new(dir, "http://127.0.0.1:1", "bucket", "key", "secret");
    c.prefix = PREFIX.to_string();
    c.retry_deadline = Duration::from_secs(5);
    c
}

fn mem() -> Arc<MemStore> {
    Arc::new(MemStore::new())
}

fn tempdir() -> TempDir {
    tempfile::tempdir().unwrap()
}

/// A path inside a new temp dir that does not exist yet.
fn fresh(t: &TempDir) -> std::path::PathBuf {
    t.path().join("db")
}

fn open_mem(store: &Arc<MemStore>, dir: &Path) -> Database {
    open_with_store(config(dir), store.clone()).unwrap_or_else(|e| panic!("open: {e}"))
}

fn create(c: &Connection) {
    c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
}

fn insert(c: &Connection, id: i64) -> Result<u64> {
    c.execute(
        "INSERT INTO t(id, v) VALUES (?, ?)",
        &[Value::from_i64(id), Value::build_text(format!("row {id}"))],
    )
}

fn ids(c: &Connection) -> Vec<i64> {
    c.query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect()
}

fn text(v: &Value) -> String {
    v.to_text()
        .unwrap_or_else(|| panic!("not text: {v:?}"))
        .to_string()
}

fn unsupported<T: std::fmt::Debug>(r: Result<T>) -> &'static str {
    match r {
        Err(Error::Unsupported(what)) => what,
        r => panic!("expected Unsupported, got {r:?}"),
    }
}

fn read_only_err<T: std::fmt::Debug>(r: Result<T>) {
    assert_eq!(unsupported(r), "writes on a read-only open");
}

fn header_bytes(dir: &Path) -> (u8, u8) {
    let b = std::fs::read(dir.join("app.db")).unwrap();
    (b[18], b[19])
}

fn state(dir: &Path) -> LocalState {
    LocalState::read(dir).unwrap().expect("local state")
}

/// A crash: nothing of the database or its connection runs on drop.
fn crash(db: Database, conns: Vec<Connection>) {
    for c in conns {
        std::mem::forget(c);
    }
    std::mem::forget(db);
}

#[test]
fn open_new_then_reopen_keeps_rows() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let db = open_mem(&store, &dir);
    assert_eq!(db.generation(), 1);
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=10 {
        assert_eq!(insert(&c, id).unwrap(), 1);
    }
    assert_eq!(
        c.execute("UPDATE t SET v = 'x' WHERE id <= 3", &[])
            .unwrap(),
        3
    );
    c.close().unwrap();
    db.close().unwrap();
    assert!(db_bucket_puts(&store) > 0);

    let db = open_mem(&store, &dir);
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), (1..=10).collect::<Vec<_>>());
    let rows = c
        .query("SELECT v FROM t WHERE id = ?", &[Value::from_i64(7)])
        .unwrap();
    assert_eq!(text(&rows[0][0]), "row 7");
    drop(c);
    drop(db);

    let r = tempdir();
    let rep = restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r)).unwrap();
    assert_eq!(rep.generation, head(&*store).generation);
    let db = open_mem(&store, &fresh(&r));
    assert_eq!(ids(&db.connect().unwrap()), (1..=10).collect::<Vec<_>>());
}

fn db_bucket_puts(store: &Arc<MemStore>) -> u64 {
    store.stats().puts
}

#[test]
fn open_on_empty_dir_with_existing_head_auto_restores() {
    let store = mem();
    let a = tempdir();
    let db = open_mem(&store, a.path());
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=20 {
        insert(&c, id).unwrap();
    }
    c.close().unwrap();
    db.close().unwrap();

    let b = tempdir();
    let db = open_mem(&store, &fresh(&b));
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), (1..=20).collect::<Vec<_>>());
    insert(&c, 21).unwrap();
    assert_eq!(ids(&c).len(), 21);
}

#[test]
fn second_open_same_dir_in_process_is_refused_and_first_keeps_working() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let first = open_mem(&store, &dir);
    let c = first.connect().unwrap();
    create(&c);
    insert(&c, 1).unwrap();

    let again = open_with_store(config(&dir), store.clone());
    assert!(
        matches!(again, Err(Error::AlreadyOpen(_))),
        "{:?}",
        again.err()
    );
    // Another spelling of the same directory.
    let again = open_with_store(config(&dir.join("..").join("db")), store.clone());
    assert!(
        matches!(again, Err(Error::AlreadyOpen(_))),
        "{:?}",
        again.err()
    );
    let again = open_read_only_with_store(config(&dir), store.clone());
    assert!(
        matches!(again, Err(Error::AlreadyOpen(_))),
        "{:?}",
        again.err()
    );
    let again = restore_with_store(&config(&dir), store.clone(), &dir);
    assert!(
        matches!(again, Err(Error::AlreadyOpen(_))),
        "{:?}",
        again.err()
    );

    // The first instance kept its locks and keeps committing.
    for id in 2..=5 {
        insert(&c, id).unwrap();
    }
    assert_eq!(ids(&c), vec![1, 2, 3, 4, 5]);
    drop(c);
    drop(first);

    let db = open_mem(&store, &dir);
    assert_eq!(ids(&db.connect().unwrap()), vec![1, 2, 3, 4, 5]);
}

#[test]
fn close_is_refused_while_connections_remain() {
    let store = mem();
    let t = tempdir();
    let db = open_mem(&store, t.path());
    let c = db.connect().unwrap();
    create(&c);
    let r = db.clone().close();
    assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
    insert(&c, 1).unwrap();
    c.close().unwrap();
    db.close().unwrap();
}

#[test]
fn open_refuses_existing_mvcc_database() {
    let store = mem();
    let t = tempdir();
    {
        use turso_core::{OpenOptions, PlatformIO, SqliteDialect};
        let io: Arc<dyn turso_core::IO> = Arc::new(PlatformIO::new().unwrap());
        let path = t.path().join("app.db");
        let opts = OpenOptions::new(Arc::new(SqliteDialect));
        let db = turso_core::Database::open(io, path.to_str().unwrap(), opts).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode='mvcc'").unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1)").unwrap();
        conn.close().unwrap();
    }
    assert_eq!(header_bytes(t.path()), (255, 255));
    let r = open_with_store(config(t.path()), store.clone());
    assert_eq!(unsupported(r), "MVCC");
    assert_eq!(store.stats().puts, 0, "the bucket was touched");
    // The refusal released the directory.
    let r = open_with_store(config(t.path()), store.clone());
    assert_eq!(unsupported(r), "MVCC");
}

#[test]
fn mvcc_pragma_is_refused_and_header_unchanged() {
    let store = mem();
    let t = tempdir();
    let db = open_mem(&store, t.path());
    let c = db.connect().unwrap();
    create(&c);
    insert(&c, 1).unwrap();
    for sql in [
        "PRAGMA journal_mode='mvcc'",
        "PRAGMA journal_mode=mvcc",
        "PRAGMA journal_mode='EXPERIMENTAL_MVCC'",
        "PRAGMA main.journal_mode = 'mvcc'",
    ] {
        assert_eq!(unsupported(c.execute(sql, &[])), "MVCC", "{sql}");
        assert_eq!(unsupported(c.query(sql, &[])), "MVCC", "{sql}");
    }
    insert(&c, 2).unwrap();
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(header_bytes(t.path()), (2, 2));

    let db = open_mem(&store, t.path());
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), vec![1, 2]);
    let mode = c.query("PRAGMA journal_mode", &[]).unwrap();
    assert_eq!(text(&mode[0][0]), "wal");
}

#[test]
fn non_wal_journal_modes_are_refused() {
    let store = mem();
    let t = tempdir();
    let db = open_mem(&store, t.path());
    let c = db.connect().unwrap();
    for mode in ["delete", "truncate", "persist", "memory", "off"] {
        let sql = format!("PRAGMA journal_mode={mode}");
        assert_eq!(
            unsupported(c.execute(&sql, &[])),
            "journal_mode other than WAL",
            "{sql}"
        );
    }
    let set = c.query("PRAGMA journal_mode=wal", &[]).unwrap();
    assert_eq!(text(&set[0][0]), "wal");
    let read = c.query("PRAGMA journal_mode", &[]).unwrap();
    assert_eq!(text(&read[0][0]), "wal");
}

#[test]
fn attach_is_refused() {
    let store = mem();
    let t = tempdir();
    let db = open_mem(&store, &fresh(&t));
    let c = db.connect().unwrap();
    let cwd_file = std::env::current_dir().unwrap().join("x.db");
    let existed = cwd_file.exists();
    assert_eq!(unsupported(c.execute("ATTACH 'x.db' AS x", &[])), "ATTACH");
    let in_dir = t.path().join("x.db");
    let sql = format!("attach database '{}' as x", in_dir.display());
    assert_eq!(unsupported(c.execute(&sql, &[])), "ATTACH");
    assert_eq!(unsupported(c.execute_batch(&sql)), "ATTACH");
    assert!(!in_dir.exists(), "ATTACH created {}", in_dir.display());
    assert_eq!(cwd_file.exists(), existed, "ATTACH created x.db");
}

#[test]
fn mvcc_pragma_inside_batch_is_refused() {
    let store = mem();
    let t = tempdir();
    let db = open_mem(&store, t.path());
    let c = db.connect().unwrap();
    create(&c);
    let r = c.execute_batch("INSERT INTO t(id, v) VALUES (1, 'a'); PRAGMA journal_mode=mvcc;");
    assert_eq!(unsupported(r), "MVCC");
    assert!(ids(&c).is_empty(), "the INSERT ran");
    c.execute_batch("INSERT INTO t(id, v) VALUES (1, 'a'); INSERT INTO t(id, v) VALUES (2, 'b');")
        .unwrap();
    assert_eq!(ids(&c), vec![1, 2]);
}

#[test]
fn synchronous_cannot_be_lowered() {
    let store = mem();
    let t = tempdir();
    let db = open_mem(&store, t.path());
    let c = db.connect().unwrap();
    for v in ["OFF", "NORMAL", "0", "1"] {
        let sql = format!("PRAGMA synchronous={v}");
        assert_eq!(
            unsupported(c.execute(&sql, &[])),
            "synchronous below FULL",
            "{sql}"
        );
        assert_eq!(
            unsupported(c.execute_batch(&sql)),
            "synchronous below FULL",
            "{sql}"
        );
    }
    let read = c.query("PRAGMA synchronous", &[]).unwrap();
    assert_eq!(read[0][0].as_int(), Some(2));
    c.execute("PRAGMA synchronous=FULL", &[]).unwrap();
    let read = c.query("PRAGMA synchronous", &[]).unwrap();
    assert_eq!(read[0][0].as_int(), Some(2));
}

#[test]
fn upload_failure_surfaces_typed_error() {
    let inner = mem();
    let fault = Arc::new(FaultStore::new(inner.clone()));
    let t = tempdir();
    let mut cfg = config(t.path());
    cfg.retry_deadline = Duration::from_millis(300);
    let db = open_with_store(cfg, fault.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    insert(&c, 1).unwrap();

    fault.fail_next_puts_permanently(1);
    let r = insert(&c, 2);
    assert!(matches!(r, Err(Error::UploadFailed(_))), "{r:?}");

    fault.fail_next_puts(u64::MAX / 2);
    let r = insert(&c, 3);
    assert!(matches!(r, Err(Error::Timeout(_))), "{r:?}");
    fault.fail_next_puts(0);

    insert(&c, 4).unwrap();
    assert_eq!(ids(&c), vec![1, 4]);
}

#[test]
fn transaction_commits_and_rolls_back() {
    let store = mem();
    let t = tempdir();
    let db = open_mem(&store, t.path());
    let c = db.connect().unwrap();
    create(&c);
    let n = c
        .transaction(|tx| {
            tx.execute("INSERT INTO t(id, v) VALUES (1, 'a')", &[])?;
            tx.execute("INSERT INTO t(id, v) VALUES (2, 'b')", &[])?;
            let rows = tx.query("SELECT count(*) FROM t", &[])?;
            Ok(rows[0][0].as_int().unwrap())
        })
        .unwrap();
    assert_eq!(n, 2);
    let r: Result<()> = c.transaction(|tx| {
        tx.execute("INSERT INTO t(id, v) VALUES (3, 'c')", &[])?;
        Err(Error::Config("changed my mind".into()))
    });
    assert_eq!(r, Err(Error::Config("changed my mind".into())));
    // A failing statement inside rolls back too.
    let r: Result<()> = c.transaction(|tx| {
        tx.execute("INSERT INTO t(id, v) VALUES (4, 'd')", &[])?;
        tx.execute("INSERT INTO t(id, v) VALUES (1, 'dup')", &[])?;
        Ok(())
    });
    assert!(matches!(r, Err(Error::Engine(_))), "{r:?}");
    assert_eq!(ids(&c), vec![1, 2]);
    insert(&c, 5).unwrap();
    assert_eq!(ids(&c), vec![1, 2, 5]);
}

#[test]
fn several_connections_in_one_process() {
    let store = mem();
    let t = tempdir();
    let db = open_mem(&store, t.path());
    create(&db.connect().unwrap());
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let start = Instant::now();
    let writers: Vec<_> = (0..4i64)
        .map(|w| {
            let db = db.clone();
            std::thread::spawn(move || {
                let c = db.connect().unwrap();
                for i in 0..100 {
                    insert(&c, w * 1000 + i).unwrap_or_else(|e| panic!("writer {w} row {i}: {e}"));
                }
                c.close().unwrap();
            })
        })
        .collect();
    let reader = {
        let db = db.clone();
        let done = done.clone();
        std::thread::spawn(move || {
            let c = db.connect().unwrap();
            let mut reads = 0;
            while !done.load(std::sync::atomic::Ordering::SeqCst) {
                let n = c.query("SELECT count(*) FROM t", &[]).unwrap()[0][0]
                    .as_int()
                    .unwrap();
                assert!((0..=400).contains(&n));
                reads += 1;
            }
            reads
        })
    };
    for w in writers {
        w.join().unwrap();
    }
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    let reads = reader.join().unwrap();
    eprintln!(
        "several_connections: 400 inserts, {reads} reads in {:?}",
        start.elapsed()
    );
    let c = db.connect().unwrap();
    let count = c.query("SELECT count(*) FROM t", &[]).unwrap()[0][0].as_int();
    assert_eq!(count, Some(400));
    c.close().unwrap();
    db.close().unwrap();

    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    let db = open_read_only_with_store(config(r.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    let count = c.query("SELECT count(*) FROM t", &[]).unwrap()[0][0].as_int();
    assert_eq!(count, Some(400));
}

/// A writer that committed rows `1..=n` and was closed; returns its directory.
fn closed_writer(store: &Arc<MemStore>, n: i64) -> TempDir {
    let a = tempdir();
    let db = open_mem(store, a.path());
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=n {
        insert(&c, id).unwrap();
    }
    c.close().unwrap();
    db.close().unwrap();
    a
}

fn no_database_files(dir: &Path) {
    for f in ["app.db", "app.db-wal", "app.db-durable", "app.db-restoring"] {
        assert!(!dir.join(f).exists(), "{f} was left in {}", dir.display());
    }
}

#[test]
fn failed_restore_leaves_dir_clean() {
    let store = mem();
    let a = tempdir();
    let db = open_mem(&store, a.path());
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=5 {
        insert(&c, id).unwrap();
    }
    let gen = head(&*store).generation;
    // A gap: log/<gen>/1 is gone.
    store.delete(&keys().log(gen, 1)).unwrap();

    let t = tempdir();
    let dir = fresh(&t);
    let r = restore_with_store(&config(&dir), store.clone(), &dir);
    assert!(matches!(r, Err(Error::Corrupt(_))), "{r:?}");
    no_database_files(&dir);
    let r = open_with_store(config(&dir), store.clone());
    assert!(matches!(r, Err(Error::Corrupt(_))), "{:?}", r.err());
    no_database_files(&dir);
    let left: Vec<_> = std::fs::read_dir(t.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        left,
        vec!["db".to_string()],
        "staging directories left behind"
    );
    crash(db, vec![c]);
}

#[test]
fn interrupted_restore_is_cleaned_up_at_open() {
    let store = mem();
    let _a = closed_writer(&store, 5);
    let t = tempdir();
    // A restore that crashed while moving its files into place.
    let staged = tempdir();
    restore_with_store(&config(staged.path()), store.clone(), staged.path()).unwrap();
    std::fs::write(t.path().join("app.db-restoring"), b"").unwrap();
    for f in ["app.db-wal", "app.db"] {
        std::fs::copy(staged.path().join(f), t.path().join(f)).unwrap();
    }
    let db = open_mem(&store, t.path());
    assert_eq!(ids(&db.connect().unwrap()), vec![1, 2, 3, 4, 5]);
    assert!(!t.path().join("app.db-restoring").exists());
}

#[test]
fn restore_writes_nothing_to_the_bucket() {
    let store = mem();
    let _a = closed_writer(&store, 10);
    let puts = store.stats().puts;
    let h = store.get(&keys().head()).unwrap().unwrap();
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    assert_eq!(store.stats().puts, puts, "restore wrote to the bucket");
    let s = state(r.path());
    assert!(!s.claimed);
    assert_eq!(s.head_etag, h.etag);
    assert_eq!(s.writer_id, Head::decode(&h.body).unwrap().writer_id);
    // A restore never refills a directory holding a database.
    let again = restore_with_store(&config(r.path()), store.clone(), r.path());
    assert!(matches!(again, Err(Error::Config(_))), "{again:?}");
}

#[test]
fn first_writer_open_after_restore_claims() {
    let store = mem();
    let _a = closed_writer(&store, 10);
    let h0 = head(&*store);
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();

    let db = open_mem(&store, r.path());
    let s = state(r.path());
    assert!(s.claimed);
    assert_ne!(s.writer_id, h0.writer_id);
    let h1 = head(&*store);
    assert_eq!(h1.writer_id, s.writer_id);
    assert_eq!(h1.generation, h0.generation);
    assert_eq!(h1.manifest, h0.manifest);
    assert_eq!(db.generation(), h0.generation);
    let c = db.connect().unwrap();
    insert(&c, 11).unwrap();
    c.close().unwrap();
    db.close().unwrap();

    let v = tempdir();
    let db = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    assert_eq!(ids(&db.connect().unwrap()), (1..=11).collect::<Vec<_>>());
}

#[test]
fn old_writer_restarting_after_claim_is_fenced() {
    let store = mem();
    let a = closed_writer(&store, 3);
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    let b = open_mem(&store, r.path());
    insert(&b.connect().unwrap(), 4).unwrap();
    b.close().unwrap();

    let old = open_with_store(config(a.path()), store.clone());
    assert!(matches!(old, Err(Error::Fenced(_))), "{:?}", old.err());
    // Refused cleanly: the directory is free again, and refused again.
    let old = open_with_store(config(a.path()), store.clone());
    assert!(matches!(old, Err(Error::Fenced(_))), "{:?}", old.err());
}

#[test]
fn stale_claim_fails_cleanly() {
    let store = mem();
    let a = tempdir();
    let db = open_mem(&store, a.path());
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=5 {
        insert(&c, id).unwrap();
    }
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    // The head moves between the restore and the open.
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    let moved = head(&*store);
    let puts = store.stats().puts;

    let b = open_with_store(config(r.path()), store.clone());
    match b {
        Err(Error::Fenced(m)) => assert!(m.contains("restore again"), "{m}"),
        b => panic!("expected Fenced, got {:?}", b.err()),
    }
    assert_eq!(
        store.stats().puts,
        puts,
        "a stale claim wrote to the bucket"
    );
    assert_eq!(head(&*store), moved);
    assert!(!state(r.path()).claimed);
    insert(&c, 6).unwrap();
    c.close().unwrap();
    db.close().unwrap();
}

#[test]
fn open_read_only_refuses_writes_and_never_seals() {
    let store = mem();
    let _a = closed_writer(&store, 10);
    let h0 = head(&*store);
    let puts = store.stats().puts;

    let v = tempdir();
    let db = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), (1..=10).collect::<Vec<_>>());
    read_only_err(insert(&c, 11));
    read_only_err(c.execute("CREATE TABLE u(x)", &[]));
    read_only_err(c.execute_batch("DELETE FROM t"));
    read_only_err(c.transaction(|tx| tx.execute("DELETE FROM t", &[])));
    read_only_err(c.checkpoint(CheckpointMode::Truncate));
    read_only_err(c.query("PRAGMA wal_checkpoint(TRUNCATE)", &[]));
    c.execute_batch("SELECT 1; SELECT count(*) FROM t;  -- done\n")
        .unwrap();
    assert_eq!(ids(&c), (1..=10).collect::<Vec<_>>());
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(head(&*store), h0);
    assert_eq!(
        store.stats().puts,
        puts,
        "a read-only open wrote to the bucket"
    );
    assert!(!state(v.path()).claimed);

    // Again on the same, now restored, directory.
    let db = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    assert_eq!(ids(&db.connect().unwrap()), (1..=10).collect::<Vec<_>>());
    drop(db);
    assert_eq!(store.stats().puts, puts);
    assert_eq!(head(&*store), h0);
}

/// Verifying a live writer's bucket (restore, open, read, close) must not take over its head.
/// The writer is then crashed with `forget`: a drop or close would run its own close-time
/// checkpoint and seal, which a crash never does.
#[test]
fn verify_against_live_writer_loses_nothing() {
    let store = mem();
    let a = tempdir();
    let db = open_mem(&store, a.path());
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=50 {
        insert(&c, id).unwrap();
    }
    let writer_id = state(a.path()).writer_id;
    for round in 0..3i64 {
        let v = tempdir();
        let verify = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
        let vc = verify.connect().unwrap();
        let seen = ids(&vc);
        assert_eq!(seen, (1..=50 + round * 20).collect::<Vec<_>>());
        for id in 51 + round * 20..=60 + round * 20 {
            insert(&c, id).unwrap();
        }
        vc.close().unwrap();
        verify.close().unwrap();
        for id in 61 + round * 20..=70 + round * 20 {
            insert(&c, id).unwrap();
        }
    }
    assert_eq!(head(&*store).writer_id, writer_id);
    crash(db, vec![c]);

    let v = tempdir();
    let verify = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    assert_eq!(
        ids(&verify.connect().unwrap()),
        (1..=110).collect::<Vec<_>>()
    );
    assert_eq!(head(&*store).writer_id, writer_id);
}

/// The old writer acks a record between the restore and the claim: the claimed writer's
/// reconcile takes it in (`Behind`), its claim record (no syncs) follows at the next seq, then
/// its own commits; restore and reconcile both accept that log.
#[test]
fn claim_after_old_writer_appended_comes_in_behind() {
    let store = mem();
    let a = tempdir();
    let db_a = open_mem(&store, a.path());
    let ca = db_a.connect().unwrap();
    create(&ca);
    for id in 1..=10 {
        insert(&ca, id).unwrap();
    }
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    insert(&ca, 11).unwrap();
    let gen = head(&*store).generation;
    let n = log_seqs(&*store, gen).len() as u64;

    let db_b = open_mem(&store, r.path());
    assert_eq!(db_b.shared.reconciled, Some(Reconciled::Behind));
    let b_id = state(r.path()).writer_id;
    let cb = db_b.connect().unwrap();
    assert_eq!(ids(&cb), (1..=11).collect::<Vec<_>>());
    insert(&cb, 12).unwrap();
    assert_eq!(log_seqs(&*store, gen), (1..=n + 2).collect::<Vec<_>>());
    match record(&store, gen, n + 1) {
        LogRecord::Extents { writer_id, syncs } => {
            assert_eq!(writer_id, b_id);
            assert!(syncs.is_empty(), "the claim record holds syncs");
        }
        r => panic!("{r:?}"),
    }
    assert!(
        matches!(&record(&store, gen, n + 2), LogRecord::Extents { writer_id, .. } if *writer_id == b_id)
    );
    // The old writer's next append collides with the claim record.
    let r13 = insert(&ca, 13);
    assert!(matches!(r13, Err(Error::Fenced(_))), "{r13:?}");
    crash(db_a, vec![ca]);
    crash(db_b, vec![cb]);

    // Restore: [A's records, B's claim record, B's record].
    let v = tempdir();
    let verify = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    assert_eq!(
        ids(&verify.connect().unwrap()),
        (1..=12).collect::<Vec<_>>()
    );
    drop(verify);
    // Reconcile: B restarting on (a copy of) its own disk is level with that log, and its
    // next commit continues the seqs.
    let b2 = copy_db_files(r.path());
    let db_b2 = open_mem(&store, b2.path());
    assert_eq!(db_b2.shared.reconciled, Some(Reconciled::Level));
    let cb2 = db_b2.connect().unwrap();
    insert(&cb2, 14).unwrap();
    assert_eq!(log_seqs(&*store, gen), (1..=n + 3).collect::<Vec<_>>());
    assert_eq!(ids(&cb2), vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14]);
}

fn record(store: &Arc<MemStore>, gen: u64, seq: u64) -> LogRecord {
    decode_record(&store.get(&keys().log(gen, seq)).unwrap().unwrap().body).unwrap()
}

/// A copy of a (crashed) directory's database files, in a new directory.
fn copy_db_files(from: &Path) -> TempDir {
    let to = tempdir();
    for f in ["app.db", "app.db-wal", "app.db-durable", "app.db-dirty"] {
        if from.join(f).exists() {
            std::fs::copy(from.join(f), to.path().join(f)).unwrap();
        }
    }
    to
}

/// The reviewer's case: A acks after B's claim, then B seals with no append of its own in
/// between (`seal`, which returns what it acked afterwards), then A commits again. Every
/// acked row must survive a restore: either A's post-claim insert fails (fenced by B's claim
/// record) or B's open fails.
fn old_writer_after_claim_loses_nothing(seal: fn(Database, Connection) -> Vec<i64>) {
    let store = mem();
    let a = tempdir();
    let db_a = open_mem(&store, a.path());
    let ca = db_a.connect().unwrap();
    create(&ca);
    let mut acked: Vec<i64> = (1..=10).collect();
    for &id in &acked {
        insert(&ca, id).unwrap();
    }
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    let b = open_with_store(config(r.path()), store.clone());
    let a_after = insert(&ca, 11);
    if a_after.is_ok() {
        acked.push(11);
    }
    let b_failed = b.is_err();
    match b {
        Ok(db_b) => {
            let cb = db_b.connect().unwrap();
            acked.extend(seal(db_b, cb));
        }
        Err(e) => assert!(matches!(e, Error::Fenced(_)), "{e:?}"),
    }
    if insert(&ca, 12).is_ok() {
        acked.push(12);
    }
    assert!(
        b_failed || a_after.is_err(),
        "both writers acked after the claim: {a_after:?}"
    );
    assert!(matches!(a_after, Err(Error::Fenced(_))), "{a_after:?}");
    crash(db_a, vec![ca]);

    acked.sort();
    let v = tempdir();
    let verify = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    assert_eq!(ids(&verify.connect().unwrap()), acked);
}

#[test]
fn old_writer_after_claim_then_truncate_loses_nothing() {
    old_writer_after_claim_loses_nothing(|db, c| {
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        insert(&c, 100).unwrap();
        crash(db, vec![c]);
        vec![100]
    });
}

#[test]
fn old_writer_after_claim_then_close_loses_nothing() {
    old_writer_after_claim_loses_nothing(|db, c| {
        c.close().unwrap();
        db.close().unwrap();
        vec![]
    });
}

/// Every object under the test prefix (head, logs, manifests, pages), key → body.
fn bucket_objects(store: &Arc<MemStore>) -> BTreeMap<String, Vec<u8>> {
    let k = keys();
    let mut all = vec![k.head()];
    for root in [k.log_root(), k.manifest_root(), k.page_root()] {
        all.extend(store.list(&root).unwrap());
    }
    all.into_iter()
        .filter_map(|key| store.get(&key).unwrap().map(|o| (key, o.body)))
        .collect()
}

fn integrity_ok(c: &Connection) {
    let rows = c.query("PRAGMA integrity_check", &[]).unwrap();
    assert_eq!(text(&rows[0][0]), "ok");
}

/// The objects of a seal of `gen`: `pages/<gen>/*` and `manifest/<gen>`.
fn sealed_objects(all: &BTreeMap<String, Vec<u8>>, gen: u64) -> BTreeMap<String, Vec<u8>> {
    let k = keys();
    all.iter()
        .filter(|(key, _)| {
            matches!(k.parse_page(key), Some((g, _)) if g == gen)
                || k.parse_manifest(key) == Some(gen)
        })
        .map(|(key, body)| (key.clone(), body.clone()))
        .collect()
}

/// B1 (merge review): writer A at generation G; B restores, claims, commits 100..=110 and
/// seals G (TRUNCATE), then closes. `old_writer` then makes A act on its stale view of G
/// (A has not committed since the claim) and returns what A's action returned. A must not
/// touch the bucket: `pages/G/*`, `manifest/G` and every other object stay byte-identical to
/// what B left, and a fresh restore holds every row either writer acked, integrity ok.
///
/// With `trim`, B's trimmer has also run (its grace passed): `log/G` is gone, so A's
/// seal-start record cannot collide with B's claim record.
fn old_writer_after_claim_and_seal(
    prepare_a: fn(&Connection),
    trim: bool,
    old_writer: fn(Database, Connection) -> Result<()>,
) {
    let store = mem();
    let a = tempdir();
    let db_a = open_mem(&store, a.path());
    let ca = db_a.connect().unwrap();
    create(&ca);
    let mut acked: Vec<i64> = (1..=10).collect();
    for &id in &acked {
        insert(&ca, id).unwrap();
    }
    prepare_a(&ca);
    let gen = head(&*store).generation;

    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    let db_b = open_mem(&store, r.path());
    let cb = db_b.connect().unwrap();
    for id in 100..=110 {
        insert(&cb, id).unwrap();
        acked.push(id);
    }
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    cb.close().unwrap();
    db_b.close().unwrap();
    let h = head(&*store);
    assert_eq!(
        (h.generation, h.manifest),
        (gen + 1, Some(gen)),
        "B sealed G"
    );
    if trim {
        let (_, m) = crate::engine::tests::manifest(&*store);
        crate::trim::trim_once(&*store, &keys(), &h, Some(&m)).unwrap();
        assert!(log_seqs(&*store, gen).is_empty(), "log/G trimmed");
    }
    let before = bucket_objects(&store);
    assert!(!sealed_objects(&before, gen).is_empty());

    let r = old_writer(db_a, ca);
    assert!(
        r.is_ok() || matches!(r, Err(Error::Fenced(_))),
        "the old writer: {r:?}"
    );
    // A read-only open writes nothing to the bucket.
    let v = tempdir();
    let verify = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    let c = verify.connect().unwrap();
    integrity_ok(&c);
    assert_eq!(ids(&c), acked, "every acked row restores");
    let after = bucket_objects(&store);
    let changed = |a: &BTreeMap<String, Vec<u8>>, b: &BTreeMap<String, Vec<u8>>| {
        let keys: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
        keys.into_iter()
            .filter(|k| a.get(*k) != b.get(*k))
            .cloned()
            .collect::<Vec<_>>()
    };
    let g_changed = changed(&sealed_objects(&before, gen), &sealed_objects(&after, gen));
    assert!(
        g_changed.is_empty(),
        "pages/G/* and manifest/G differ from B's seal: {g_changed:?}"
    );
    let mut all_changed = changed(&before, &after);
    if trim {
        // A's seal-start record lands in the trimmed `log/G`: an orphan below the head's
        // generation, which no restore reads and the next trim deletes.
        all_changed.retain(|k| !matches!(keys().parse_log(k), Some((g, _)) if g == gen));
    }
    assert!(
        all_changed.is_empty(),
        "the old writer changed the bucket: {all_changed:?}"
    );
}

/// (a) The reviewer's probe: an idle old writer closes; its close-time TRUNCATE would seal
/// G again. close itself returns Ok (it ignores a failed close-time checkpoint).
#[test]
fn idle_old_writer_closing_after_claim_and_seal_changes_nothing() {
    old_writer_after_claim_and_seal(|_| {}, false, close_old_writer);
}

fn close_old_writer(db: Database, c: Connection) -> Result<()> {
    c.close()?;
    db.close()
}

/// (a) after B's trimmer deleted `log/G`: the head check fences A instead.
#[test]
fn idle_old_writer_closing_after_claim_seal_and_trim_changes_nothing() {
    old_writer_after_claim_and_seal(|_| {}, true, close_old_writer);
}

/// (b) The old writer commits once, and that commit crosses its own generation boundary (a
/// PASSIVE checkpoint backfilled its whole WAL, so the commit restarts the WAL and seals G in
/// the header write, before its own append). The commit fails Fenced.
#[test]
fn old_writer_commit_crossing_its_boundary_after_claim_and_seal_is_fenced() {
    old_writer_after_claim_and_seal(
        |c| c.checkpoint(CheckpointMode::Passive).unwrap(),
        false,
        |db, c| {
            let r = insert(&c, 11);
            assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
            crash(db, vec![c]);
            r.map(|_| ())
        },
    );
}

/// The merge review of the B1 fix: B claims, commits and seals G, and B's trimmer deletes
/// `log/G`. An idle old writer A then commits normally: its record would land in the empty
/// `log/G`, which no restore reads, and the commit would be acked. With A's trim grace (and
/// so its idle threshold) at zero, A checks the head before that append and is fenced.
#[test]
fn idle_old_writer_appending_into_trimmed_log_is_fenced() {
    let store = mem();
    let a = tempdir();
    let mut cfg_a = config(a.path());
    cfg_a.trim_grace = Duration::ZERO;
    let db_a = open_with_store(cfg_a, store.clone()).unwrap();
    let ca = db_a.connect().unwrap();
    create(&ca);
    let mut acked: Vec<i64> = (1..=10).collect();
    for &id in &acked {
        insert(&ca, id).unwrap();
    }
    let gen = head(&*store).generation;

    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    let db_b = open_mem(&store, r.path());
    let cb = db_b.connect().unwrap();
    for id in 100..=110 {
        insert(&cb, id).unwrap();
        acked.push(id);
    }
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    cb.close().unwrap();
    db_b.close().unwrap();
    let h = head(&*store);
    assert_eq!(
        (h.generation, h.manifest),
        (gen + 1, Some(gen)),
        "B sealed G"
    );
    let (_, m) = crate::engine::tests::manifest(&*store);
    crate::trim::trim_once(&*store, &keys(), &h, Some(&m)).unwrap();
    assert!(log_seqs(&*store, gen).is_empty(), "log/G trimmed");
    let puts = db_bucket_puts(&store);

    for id in 11..=15 {
        let r = insert(&ca, id);
        assert!(matches!(r, Err(Error::Fenced(_))), "A's insert {id}: {r:?}");
    }
    assert_eq!(
        db_bucket_puts(&store),
        puts,
        "the fenced writer wrote nothing"
    );
    crash(db_a, vec![ca]);

    let v = tempdir();
    let verify = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    let c = verify.connect().unwrap();
    integrity_ok(&c);
    assert_eq!(ids(&c), acked);
}

/// A single writer that checks the head before every append (trim grace zero) commits,
/// seals and commits again as usual: its own head (and the one each seal moves it to)
/// lets every append proceed.
#[test]
fn single_writer_with_idle_check_on_every_append_keeps_working() {
    let store = mem();
    let a = tempdir();
    let mut cfg = config(a.path());
    cfg.trim_grace = Duration::ZERO;
    let db = open_with_store(cfg.clone(), store.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=5 {
        insert(&c, id).unwrap();
    }
    std::thread::sleep(Duration::from_millis(20));
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    for id in 6..=10 {
        insert(&c, id).unwrap();
    }
    c.close().unwrap();
    db.close().unwrap();
    assert!(head(&*store).generation >= 2, "sealed");

    let db = open_with_store(cfg, store.clone()).unwrap();
    let c = db.connect().unwrap();
    insert(&c, 11).unwrap();
    assert_eq!(ids(&c), (1..=11).collect::<Vec<_>>());
    crash(db, vec![c]);
    let v = tempdir();
    let verify = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    assert_eq!(
        ids(&verify.connect().unwrap()),
        (1..=11).collect::<Vec<_>>()
    );
}

/// The head GETs and log PUTs among `events` (a `FaultStore` log), in order.
fn head_and_log_events(events: &[String]) -> Vec<String> {
    let head = format!("get {}", keys().head());
    events
        .iter()
        .filter(|e| **e == head || e.starts_with("put_if_absent ") && e.contains("/log/"))
        .map(|e| {
            if *e == head {
                "get head".to_string()
            } else {
                "put log".to_string()
            }
        })
        .collect()
}

/// The `/2` margin, end to end: with `trim_grace` = 200 ms a commit right after another
/// makes no head GET, and a commit after 120 ms of idle (past the 100 ms threshold) GETs the
/// head before its log PUT.
#[test]
fn commit_after_half_the_trim_grace_checks_the_head() {
    let mem = mem();
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let a = tempdir();
    let mut cfg = config(a.path());
    cfg.trim_grace = Duration::from_millis(200);
    cfg.retry_deadline = Duration::from_millis(40);
    cfg.request_timeout = Duration::from_millis(50);
    let db = open_with_store(cfg, fault.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    insert(&c, 1).unwrap();

    let before = fault.events().len();
    insert(&c, 2).unwrap();
    assert_eq!(
        head_and_log_events(&fault.events()[before..]),
        ["put log"],
        "right after a commit: no head GET"
    );

    std::thread::sleep(Duration::from_millis(120));
    let before = fault.events().len();
    insert(&c, 3).unwrap();
    assert_eq!(
        head_and_log_events(&fault.events()[before..]),
        ["get head", "put log"],
        "after 120 ms of idle: the head first"
    );
    crash(db, vec![c]);
}

/// The claim record (and any record appended before the engine opens) uses the configured
/// threshold, not the built-in one: with `trim_grace` = 0 the claimer GETs the head right
/// before it.
#[test]
fn claim_record_append_uses_the_configured_idle_threshold() {
    let mem = mem();
    let a = tempdir();
    let db_a = open_mem(&mem, a.path());
    let ca = db_a.connect().unwrap();
    create(&ca);
    insert(&ca, 1).unwrap();
    let gen = head(&*mem).generation;
    let next = *log_seqs(&*mem, gen).last().unwrap() + 1;
    let r = tempdir();
    restore_with_store(&config(r.path()), mem.clone(), r.path()).unwrap();
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let mut cfg = config(r.path());
    cfg.trim_grace = Duration::ZERO;
    let db_b = open_with_store(cfg, fault.clone()).unwrap();
    let ev = fault.events();
    let claim_put = format!("put_if_absent {}", keys().log(gen, next));
    let at = ev
        .iter()
        .position(|e| *e == claim_put)
        .expect("the claim record");
    assert_eq!(ev[at - 1], format!("get {}", keys().head()), "{ev:?}");
    drop(db_b);
    crash(db_a, vec![ca]);
}

/// A claim whose record never landed (the open failed after the head write): the next open
/// sees the generation's last record is another writer's and appends one then.
#[test]
fn reopen_after_claim_without_its_record_appends_one() {
    let store = mem();
    let a = tempdir();
    let db_a = open_mem(&store, a.path());
    let ca = db_a.connect().unwrap();
    create(&ca);
    for id in 1..=10 {
        insert(&ca, id).unwrap();
    }
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    let mut s = state(r.path());
    let b_id = "b".repeat(32);
    let cfg = config(r.path()).engine(false);
    s.head_etag = crate::bucket::claim(&*store, &keys(), &cfg.retry, &s.head_etag, &b_id).unwrap();
    s.writer_id = b_id.clone();
    s.claimed = true;
    s.write_atomic(r.path()).unwrap();
    // No claim record yet: A still acks.
    insert(&ca, 11).unwrap();

    let db_b = open_mem(&store, r.path());
    assert_eq!(db_b.shared.reconciled, Some(Reconciled::Behind));
    let gen = head(&*store).generation;
    let last = *log_seqs(&*store, gen).last().unwrap();
    assert!(
        matches!(record(&store, gen, last), LogRecord::Extents { writer_id, syncs } if writer_id == b_id && syncs.is_empty())
    );
    let r12 = insert(&ca, 12);
    assert!(matches!(r12, Err(Error::Fenced(_))), "{r12:?}");
    let cb = db_b.connect().unwrap();
    insert(&cb, 100).unwrap();
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    crash(db_a, vec![ca]);
    crash(db_b, vec![cb]);

    let v = tempdir();
    let verify = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    let mut want: Vec<i64> = (1..=11).collect();
    want.push(100);
    assert_eq!(ids(&verify.connect().unwrap()), want);
}

/// Close stops and joins the prefetch thread before it releases the directory: nothing
/// writes `app.db` after close, and an immediate reopen works.
#[test]
fn close_stops_prefetch_before_releasing_the_dir() {
    let store = mem();
    {
        let a = tempdir();
        let db = open_mem(&store, a.path());
        let c = db.connect().unwrap();
        create(&c);
        for id in 1..=300 {
            c.execute(
                "INSERT INTO t(id, v) VALUES (?, zeroblob(2000))",
                &[Value::from_i64(id)],
            )
            .unwrap();
        }
        c.close().unwrap();
        db.close().unwrap();
    }
    let pages = crate::engine::tests::manifest(&*store).1.pages.len() as u64;
    let fault = Arc::new(FaultStore::new(store.clone()));
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    fault.only_keys_containing("/pages/");
    fault.delay_gets(Duration::from_millis(3));
    let mut cfg = config(r.path());
    cfg.prefetch = true;
    let db = open_with_store(cfg, fault.clone()).unwrap();
    let (storage, done) = {
        let open = db.shared.open.lock().unwrap();
        let e = &open.as_ref().unwrap().engine;
        (e.storage.clone(), e.prefetch.as_ref().unwrap().done.clone())
    };
    std::thread::sleep(Duration::from_millis(20));
    let t = Instant::now();
    db.close().unwrap();
    assert!(
        done.load(std::sync::atomic::Ordering::SeqCst),
        "prefetch still running"
    );
    let fetched = storage.lazy_gets();
    eprintln!(
        "prefetch: {fetched} of {pages} pages before close; close took {:?}",
        t.elapsed()
    );
    assert!(
        fetched < pages,
        "prefetch finished before close; the test proves nothing"
    );
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(storage.lazy_gets(), fetched, "prefetch fetched after close");
    drop(storage);

    let db = open_with_store(config(r.path()), store.clone()).unwrap();
    assert_eq!(ids(&db.connect().unwrap()), (1..=300).collect::<Vec<_>>());
}

/// A finished prefetch is recorded in the local state at once, not at the next seal: a later
/// process of the directory fetches nothing, even when no seal ever ran in between.
#[test]
fn finished_prefetch_persists_complete() {
    let store = mem();
    let _a = closed_writer(&store, 200);
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    let state = || LocalState::read(r.path()).unwrap().unwrap();
    assert!(!state().complete);

    let mut cfg = config(r.path());
    cfg.prefetch = true;
    // No connection: close runs no checkpoint, so nothing seals in this process.
    let db = open_with_store(cfg, store.clone()).unwrap();
    let (storage, done) = {
        let open = db.shared.open.lock().unwrap();
        let e = &open.as_ref().unwrap().engine;
        (e.storage.clone(), e.prefetch.as_ref().unwrap().done.clone())
    };
    let t = Instant::now();
    while !done.load(std::sync::atomic::Ordering::SeqCst) {
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "prefetch never ended"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(storage.is_complete());
    assert!(storage.lazy_gets() > 0);
    assert!(
        state().complete,
        "a finished prefetch is not in the local state"
    );
    let generation = state().generation;
    drop(storage);
    db.close().unwrap();
    assert_eq!(state().generation, generation, "nothing sealed");

    let db = open_with_store(config(r.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), (1..=200).collect::<Vec<_>>());
    let storage = {
        let open = db.shared.open.lock().unwrap();
        open.as_ref().unwrap().engine.storage.clone()
    };
    assert_eq!(storage.lazy_gets(), 0, "a complete directory fetched pages");
}

/// A read-only open never checkpoints, so after its prefetch `app.db` holds only the
/// manifest's pages; the generation's changes live in the local `-wal` alone. Its prefetch
/// must not persist `complete=true`: once the live writer seals, the next read-only open
/// replaces that `-wal` with the new generation (`NewGeneration`) and has to fetch the pages
/// the seal changed rather than trust `app.db`.
#[test]
fn read_only_prefetch_then_seal_reads_fresh_pages() {
    let store = mem();
    let a = tempdir();
    let db = open_mem(&store, a.path());
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=50 {
        insert(&c, id).unwrap();
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    // Unsealed updates in the current generation.
    c.execute("UPDATE t SET v = 'new'", &[]).unwrap();

    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    let mut cfg = config(r.path());
    cfg.prefetch = true;
    let ro = open_read_only_with_store(cfg, store.clone()).unwrap();
    let (storage, done) = {
        let open = ro.shared.open.lock().unwrap();
        let e = &open.as_ref().unwrap().engine;
        (e.storage.clone(), e.prefetch.as_ref().unwrap().done.clone())
    };
    let t = Instant::now();
    while !done.load(std::sync::atomic::Ordering::SeqCst) {
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "prefetch never ended"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(storage.is_complete(), "complete in memory for this open");
    drop(storage);
    let new_rows = |db: &Database| {
        let rc = db.connect().unwrap();
        let rows = rc
            .query("SELECT count(*) FROM t WHERE v = 'new'", &[])
            .unwrap();
        rc.close().unwrap();
        rows[0][0].as_int().unwrap()
    };
    assert_eq!(new_rows(&ro), 50);
    ro.close().unwrap();
    assert!(
        !state(r.path()).complete,
        "a read-only prefetch persisted complete"
    );

    // The live writer seals the generation.
    c.checkpoint(CheckpointMode::Truncate).unwrap();

    let ro = open_read_only_with_store(config(r.path()), store.clone()).unwrap();
    assert_eq!(ro.shared.reconciled, Some(Reconciled::NewGeneration));
    assert_eq!(new_rows(&ro), 50, "read-only reopen read stale pages");
    ro.close().unwrap();
    c.close().unwrap();
    db.close().unwrap();
}

/// `is_claimed` says whether a writer open would continue the directory's own database
/// (true) or restore, claim or create one (false): empty, missing, restored and never
/// opened, or mid-restore.
#[test]
fn is_claimed_only_for_a_writers_own_directory() {
    let store = mem();
    let t = tempdir();
    assert!(!crate::is_claimed(&fresh(&t)).unwrap(), "missing dir");
    assert!(!crate::is_claimed(t.path()).unwrap(), "empty dir");
    let a = closed_writer(&store, 3);
    assert!(crate::is_claimed(a.path()).unwrap(), "the writer's own dir");
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    assert!(
        !crate::is_claimed(r.path()).unwrap(),
        "restored, never opened"
    );
    std::fs::write(r.path().join("app.db-restoring"), b"").unwrap();
    assert!(
        !crate::is_claimed(r.path()).unwrap(),
        "an interrupted restore"
    );
    std::fs::remove_file(r.path().join("app.db-restoring")).unwrap();
    open_mem(&store, r.path()).close().unwrap();
    assert!(
        crate::is_claimed(r.path()).unwrap(),
        "claimed by its first open"
    );
}

/// Staging directories of a restore that crashed mid-way are swept at the next open.
#[test]
fn leftover_staging_dirs_are_swept() {
    let store = mem();
    let _a = closed_writer(&store, 3);
    let t = tempdir();
    let dir = fresh(&t);
    let leftover = t.path().join(".db.restore-123-456");
    std::fs::create_dir_all(&leftover).unwrap();
    std::fs::write(leftover.join("app.db-wal"), b"partial").unwrap();
    let other = t.path().join(".dbx.restore-1-2");
    std::fs::create_dir_all(&other).unwrap();
    let db = open_mem(&store, &dir);
    assert!(!leftover.exists(), "a staging dir was left behind");
    assert!(other.exists(), "another directory's staging dir was swept");
    assert_eq!(ids(&db.connect().unwrap()), vec![1, 2, 3]);
}

/// The read-only backstop below the Connection's own check: an engine connection that tries
/// to commit is refused by the IO layer, and nothing reaches the bucket.
#[test]
fn read_only_io_refuses_engine_writes() {
    let store = mem();
    let _a = closed_writer(&store, 5);
    let h0 = head(&*store);
    let puts = store.stats().puts;
    let v = tempdir();
    let db = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    let core = {
        let open = db.shared.open.lock().unwrap();
        open.as_ref().unwrap().engine.db.connect().unwrap()
    };
    crate::io::stash::clear();
    let r = core
        .prepare("INSERT INTO t(id, v) VALUES (6, 'x')")
        .and_then(|mut s| s.run_ignore_rows());
    assert!(r.is_err(), "the engine committed on a read-only open");
    assert_eq!(
        crate::io::stash::take(),
        Some(Error::Unsupported("writes on a read-only open"))
    );
    let rows = core
        .prepare("SELECT count(*) FROM t")
        .unwrap()
        .run_collect_rows();
    assert_eq!(rows.unwrap()[0][0].as_int(), Some(5));
    core.wal_auto_actions_disable();
    core.close().unwrap();
    drop(core);
    db.close().unwrap();
    assert_eq!(head(&*store), h0);
    assert_eq!(store.stats().puts, puts);
}
