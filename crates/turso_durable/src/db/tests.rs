use super::*;
use crate::engine::tests::{head, keys, log_seqs, PREFIX};
use crate::format::{Head, LocalState};
use crate::reconcile::Reconciled;
use crate::restore::restore_with_store;
use crate::store::{FaultStore, MemStore};
use crate::{CheckpointMode, Value};
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
/// reconcile takes it in (`Behind`), and its own commits follow it in the same generation.
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
    let seqs = log_seqs(&*store, gen);

    let db_b = open_mem(&store, r.path());
    assert_eq!(db_b.shared.reconciled, Some(Reconciled::Behind));
    let cb = db_b.connect().unwrap();
    assert_eq!(ids(&cb), (1..=11).collect::<Vec<_>>());
    insert(&cb, 12).unwrap();
    let after = log_seqs(&*store, gen);
    assert_eq!(after.len(), seqs.len() + 1);
    assert_eq!(after.last(), Some(&(seqs.len() as u64 + 1)));
    crash(db_a, vec![ca]);
    crash(db_b, vec![cb]);

    let v = tempdir();
    let verify = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    assert_eq!(
        ids(&verify.connect().unwrap()),
        (1..=12).collect::<Vec<_>>()
    );
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
