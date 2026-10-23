use super::*;
use crate::engine::tests::{head, keys, log_seqs, PREFIX};
use crate::format::LogFile;
use crate::format::{decode_record, Head, LocalState, LogRecord};
use crate::reconcile::Reconciled;
use crate::restore::restore_with_store;
use crate::store::{CondPut, FaultStore, MemStore};
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

/// `config` with a lease of `lease` and the smallest valid budget around it: retry deadline
/// and request timeout 100 ms, trim grace 1 s.
fn config_with_lease(dir: &Path, lease: Duration) -> Config {
    let mut c = config(dir);
    c.lease = lease;
    c.retry_deadline = Duration::from_millis(100);
    c.request_timeout = Duration::from_millis(100);
    c.trim_grace = Duration::from_secs(1);
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

const SWITCH_TO_MVCC: &str = "MVCC: switching an existing WAL database";
const SWITCH_BACK: &str = "MVCC: switching back to WAL";

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
        assert_eq!(unsupported(c.execute(sql, &[])), SWITCH_TO_MVCC, "{sql}");
        assert_eq!(unsupported(c.query(sql, &[])), SWITCH_TO_MVCC, "{sql}");
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
    assert_eq!(unsupported(r), SWITCH_TO_MVCC);
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

/// A `seal_quiet_ms` over a day in the local state is refused at open as `Corrupt`: the open
/// would succeed and its first seal would wait weeks (`u64::MAX`: practically for ever).
#[test]
fn open_refuses_absurd_seal_quiet_in_local_state() {
    for v in [30 * 86_400_000u64, u64::MAX] {
        let store = mem();
        let a = closed_writer(&store, 3);
        let mut s = state(a.path());
        s.seal_not_before_ms = 1;
        s.seal_quiet_ms = v;
        s.write_atomic(a.path()).unwrap();
        match open_with_store(config(a.path()), store.clone()) {
            Err(Error::Corrupt(m)) => assert!(m.contains("seal_quiet_ms"), "{m}"),
            Err(e) => panic!("seal_quiet_ms = {v}: {e}"),
            Ok(db) => {
                std::mem::forget(db);
                panic!("opened with seal_quiet_ms = {v}");
            }
        }
    }
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

/// The old writer acks a record between the restore and the claim: the claim's fence record
/// lands after it, the claimed writer's reconcile takes it in (`Behind`), then its own commits
/// follow; restore and reconcile both accept that log.
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
    assert_eq!(
        record(&store, gen, n + 1),
        LogRecord::Fence {
            writer_id: b_id.clone(),
            epoch: head(&*store).epoch
        },
        "the claim's fence record"
    );
    assert!(
        matches!(&record(&store, gen, n + 2), LogRecord::Extents { writer_id, .. } if *writer_id == b_id)
    );
    // The old writer's next append collides with the fence record.
    let r13 = insert(&ca, 13);
    assert!(matches!(r13, Err(Error::Fenced(_))), "{r13:?}");
    crash(db_a, vec![ca]);
    crash(db_b, vec![cb]);

    // Restore: [A's records, B's fence record, B's record].
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
/// seal-start record cannot collide with B's fence record.
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
/// `log/G`, which no restore reads, and the commit would be acked. With A's lease at zero,
/// A checks the head before that append and is fenced.
#[test]
fn idle_old_writer_appending_into_trimmed_log_is_fenced() {
    let store = mem();
    let a = tempdir();
    let cfg_a = config_with_lease(a.path(), Duration::ZERO);
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

/// A single writer that checks the head before every append and every page PUT (lease
/// zero) commits, seals and commits again as usual: its own head (and the one each seal
/// moves it to) lets every append and page PUT proceed.
#[test]
fn single_writer_with_lease_check_on_every_append_keeps_working() {
    let store = mem();
    let a = tempdir();
    let cfg = config_with_lease(a.path(), Duration::ZERO);
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

/// The lease, end to end: with `lease` = 100 ms a commit right after another makes no head
/// GET, and a commit after 120 ms of idle (past the lease) GETs the head before its log PUT.
#[test]
fn commit_after_the_lease_checks_the_head() {
    let mem = mem();
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let a = tempdir();
    let cfg = config_with_lease(a.path(), Duration::from_millis(100));
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

/// The claim's fence record (and any record appended before the engine opens) uses the
/// configured lease, not the built-in one: with `lease` = 0 the claimer GETs the head right
/// before it.
#[test]
fn claim_record_append_uses_the_configured_lease() {
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
    let cfg = config_with_lease(r.path(), Duration::ZERO);
    let db_b = open_with_store(cfg, fault.clone()).unwrap();
    let ev = fault.events();
    let claim_put = format!("put_if_absent {}", keys().log(gen, next));
    let at = ev
        .iter()
        .position(|e| *e == claim_put)
        .expect("the fence record");
    assert_eq!(ev[at - 1], format!("get {}", keys().head()), "{ev:?}");
    drop(db_b);
    crash(db_a, vec![ca]);
}

/// A claim whose fence record never landed (the open failed after the claimed state was
/// written): the next open sees the generation's last record is another writer's and appends
/// the fence then.
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
    let (h, etag) =
        crate::bucket::take_over(&*store, &keys(), &cfg.retry, s.generation, &b_id).unwrap();
    s.head_etag = etag;
    s.epoch = h.epoch;
    s.writer_id = b_id.clone();
    s.claimed = true;
    s.write_atomic(r.path()).unwrap();
    // No fence record yet: A still acks.
    insert(&ca, 11).unwrap();

    let db_b = open_mem(&store, r.path());
    assert_eq!(db_b.shared.reconciled, Some(Reconciled::Behind));
    let gen = head(&*store).generation;
    let last = *log_seqs(&*store, gen).last().unwrap();
    assert!(
        matches!(record(&store, gen, last), LogRecord::Fence { writer_id, .. } if writer_id == b_id)
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

/// The head's ETag right now.
fn head_etag(store: &Arc<MemStore>) -> String {
    store.get(&keys().head()).unwrap().unwrap().etag
}

/// Every id a fresh read-only open (a restore into a new directory) sees.
fn restored_ids(store: &Arc<MemStore>) -> Vec<i64> {
    let v = tempdir();
    let db = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    let ids = ids(&c);
    c.close().unwrap();
    db.close().unwrap();
    ids
}

/// A new database starts at epoch 1; each writer open raises it by one in the head and the
/// local state. A read-only open writes nothing and raises nothing.
#[test]
fn writer_open_raises_the_epoch() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    open_mem(&store, &dir).close().unwrap();
    assert_eq!((head(&*store).epoch, state(&dir).epoch), (1, 1));
    open_mem(&store, &dir).close().unwrap();
    assert_eq!((head(&*store).epoch, state(&dir).epoch), (2, 2));
    let puts = store.stats().puts;
    let db = open_read_only_with_store(config(&dir), store.clone()).unwrap();
    db.close().unwrap();
    assert_eq!((head(&*store).epoch, state(&dir).epoch), (2, 2));
    assert_eq!(store.stats().puts, puts, "a read-only open writes nothing");
}

/// README race 2: a head PUT that was in flight when the writer crashed (If-Match on the
/// head's ETag then) must not land after the writer restarted and acked more commits: the
/// head would publish the generation without them, so a restore (and the writer's own next
/// open, which takes the published generation in) would miss them. The restart's epoch raise
/// moves the ETag first. The late PUT is sent once the reopen returned (an open slower than
/// a fixed sleep would see it land before its own head read, which is the case reconcile
/// already handles) and runs alongside the new commits; it is joined before the close, whose
/// seal would otherwise move the head first and hide the race.
#[test]
fn late_head_put_after_restart_cannot_land() {
    let store = mem();
    let a = closed_writer(&store, 10);
    let e = head_etag(&store);
    let h = head(&*store);
    let late = Head {
        generation: h.generation + 1,
        manifest: Some(h.generation),
        ..h
    };
    // The crashed seal uploaded its manifest before its head PUT: the generation's pages
    // are unchanged since the last seal, so it maps them as that one did.
    let m = store
        .get(&keys().manifest(h.generation - 1))
        .unwrap()
        .unwrap();
    store.put(&keys().manifest(h.generation), &m.body).unwrap();
    let (s2, body) = (store.clone(), late.encode());
    let (opened, wait) = mpsc::channel::<()>();
    let put = std::thread::spawn(move || {
        wait.recv().unwrap();
        s2.put_if_match(&keys().head(), &body, &e)
    });
    let db = open_mem(&store, a.path());
    opened.send(()).unwrap();
    let c = db.connect().unwrap();
    for id in 11..=20 {
        insert(&c, id).unwrap();
    }
    let r = put.join().unwrap();
    assert!(matches!(r, Ok(CondPut::PreconditionFailed)), "{r:?}");
    assert_ne!(head(&*store), late);
    assert_eq!(restored_ids(&store), (1..=20).collect::<Vec<_>>());
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(restored_ids(&store), (1..=20).collect::<Vec<_>>());
}

/// A stage-2 database upgraded in place: a head without `epoch` and a local state without the
/// stage-3 keys open as before, and the open raises the epoch to 1.
#[test]
fn stage2_head_and_local_state_reopen() {
    let store = mem();
    let a = closed_writer(&store, 5);
    let h = head(&*store);
    let body = String::from_utf8(h.encode())
        .unwrap()
        .replace(&format!(",\"epoch\":{}", h.epoch), "");
    assert!(!body.contains("epoch"), "{body}");
    store.put(&keys().head(), body.as_bytes()).unwrap();
    let s = state(a.path());
    let stage2 = String::from_utf8(s.encode()).unwrap().replace(
        &format!(
            ",\"epoch\":{},\"claim_id\":\"\",\"seal_not_before_ms\":0,\"seal_quiet_ms\":0",
            s.epoch
        ),
        "",
    );
    assert!(!stage2.contains("epoch"), "{stage2}");
    std::fs::write(a.path().join(crate::files::STATE_FILE), stage2).unwrap();

    let db = open_mem(&store, a.path());
    assert_eq!((head(&*store).epoch, state(a.path()).epoch), (1, 1));
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), (1..=5).collect::<Vec<_>>());
    c.close().unwrap();
    db.close().unwrap();
}

/// A seal writes the new generation to the local state and keeps the keys it does not own:
/// the epoch and the claim id. The seal-not-before time belonged to the generation just
/// sealed, so it is cleared to 0.
#[test]
fn seal_keeps_the_epoch_and_claim_keys_in_the_local_state() {
    let store = mem();
    let a = closed_writer(&store, 1);
    let mut s = state(a.path());
    s.claim_id = "c".repeat(32);
    s.seal_not_before_ms = 12_345;
    s.write_atomic(a.path()).unwrap();

    let db = open_mem(&store, a.path());
    let gen = db.generation();
    let c = db.connect().unwrap();
    insert(&c, 2).unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    assert_eq!(db.generation(), gen + 1);
    let after = state(a.path());
    assert_eq!(after.generation, gen + 1);
    assert_eq!(
        (
            after.epoch,
            after.claim_id.as_str(),
            after.seal_not_before_ms
        ),
        (2, s.claim_id.as_str(), 0)
    );
    c.close().unwrap();
    db.close().unwrap();
}

/// The seal does not read the local state file: one that went bad while the database was open
/// cannot fail a seal whose head is already published. The seal writes it whole again.
#[test]
fn seal_does_not_read_the_local_state_file() {
    let store = mem();
    let a = closed_writer(&store, 1);
    let db = open_mem(&store, a.path());
    let gen = db.generation();
    let c = db.connect().unwrap();
    insert(&c, 2).unwrap();
    std::fs::write(a.path().join(crate::files::STATE_FILE), b"not json").unwrap();
    let r = c.checkpoint(CheckpointMode::Truncate);
    assert!(r.is_ok(), "{r:?}");
    assert_eq!(head(&*store).generation, gen + 1);
    assert_eq!(state(a.path()).generation, gen + 1);
    c.close().unwrap();
    db.close().unwrap();
}

/// README race 3: a claim that crashed after its head write and before the claimed local
/// state was written. The claim id was persisted first, so the next open resumes it: the head
/// already names that id, the epoch went up once, and the open ends with that writer's fence.
#[test]
fn crash_after_the_claim_head_resumes() {
    let store = mem();
    let a = tempdir();
    let db_a = open_mem(&store, a.path());
    let ca = db_a.connect().unwrap();
    create(&ca);
    for id in 1..=10 {
        insert(&ca, id).unwrap();
    }
    let e0 = head(&*store).epoch;
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();

    FAIL_AFTER_CLAIM_HEAD.with(|f| f.set(true));
    let first = open_with_store(config(r.path()), store.clone());
    FAIL_AFTER_CLAIM_HEAD.with(|f| f.set(false));
    assert!(matches!(first, Err(Error::Io(_))), "{:?}", first.err());
    let claim_id = state(r.path()).claim_id;
    assert!(
        !crate::is_claimed(r.path()).unwrap(),
        "an interrupted claim"
    );

    let db_b = open_with_store(config(r.path()), store.clone())
        .unwrap_or_else(|e| panic!("reopen after the crashed claim: {e:?}"));
    let h = head(&*store);
    assert!(!claim_id.is_empty(), "the claim id was persisted first");
    assert_eq!((h.writer_id.as_str(), h.epoch), (claim_id.as_str(), e0 + 1));
    let s = state(r.path());
    assert_eq!(
        (
            s.writer_id.as_str(),
            s.claimed,
            s.claim_id.as_str(),
            s.epoch
        ),
        (claim_id.as_str(), true, "", e0 + 1)
    );
    let last = *log_seqs(&*store, h.generation).last().unwrap();
    assert_eq!(
        record(&store, h.generation, last),
        LogRecord::Fence {
            writer_id: claim_id.clone(),
            epoch: e0 + 1
        }
    );
    let r11 = insert(&ca, 11);
    assert!(matches!(r11, Err(Error::Fenced(_))), "{r11:?}");
    let cb = db_b.connect().unwrap();
    assert_eq!(ids(&cb), (1..=10).collect::<Vec<_>>());
    insert(&cb, 100).unwrap();
    crash(db_a, vec![ca]);
    cb.close().unwrap();
    db_b.close().unwrap();
    let mut want: Vec<i64> = (1..=10).collect();
    want.push(100);
    assert_eq!(restored_ids(&store), want);
}

/// A MemStore that, at the first `put_if_match` of the head, first lands `late` (a head PUT
/// a crashed process still had in flight, If-Match on its ETag), then passes the call on.
struct LateHeadBeforeRaise {
    inner: Arc<MemStore>,
    late: Mutex<Option<(Vec<u8>, String)>>,
}

impl ObjectStore for LateHeadBeforeRaise {
    fn put(&self, key: &str, body: &[u8]) -> std::result::Result<String, crate::store::StoreError> {
        self.inner.put(key, body)
    }
    fn put_if_absent(
        &self,
        key: &str,
        body: &[u8],
    ) -> std::result::Result<CondPut, crate::store::StoreError> {
        self.inner.put_if_absent(key, body)
    }
    fn put_if_match(
        &self,
        key: &str,
        body: &[u8],
        etag: &str,
    ) -> std::result::Result<CondPut, crate::store::StoreError> {
        if key == keys().head() {
            if let Some((late, e)) = self.late.lock().unwrap().take() {
                let landed = self.inner.put_if_match(key, &late, &e).unwrap();
                assert!(matches!(landed, CondPut::Created { .. }), "{landed:?}");
            }
        }
        self.inner.put_if_match(key, body, etag)
    }
    fn get(
        &self,
        key: &str,
    ) -> std::result::Result<Option<crate::store::Object>, crate::store::StoreError> {
        self.inner.get(key)
    }
    fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, crate::store::StoreError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> std::result::Result<(), crate::store::StoreError> {
        self.inner.delete(key)
    }
    fn stat(
        &self,
        key: &str,
    ) -> std::result::Result<Option<crate::store::ObjectStat>, crate::store::StoreError> {
        self.inner.stat(key)
    }
    fn stats(&self) -> crate::store::StatsSnap {
        self.inner.stats()
    }
}

/// A crashed seal's head PUT (this writer's own sealed head of its generation) lands between
/// the open's head read and its epoch raise: the raise's 412 finds that head, and the open
/// attaches again (now at the sealed generation) instead of failing `Fenced`.
#[test]
fn open_attaches_again_when_its_own_late_seal_head_lands_before_the_raise() {
    let store = mem();
    let a = closed_writer(&store, 10);
    let h = head(&*store);
    let e = head_etag(&store);
    let late = Head {
        generation: h.generation + 1,
        manifest: Some(h.generation),
        ..h.clone()
    };
    // The crashed seal uploaded its manifest before its head PUT (as in
    // `late_head_put_after_restart_cannot_land`).
    let m = store
        .get(&keys().manifest(h.generation - 1))
        .unwrap()
        .unwrap();
    store.put(&keys().manifest(h.generation), &m.body).unwrap();
    let wrapped = Arc::new(LateHeadBeforeRaise {
        inner: store.clone(),
        late: Mutex::new(Some((late.encode(), e))),
    });
    let db = open_with_store(config(a.path()), wrapped.clone())
        .unwrap_or_else(|e| panic!("open with its own late seal head: {e:?}"));
    assert!(wrapped.late.lock().unwrap().is_none(), "the late PUT ran");
    let now = head(&*store);
    assert_eq!(
        (now.generation, now.epoch, now.writer_id.as_str()),
        (h.generation + 1, h.epoch + 1, h.writer_id.as_str())
    );
    assert_eq!(db.generation(), h.generation + 1);
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), (1..=10).collect::<Vec<_>>());
    insert(&c, 11).unwrap();
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(restored_ids(&store), (1..=11).collect::<Vec<_>>());
}

/// The two settings every fencing test runs under.
fn batchings() -> [crate::Batching; 2] {
    [
        crate::Batching::Off,
        crate::Batching::Window {
            max_wait: Duration::from_millis(5),
            max_commits: 64,
            max_bytes: 1 << 20,
        },
    ]
}

fn config_batching(dir: &Path, b: crate::Batching) -> Config {
    let mut c = config(dir);
    c.batching = b;
    c
}

/// The claim checks the restored generation, not the restore's ETag: the old writer acks
/// 51..=80 in the same generation after the restore (no seal), and the claim still goes
/// ahead. Its fence lands right after the old writer's last record, reconcile takes those
/// records in (`Behind`), and the old writer is fenced.
#[test]
fn old_writer_records_after_the_restore_are_taken_in() {
    for b in batchings() {
        let store = mem();
        let a = tempdir();
        let db_a = open_with_store(config_batching(a.path(), b), store.clone()).unwrap();
        let ca = db_a.connect().unwrap();
        create(&ca);
        for id in 1..=50 {
            insert(&ca, id).unwrap();
        }
        let r = tempdir();
        restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
        let gen = head(&*store).generation;
        for id in 51..=80 {
            insert(&ca, id).unwrap();
        }
        assert_eq!(head(&*store).generation, gen, "{b:?}: A did not seal");
        let a_last = *log_seqs(&*store, gen).last().unwrap();
        let a_id = state(a.path()).writer_id;

        let db_b = open_with_store(config_batching(r.path(), b), store.clone()).unwrap();
        assert_eq!(db_b.shared.reconciled, Some(Reconciled::Behind), "{b:?}");
        let b_id = state(r.path()).writer_id;
        assert!(
            matches!(record(&store, gen, a_last), LogRecord::Extents { writer_id, .. } if writer_id == a_id),
            "{b:?}: A's last record"
        );
        assert_eq!(
            record(&store, gen, a_last + 1),
            LogRecord::Fence {
                writer_id: b_id.clone(),
                epoch: head(&*store).epoch
            },
            "{b:?}: B's fence right after A's last record"
        );
        let r81 = insert(&ca, 81);
        assert!(matches!(r81, Err(Error::Fenced(_))), "{b:?}: {r81:?}");
        let cb = db_b.connect().unwrap();
        assert_eq!(ids(&cb), (1..=80).collect::<Vec<_>>(), "{b:?}");
        insert(&cb, 1001).unwrap();
        crash(db_a, vec![ca]);
        cb.close().unwrap();
        db_b.close().unwrap();
        let mut want: Vec<i64> = (1..=80).collect();
        want.push(1001);
        assert_eq!(restored_ids(&store), want, "{b:?}");
    }
}

/// The old writer seals after the restore, so the generation moved: the claim fails
/// "restore again" and writes nothing to the bucket (no head change, no fence).
#[test]
fn old_writer_seal_after_the_restore_makes_the_claim_restore_again() {
    for b in batchings() {
        let store = mem();
        let a = tempdir();
        let db_a = open_with_store(config_batching(a.path(), b), store.clone()).unwrap();
        let ca = db_a.connect().unwrap();
        create(&ca);
        for id in 1..=5 {
            insert(&ca, id).unwrap();
        }
        let r = tempdir();
        restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
        insert(&ca, 6).unwrap();
        ca.checkpoint(CheckpointMode::Truncate).unwrap();
        let before = bucket_objects(&store);

        let res = open_with_store(config_batching(r.path(), b), store.clone());
        match res {
            Err(Error::Fenced(m)) => assert!(m.contains("restore again"), "{b:?}: {m}"),
            res => panic!("{b:?}: expected Fenced, got {:?}", res.err()),
        }
        assert_eq!(
            bucket_objects(&store),
            before,
            "{b:?}: B wrote to the bucket"
        );
        assert!(!state(r.path()).claimed);
        insert(&ca, 7).unwrap();
        ca.close().unwrap();
        db_a.close().unwrap();
        assert_eq!(restored_ids(&store), (1..=7).collect::<Vec<_>>(), "{b:?}");
    }
}

/// Task 6's settings: lease 300 ms and request timeout 500 ms (a takeover quiet of 800 ms),
/// retry deadline 2 s, trim grace 10 s (above the 2.8 s they add up to), one page PUT at a
/// time, and `events`.
fn quiet_config(dir: &Path, events: Option<EventHook>) -> Config {
    let mut c = config(dir);
    c.lease = Duration::from_millis(300);
    c.request_timeout = Duration::from_millis(500);
    c.retry_deadline = Duration::from_secs(2);
    c.trim_grace = Duration::from_secs(10);
    c.upload_concurrency = 1;
    c.on_event = events;
    c
}

/// `lease + request_timeout` of `quiet_config`.
const QUIET: Duration = Duration::from_millis(800);

/// An event hook that keeps every event.
fn recorder() -> (EventHook, Arc<Mutex<Vec<Event>>>) {
    let seen: Arc<Mutex<Vec<Event>>> = Arc::default();
    let s = seen.clone();
    (
        Arc::new(move |e: &Event| s.lock().unwrap().push(e.clone())),
        seen,
    )
}

/// The `SealQuiet` events seen: (generation, wait).
fn seal_quiets(seen: &Mutex<Vec<Event>>) -> Vec<(u64, Duration)> {
    seen.lock()
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            Event::SealQuiet { generation, wait } => Some((*generation, *wait)),
            _ => None,
        })
        .collect()
}

/// Every key whose body differs between `a` and `b`, or that only one of them holds.
fn changed_keys(a: &BTreeMap<String, Vec<u8>>, b: &BTreeMap<String, Vec<u8>>) -> Vec<String> {
    let keys: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
    keys.into_iter()
        .filter(|k| a.get(*k) != b.get(*k))
        .cloned()
        .collect()
}

/// A writer that committed `1..=n` and closed, then its seal-start record (an empty Extents
/// record of its writer id) at the next seq of the head's generation: as if its next seal had
/// started and its process were gone with the page PUTs in flight.
fn writer_left_in_a_seal(store: &Arc<MemStore>, n: i64) -> TempDir {
    // With `quiet_config`'s timeouts (its head carries them): a takeover's quiet is the larger
    // of both writers' sums, and the takeovers here use `quiet_config` too.
    let a = tempdir();
    let db = open_with_store(quiet_config(a.path(), None), store.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=n {
        insert(&c, id).unwrap();
    }
    c.close().unwrap();
    db.close().unwrap();
    let gen = head(&**store).generation;
    let next = log_seqs(&**store, gen).last().map_or(1, |s| s + 1);
    let rec = LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: state(a.path()).writer_id,
        syncs: Vec::new(),
    };
    let r = store
        .put_if_absent(&keys().log(gen, next), &crate::format::encode_record(&rec))
        .unwrap();
    assert!(matches!(r, CondPut::Created { .. }), "{r:?}");
    a
}

/// A row of 1 KB.
fn insert_kb(c: &Connection, id: i64) {
    c.execute(
        "INSERT INTO t(id, v) VALUES (?, ?)",
        &[Value::from_i64(id), Value::build_text("x".repeat(1024))],
    )
    .unwrap();
}

/// The takeover quiet. Old writer A seals generation G with its page PUTs slowed to 400 ms
/// each (one at a time, lease 300 ms); on A's `SealStarted`, B takes G over (its fence lands
/// right after A's seal-start record), commits 1001..=1010 and seals G itself. A's page PUT
/// already in flight lands after B's seal began; without the quiet it overwrites B's
/// `pages/G/*`. With it, B's seal waits `lease + request_timeout` (800 ms) after the
/// takeover, so B's pages and manifest land last and stay as B wrote them. A is fenced at its
/// next lease check, and a restore holds both writers' rows.
#[test]
fn old_writer_page_puts_in_flight_cannot_overwrite_the_new_seal() {
    let store = mem();
    let fault = Arc::new(FaultStore::new(store.clone()));
    let (started_tx, started_rx) = mpsc::channel::<u64>();
    let started_tx = Mutex::new(started_tx);
    let a_hook: EventHook = Arc::new(move |e: &Event| {
        if let Event::SealStarted { generation, .. } = e {
            let _ = started_tx.lock().unwrap().send(*generation);
        }
    });
    let a = tempdir();
    let db_a = open_with_store(quiet_config(a.path(), Some(a_hook)), fault.clone()).unwrap();
    let ca = db_a.connect().unwrap();
    create(&ca);
    for id in 1..=30 {
        insert_kb(&ca, id);
    }
    let gen = db_a.generation();
    fault.only_keys_containing("/pages/");
    fault.delay_puts(Duration::from_millis(400));
    let a_seal = std::thread::spawn(move || {
        let r = ca.checkpoint(CheckpointMode::Truncate);
        (r, ca, db_a)
    });
    assert_eq!(
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
        gen,
        "A's seal started"
    );

    let bt = tempdir();
    let b_dir = fresh(&bt);
    let took = Instant::now();
    let rep = crate::takeover_with_store(quiet_config(&b_dir, None), store.clone()).unwrap();
    assert_eq!(rep.generation, gen);
    let (hook, seen) = recorder();
    let db_b = open_with_store(quiet_config(&b_dir, Some(hook)), store.clone()).unwrap();
    let cb = db_b.connect().unwrap();
    // B's rows grow the database, so B's page 1 (the header's page count) differs from A's.
    for id in 1001..=1010 {
        insert_kb(&cb, id);
    }
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    let sealed_after = took.elapsed();
    assert_eq!(head(&*store).generation, gen + 1, "B sealed G");
    let first = sealed_objects(&bucket_objects(&store), gen);
    assert!(!first.is_empty());

    let (a_result, ca, db_a) = a_seal.join().unwrap();
    std::thread::sleep(Duration::from_millis(600));
    let second = sealed_objects(&bucket_objects(&store), gen);
    let changed = changed_keys(&first, &second);
    assert!(
        changed.is_empty(),
        "A's page PUTs overwrote B's seal of generation {gen}: {changed:?} \
         (B's seal returned {sealed_after:?} after the takeover started)"
    );
    assert!(matches!(a_result, Err(Error::Fenced(_))), "{a_result:?}");
    assert_eq!(rep.quiet, QUIET, "{rep:?}");
    assert!(
        sealed_after >= QUIET,
        "B's seal returned {sealed_after:?} after the takeover started"
    );
    let quiets = seal_quiets(&seen);
    assert!(
        matches!(quiets[..], [(g, wait)] if g == gen && wait <= QUIET),
        "{quiets:?}"
    );
    crash(db_a, vec![ca]);
    cb.close().unwrap();
    db_b.close().unwrap();
    let mut want: Vec<i64> = (1..=30).collect();
    want.extend(1001..=1010);
    assert_eq!(restored_ids(&store), want);
}

/// The record before the fence is the old writer's commit, not a seal-start record: no quiet,
/// nothing persisted, and B's first seal does not wait.
#[test]
fn takeover_after_a_commit_has_no_quiet() {
    let store = mem();
    let a = tempdir();
    let db_a = open_with_store(quiet_config(a.path(), None), store.clone()).unwrap();
    let ca = db_a.connect().unwrap();
    create(&ca);
    for id in 1..=5 {
        insert(&ca, id).unwrap();
    }
    let bt = tempdir();
    let b_dir = fresh(&bt);
    let rep = crate::takeover_with_store(quiet_config(&b_dir, None), store.clone()).unwrap();
    assert_eq!(rep.quiet, Duration::ZERO, "{rep:?}");
    assert_eq!(state(&b_dir).seal_not_before_ms, 0);
    let (hook, seen) = recorder();
    let db_b = open_with_store(quiet_config(&b_dir, Some(hook)), store.clone()).unwrap();
    let cb = db_b.connect().unwrap();
    insert(&cb, 1001).unwrap();
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    assert_eq!(head(&*store).generation, rep.generation + 1);
    assert_eq!(seal_quiets(&seen), vec![]);
    crash(db_a, vec![ca]);
    cb.close().unwrap();
    db_b.close().unwrap();
}

/// The quiet's deadline is persisted as wall-clock unix ms, so a later open of the directory
/// keeps it; the seal of that generation clears it to 0.
#[test]
fn quiet_deadline_is_persisted() {
    let store = mem();
    let _a = writer_left_in_a_seal(&store, 5);
    let bt = tempdir();
    let b_dir = fresh(&bt);
    let rep = crate::takeover_with_store(quiet_config(&b_dir, None), store.clone()).unwrap();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert_eq!(rep.quiet, QUIET, "{rep:?}");
    let s = state(&b_dir);
    let want = now_ms + QUIET.as_millis() as u64;
    assert!(
        s.seal_not_before_ms.abs_diff(want) <= 100,
        "seal_not_before_ms {} is not within 100 ms of now + 800 ms ({want})",
        s.seal_not_before_ms
    );
    assert_eq!(s.seal_quiet_ms, QUIET.as_millis() as u64, "its length");
    let (hook, seen) = recorder();
    let db_b = open_with_store(quiet_config(&b_dir, Some(hook)), store.clone()).unwrap();
    let cb = db_b.connect().unwrap();
    insert(&cb, 1001).unwrap();
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    assert_eq!(seal_quiets(&seen).len(), 1, "the reopen kept the quiet");
    let s = state(&b_dir);
    assert_eq!(
        (s.seal_not_before_ms, s.seal_quiet_ms),
        (0, 0),
        "the seal cleared both"
    );
    cb.close().unwrap();
    db_b.close().unwrap();
    let mut want: Vec<i64> = (1..=5).collect();
    want.push(1001);
    assert_eq!(restored_ids(&store), want);
}

/// A claim that crashed after its fence landed, before the takeover quiet was persisted: the
/// claimed state already holds a provisional deadline, so the next open (whose last record is
/// now its own fence, so it appends no fence and decides no quiet) still waits out the quiet.
///
/// Only the wall clock decides, so the test does too: the deadline is at least `QUIET` after
/// the first open began, and the seal of the generation ends no earlier than it. A reopen that
/// lost the deadline would seal at once. Whether the seal had to wait (`SealQuiet`) depends on
/// how long the reopen took: under load the deadline may have passed already.
#[test]
fn crash_after_the_fence_keeps_the_quiet() {
    let now_ms = || crate::bucket::unix_ms(std::time::SystemTime::now());
    let store = mem();
    let _a = writer_left_in_a_seal(&store, 5);
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    let began = now_ms();
    FAIL_AFTER_CLAIM_FENCE.with(|f| f.set(true));
    let first = open_with_store(quiet_config(r.path(), None), store.clone());
    FAIL_AFTER_CLAIM_FENCE.with(|f| f.set(false));
    let ended = now_ms();
    assert!(matches!(first, Err(Error::Io(_))), "{:?}", first.err());
    let deadline = state(r.path()).seal_not_before_ms;
    let quiet = QUIET.as_millis() as u64;
    assert!(
        (began + quiet..=ended + quiet).contains(&deadline),
        "seal_not_before_ms {deadline} not within the open ({began}..={ended}) + {quiet} ms"
    );
    let (hook, seen) = recorder();
    let db = open_with_store(quiet_config(r.path(), Some(hook)), store.clone()).unwrap();
    let c = db.connect().unwrap();
    insert(&c, 1001).unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    let sealed = now_ms();
    let gen = head(&*store).generation - 1;
    assert!(
        sealed >= deadline,
        "the seal of {gen} ended at {sealed}, before the quiet's end {deadline}"
    );
    let quiets = seal_quiets(&seen);
    assert!(
        quiets.iter().all(|&(g, _)| g == gen) && quiets.len() <= 1,
        "{quiets:?}"
    );
    c.close().unwrap();
    db.close().unwrap();
}

/// Old writer A seals G (lease 300 ms, one page PUT at a time) and its next PUT of a key
/// containing `held` is held after its lease check, as if A were paused (SIGSTOP) right before
/// the send. B takes over (its fence lands after A's seal-start record, so B's seal waits out
/// the 800 ms quiet), commits 1001..=1010 and seals G, publishing its head. Then A's PUT goes
/// on: it fails its condition, B's `pages/G/*` and `manifest/G` stay byte-identical, A is
/// fenced, and a restore holds both writers' rows.
fn stale_writers_late_put_after_the_new_seal(batching: crate::Batching, held: &str) {
    let store = mem();
    let hold = Arc::new(crate::store::fault::HoldStore::new(store.clone()));
    let a = tempdir();
    let mut a_cfg = quiet_config(a.path(), None);
    a_cfg.batching = batching;
    let db_a = open_with_store(a_cfg, hold.clone()).unwrap();
    let ca = db_a.connect().unwrap();
    create(&ca);
    for id in 1..=30 {
        insert_kb(&ca, id);
    }
    let gen = db_a.generation();
    let (held_rx, go) = hold.hold_next_put(held);
    let a_seal = std::thread::spawn(move || {
        let r = ca.checkpoint(CheckpointMode::Truncate);
        (r, ca, db_a)
    });
    let key = held_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("A's PUT is held");

    let bt = tempdir();
    let b_dir = fresh(&bt);
    let mut b_cfg = quiet_config(&b_dir, None);
    b_cfg.batching = batching;
    let rep = crate::takeover_with_store(b_cfg.clone(), store.clone()).unwrap();
    assert_eq!((rep.generation, rep.quiet), (gen, QUIET), "{rep:?}");
    let db_b = open_with_store(b_cfg, store.clone()).unwrap();
    let cb = db_b.connect().unwrap();
    for id in 1001..=1010 {
        insert_kb(&cb, id);
    }
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    assert_eq!(head(&*store).generation, gen + 1, "B sealed G");
    let published = sealed_objects(&bucket_objects(&store), gen);
    assert!(!published.is_empty());

    go.send(()).unwrap();
    let (a_result, ca, db_a) = a_seal.join().unwrap();
    let after = sealed_objects(&bucket_objects(&store), gen);
    let changed = changed_keys(&published, &after);
    assert!(
        changed.is_empty(),
        "{batching:?}: A's late PUT of {key} overwrote B's seal of generation {gen}: \
         {changed:?}; held PUT: {:?}",
        hold.outcomes()
    );
    let outcomes = hold.outcomes();
    assert!(
        matches!(&outcomes[..], [o] if o.ends_with(": 412")),
        "{batching:?}: A's held PUT must fail its condition: {outcomes:?}"
    );
    assert!(
        matches!(a_result, Err(Error::Fenced(_))),
        "{batching:?}: {a_result:?}"
    );
    crash(db_a, vec![ca]);
    cb.close().unwrap();
    db_b.close().unwrap();
    let mut want: Vec<i64> = (1..=30).collect();
    want.extend(1001..=1010);
    assert_eq!(restored_ids(&store), want, "{batching:?}");
}

/// A stale writer's late page PUT, under `Off` and a `Window`.
#[test]
fn a_stale_writers_late_page_put_cannot_overwrite_a_published_seal() {
    for b in batchings() {
        stale_writers_late_put_after_the_new_seal(b, "/pages/");
    }
}

/// A stale writer's late manifest PUT (its pages went out), under `Off` and a `Window`.
#[test]
fn a_stale_writers_late_manifest_put_cannot_overwrite_a_published_seal() {
    for b in batchings() {
        stale_writers_late_put_after_the_new_seal(b, "/manifest/");
    }
}

/// The new writer B crashes mid-seal: its page PUTs of G landed, its manifest PUT failed, and
/// its process is gone. B's next process (its files copied to a new directory) has none of
/// those ETags: each leftover page is foreign to it, so its seal of G stats it and overwrites
/// it with `put_if_match` on the ETag it read. A restore holds every row.
#[test]
fn a_crash_of_the_new_writer_mid_seal_resumes_over_its_own_leftovers() {
    let store = mem();
    let _a = closed_writer(&store, 10);
    let bt = tempdir();
    let b_dir = fresh(&bt);
    crate::takeover_with_store(config(&b_dir), store.clone()).unwrap();
    let fault = Arc::new(FaultStore::new(store.clone()));
    let db_b = open_with_store(config(&b_dir), fault.clone()).unwrap();
    let cb = db_b.connect().unwrap();
    for id in 1001..=1010 {
        insert(&cb, id).unwrap();
    }
    let gen = db_b.generation();
    fault.only_keys_containing("/manifest/");
    fault.fail_next_puts_permanently(1);
    let r = cb.checkpoint(CheckpointMode::Truncate);
    assert!(r.is_err(), "{r:?}");
    let k = keys();
    let leftovers = store.list(&format!("{}{gen:010}/", k.page_root())).unwrap();
    assert!(!leftovers.is_empty(), "B's pages of {gen} landed");
    assert!(store.get(&k.manifest(gen)).unwrap().is_none());
    assert_eq!(head(&*store).generation, gen);
    crash(db_b, vec![cb]);

    let b2 = copy_db_files(&b_dir);
    let before = fault.events().len();
    let db_b = open_with_store(config(b2.path()), fault.clone()).unwrap();
    let cb = db_b.connect().unwrap();
    insert(&cb, 1011).unwrap();
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    assert!(head(&*store).generation > gen);
    let events = fault.events()[before..].to_vec();
    let mut resumed = 0;
    for key in &leftovers {
        let on_key: Vec<&str> = events
            .iter()
            .filter_map(|e| e.strip_suffix(key.as_str()))
            .map(str::trim_end)
            .collect();
        if on_key.is_empty() {
            continue;
        }
        assert_eq!(
            on_key,
            ["put_if_absent", "stat", "put_if_match"],
            "{key}: {events:?}"
        );
        resumed += 1;
    }
    assert!(
        resumed > 0,
        "the reopen sealed G over its leftovers: {events:?}"
    );
    cb.close().unwrap();
    db_b.close().unwrap();
    let mut want: Vec<i64> = (1..=10).collect();
    want.extend(1001..=1011);
    assert_eq!(restored_ids(&store), want);
}

/// C1's settings: `quiet_config` with a lease of 2 s, so a quiet of 2.5 s. A writer held
/// after its lease check (the old writer A below) is then still inside its lease when the
/// later writers have taken over and sealed, unless the last of them waits out the quiet.
fn chain_config(dir: &Path, b: crate::Batching, events: Option<EventHook>) -> Config {
    let mut c = quiet_config(dir, events);
    c.lease = Duration::from_secs(2);
    c.batching = b;
    c
}

/// `lease + request_timeout` of `chain_config`.
const CHAIN_QUIET: Duration = Duration::from_millis(2500);

/// C1: a chained takeover over a writer left mid-seal. Old writer A seals G and its first
/// page PUT is held after its lease check, as if A were paused right before the send. B takes
/// G over (its fence follows A's seal-start record, so B gets the quiet); with `b_commits`,
/// B opens, commits 2001..=2003 and crashes without sealing. Then C takes G over: the record
/// just before C's fence is B's, but A's last record is still its seal-start record, so A's
/// page PUTs may still be in flight and C gets the quiet too. C commits 3001..=3010 and seals
/// G, waiting the quiet out. Then A's PUT goes on: it fails its condition, A's next request
/// is past its lease, and the head check fences A. C's `pages/G/*` and `manifest/G` stay
/// byte-identical, and a restore holds every writer's rows.
///
/// Without the quiet, C seals at once and A, still inside its lease, stats C's page and
/// overwrites it with `put_if_match` on the ETag it read.
fn chained_takeover_over_a_writer_mid_seal(batching: crate::Batching, b_commits: bool) {
    let store = mem();
    let hold = Arc::new(crate::store::fault::HoldStore::new(store.clone()));
    let a = tempdir();
    let db_a = open_with_store(chain_config(a.path(), batching, None), hold.clone()).unwrap();
    let ca = db_a.connect().unwrap();
    create(&ca);
    for id in 1..=30 {
        insert_kb(&ca, id);
    }
    let gen = db_a.generation();
    let (held_rx, go) = hold.hold_next_put("/pages/");
    let a_seal = std::thread::spawn(move || {
        let r = ca.checkpoint(CheckpointMode::Truncate);
        (r, ca, db_a)
    });
    let key = held_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("A's page PUT is held");

    let bt = tempdir();
    let b_dir = fresh(&bt);
    let rep_b =
        crate::takeover_with_store(chain_config(&b_dir, batching, None), store.clone()).unwrap();
    assert_eq!(
        (rep_b.generation, rep_b.quiet),
        (gen, CHAIN_QUIET),
        "{rep_b:?}"
    );
    if b_commits {
        let db_b = open_with_store(chain_config(&b_dir, batching, None), store.clone()).unwrap();
        let cb = db_b.connect().unwrap();
        for id in 2001..=2003 {
            insert_kb(&cb, id);
        }
        crash(db_b, vec![cb]);
    }

    let ct = tempdir();
    let c_dir = fresh(&ct);
    let took = Instant::now();
    let rep_c =
        crate::takeover_with_store(chain_config(&c_dir, batching, None), store.clone()).unwrap();
    let (hook, seen) = recorder();
    let db_c = open_with_store(chain_config(&c_dir, batching, Some(hook)), store.clone()).unwrap();
    let cc = db_c.connect().unwrap();
    for id in 3001..=3010 {
        insert_kb(&cc, id);
    }
    cc.checkpoint(CheckpointMode::Truncate).unwrap();
    let sealed_after = took.elapsed();
    assert_eq!(head(&*store).generation, gen + 1, "C sealed G");
    let published = sealed_objects(&bucket_objects(&store), gen);
    assert!(!published.is_empty());

    go.send(()).unwrap();
    let (a_result, ca, db_a) = a_seal.join().unwrap();
    let after = sealed_objects(&bucket_objects(&store), gen);
    let changed = changed_keys(&published, &after);
    let tag = format!("{batching:?}, B commits: {b_commits}");
    assert!(
        changed.is_empty(),
        "{tag}: A's late PUT of {key} overwrote C's seal of generation {gen}: {changed:?}; \
         C: epoch {} fence_seq {} quiet {:?}, sealed {sealed_after:?} after its takeover",
        rep_c.epoch,
        rep_c.fence_seq,
        rep_c.quiet
    );
    let outcomes = hold.outcomes();
    assert!(
        matches!(&outcomes[..], [o] if o.ends_with(": 412")),
        "{tag}: A's held PUT must fail its condition: {outcomes:?}"
    );
    assert!(
        matches!(a_result, Err(Error::Fenced(_))),
        "{tag}: {a_result:?}"
    );
    assert_eq!(rep_c.quiet, CHAIN_QUIET, "{tag}: {rep_c:?}");
    assert!(
        sealed_after >= CHAIN_QUIET,
        "{tag}: C's seal returned {sealed_after:?} after its takeover started"
    );
    let quiets = seal_quiets(&seen);
    assert!(
        matches!(quiets[..], [(g, wait)] if g == gen && wait <= CHAIN_QUIET),
        "{tag}: {quiets:?}"
    );
    crash(db_a, vec![ca]);
    cc.close().unwrap();
    db_c.close().unwrap();
    let mut want: Vec<i64> = (1..=30).collect();
    if b_commits {
        want.extend(2001..=2003);
    }
    want.extend(3001..=3010);
    assert_eq!(restored_ids(&store), want, "{tag}");
}

/// C1, B → C over A mid-seal, B committing nothing, under `Off` and a `Window`.
#[test]
fn a_chained_takeover_over_a_writer_mid_seal_keeps_the_quiet() {
    for b in batchings() {
        chained_takeover_over_a_writer_mid_seal(b, false);
    }
}

/// C1, B → C over A mid-seal with B's commits between the two fences, under `Off` and a
/// `Window`: A's seal-start record sits several records before C's fence.
#[test]
fn a_chained_takeover_after_the_middle_writer_committed_keeps_the_quiet() {
    for b in batchings() {
        chained_takeover_over_a_writer_mid_seal(b, true);
    }
}

/// I1 and M2, and B1: a reopen waits the quiet's persisted length (800 ms) from the open, on
/// the monotonic clock, never a wall-clock remainder. A persisted deadline a day ahead (a
/// wall clock stepped back after the takeover, or set wrong) makes the reopen's first seal
/// wait at most that length, not the day, and the deadline is persisted again at most that
/// length from now. Maintenance's view (`Uploader::seal_quiet_remaining`) agrees with the
/// seal's wait.
#[test]
fn a_persisted_quiet_far_ahead_is_capped_at_reopen() {
    let store = mem();
    let _a = writer_left_in_a_seal(&store, 5);
    let bt = tempdir();
    let b_dir = fresh(&bt);
    let rep = crate::takeover_with_store(quiet_config(&b_dir, None), store.clone()).unwrap();
    assert_eq!(rep.quiet, QUIET, "{rep:?}");
    let day = Duration::from_secs(86_400);
    let mut s = state(&b_dir);
    s.seal_not_before_ms = crate::bucket::unix_ms(std::time::SystemTime::now() + day);
    s.write_atomic(&b_dir).unwrap();

    let (tx, rx) = mpsc::channel::<Duration>();
    let tx = Mutex::new(tx);
    let hook: EventHook = Arc::new(move |e: &Event| {
        if let Event::SealQuiet { wait, .. } = e {
            let _ = tx.lock().unwrap().send(*wait);
        }
    });
    let db_b = open_with_store(quiet_config(&b_dir, Some(hook)), store.clone()).unwrap();
    let viewed = db_b.shared.uploader.seal_quiet_remaining();
    let cap = crate::bucket::unix_ms(std::time::SystemTime::now() + QUIET);
    let persisted = state(&b_dir);
    assert!(
        persisted.seal_not_before_ms <= cap,
        "persisted {}, not capped at {cap}",
        persisted.seal_not_before_ms
    );
    assert_eq!(persisted.seal_quiet_ms, QUIET.as_millis() as u64);
    let cb = db_b.connect().unwrap();
    insert(&cb, 1001).unwrap();
    let began = Instant::now();
    let seal = std::thread::spawn(move || {
        let r = cb.checkpoint(CheckpointMode::Truncate);
        (r, cb, db_b)
    });
    let wait = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the seal waits out a quiet");
    let margin = Duration::from_millis(200);
    assert!(
        wait <= QUIET,
        "the reopen's seal waits {wait:?} (maintenance saw {viewed:?} left), past the quiet \
         {QUIET:?}"
    );
    assert!(
        viewed <= QUIET && viewed + margin >= wait && wait <= viewed,
        "maintenance saw {viewed:?} left, the seal waits {wait:?}"
    );
    let (r, cb, db_b) = seal.join().unwrap();
    r.unwrap();
    assert!(
        began.elapsed() <= QUIET + Duration::from_secs(2),
        "{:?}",
        began.elapsed()
    );
    let s = state(&b_dir);
    assert_eq!(
        (s.seal_not_before_ms, s.seal_quiet_ms),
        (0, 0),
        "the seal cleared both"
    );
    cb.close().unwrap();
    db_b.close().unwrap();
    let mut want: Vec<i64> = (1..=5).collect();
    want.push(1001);
    assert_eq!(restored_ids(&store), want);
}

/// B1: the wall clock steps forward between `takeover` and the `open` that follows (a new
/// process, as with the CLI; or a crash and reopen during the quiet). The step is simulated
/// by moving the persisted deadline to "now", a forward step of the whole quiet. Old writer A
/// is mid-seal with a page PUT held after its lease check, inside its 2 s lease. B's reopen
/// must still wait the full persisted length (2.5 s) from the open, on the monotonic clock,
/// so A's held PUT, released after B's seal, fails its condition: B's `pages/G/*` and
/// `manifest/G` stay byte-identical, A is fenced, and a restore holds both writers' rows.
fn forward_wall_step_between_takeover_and_open(batching: crate::Batching) {
    let store = mem();
    let hold = Arc::new(crate::store::fault::HoldStore::new(store.clone()));
    let a = tempdir();
    let db_a = open_with_store(chain_config(a.path(), batching, None), hold.clone()).unwrap();
    let ca = db_a.connect().unwrap();
    create(&ca);
    for id in 1..=30 {
        insert_kb(&ca, id);
    }
    let gen = db_a.generation();
    let (held_rx, go) = hold.hold_next_put("/pages/");
    let a_seal = std::thread::spawn(move || {
        let r = ca.checkpoint(CheckpointMode::Truncate);
        (r, ca, db_a)
    });
    let key = held_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("A's page PUT is held");

    let bt = tempdir();
    let b_dir = fresh(&bt);
    let rep_b =
        crate::takeover_with_store(chain_config(&b_dir, batching, None), store.clone()).unwrap();
    assert_eq!(rep_b.quiet, CHAIN_QUIET, "{rep_b:?}");
    let mut s = state(&b_dir);
    assert_eq!(
        s.seal_quiet_ms,
        CHAIN_QUIET.as_millis() as u64,
        "{batching:?}"
    );
    s.seal_not_before_ms = crate::bucket::unix_ms(std::time::SystemTime::now());
    s.write_atomic(&b_dir).unwrap();

    let took = Instant::now();
    let db_b = open_with_store(chain_config(&b_dir, batching, None), store.clone()).unwrap();
    let cb = db_b.connect().unwrap();
    for id in 3001..=3010 {
        insert_kb(&cb, id);
    }
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    let sealed_after = took.elapsed();
    assert_eq!(head(&*store).generation, gen + 1, "B sealed G");
    let published = sealed_objects(&bucket_objects(&store), gen);
    assert!(!published.is_empty());

    go.send(()).unwrap();
    let (a_result, ca, db_a) = a_seal.join().unwrap();
    let after = sealed_objects(&bucket_objects(&store), gen);
    let changed = changed_keys(&published, &after);
    let outcomes = hold.outcomes();
    eprintln!(
        "{batching:?}: B sealed {sealed_after:?} after open; A's held {key}: {outcomes:?}; \
         changed keys of B's published seal: {changed:?}"
    );
    assert!(
        sealed_after >= CHAIN_QUIET,
        "{batching:?}: B's seal returned {sealed_after:?} after the open, before the \
         persisted quiet {CHAIN_QUIET:?}"
    );
    assert!(
        changed.is_empty(),
        "{batching:?}: A's late PUT of {key} overwrote B's seal of generation {gen}: {changed:?}"
    );
    assert!(
        matches!(&outcomes[..], [o] if o.ends_with(": 412")),
        "{batching:?}: A's held PUT must fail its condition: {outcomes:?}"
    );
    assert!(
        matches!(a_result, Err(Error::Fenced(_))),
        "{batching:?}: {a_result:?}"
    );
    crash(db_a, vec![ca]);
    cb.close().unwrap();
    db_b.close().unwrap();
    let mut want: Vec<i64> = (1..=30).collect();
    want.extend(3001..=3010);
    assert_eq!(restored_ids(&store), want, "{batching:?}");
}

/// B1 under `Off` and a `Window`.
#[test]
fn a_forward_wall_step_between_takeover_and_open_keeps_the_quiet() {
    for b in batchings() {
        forward_wall_step_between_takeover_and_open(b);
    }
}

/// B1: the quiet's length is persisted with it, so a reopen configured with a shorter lease
/// (quiet 800 ms) than the takeover's (2.5 s) still waits the persisted 2.5 s from the open.
#[test]
fn a_reopen_with_a_shorter_lease_waits_the_persisted_quiet() {
    let store = mem();
    let _a = writer_left_in_a_seal(&store, 5);
    let bt = tempdir();
    let b_dir = fresh(&bt);
    let rep = crate::takeover_with_store(
        chain_config(&b_dir, crate::Batching::Off, None),
        store.clone(),
    )
    .unwrap();
    assert_eq!(rep.quiet, CHAIN_QUIET, "{rep:?}");
    assert_eq!(state(&b_dir).seal_quiet_ms, CHAIN_QUIET.as_millis() as u64);
    let (hook, seen) = recorder();
    let took = Instant::now();
    let db_b = open_with_store(quiet_config(&b_dir, Some(hook)), store.clone()).unwrap();
    let cb = db_b.connect().unwrap();
    insert(&cb, 1001).unwrap();
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    let sealed_after = took.elapsed();
    let quiets = seal_quiets(&seen);
    eprintln!("shorter lease: sealed {sealed_after:?} after open; waits {quiets:?}");
    assert!(
        sealed_after >= CHAIN_QUIET,
        "the reopen's seal returned {sealed_after:?} after the open, before the persisted \
         quiet {CHAIN_QUIET:?} (waits {quiets:?})"
    );
    assert!(
        matches!(quiets[..], [(_, wait)] if wait > QUIET && wait <= CHAIN_QUIET),
        "{quiets:?}"
    );
    let s = state(&b_dir);
    assert_eq!(
        (s.seal_not_before_ms, s.seal_quiet_ms),
        (0, 0),
        "the seal cleared both"
    );
    cb.close().unwrap();
    db_b.close().unwrap();
    let mut want: Vec<i64> = (1..=5).collect();
    want.push(1001);
    assert_eq!(restored_ids(&store), want);
}

/// B1: a pending quiet persisted before `seal_quiet_ms` existed (deadline set, no length)
/// reads as the reopening process's `lease + request_timeout`, waited in full from the open,
/// even with the deadline already past on the wall clock.
#[test]
fn a_pending_quiet_without_a_length_waits_the_reopens_quiet() {
    let store = mem();
    let _a = writer_left_in_a_seal(&store, 5);
    let bt = tempdir();
    let b_dir = fresh(&bt);
    let rep = crate::takeover_with_store(quiet_config(&b_dir, None), store.clone()).unwrap();
    assert_eq!(rep.quiet, QUIET, "{rep:?}");
    let mut s = state(&b_dir);
    s.seal_not_before_ms = crate::bucket::unix_ms(std::time::SystemTime::now());
    s.seal_quiet_ms = 0;
    s.write_atomic(&b_dir).unwrap();
    let (hook, seen) = recorder();
    let took = Instant::now();
    let db_b = open_with_store(quiet_config(&b_dir, Some(hook)), store.clone()).unwrap();
    assert_eq!(state(&b_dir).seal_quiet_ms, QUIET.as_millis() as u64);
    let cb = db_b.connect().unwrap();
    insert(&cb, 1001).unwrap();
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    let sealed_after = took.elapsed();
    assert!(
        sealed_after >= QUIET,
        "sealed {sealed_after:?} after the open"
    );
    assert!(
        matches!(seal_quiets(&seen)[..], [(_, wait)] if wait <= QUIET),
        "{:?}",
        seal_quiets(&seen)
    );
    cb.close().unwrap();
    db_b.close().unwrap();
}

// Encryption (D16).

/// The test key: 32 bytes of 0x11, as hex.
fn key_hex() -> String {
    "11".repeat(32)
}

/// `config(dir)` with encryption on: `hex` written to `keydir/key`.
fn enc_config(dir: &Path, keydir: &TempDir, hex: &str) -> Config {
    let path = keydir.path().join("key");
    std::fs::write(&path, hex).unwrap();
    let mut c = config(dir);
    c.encryption_key_file = Some(path);
    c
}

fn encryption_err<T: std::fmt::Debug>(r: Result<T>) -> String {
    match r {
        Err(Error::Encryption(m)) => m,
        r => panic!("expected Encryption, got {r:?}"),
    }
}

/// An encrypted writer, closed: rows 1..=50 from two connections, a TRUNCATE checkpoint (a
/// seal), then rows 51..=60. Returns its directory.
fn encrypted_writer(store: &Arc<MemStore>, keydir: &TempDir) -> TempDir {
    let a = tempdir();
    let db = open_with_store(enc_config(a.path(), keydir, &key_hex()), store.clone()).unwrap();
    let (c1, c2) = (db.connect().unwrap(), db.connect().unwrap());
    create(&c1);
    for id in 1..=50 {
        insert(if id % 2 == 0 { &c1 } else { &c2 }, id).unwrap();
    }
    let gen = db.generation();
    c1.checkpoint(CheckpointMode::Truncate).unwrap();
    assert!(db.generation() > gen, "the checkpoint sealed");
    for id in 51..=60 {
        insert(&c2, id).unwrap();
    }
    c1.close().unwrap();
    c2.close().unwrap();
    db.close().unwrap();
    a
}

#[test]
fn encrypted_open_commit_reopen_keeps_rows() {
    let store = mem();
    let k = tempdir();
    let a = encrypted_writer(&store, &k);
    let page1 = std::fs::read(a.path().join("app.db")).unwrap();
    assert!(page1.starts_with(b"Turso"), "app.db is not encrypted");
    let db = open_with_store(enc_config(a.path(), &k, &key_hex()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), (1..=60).collect::<Vec<_>>());
    integrity_ok(&c);
}

#[test]
fn encrypted_restore_fetches_pages_lazily_and_decrypts() {
    let store = mem();
    let k = tempdir();
    let _a = encrypted_writer(&store, &k);
    for prefetch in [false, true] {
        let r = tempdir();
        let mut cfg = enc_config(&fresh(&r), &k, &key_hex());
        cfg.prefetch = prefetch;
        let db = open_with_store(cfg, store.clone()).unwrap();
        let c = db.connect().unwrap();
        assert_eq!(ids(&c), (1..=60).collect::<Vec<_>>(), "prefetch {prefetch}");
        integrity_ok(&c);
        let lazy = {
            let open = db.shared.open.lock().unwrap();
            open.as_ref().unwrap().engine.storage.lazy_gets()
        };
        assert!(
            lazy > 0,
            "prefetch {prefetch}: no page came from the bucket"
        );
        c.close().unwrap();
        db.close().unwrap();
    }
}

const MARKER: &str = "buckwal-plaintext-marker";

/// A key with letters, so its lower- and upper-case hex differ.
fn mixed_key_hex() -> String {
    "a1".repeat(32)
}

/// The bucket workload: a table and an index, 200 marked rows (half under Off, half under a
/// window), a TRUNCATE checkpoint, a takeover into a second directory that commits 10 rows,
/// then GC.
fn bucket_workload(store: &Arc<MemStore>, keydir: Option<&TempDir>) {
    let cfg = |dir: &Path| match keydir {
        Some(k) => enc_config(dir, k, &mixed_key_hex()),
        None => config(dir),
    };
    let a = tempdir();
    let db = open_with_store(cfg(a.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    c.execute("CREATE INDEX t_v ON t(v)", &[]).unwrap();
    for id in 1..=200 {
        if id == 101 {
            c.set_batching(Some("window 5ms 8 1MiB".parse().unwrap()))
                .unwrap();
        }
        c.execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            &[
                Value::from_i64(id),
                Value::build_text(format!("{MARKER}-{id}")),
            ],
        )
        .unwrap();
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    c.close().unwrap();
    db.close().unwrap();

    let b = tempdir();
    let b_dir = fresh(&b);
    crate::takeover_with_store(cfg(&b_dir), store.clone()).unwrap();
    let db = open_with_store(cfg(&b_dir), store.clone()).unwrap();
    let c = db.connect().unwrap();
    for id in 201..=210 {
        insert(&c, id).unwrap();
    }
    assert_eq!(ids(&c), (1..=210).collect::<Vec<_>>());
    c.close().unwrap();
    db.close().unwrap();
    crate::gc_with_store(&cfg(&b_dir), store.clone()).unwrap();
}

/// Which of the needles some object in the bucket holds (GETting every key).
fn needles_found(store: &Arc<MemStore>) -> BTreeSet<&'static str> {
    let hex = mixed_key_hex();
    let needles: [(&str, Vec<u8>); 6] = [
        ("marker", MARKER.into()),
        ("CREATE TABLE", "CREATE TABLE".into()),
        ("SQLite header", b"SQLite format 3\0".to_vec()),
        ("key hex", hex.clone().into_bytes()),
        ("KEY HEX", hex.to_uppercase().into_bytes()),
        ("raw key", vec![0xa1; 32]),
    ];
    let keys = store.list("").unwrap();
    assert!(keys.len() > 5, "{keys:?}");
    let mut found = BTreeSet::new();
    for key in keys {
        let body = store.get(&key).unwrap().unwrap().body;
        for (name, needle) in &needles {
            if body.windows(needle.len()).any(|w| w == &needle[..]) {
                found.insert(*name);
            }
        }
    }
    found
}

#[test]
fn encrypted_bucket_has_no_plaintext() {
    let store = mem();
    let k = tempdir();
    bucket_workload(&store, Some(&k));
    assert_eq!(needles_found(&store), BTreeSet::new());
}

/// The control of `encrypted_bucket_has_no_plaintext`: without encryption the same search
/// finds the rows and the schema.
#[test]
fn plaintext_bucket_shows_the_schema() {
    let store = mem();
    bucket_workload(&store, None);
    let found = needles_found(&store);
    assert!(
        found.contains("marker") && found.contains("CREATE TABLE"),
        "{found:?}"
    );
}

#[test]
fn open_without_key_on_encrypted_database_is_refused() {
    let store = mem();
    let k = tempdir();
    let a = encrypted_writer(&store, &k);
    let etag = head_etag(&store);
    // Its own directory.
    let m = encryption_err(open_with_store(config(a.path()), store.clone()));
    assert!(m.contains("BUCKWAL_ENCRYPTION_KEY_FILE"), "{m}");
    // A fresh directory that auto-restores: refused before the takeover.
    let r = tempdir();
    let m = encryption_err(open_with_store(config(&fresh(&r)), store.clone()));
    assert!(m.contains("BUCKWAL_ENCRYPTION_KEY_FILE"), "{m}");
    assert_eq!(head_etag(&store), etag, "the refused opens wrote the head");
    // The writer still owns it.
    let db = open_with_store(enc_config(a.path(), &k, &key_hex()), store.clone()).unwrap();
    assert_eq!(ids(&db.connect().unwrap()), (1..=60).collect::<Vec<_>>());
}

#[test]
fn open_with_wrong_key_is_refused() {
    let store = mem();
    let k = tempdir();
    let a = encrypted_writer(&store, &k);
    let wrong = tempdir();
    let wrong_hex = "22".repeat(32);
    for dir in [a.path().to_path_buf(), fresh(&tempdir())] {
        let m = encryption_err(open_with_store(
            enc_config(&dir, &wrong, &wrong_hex),
            store.clone(),
        ));
        assert!(m.contains("does not decrypt"), "{m}");
        assert!(!m.contains(&key_hex()) && !m.contains(&wrong_hex), "{m}");
    }
}

/// Every object in the bucket with its ETag.
fn etags(store: &Arc<MemStore>) -> BTreeMap<String, String> {
    store
        .list("")
        .unwrap()
        .into_iter()
        .map(|k| {
            let etag = store.get(&k).unwrap().unwrap().etag;
            (k, etag)
        })
        .collect()
}

#[test]
fn open_with_key_on_plaintext_database_is_refused() {
    let store = mem();
    let a = closed_writer(&store, 10);
    let db_bytes = std::fs::read(a.path().join("app.db")).unwrap();
    let objects = etags(&store);
    let k = tempdir();
    let m = encryption_err(open_with_store(
        enc_config(a.path(), &k, &key_hex()),
        store.clone(),
    ));
    assert!(m.contains("cannot be encrypted in place"), "{m}");
    assert_eq!(std::fs::read(a.path().join("app.db")).unwrap(), db_bytes);
    assert_eq!(
        etags(&store),
        objects,
        "the refused open wrote to the bucket"
    );
    let db = open_mem(&store, a.path());
    assert_eq!(ids(&db.connect().unwrap()), (1..=10).collect::<Vec<_>>());
}

#[test]
fn cipher_mismatch_is_refused() {
    let store = mem();
    let k = tempdir();
    let a = encrypted_writer(&store, &k);
    let mut c = enc_config(a.path(), &k, &key_hex());
    c.encryption_cipher = "aes256gcm".into();
    let m = encryption_err(open_with_store(c, store.clone()));
    assert!(m.contains("aegis256") && m.contains("aes256gcm"), "{m}");
}

#[test]
fn encrypted_verify_needs_the_key() {
    let store = mem();
    let k = tempdir();
    let _a = encrypted_writer(&store, &k);
    let puts = store.stats().puts;
    let v = tempdir();
    let m = encryption_err(open_read_only_with_store(config(v.path()), store.clone()));
    assert!(m.contains("BUCKWAL_ENCRYPTION_KEY_FILE"), "{m}");
    let db =
        open_read_only_with_store(enc_config(v.path(), &k, &key_hex()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), (1..=60).collect::<Vec<_>>());
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(store.stats().puts, puts, "verify wrote to the bucket");
}

#[test]
fn encrypted_open_refuses_mvcc_header() {
    let store = mem();
    let k = tempdir();
    let a = encrypted_writer(&store, &k);
    let path = a.path().join("app.db");
    let mut b = std::fs::read(&path).unwrap();
    assert!(b.starts_with(b"Turso"));
    b[18] = 255;
    std::fs::write(&path, b).unwrap();
    let r = open_with_store(enc_config(a.path(), &k, &key_hex()), store.clone());
    assert_eq!(unsupported(r), "MVCC");
}

#[test]
fn encrypted_mvcc_pragma_is_refused_and_header_unchanged() {
    let store = mem();
    let t = tempdir();
    let k = tempdir();
    let cfg = || enc_config(t.path(), &k, &key_hex());
    let db = open_with_store(cfg(), store.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    insert(&c, 1).unwrap();
    for sql in [
        "PRAGMA journal_mode='mvcc'",
        "PRAGMA journal_mode=mvcc",
        "PRAGMA journal_mode='EXPERIMENTAL_MVCC'",
        "PRAGMA main.journal_mode = 'mvcc'",
    ] {
        assert_eq!(unsupported(c.execute(sql, &[])), SWITCH_TO_MVCC, "{sql}");
        assert_eq!(unsupported(c.query(sql, &[])), SWITCH_TO_MVCC, "{sql}");
    }
    insert(&c, 2).unwrap();
    c.close().unwrap();
    db.close().unwrap();
    assert!(std::fs::read(t.path().join("app.db"))
        .unwrap()
        .starts_with(b"Turso"));
    assert_eq!(header_bytes(t.path()), (2, 2));

    let db = open_with_store(cfg(), store.clone()).unwrap();
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), vec![1, 2]);
    let mode = c.query("PRAGMA journal_mode", &[]).unwrap();
    assert_eq!(text(&mode[0][0]), "wal");
}

/// Every id a fresh read-only open with the test key sees.
fn restored_ids_encrypted(store: &Arc<MemStore>, keydir: &TempDir) -> Vec<i64> {
    let v = tempdir();
    let cfg = enc_config(v.path(), keydir, &key_hex());
    let db = open_read_only_with_store(cfg, store.clone()).unwrap();
    let c = db.connect().unwrap();
    let ids = ids(&c);
    c.close().unwrap();
    db.close().unwrap();
    ids
}

#[test]
fn encrypted_window_batching_shares_puts() {
    let store = mem();
    let t = tempdir();
    let k = tempdir();
    let mut cfg = enc_config(t.path(), &k, &key_hex());
    cfg.batching = "window 50ms 64 1MiB".parse().unwrap();
    let db = open_with_store(cfg, store.clone()).unwrap();
    create(&db.connect().unwrap());
    let threads: Vec<_> = (0..4)
        .map(|n| {
            let db = db.clone();
            std::thread::spawn(move || {
                let c = db.connect().unwrap();
                (0..10).map(|i| 1 + n * 10 + i).for_each(|id| {
                    insert(&c, id).unwrap();
                });
                c.close().unwrap();
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let s = db.upload_stats();
    assert!(s.commits_per_log_put > 1.0, "{s:?}");
    db.close().unwrap();
    assert_eq!(
        restored_ids_encrypted(&store, &k),
        (1..=40).collect::<Vec<_>>()
    );
}

#[test]
fn encrypted_maintenance_checkpoint_seals() {
    let store = mem();
    let t = tempdir();
    let k = tempdir();
    let mut cfg = enc_config(t.path(), &k, &key_hex());
    cfg.maintenance_log_objects = 5;
    let db = open_with_store(cfg, store.clone()).unwrap();
    let gen = db.generation();
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=30 {
        insert(&c, id).unwrap();
    }
    let until = Instant::now() + Duration::from_secs(10);
    while db.generation() == gen {
        assert!(Instant::now() < until, "maintenance never sealed");
        std::thread::sleep(Duration::from_millis(5));
    }
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(
        restored_ids_encrypted(&store, &k),
        (1..=30).collect::<Vec<_>>()
    );
}

/// A damaged page fails the cipher's authentication when the engine reads it, instead of
/// reading as garbage.
#[test]
fn encrypted_damaged_page_fails_authentication() {
    let store = mem();
    let k = tempdir();
    let a = encrypted_writer(&store, &k);
    let path = a.path().join("app.db");
    let mut b = std::fs::read(&path).unwrap();
    let ps = 4096;
    assert!(b.len() >= 2 * ps, "{} bytes", b.len());
    b[ps + 200] ^= 1; // page 2: t's root
    std::fs::write(&path, b).unwrap();
    let db = open_with_store(enc_config(a.path(), &k, &key_hex()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    match c.query("SELECT id FROM t ORDER BY id", &[]) {
        Err(Error::Engine(m)) => assert!(m.contains("Decryption failed"), "{m}"),
        r => panic!("read a damaged page: {r:?}"),
    }
}

/// A crash image of a writer's directory with page 1 torn in `app.db`: a seal put page 1
/// there, then `CREATE TABLE u` committed a newer page 1 to the WAL, and the second half of
/// `app.db`'s page 1 is zeroed (a torn write). Rows 1..=40 are acked.
fn torn_page_one_image(store: &Arc<MemStore>, cfg: impl Fn(&Path) -> Config) -> TempDir {
    let a = tempdir();
    let db = open_with_store(cfg(a.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=30 {
        insert(&c, id).unwrap();
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    c.execute("CREATE TABLE u(x)", &[]).unwrap();
    for id in 31..=40 {
        insert(&c, id).unwrap();
    }
    let img = tempdir();
    for f in ["app.db", "app.db-wal", "app.db-durable", "app.db-dirty"] {
        if a.path().join(f).exists() {
            std::fs::copy(a.path().join(f), img.path().join(f)).unwrap();
        }
    }
    crash(db, vec![c]);
    let path = img.path().join("app.db");
    let mut b = std::fs::read(&path).unwrap();
    assert!(b.len() >= 4096, "{} bytes", b.len());
    b[2048..4096].fill(0);
    std::fs::write(&path, b).unwrap();
    img
}

/// A torn page 1 in `app.db`: a plaintext directory opens (the WAL's page 1 is the one read),
/// an encrypted one is refused at open (page 1 fails authentication; turso reads it from
/// `app.db` too). Nothing acked is lost: a restore from the bucket has every row.
#[test]
fn encrypted_torn_page_one_is_unopenable_but_restores() {
    let store = mem();
    let img = torn_page_one_image(&store, config);
    let db = open_mem(&store, img.path());
    assert_eq!(ids(&db.connect().unwrap()), (1..=40).collect::<Vec<_>>());
    db.close().unwrap();

    let store = mem();
    let k = tempdir();
    let img = torn_page_one_image(&store, |d| enc_config(d, &k, &key_hex()));
    let m = encryption_err(open_with_store(
        enc_config(img.path(), &k, &key_hex()),
        store.clone(),
    ));
    assert!(
        m.contains("does not decrypt") && m.contains("page 1 is damaged"),
        "{m}"
    );
    assert_eq!(
        restored_ids_encrypted(&store, &k),
        (1..=40).collect::<Vec<_>>()
    );
}

/// A takeover whose key does not fit the database is refused before its claim: the bucket is
/// unchanged, the directory holds no database, and the running writer keeps committing.
fn refused_takeover(store: &Arc<MemStore>, cfg: impl Fn(&Path) -> Config) -> String {
    let before = etags(store);
    let t = tempdir();
    let dir = fresh(&t);
    let m = encryption_err(crate::takeover_with_store(cfg(&dir), store.clone()));
    assert_eq!(
        etags(store),
        before,
        "the refused takeover wrote to the bucket"
    );
    no_database_files(&dir);
    m
}

#[test]
fn takeover_with_a_mismatched_key_is_refused_before_the_claim() {
    let store = mem();
    let k = tempdir();
    let a = encrypted_writer(&store, &k);
    // The running writer, nothing committed in its generation yet: page 1 is only in the
    // bucket, so the takeover checks the page its restore fetched.
    let db = open_with_store(enc_config(a.path(), &k, &key_hex()), store.clone()).unwrap();
    let c = db.connect().unwrap();

    let wrong = tempdir();
    let m = refused_takeover(&store, |d| enc_config(d, &wrong, &"22".repeat(32)));
    assert!(m.contains("does not decrypt"), "{m}");
    assert!(
        !m.contains(&key_hex()) && !m.contains(&"22".repeat(32)),
        "{m}"
    );
    let m = refused_takeover(&store, |d| {
        let mut cfg = enc_config(d, &k, &key_hex());
        cfg.encryption_cipher = "aes256gcm".into();
        cfg
    });
    assert!(m.contains("aegis256") && m.contains("aes256gcm"), "{m}");
    insert(&c, 61).unwrap();

    // Page 1 now in the log too: still refused.
    c.execute("CREATE TABLE u(x)", &[]).unwrap();
    let wrong = tempdir();
    refused_takeover(&store, |d| enc_config(d, &wrong, &"22".repeat(32)));
    insert(&c, 62).unwrap();

    // The right key takes over as before.
    let t = tempdir();
    let rep =
        crate::takeover_with_store(enc_config(&fresh(&t), &k, &key_hex()), store.clone()).unwrap();
    assert!(rep.fence_seq > 0, "{rep:?}");
    assert!(matches!(insert(&c, 63), Err(Error::Fenced(_))));
    std::mem::forget(c);
    std::mem::forget(db);
    let db = open_with_store(enc_config(&fresh(&t), &k, &key_hex()), store.clone()).unwrap();
    assert_eq!(ids(&db.connect().unwrap()), (1..=62).collect::<Vec<_>>());
}

#[test]
fn takeover_with_a_key_on_a_plaintext_database_is_refused() {
    let store = mem();
    let a = closed_writer(&store, 10);
    let db = open_mem(&store, a.path());
    let c = db.connect().unwrap();
    let k = tempdir();
    let m = refused_takeover(&store, |d| enc_config(d, &k, &key_hex()));
    assert!(m.contains("cannot be encrypted in place"), "{m}");
    insert(&c, 11).unwrap();
    assert_eq!(ids(&c), (1..=11).collect::<Vec<_>>());
}

/// A takeover without a key of an encrypted database is refused before its claim (the
/// restored page 1 says it is encrypted): the running writer is not fenced. Page 1 only in the
/// bucket, then in the log too.
#[test]
fn takeover_without_a_key_on_an_encrypted_database_is_refused() {
    let store = mem();
    let k = tempdir();
    let a = encrypted_writer(&store, &k);
    let db = open_with_store(enc_config(a.path(), &k, &key_hex()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    let m = refused_takeover(&store, config);
    assert!(
        m.contains("encrypted") && m.contains("BUCKWAL_ENCRYPTION_KEY_FILE"),
        "{m}"
    );
    insert(&c, 61).unwrap();
    c.execute("CREATE TABLE u(x)", &[]).unwrap();
    refused_takeover(&store, config);
    insert(&c, 62).unwrap();
    assert_eq!(ids(&c), (1..=62).collect::<Vec<_>>());
}

/// A takeover without a key of a plaintext database goes ahead (the page-1 check passes).
#[test]
fn takeover_without_a_key_on_a_plaintext_database_goes_ahead() {
    let store = mem();
    let a = closed_writer(&store, 10);
    let db = open_mem(&store, a.path());
    let c = db.connect().unwrap();
    insert(&c, 11).unwrap();
    let t = tempdir();
    let rep = crate::takeover_with_store(config(&fresh(&t)), store.clone()).unwrap();
    assert!(rep.fence_seq > 0, "{rep:?}");
    assert!(matches!(insert(&c, 12), Err(Error::Fenced(_))));
    std::mem::forget(c);
    std::mem::forget(db);
    let db = open_mem(&store, &fresh(&t));
    assert_eq!(ids(&db.connect().unwrap()), (1..=11).collect::<Vec<_>>());
}

fn mvcc_config(dir: &Path) -> Config {
    let mut c = config(dir);
    c.journal_mode = JournalMode::Mvcc;
    c
}

/// A WAL database with one row, sealed, closed. Returns the store and the directory.
fn closed_wal_database() -> (Arc<MemStore>, TempDir) {
    let store = mem();
    let t = tempdir();
    let db = open_mem(&store, t.path());
    let c = db.connect().unwrap();
    create(&c);
    insert(&c, 1).unwrap();
    c.close().unwrap();
    db.close().unwrap();
    (store, t)
}

/// The bucket's head rewritten as an MVCC bucket's (format 2), and a local state to match.
fn make_bucket_mvcc(store: &MemStore, dir: Option<&Path>) {
    let mut h = head(store);
    h.format = crate::format::FORMAT_V2;
    h.journal_mode = JournalMode::Mvcc;
    store.put(&keys().head(), &h.encode()).unwrap();
    if let Some(dir) = dir {
        let mut s = state(dir);
        s.format = crate::format::FORMAT_V2;
        s.write_atomic(dir).unwrap();
    }
}

#[test]
fn wal_bucket_opened_as_mvcc_is_refused() {
    let (store, t) = closed_wal_database();
    // The directory that created it (local state, claimed bucket).
    let before = store.stats().puts;
    let r = open_with_store(mvcc_config(t.path()), store.clone());
    assert_eq!(unsupported(r), SWITCH_TO_MVCC);
    // A fresh disk (restore path).
    let t2 = tempdir();
    let r = open_with_store(mvcc_config(&fresh(&t2)), store.clone());
    assert_eq!(unsupported(r), SWITCH_TO_MVCC);
    assert_eq!(store.stats().puts, before, "the bucket was written");
    // Read-only too.
    let t3 = tempdir();
    let r = open_read_only_with_store(mvcc_config(&fresh(&t3)), store.clone());
    assert_eq!(unsupported(r), SWITCH_TO_MVCC);
    // Nothing changed for WAL: it opens and keeps its row.
    let db = open_mem(&store, t.path());
    assert_eq!(ids(&db.connect().unwrap()), vec![1]);
}

#[test]
fn mvcc_bucket_opened_as_wal_is_refused() {
    let (store, t) = closed_wal_database();
    make_bucket_mvcc(&store, Some(t.path()));
    let before = store.stats().puts;
    // Local state of an MVCC database (claim, raise and fence are bucket writes).
    let r = open_with_store(config(t.path()), store.clone());
    assert_eq!(unsupported(r), SWITCH_BACK);
    // A fresh disk (restore path).
    let t2 = tempdir();
    let r = open_with_store(config(&fresh(&t2)), store.clone());
    assert_eq!(unsupported(r), SWITCH_BACK);
    assert!(
        !fresh(&t2).join("app.db").exists(),
        "the refusal left files"
    );
    let r = open_read_only_with_store(config(&fresh(&tempdir())), store.clone());
    assert_eq!(unsupported(r), SWITCH_BACK);
    assert_eq!(store.stats().puts, before, "the bucket was written");
    // restore() has the same check.
    let t4 = tempdir();
    let r = restore_with_store(&config(&fresh(&t4)), store.clone(), &fresh(&t4));
    assert_eq!(unsupported(r), SWITCH_BACK);
}

#[test]
fn new_mvcc_database_is_created_and_opens() {
    // `open` creates the MVCC bucket (never a WAL one), then opens the engine on it in MVCC
    // mode.
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let db = open_with_store(mvcc_config(&dir), store.clone()).unwrap();
    let (h, image) = created_bucket(&*store);
    assert_eq!(h.epoch, 1);
    assert_eq!(state(&dir).format, crate::format::FORMAT_V2);
    let c = db.connect().unwrap();
    let mode = c.query("PRAGMA journal_mode", &[]).unwrap();
    assert_eq!(text(&mode[0][0]), "mvcc");
    c.close().unwrap();
    db.close().unwrap();
    // A reopen continues the directory's database.
    let db = open_with_store(mvcc_config(&dir), store.clone()).unwrap();
    assert_eq!(db.generation(), 2);
    assert_eq!(created_bucket(&*store).1, image);
    db.close().unwrap();
}

#[test]
fn window_batching_is_refused_in_mvcc_before_anything_is_touched() {
    let store = mem();
    let t = tempdir();
    let mut c = mvcc_config(&fresh(&t));
    c.batching = Batching::Window {
        max_wait: Duration::from_millis(10),
        max_commits: 4,
        max_bytes: 1024,
    };
    let r = open_with_store(c, store.clone());
    assert_eq!(unsupported(r), "MVCC: Window batching");
    assert_eq!(store.stats().puts, 0);
}

#[test]
fn set_batching_window_still_works_in_wal_mode() {
    let store = mem();
    let t = tempdir();
    let db = open_mem(&store, t.path());
    let w = Batching::Window {
        max_wait: Duration::from_millis(10),
        max_commits: 4,
        max_bytes: 1024,
    };
    db.set_batching(w).unwrap();
    let c = db.connect().unwrap();
    c.set_batching(Some(w)).unwrap();
}

#[test]
fn refuse_mode_mismatch_table() {
    use JournalMode::{Mvcc, Wal};
    let head = |mode| Head {
        format: if mode == Mvcc {
            crate::format::FORMAT_V2
        } else {
            1
        },
        generation: 1,
        manifest: None,
        writer_id: "w".into(),
        epoch: 1,
        journal_mode: mode,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
    };
    let wal_page = Some([2, 2]);
    let mvcc_page = Some([255, 255]);
    assert_eq!(refuse_mode_mismatch(&head(Wal), wal_page, Wal), Ok(()));
    assert_eq!(refuse_mode_mismatch(&head(Wal), None, Wal), Ok(()));
    assert_eq!(refuse_mode_mismatch(&head(Mvcc), mvcc_page, Mvcc), Ok(()));
    assert_eq!(refuse_mode_mismatch(&head(Mvcc), None, Mvcc), Ok(()));
    assert_eq!(
        refuse_mode_mismatch(&head(Wal), wal_page, Mvcc),
        Err(Error::Unsupported(SWITCH_TO_MVCC))
    );
    assert_eq!(
        refuse_mode_mismatch(&head(Mvcc), mvcc_page, Wal),
        Err(Error::Unsupported(SWITCH_BACK))
    );
    // The head's mode is checked before the page.
    assert_eq!(
        refuse_mode_mismatch(&head(Mvcc), None, Wal),
        Err(Error::Unsupported(SWITCH_BACK))
    );
    // A WAL bucket whose page 1 says MVCC is not something this build opens.
    assert_eq!(
        refuse_mode_mismatch(&head(Wal), mvcc_page, Wal),
        Err(Error::Unsupported("MVCC"))
    );
    assert_eq!(
        refuse_mode_mismatch(&head(Wal), Some([255, 2]), Wal),
        Err(Error::Unsupported("MVCC"))
    );
}

#[test]
fn mvcc_directory_opened_as_wal_says_switching_back() {
    let (store, t) = closed_wal_database();
    // Local state of an MVCC database and an app.db carrying the MVCC header.
    make_bucket_mvcc(&store, Some(t.path()));
    let path = t.path().join("app.db");
    let mut b = std::fs::read(&path).unwrap();
    b[18] = 255;
    b[19] = 255;
    std::fs::write(&path, &b).unwrap();
    let before = store.stats().puts;
    let r = open_with_store(config(t.path()), store.clone());
    assert_eq!(unsupported(r), SWITCH_BACK);
    assert_eq!(store.stats().puts, before, "the bucket was written");
}

#[test]
fn takeover_refuses_a_mode_mismatch_without_writing() {
    let (store, t) = closed_wal_database();
    let h0 = store.get(&keys().head()).unwrap().unwrap().body;
    let puts = store.stats().puts;
    let log0 = log_seqs(&*store, head(&*store).generation);
    // A WAL bucket with an MVCC config.
    let d = tempdir();
    let r = crate::takeover_with_store(mvcc_config(&fresh(&d)), store.clone());
    assert_eq!(unsupported(r), SWITCH_TO_MVCC);
    // An MVCC bucket with a WAL config.
    make_bucket_mvcc(&store, Some(t.path()));
    let h1 = store.get(&keys().head()).unwrap().unwrap().body;
    assert_ne!(h0, h1);
    let puts = puts + 1; // make_bucket_mvcc's own head PUT
    let d = tempdir();
    let r = crate::takeover_with_store(config(&fresh(&d)), store.clone());
    assert_eq!(unsupported(r), SWITCH_BACK);
    assert_eq!(store.stats().puts, puts, "a takeover wrote to the bucket");
    assert_eq!(store.get(&keys().head()).unwrap().unwrap().body, h1);
    assert_eq!(
        log_seqs(&*store, head(&*store).generation),
        log0,
        "a fence was appended"
    );
}

// Creating a new MVCC database.

/// The engine config `open` builds for `c`, with its encryption.
fn engine_cfg(c: &Config) -> EngineConfig {
    let mut cfg = c.engine(false);
    cfg.encryption = encryption::load(c).unwrap();
    cfg
}

/// `create_new_mvcc` for `c` against `store`, as `open` runs it (the directory exists).
fn create_mvcc(store: Arc<dyn ObjectStore>, c: &Config) -> Result<(Bucket, LocalState)> {
    std::fs::create_dir_all(&c.dir).unwrap();
    create_new_mvcc(&c.dir, &store, &c.keys(), &engine_cfg(c))
}

/// `mvcc_config` with a 100 ms retry deadline, so a failing store fails fast.
fn mvcc_fast_config(dir: &Path) -> Config {
    let mut c = config_with_lease(dir, Duration::from_millis(500));
    c.journal_mode = JournalMode::Mvcc;
    c
}

/// The bucket a creation left: the head (format 2, MVCC, generation 2, manifest 1),
/// `manifest/1` mapping every page to generation 1, `pages/1/*` of the manifest's page size,
/// no log object. Returns the head and the `app.db` image the pages make.
fn created_bucket(store: &dyn ObjectStore) -> (Head, Vec<u8>) {
    let h = head(store);
    assert_eq!(
        (h.format, h.generation, h.manifest, h.journal_mode),
        (crate::format::FORMAT_V2, 2, Some(1), JournalMode::Mvcc),
        "{h:?}"
    );
    let body = store
        .get(&keys().manifest(1))
        .unwrap()
        .expect("manifest/1")
        .body;
    let m = crate::format::Manifest::decode(&body).unwrap();
    assert_eq!(m.page_size, 4096);
    assert!(m.page_count >= 1, "{m:?}");
    assert_eq!(m.pages, (1..=m.page_count).map(|p| (p, 1)).collect());
    let mut image = Vec::new();
    for p in 1..=m.page_count {
        let page = store.get(&keys().page(1, p)).unwrap().expect("page").body;
        assert_eq!(page.len(), 4096);
        image.extend(page);
    }
    assert_eq!(
        store.list(&keys().log_root()).unwrap(),
        Vec::<String>::new()
    );
    (h, image)
}

/// No head without its manifest and every page that manifest maps.
fn head_has_its_manifest(store: &dyn ObjectStore) {
    let Some(obj) = store.get(&keys().head()).unwrap() else {
        return;
    };
    let h = Head::decode(&obj.body).unwrap();
    let Some(g) = h.manifest else {
        return;
    };
    let body = store.get(&keys().manifest(g)).unwrap();
    let m = crate::format::Manifest::decode(&body.expect("the head's manifest").body).unwrap();
    for (p, pg) in m.pages {
        assert!(
            store.get(&keys().page(pg, p)).unwrap().is_some(),
            "page {p}"
        );
    }
}

/// The engine's log header: `LOG_MAGIC` ("LML2" LE), `hdr_len` 56, 56 bytes.
fn is_log_header(b: &[u8]) -> bool {
    b.len() == 56 && b[0..4] == 0x4C4D_4C32u32.to_le_bytes() && b[6..8] == 56u16.to_le_bytes()
}

/// `db` as `app.db` and `log` as `app.db-log` in a new directory, opened with turso_core over
/// plain IO (with `enc`'s key): MVCC mode and `integrity_check` ok, a commit, and the row
/// still there after a reopen.
fn plain_mvcc_open_works(db: &[u8], log: &[u8], enc: Option<&encryption::Encryption>) {
    use turso_core::{DatabaseOpts, EncryptionOpts, OpenOptions, PlatformIO, SqliteDialect};
    let t = tempdir();
    std::fs::write(t.path().join("app.db"), db).unwrap();
    std::fs::write(t.path().join("app.db-log"), log).unwrap();
    let path = t.path().join("app.db");
    let query = |conn: &Arc<turso_core::Connection>, sql: &str| {
        conn.query(sql)
            .unwrap()
            .unwrap()
            .run_collect_rows()
            .unwrap()
    };
    for round in 0..2 {
        let io: Arc<dyn turso_core::IO> = Arc::new(PlatformIO::new().unwrap());
        let mut opts = OpenOptions::new(Arc::new(SqliteDialect));
        if let Some(enc) = enc {
            opts = opts
                .db_opts(DatabaseOpts::new().with_encryption(true))
                .encryption(EncryptionOpts {
                    cipher: enc.cipher.to_string(),
                    hexkey: hex::encode(enc.key.as_slice()),
                });
        }
        let db = turso_core::Database::open(io, path.to_str().unwrap(), opts).unwrap();
        assert!(db.mvcc_enabled());
        let conn = db
            .connect_with_encryption(enc.map(|e| e.key.clone()))
            .unwrap();
        let mode = query(&conn, "PRAGMA journal_mode");
        assert_eq!(text(&mode[0][0]), "mvcc");
        let check = query(&conn, "PRAGMA integrity_check");
        assert_eq!(text(&check[0][0]), "ok");
        if round == 0 {
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
                .unwrap();
            conn.execute("INSERT INTO t VALUES (7)").unwrap();
        } else {
            let rows = query(&conn, "SELECT id FROM t");
            assert_eq!(rows[0][0].as_int(), Some(7));
        }
        conn.close().unwrap();
    }
}

/// The local files of a finished creation in `dir`: `app.db` (bytes 18/19 = 255) equal to the
/// bucket's image, `app.db-log` holding the 56-byte header only, the local state (format 2,
/// the head's generation and epoch, claimed, complete), no staging directory. (An engine
/// open after it leaves an empty `-wal`; `create_new_mvcc` moves none in.)
fn created_locally(dir: &Path, image: &[u8], h: &Head) {
    created_locally_with(dir, image, h, false);
}

/// [`created_locally`]; with `reopened`, the log may also be empty: the reopen's reconcile
/// replaces a header-only local `.db-log` with the new generation's empty image (Task 6).
fn created_locally_with(dir: &Path, image: &[u8], h: &Head, reopened: bool) {
    let db = std::fs::read(dir.join("app.db")).unwrap();
    assert_eq!(db, image);
    let log = std::fs::read(dir.join("app.db-log")).unwrap();
    assert!(
        is_log_header(&log) || (reopened && log.is_empty()),
        "{log:?}"
    );
    let s = state(dir);
    assert_eq!(
        (s.format, s.generation, s.epoch, s.claimed, s.complete),
        (crate::format::FORMAT_V2, h.generation, h.epoch, true, true)
    );
    assert_eq!(s.writer_id, h.writer_id);
    assert!(!dir.join("app.db-creating").exists());
}

#[test]
fn create_mvcc_makes_head_manifest_pages_and_local_files() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (bucket, st) = create_mvcc(store.clone(), &mvcc_config(&dir)).unwrap();
    let (h, image) = created_bucket(&*store);
    assert_eq!(h.epoch, 1);
    assert_eq!(h.writer_id, st.writer_id);
    assert_eq!(bucket.head(), &h);
    assert_eq!(bucket.manifest().map(|m| m.page_count), Some(2));
    assert_eq!(header_bytes(&dir), (255, 255));
    let log = std::fs::read(dir.join("app.db-log")).unwrap();
    assert_eq!(log.len(), 56);
    created_locally(&dir, &image, &h);
    assert!(!dir.join("app.db-wal").exists());
    assert_eq!(state(&dir), st);
    // The head's If-None-Match is the only conditional PUT: 2 pages, the manifest, the head.
    assert_eq!(store.stats().puts, 4);
    // Trim against this head deletes nothing: it is a normal sealed head.
    let m = bucket.manifest().unwrap();
    assert_eq!(crate::trim::trim_once(&*store, &keys(), &h, Some(m)), Ok(0));
    // The local files are an MVCC database turso opens and writes.
    plain_mvcc_open_works(&image, &log, None);
    // A second machine: the restore reads the head and manifest; the bucket's pages alone
    // (with the empty log a TRUNCATE checkpoint leaves) are the same database.
    let t2 = tempdir();
    let rep = restore_with_store(&mvcc_config(&fresh(&t2)), store.clone(), &fresh(&t2)).unwrap();
    assert_eq!(rep.generation, 2);
    assert_eq!(state(&fresh(&t2)).format, crate::format::FORMAT_V2);
    plain_mvcc_open_works(&image, &[], None);
}

#[test]
fn create_crash_at_each_step() {
    // A clean creation's PUTs: the pages, the manifest, the head.
    let clean = mem();
    let t = tempdir();
    create_mvcc(clean.clone(), &mvcc_config(&fresh(&t))).unwrap();
    let total = clean.stats().puts;
    assert_eq!(total, 4);
    for n in 0..=total {
        let inner = mem();
        let fault = Arc::new(FaultStore::new(inner.clone()));
        fault.fail_puts_after(n);
        let t = tempdir();
        let dir = fresh(&t);
        let c = mvcc_fast_config(&dir);
        let r = create_mvcc(fault.clone(), &c);
        head_has_its_manifest(&*inner);
        if n < total {
            assert!(r.is_err(), "n={n}: {:?}", r.map(|(_, s)| s));
            assert!(inner.get(&keys().head()).unwrap().is_none(), "n={n}");
            assert!(LocalState::read(&dir).unwrap().is_none(), "n={n}");
            assert!(!dir.join("app.db").exists(), "n={n}");
            // A crash mid-build leaves a staging dir that must never be opened again.
            let staging = dir.join("app.db-creating");
            assert!(staging.exists(), "n={n}");
            std::fs::write(staging.join("app.db"), b"torn switch").unwrap();
        } else {
            r.unwrap();
        }
        // The next open removes the staging dir, never opening it, and creates again
        // (overwriting what the crashed attempt left), then opens the engine.
        fault.fail_puts_after(u64::MAX);
        open_with_store(c.clone(), fault.clone())
            .unwrap_or_else(|e| panic!("n={n}: {e}"))
            .close()
            .unwrap();
        let (h, image) = created_bucket(&*inner);
        created_locally_with(&dir, &image, &h, true);
        head_has_its_manifest(&*inner);
        // A second machine restores the final state.
        let t2 = tempdir();
        let rep = restore_with_store(&mvcc_config(&fresh(&t2)), inner.clone(), &fresh(&t2));
        assert_eq!(rep.unwrap().generation, 2, "n={n}");
        plain_mvcc_open_works(&image, &[], None);
    }

    // Stored, then an error: a page or the manifest is PUT again; the head is found as ours.
    for key in ["pages/", "manifest/", "head"] {
        let inner = mem();
        let fault = Arc::new(FaultStore::new(inner.clone()));
        fault.only_keys_containing(key);
        fault.store_then_fail_next_puts(1);
        let t = tempdir();
        let dir = fresh(&t);
        create_mvcc(fault.clone(), &mvcc_fast_config(&dir))
            .unwrap_or_else(|e| panic!("{key}: {e}"));
        let (h, image) = created_bucket(&*inner);
        created_locally(&dir, &image, &h);
    }

    // A crash while moving the files in, after the head landed (0..=2 files moved, or all of
    // them with the staging dir left).
    for moved in 0..=3 {
        let store = mem();
        let t = tempdir();
        let dir = fresh(&t);
        crate::create::CRASH_AFTER_MOVING.with(|c| c.set(Some(moved)));
        let r = create_mvcc(store.clone(), &mvcc_config(&dir)).map(|(_, s)| s);
        crate::create::CRASH_AFTER_MOVING.with(|c| c.set(None));
        assert!(matches!(r, Err(Error::Io(_))), "moved={moved}: {r:?}");
        assert!(dir.join("app.db-creating").exists());
        let (_, image) = created_bucket(&*store);
        // The next open removes the staging dir; a directory without its state file is
        // emptied first, and the database comes back from the bucket (restored, claimed).
        open_with_store(mvcc_config(&dir), store.clone())
            .unwrap_or_else(|e| panic!("moved={moved}: {e}"))
            .close()
            .unwrap();
        assert!(!dir.join("app.db-creating").exists(), "moved={moved}");
        let s = state(&dir);
        assert_eq!(
            (s.format, s.generation, s.claimed),
            (crate::format::FORMAT_V2, 2, true)
        );
        let (h, again) = head_and_image(&*store);
        assert_eq!(again, image, "moved={moved}");
        assert_eq!(h.writer_id, s.writer_id, "moved={moved}");
        head_has_its_manifest(&*store);
        let t2 = tempdir();
        let rep = restore_with_store(&mvcc_config(&fresh(&t2)), store.clone(), &fresh(&t2));
        assert_eq!(rep.unwrap().generation, 2, "moved={moved}");
    }
}

/// A stale `app.db-creating` and no state file must not make the open delete an `app.db`
/// that is not the creation's: the foreign-file guard refuses it, the file untouched.
#[test]
fn stale_staging_dir_never_deletes_a_foreign_app_db() {
    for state_in_staging in [false, true] {
        let store = mem();
        let t = tempdir();
        let dir = fresh(&t);
        std::fs::create_dir_all(dir.join("app.db-creating")).unwrap();
        // Still in staging: the creation had not moved it, so the one in `dir` is not its.
        std::fs::write(dir.join("app.db-creating").join("app.db"), b"staged").unwrap();
        std::fs::write(dir.join("app.db-creating").join("app.db-log"), b"staged").unwrap();
        if state_in_staging {
            std::fs::write(dir.join("app.db-creating").join("app.db-durable"), b"{}").unwrap();
        }
        std::fs::write(dir.join("app.db"), b"foreign sqlite").unwrap();
        std::fs::write(dir.join("app.db-log"), b"foreign log").unwrap();
        let e = open_with_store(mvcc_config(&dir), store.clone())
            .err()
            .unwrap_or_else(|| panic!("opened over a foreign app.db"));
        assert!(
            matches!(&e, Error::Config(m) if m.contains("holds an app.db not created by buckwal")),
            "state_in_staging={state_in_staging}: {e:?}"
        );
        assert_eq!(
            std::fs::read(dir.join("app.db")).unwrap(),
            b"foreign sqlite"
        );
        assert_eq!(
            std::fs::read(dir.join("app.db-log")).unwrap(),
            b"foreign log"
        );
    }
}

/// The head and the `app.db` image of `manifest/1`, whatever the log holds (a takeover's
/// fence record, say).
fn head_and_image(store: &dyn ObjectStore) -> (Head, Vec<u8>) {
    let h = head(store);
    assert_eq!((h.generation, h.manifest), (2, Some(1)), "{h:?}");
    let body = store.get(&keys().manifest(1)).unwrap().unwrap().body;
    let m = crate::format::Manifest::decode(&body).unwrap();
    let image = (1..=m.page_count)
        .flat_map(|p| store.get(&keys().page(1, p)).unwrap().unwrap().body)
        .collect();
    (h, image)
}

#[test]
fn racing_creators_one_head_wins() {
    let store = mem();
    let (ta, tb) = (tempdir(), tempdir());
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let runs: Vec<_> = [fresh(&ta), fresh(&tb)]
        .into_iter()
        .map(|dir| {
            let (store, barrier) = (store.clone(), barrier.clone());
            std::thread::spawn(move || {
                let c = mvcc_config(&dir);
                barrier.wait();
                create_mvcc(store, &c).map(|(_, s)| (dir, s))
            })
        })
        .collect();
    let results: Vec<_> = runs.into_iter().map(|h| h.join().unwrap()).collect();
    let won: Vec<_> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
    assert_eq!(won.len(), 1, "{results:?}");
    let lost: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
    assert_eq!(
        lost,
        vec![&Error::Config(
            "a database already exists at this prefix".into()
        )]
    );
    let (dir, st) = won[0];
    let (h, image) = created_bucket(&*store);
    assert_eq!(h.writer_id, st.writer_id);
    head_has_its_manifest(&*store);
    // The loser's pages may have replaced the winner's: either way the bucket is a whole
    // empty MVCC database, and the winner's directory is its own.
    plain_mvcc_open_works(&image, &[], None);
    let local = std::fs::read(dir.join("app.db")).unwrap();
    let log = std::fs::read(dir.join("app.db-log")).unwrap();
    plain_mvcc_open_works(&local, &log, None);
}

/// Plaintext a creation would show without encryption: the SQLite header, the schema of
/// the engine's MVCC meta table, and the key in every spelling.
fn creation_needles(store: &dyn ObjectStore, local: &[Vec<u8>]) -> BTreeSet<&'static str> {
    let hex = key_hex();
    let needles: [(&str, Vec<u8>); 5] = [
        ("SQLite header", b"SQLite format 3\0".to_vec()),
        ("CREATE TABLE", "CREATE TABLE".into()),
        ("meta table", "__turso_internal_mvcc_meta".into()),
        ("key hex", hex.into_bytes()),
        ("raw key", vec![0x11; 32]),
    ];
    let mut bodies: Vec<Vec<u8>> = store
        .list("")
        .unwrap()
        .iter()
        .map(|k| store.get(k).unwrap().unwrap().body)
        .collect();
    bodies.extend(local.iter().cloned());
    let mut found = BTreeSet::new();
    for body in bodies {
        for (name, needle) in &needles {
            if body.windows(needle.len()).any(|w| w == &needle[..]) {
                found.insert(*name);
            }
        }
    }
    found
}

#[test]
fn create_mvcc_encrypted_has_no_plaintext() {
    let store = mem();
    let k = tempdir();
    let t = tempdir();
    let dir = fresh(&t);
    let mut c = enc_config(&dir, &k, &key_hex());
    c.journal_mode = JournalMode::Mvcc;
    create_mvcc(store.clone(), &c).unwrap();
    let (h, image) = created_bucket(&*store);
    created_locally(&dir, &image, &h);
    assert!(image.starts_with(b"Turso"), "app.db is not encrypted");
    let enc = encryption::load(&c).unwrap().unwrap();
    // Page 1 decrypts with the key, and its header is MVCC's.
    let ctx = turso_core::EncryptionContext::new(enc.cipher, &enc.key, 4096).unwrap();
    let page1 = ctx.decrypt_page(&image[..4096], 1).unwrap();
    assert_eq!((page1[18], page1[19]), (255, 255));
    let local = vec![
        std::fs::read(dir.join("app.db")).unwrap(),
        std::fs::read(dir.join("app.db-log")).unwrap(),
    ];
    assert_eq!(creation_needles(&*store, &local), BTreeSet::new());
    plain_mvcc_open_works(&local[0], &local[1], Some(&enc));
    plain_mvcc_open_works(&image, &[], Some(&enc));

    // The control: without encryption the same search finds the header and the schema.
    let plain = mem();
    let t2 = tempdir();
    create_mvcc(plain.clone(), &mvcc_config(&fresh(&t2))).unwrap();
    let found = creation_needles(&*plain, &[]);
    assert!(
        found.contains("SQLite header") && found.contains("meta table"),
        "{found:?}"
    );
}

mod mvcc;

/// `quiet_config` with the lease and request timeout (ms) given.
fn timeouts_config(dir: &Path, lease_ms: u64, request_timeout_ms: u64) -> Config {
    let mut c = quiet_config(dir, None);
    c.lease = Duration::from_millis(lease_ms);
    c.request_timeout = Duration::from_millis(request_timeout_ms);
    c
}

/// A writer with the given lease and request timeout that committed 1..=5 and closed, then
/// its seal-start record (`writer_left_in_a_seal`).
fn left_in_a_seal_with(store: &Arc<MemStore>, lease_ms: u64, request_timeout_ms: u64) -> TempDir {
    let a = tempdir();
    let db = open_with_store(
        timeouts_config(a.path(), lease_ms, request_timeout_ms),
        store.clone(),
    )
    .unwrap();
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=5 {
        insert(&c, id).unwrap();
    }
    c.close().unwrap();
    db.close().unwrap();
    let gen = head(&**store).generation;
    let next = log_seqs(&**store, gen).last().map_or(1, |s| s + 1);
    let rec = LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: state(a.path()).writer_id,
        syncs: Vec::new(),
    };
    let r = store
        .put_if_absent(&keys().log(gen, next), &crate::format::encode_record(&rec))
        .unwrap();
    assert!(matches!(r, CondPut::Created { .. }), "{r:?}");
    a
}

/// Item 2: `TakeoverReport::quiet` is the larger of the replaced writer's `lease + request
/// timeout` (from its head) and the new writer's own, and `seal_quiet_ms` holds it.
#[test]
fn quiet_is_larger_of_both_writers() {
    // (old lease, old request timeout, own lease, own request timeout, quiet used), in ms.
    let cases = [
        (400, 600, 300, 500, 1000), // the old writer's sum is longer
        (300, 500, 400, 700, 1100), // the new writer's is longer
    ];
    for (al, ar, bl, br, want) in cases {
        let store = mem();
        let _a = left_in_a_seal_with(&store, al, ar);
        let old = head(&*store);
        assert_eq!((old.lease_ms, old.request_timeout_ms), (Some(al), Some(ar)));
        let bt = tempdir();
        let b_dir = fresh(&bt);
        let rep =
            crate::takeover_with_store(timeouts_config(&b_dir, bl, br), store.clone()).unwrap();
        let want = Duration::from_millis(want);
        assert_eq!(rep.quiet, want, "old {al}+{ar}, own {bl}+{br}: {rep:?}");
        assert_eq!(state(&b_dir).seal_quiet_ms, want.as_millis() as u64);
        let h = head(&*store);
        assert_eq!(
            (h.lease_ms, h.request_timeout_ms),
            (Some(bl), Some(br)),
            "{h:?}"
        );
    }
}

/// A head without the fields (written before this change) leaves the quiet at the new
/// writer's own values.
#[test]
fn quiet_of_a_head_without_the_fields_is_the_own_sum() {
    let store = mem();
    let _a = left_in_a_seal_with(&store, 1000, 2000);
    let h = Head {
        lease_ms: None,
        request_timeout_ms: None,
        ..head(&*store)
    };
    store.put(&keys().head(), &h.encode()).unwrap();
    let bt = tempdir();
    let b_dir = fresh(&bt);
    let rep = crate::takeover_with_store(timeouts_config(&b_dir, 300, 500), store.clone()).unwrap();
    assert_eq!(rep.quiet, QUIET, "{rep:?}");
}

/// A head whose `lease_ms + request_timeout_ms` is over a day is `Corrupt` at the takeover,
/// not a day-long wait.
#[test]
fn quiet_over_max_is_corrupt() {
    let store = mem();
    let _a = left_in_a_seal_with(&store, 300, 500);
    let huge = Head {
        lease_ms: Some(86_400_000),
        request_timeout_ms: Some(1),
        ..head(&*store)
    };
    store.put(&keys().head(), &huge.encode()).unwrap();
    let bt = tempdir();
    let b_dir = fresh(&bt);
    match crate::takeover_with_store(timeouts_config(&b_dir, 300, 500), store.clone()) {
        Err(Error::Corrupt(m)) => assert!(m.contains("head"), "{m}"),
        other => panic!("{other:?}"),
    }
}

/// Every head a writer writes carries its own lease and request timeout: the create, a seal
/// and an open's epoch raise (the takeover's head is checked in `quiet_is_larger_of_both_writers`).
#[test]
fn every_head_carries_the_writers_timeouts() {
    let store = mem();
    let a = tempdir();
    let own = |h: &Head| (h.lease_ms, h.request_timeout_ms);
    let db = open_with_store(timeouts_config(a.path(), 300, 500), store.clone()).unwrap();
    assert_eq!(own(&head(&*store)), (Some(300), Some(500)), "create");
    let c = db.connect().unwrap();
    create(&c);
    insert(&c, 1).unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    assert_eq!(own(&head(&*store)), (Some(300), Some(500)), "seal");
    c.close().unwrap();
    db.close().unwrap();
    let db = open_with_store(timeouts_config(a.path(), 350, 550), store.clone()).unwrap();
    assert_eq!(own(&head(&*store)), (Some(350), Some(550)), "raise at open");
    db.close().unwrap();
}
