//! Task 8: `Connection::run`, `describe` and `is_autocommit` in MVCC mode.
use super::*;

#[test]
fn run_refuses_journal_mode_wal_in_mvcc() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 1);
    for sql in [
        "PRAGMA journal_mode=wal",
        "pragma journal_mode = 'WAL'",
        "PRAGMA main.journal_mode=wal",
    ] {
        let r = c.run(sql, &[], &[]);
        assert!(
            matches!(r, Err(Error::Unsupported(_))),
            "run({sql}) = {r:?}"
        );
        let r = c.describe(sql);
        assert!(
            matches!(r, Err(Error::Unsupported(_))),
            "describe({sql}) = {r:?}"
        );
        let r = c.execute(sql, &[]);
        assert!(matches!(r, Err(Error::Unsupported(_))), "execute: {r:?}");
    }
    // Reading the mode is fine, and says mvcc.
    let out = c.run("PRAGMA journal_mode", &[], &[]).unwrap();
    assert_eq!(text(&out.rows[0][0]), "mvcc");
    insert_c(&c, 2).unwrap();
    c.close().unwrap();
    db.close().unwrap();
}

/// `run` of a COMMIT is acked like `execute`'s, and a failed upload is an `Err`.
#[test]
fn run_commit_failure_returns_err_and_is_autocommit_tracks_it() {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 1);
    assert!(c.is_autocommit());
    let acked = db.upload_stats().commits_acked;
    c.run("BEGIN CONCURRENT", &[], &[]).unwrap();
    assert!(!c.is_autocommit());
    c.run(
        "INSERT INTO t(id, v) VALUES (?, ?)",
        &[Value::from_i64(2), Value::build_text("two")],
        &[],
    )
    .unwrap();
    c.run("COMMIT", &[], &[]).unwrap();
    assert!(c.is_autocommit());
    assert_eq!(
        db.upload_stats().commits_acked,
        acked + 1,
        "the run COMMIT was acked"
    );
    assert_eq!(bucket_rows(&*mem, None), vec![1, 2]);

    // The upload fails.
    c.run("BEGIN CONCURRENT", &[], &[]).unwrap();
    c.run("INSERT INTO t(id, v) VALUES (3, 'three')", &[], &[])
        .unwrap();
    store.only_keys_containing("/log/");
    store.fail_puts_after(0);
    let r = c.run("COMMIT", &[], &[]);
    store.fail_puts_after(u64::MAX);
    store.only_keys_containing("");
    assert!(r.is_err(), "{r:?}");
    assert_eq!(db.upload_stats().commits_acked, acked + 1, "not acked");
    // In MVCC a failed upload keeps the commit locally (the engine's transaction ended, the
    // log holds its frame), so the connection is back in autocommit; unlike WAL mode, where
    // the failed commit is rolled back. The caller got an Err, so the row is in doubt: it is
    // visible locally and not acked, and the next commit's sync ships it.
    assert!(
        c.is_autocommit(),
        "MVCC: the failed COMMIT ended the transaction"
    );
    assert_eq!(ids(&c), vec![1, 2, 3], "kept locally");
    assert_eq!(bucket_rows(&*mem, None), vec![1, 2], "not in the bucket");
    insert_c(&c, 4).unwrap();
    assert_eq!(
        bucket_rows(&*mem, None),
        vec![1, 2, 3, 4],
        "shipped with the next"
    );
    assert_eq!(db.upload_stats().commits_acked, acked + 2);
    c.close().unwrap();
    db.close().unwrap();
}

/// `run` waits at the maintenance gate like `execute`.
#[test]
fn run_waits_for_maintenance_gate() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 1);
    assert!(db.shared.gate.close_and_drain(Duration::from_secs(5)));
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::scope(|s| {
        s.spawn(|| {
            let r = c.run("SELECT count(*) FROM t", &[], &[]).unwrap();
            let d = c.describe("SELECT id FROM t").unwrap();
            tx.send((r.rows[0][0].as_int().unwrap(), d.columns.len()))
                .unwrap();
        });
        assert!(
            rx.recv_timeout(Duration::from_millis(150)).is_err(),
            "run got through a closed gate"
        );
        db.shared.gate.open();
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), (1, 1));
    });
    c.close().unwrap();
    db.close().unwrap();
}

/// `BEGIN CONCURRENT` through `run` and through `execute`: both commit, both are acked and
/// shipped, and `describe` neither steps nor acks.
#[test]
fn begin_concurrent_via_run_and_execute() {
    let mem = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(mem.clone(), mvcc(&dir), 0);
    let other = db.connect().unwrap();
    let acked = db.upload_stats().commits_acked;
    c.run("BEGIN CONCURRENT", &[], &[]).unwrap();
    other.execute("BEGIN CONCURRENT", &[]).unwrap();
    assert!(!c.is_autocommit() && !other.is_autocommit());
    c.run("INSERT INTO t(id, v) VALUES (1, 'a')", &[], &[])
        .unwrap();
    other
        .execute("INSERT INTO t(id, v) VALUES (2, 'b')", &[])
        .unwrap();
    // Describing an INSERT runs nothing and acks nothing.
    let puts = db.upload_stats().log_puts;
    let d = c.describe("INSERT INTO t(id, v) VALUES (9, 'z')").unwrap();
    assert!(!d.is_readonly);
    assert_eq!(db.upload_stats().log_puts, puts);
    c.run("COMMIT", &[], &[]).unwrap();
    other.execute("COMMIT", &[]).unwrap();
    assert!(c.is_autocommit() && other.is_autocommit());
    assert_eq!(db.upload_stats().commits_acked, acked + 2);
    assert_eq!(ids(&c), vec![1, 2], "describe inserted nothing");
    assert_eq!(bucket_rows(&*mem, None), vec![1, 2]);
    other.close().unwrap();
    c.close().unwrap();
    db.close().unwrap();
}
