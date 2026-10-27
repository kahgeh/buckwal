//! Another tool changes `app.db` while buckwal is closed (spec outside-change-detect).
//! The tool here is turso's own engine, opened on the file without buckwal.
use super::*;
use crate::files::DB_FILE;
use turso_core::{
    Database as Engine, DatabaseOpts, OpenFlags, OpenOptions, PlatformIO, SqliteDialect, IO,
};

/// What `buckwal` is not: open `app.db` with the engine alone (vacuum turned on) and run
/// `VACUUM`. The engine removes the `-wal` when it closes.
pub(super) fn outside_vacuum(dir: &Path) {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let path = dir.join(DB_FILE);
    let opts = OpenOptions::new(Arc::new(SqliteDialect))
        .flags(OpenFlags::Create)
        .db_opts(DatabaseOpts::new().with_vacuum(true));
    let db = Engine::open(io, path.to_str().unwrap(), opts).unwrap();
    let conn = db.connect().unwrap();
    conn.prepare("VACUUM").unwrap().run_collect_rows().unwrap();
    conn.close().unwrap();
}

/// A closed writer with 200 rows of which the first 150 were deleted: a vacuum moves rows.
fn closed_writer(store: &Arc<MemStore>, dir: &Path) {
    let db = open_mem(store, dir);
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=200 {
        c.execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            &[Value::from_i64(id), Value::build_text("x".repeat(500))],
        )
        .unwrap();
    }
    c.execute("DELETE FROM t WHERE id <= 150", &[]).unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    for id in 201..=210 {
        insert(&c, id).unwrap();
    }
    c.close().unwrap();
    db.close().unwrap();
}

fn state_of(db: &Database) -> (String, Vec<i64>) {
    let c = db.connect().unwrap();
    let integrity = text(&c.query("PRAGMA integrity_check", &[]).unwrap()[0][0]);
    let ids = c
        .query("SELECT id FROM t ORDER BY id", &[])
        .map(|r| r.iter().map(|r| r[0].as_int().unwrap()).collect())
        .unwrap_or_default();
    (integrity, ids)
}

/// Test 1, on today's code: the open succeeds on a vacuumed file. The damage shows after a
/// later commit: the local database and a restore of the bucket then differ.
#[test]
fn outside_vacuum_after_close_corrupts_today() {
    let store = mem();
    let dir = tempdir();
    closed_writer(&store, dir.path());
    outside_vacuum(dir.path());
    let db = open_mem(&store, dir.path());
    let c = db.connect().unwrap();
    insert(&c, 300).unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    let local = state_of(&db);
    c.close().unwrap();
    db.close().unwrap();
    let r = tempdir();
    restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r)).unwrap();
    let restored = open_with_store(config(&fresh(&r)), store.clone()).unwrap();
    let from_bucket = state_of(&restored);
    println!(
        "BASELINE local integrity={:?} rows={}",
        local.0,
        local.1.len()
    );
    println!(
        "BASELINE restore integrity={:?} rows={}",
        from_bucket.0,
        from_bucket.1.len()
    );
    assert_ne!(local, from_bucket, "the damage did not show");
}

/// Test 2, on today's code: while the writer is open, does the engine's lock stop `sqlite3`?
/// The outcome goes to the output (it is the baseline); the test asserts nothing about it.
#[test]
fn outside_change_while_open_sqlite3() {
    let sqlite3 = Path::new("/usr/bin/sqlite3");
    if !sqlite3.exists() {
        println!("BASELINE2 skipped: /usr/bin/sqlite3 does not exist");
        return;
    }
    let store = mem();
    let dir = tempdir();
    closed_writer(&store, dir.path());
    let db = open_mem(&store, dir.path());
    let c = db.connect().unwrap();
    insert(&c, 500).unwrap();
    let out = std::process::Command::new(sqlite3)
        .arg(dir.path().join(DB_FILE))
        .arg("VACUUM;")
        .output()
        .unwrap();
    println!(
        "BASELINE2 sqlite3 while open: status={:?} stderr={:?}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    c.close().unwrap();
    db.close().unwrap();
}
