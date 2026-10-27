//! Another tool changes `app.db` while buckwal is closed (spec outside-change-detect).
//! The tool here is turso's own engine, opened on the file without buckwal.
use super::*;
use crate::files::DB_FILE;
use crate::telemetry::testing::Capture;
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

const REFUSED: &str =
    "app.db was changed outside buckwal since buckwal last used it; restore into a new directory";

/// The bytes of every file in `dir`, by name.
fn snapshot(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_type().unwrap().is_file() && e.file_name() != "durable.lock")
        .map(|e| {
            (
                e.file_name().to_string_lossy().into_owned(),
                std::fs::read(e.path()).unwrap(),
            )
        })
        .collect()
}

fn objects(store: &Arc<MemStore>) -> Vec<String> {
    store.list("").unwrap()
}

/// The refusal: `Corrupt` with the exact text.
fn assert_refused<T: std::fmt::Debug>(r: Result<T>) {
    match r {
        Err(Error::Corrupt(m)) => assert_eq!(m, REFUSED),
        r => panic!("not refused: {r:?}"),
    }
}

/// Test 3: an outside `VACUUM` after a clean close. The open is refused, logs one ERROR with
/// the file and the detail, and changes nothing, locally and in the bucket.
#[test]
fn outside_vacuum_after_close_is_refused_and_changes_nothing() {
    let store = mem();
    let dir = tempdir();
    closed_writer(&store, dir.path());
    outside_vacuum(dir.path());
    let (files, objs) = (snapshot(dir.path()), objects(&store));
    let cap = Capture::new();
    let r = tracing::subscriber::with_default(cap.subscriber(), || {
        open_with_store(config(dir.path()), store.clone())
    });
    assert_refused(r);
    assert_eq!(
        snapshot(dir.path()),
        files,
        "the refused open changed a file"
    );
    assert_eq!(objects(&store), objs, "the refused open uploaded");
    let spans = cap.spans();
    let errors: Vec<_> = spans
        .events
        .iter()
        .filter(|e| e.field("message") == Some("files changed outside buckwal"))
        .collect();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].level, tracing::Level::ERROR);
    assert_eq!(errors[0].field("file"), Some("app.db"));
    assert!(matches!(
        errors[0].field("detail"),
        Some("size" | "time" | "page1")
    ));
    assert_eq!(
        spans.last_named("buckwal.open").field("buckwal.outcome"),
        Some("corrupt")
    );
}

/// Test 4: frames that another tool left in the `-wal` after a clean close are refused and not
/// uploaded. (The bytes stand for the frames: the check reads the size and the header.)
#[test]
fn outside_wal_content_after_close_is_refused_and_not_uploaded() {
    let store = mem();
    let dir = tempdir();
    closed_writer(&store, dir.path());
    std::fs::write(dir.path().join("app.db-wal"), vec![0x55u8; 4096 + 32]).unwrap();
    let (files, objs) = (snapshot(dir.path()), objects(&store));
    assert_refused(open_with_store(config(dir.path()), store.clone()));
    assert_eq!(snapshot(dir.path()), files);
    assert_eq!(objects(&store), objs);
}

/// Test 5: a normal close and reopen, many times, never refuses.
#[test]
fn reopen_many_times_never_refuses() {
    for (mvcc, encrypted) in [(false, false), (false, true), (true, false), (true, true)] {
        let store = mem();
        let (dir, keys) = (tempdir(), tempdir());
        let mut cfg = if encrypted {
            enc_config(dir.path(), &keys, &key_hex())
        } else {
            config(dir.path())
        };
        if mvcc {
            cfg.journal_mode = JournalMode::Mvcc;
        }
        for i in 0..10 {
            let db = open_with_store(cfg.clone(), store.clone())
                .unwrap_or_else(|e| panic!("mvcc={mvcc} encrypted={encrypted} open {i}: {e}"));
            let c = db.connect().unwrap();
            if i == 0 {
                create(&c);
            }
            if mvcc {
                c.execute("BEGIN CONCURRENT", &[]).unwrap();
                insert(&c, i + 1).unwrap();
                c.execute("COMMIT", &[]).unwrap();
            } else {
                insert(&c, i + 1).unwrap();
            }
            c.close().unwrap();
            db.close().unwrap();
        }
    }
}

/// Test 9: a read-only open (`buckwal verify`), then a writer open: no false refusal.
#[test]
fn read_only_open_then_writer_open_is_not_refused() {
    let store = mem();
    let dir = tempdir();
    closed_writer(&store, dir.path());
    for _ in 0..2 {
        let ro = open_read_only_with_store(config(dir.path()), store.clone()).unwrap();
        let c = ro.connect().unwrap();
        c.query("SELECT count(*) FROM t", &[]).unwrap();
        c.close().unwrap();
        ro.close().unwrap();
        let db = open_mem(&store, dir.path());
        db.close().unwrap();
    }
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
