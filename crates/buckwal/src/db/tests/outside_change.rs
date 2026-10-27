//! Another tool changes `app.db` while buckwal is closed (spec outside-change-detect).
//! The tool here is turso's own engine, opened on the file without buckwal.
use super::*;
use crate::files::DB_FILE;
use crate::telemetry::testing::Capture;
use std::sync::Mutex;
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

/// Rows of `t` as ids, from a database that opened without a refusal.
fn open_ids(store: &Arc<MemStore>, dir: &Path) -> Vec<i64> {
    let db = open_with_store(config(dir), store.clone())
        .unwrap_or_else(|e| panic!("the open of {} was refused: {e}", dir.display()));
    let c = db.connect().unwrap();
    let ids = c
        .query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect();
    c.close().unwrap();
    db.close().unwrap();
    ids
}

/// A writer with rows 1..=20, a TRUNCATE checkpoint, then rows 21..=30 that stay in the `-wal`.
fn writer_with_wal_rows(store: &Arc<MemStore>, dir: &Path) -> (Database, Connection) {
    let db = open_mem(store, dir);
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=20 {
        insert(&c, id).unwrap();
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    for id in 21..=30 {
        insert(&c, id).unwrap();
    }
    (db, c)
}

/// Test 6: a checkpoint, then more commits, then a crash. The open succeeds and has every row.
#[test]
fn crash_after_checkpoint_and_more_commits_opens() {
    let store = mem();
    let a = tempdir();
    let (db, c) = writer_with_wal_rows(&store, a.path());
    let img = copy_db_files(a.path());
    crash(db, vec![c]);
    let rec = state(img.path())
        .files
        .expect("a record after the checkpoint");
    assert!(!rec.clean_close && !rec.db_write_open);
    assert_eq!(open_ids(&store, img.path()), (1..=30).collect::<Vec<_>>());
}

/// Test 7, the explicit checkpoint: a crash after `begin_write` and before the write ends.
/// `app.db` is then in an unknown state (here: a new modification time), and the open succeeds.
#[test]
fn crash_in_the_middle_of_a_checkpoint_opens() {
    // Modes that restart the WAL also switch the generation in the bucket, which a copy taken in
    // the middle of the checkpoint cannot match; the write path is the same.
    for mode in [CheckpointMode::Passive, CheckpointMode::Full] {
        let store = mem();
        let a = tempdir();
        let (db, c) = writer_with_wal_rows(&store, a.path());
        let image = Arc::new(Mutex::new(None::<TempDir>));
        let (from, slot) = (a.path().to_path_buf(), image.clone());
        let hook: crate::file_record::WriteHook = Arc::new(move || {
            slot.lock()
                .unwrap()
                .get_or_insert_with(|| copy_db_files(&from));
        });
        crate::file_record::AFTER_BEGIN
            .lock()
            .unwrap()
            .push((a.path().canonicalize().unwrap(), hook));
        c.checkpoint(mode).unwrap();
        crate::file_record::AFTER_BEGIN
            .lock()
            .unwrap()
            .retain(|(d, _)| *d != a.path().canonicalize().unwrap());
        crash(db, vec![c]);
        let img = image
            .lock()
            .unwrap()
            .take()
            .expect("the checkpoint wrote nothing");
        assert!(state(img.path()).files.unwrap().db_write_open);
        std::fs::File::options()
            .write(true)
            .open(img.path().join(DB_FILE))
            .unwrap()
            .set_modified(std::time::SystemTime::now() + Duration::from_secs(30))
            .unwrap();
        assert_eq!(open_ids(&store, img.path()), (1..=30).collect::<Vec<_>>());
    }
}

/// Test 7, the seed of a never-sealed database at open: a crash image of a writer that never
/// checkpointed has an empty `app.db` and frames in the `-wal`. The open seeds `app.db`, and a
/// crash after the seed, then a reopen, is not refused.
#[test]
fn crash_around_the_seed_of_a_never_sealed_database_opens() {
    let store = mem();
    let a = tempdir();
    let db = open_mem(&store, a.path());
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=10 {
        insert(&c, id).unwrap();
    }
    let img = copy_db_files(a.path());
    crash(db, vec![c]);
    let db = open_mem(&store, img.path());
    let c = db.connect().unwrap();
    let ids: Vec<i64> = c
        .query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect();
    assert_eq!(ids, (1..=10).collect::<Vec<_>>());
    let again = copy_db_files(img.path());
    crash(db, vec![c]);
    assert_eq!(open_ids(&store, again.path()), (1..=10).collect::<Vec<_>>());
}

/// Test 8: an outside change to `app.db` after a crash, while no `app.db` write was
/// unfinished, is refused: the `app.db` part of the check works after a crash too.
#[test]
fn outside_vacuum_after_a_crash_is_refused() {
    let store = mem();
    let a = tempdir();
    let (db, c) = writer_with_wal_rows(&store, a.path());
    let img = copy_db_files(a.path());
    crash(db, vec![c]);
    let rec = state(img.path()).files.unwrap();
    assert!(!rec.clean_close && !rec.db_write_open);
    outside_vacuum(img.path());
    assert_refused(open_with_store(config(img.path()), store.clone()));
}

/// Test 10: after a refusal, a restore into a new directory has every acked commit.
#[test]
fn restore_after_a_refusal_has_every_acked_commit() {
    let store = mem();
    let dir = tempdir();
    closed_writer(&store, dir.path());
    outside_vacuum(dir.path());
    assert_refused(open_with_store(config(dir.path()), store.clone()));
    let r = tempdir();
    restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r)).unwrap();
    assert_eq!(
        open_ids(&store, &fresh(&r)),
        (151..=210).collect::<Vec<_>>()
    );
}

/// Test 7, the lazy page fetch of formats 1 to 4: a read that fetches a page writes `app.db`,
/// so it follows rule 1. A crash after the read, with a checkpoint's record before it, is not
/// refused.
#[test]
fn crash_after_a_lazy_page_fetch_opens() {
    let _legacy = crate::format::LegacySeal::on();
    let store = mem();
    let _a = super::closed_writer(&store, 3000);
    let r = tempdir();
    restore_with_store(&config(r.path()), store.clone(), r.path()).unwrap();
    let db = open_mem(&store, r.path());
    let c = db.connect().unwrap();
    insert(&c, 3001).unwrap();
    c.checkpoint(CheckpointMode::Passive).unwrap();
    let lazy = || {
        let open = db.shared.open.lock().unwrap();
        open.as_ref().unwrap().engine.storage.lazy_gets()
    };
    let gets_before = lazy();
    let before = state(r.path())
        .files
        .expect("a record after the checkpoint");
    assert!(!before.db_write_open);
    // Reads every page: the pages that are not local yet are fetched into `app.db`.
    let n = c.query("SELECT count(*) FROM t", &[]).unwrap()[0][0]
        .as_int()
        .unwrap();
    assert_eq!(n, 3001);
    assert!(lazy() > gets_before, "the read fetched no page");
    let img = copy_db_files(r.path());
    crash(db, vec![c]);
    assert_eq!(open_ids(&store, img.path()), (1..=3001).collect::<Vec<_>>());
}
