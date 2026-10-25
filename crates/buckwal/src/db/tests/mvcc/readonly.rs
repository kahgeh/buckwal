//! Task 7: a read-only MVCC open (`verify`). The engine's bootstrap may write the local
//! files (backfill, the log header, the `-wal` truncate); those writes stay local and
//! nothing reaches the bucket. A read-only WAL open still refuses every engine write.
use super::*;
use crate::store::StatsSnap;

fn integrity_of(c: &Connection) -> String {
    text(&c.query("PRAGMA integrity_check", &[]).unwrap()[0][0])
}

/// The rows a restore into a new directory holds (a writer open: it claims).
fn restored_rows(store: &Arc<MemStore>, want: &[i64]) {
    let t = tempdir();
    let db = open_mvcc(store.clone(), mvcc(&fresh(&t)));
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), want);
    assert_eq!(integrity_of(&c), "ok");
    crash(db, vec![c]);
}

fn files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap())
        .filter(|e| e.file_type().unwrap().is_file() && e.file_name() != "durable.lock")
        .map(|e| {
            (
                e.file_name().to_string_lossy().into_owned(),
                std::fs::read(e.path()).unwrap(),
            )
        })
        .collect();
    v.sort();
    v
}

/// The bucket after a seal that failed: kind-1 frames next to the whole kind-3 log (the
/// crash between the checkpoint's WAL frames and its log truncate).
fn mid_checkpoint_bucket() -> (Arc<MemStore>, Vec<i64>) {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = switch_fails_then_two_commits(&store, &dir);
    crash(db, vec![c]);
    (mem, (1..=12).collect())
}

fn plain_bucket() -> (Arc<MemStore>, Vec<i64>) {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 5);
    crash(db, vec![c]);
    (store, (1..=5).collect())
}

fn restore_into(store: &Arc<MemStore>, d: &TempDir) -> PathBuf {
    let dir = fresh(d);
    restore_with_store(&mvcc(&dir), store.clone(), &dir).unwrap();
    dir
}

fn counts(store: &MemStore) -> (u64, u64) {
    let s: StatsSnap = store.stats();
    (s.puts, s.deletes)
}

/// Read-only open of `dir`: rows, integrity, and the bucket's PUT and DELETE counts.
fn verify_open(store: &Arc<MemStore>, dir: &Path, want: &[i64]) {
    let before = counts(store);
    let db = open_read_only_with_store(mvcc(dir), store.clone())
        .unwrap_or_else(|e| panic!("read-only open: {e}"));
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), want);
    assert_eq!(integrity_of(&c), "ok");
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(counts(store), before, "a read-only open reached the bucket");
}

#[test]
fn read_only_mvcc_open_of_mid_checkpoint_restore_succeeds_and_ships_nothing() {
    let (store, want) = mid_checkpoint_bucket();
    let d = tempdir();
    let dir = restore_into(&store, &d);
    assert!(
        !read(&dir, "app.db-wal").is_empty(),
        "frames next to the log"
    );
    let objects = store.list("").unwrap();
    verify_open(&store, &dir, &want);
    assert_eq!(
        store.list("").unwrap(),
        objects,
        "no object added or removed"
    );
    // Observed: the engine's bootstrap emptied the `-wal` (its frames are in the pages the
    // restore wrote or in the log) locally; the bucket still holds them.
    assert!(
        read(&dir, "app.db-wal").is_empty(),
        "the -wal was cut locally"
    );
    verify_open(&store, &dir, &want);
}

#[test]
fn read_only_plain_restore_runs_early_wal_truncate_locally() {
    let (store, want) = plain_bucket();
    let d = tempdir();
    let dir = restore_into(&store, &d);
    let before = files(&dir);
    verify_open(&store, &dir, &want);
    let after = files(&dir);
    let changed: Vec<_> = before
        .iter()
        .zip(after.iter())
        .filter(|(b, a)| b != a)
        .map(|(b, _)| b.0.as_str())
        .collect();
    // Observed: a restored format 5 MVCC directory holds the whole first pack as `app.db`
    // (one block, local after the restore), so the bootstrap writes no file: the database
    // file, the log, the empty `-wal` and the state stay as they were. (A format 2 restore
    // left an empty `app.db`, which the bootstrap wrote.)
    assert_eq!(changed, Vec::<&str>::new());
    assert!(read(&dir, "app.db-wal").is_empty());
}

/// A read-only WAL database still refuses every engine write (the wrappers are strict
/// outside MVCC): the `-wal` and the database file.
#[test]
fn read_only_wal_database_still_refuses_writes() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let db = open_mem(&store, &dir);
    let c = db.connect().unwrap();
    create(&c);
    insert(&c, 1).unwrap();
    c.close().unwrap();
    db.close().unwrap();
    let before = counts(&store);
    let v = tempdir();
    let ro = open_read_only_with_store(config(v.path()), store.clone()).unwrap();
    let io = ro.engine_io();
    let wal = std::fs::canonicalize(v.path()).unwrap().join("app.db-wal");
    let f = io
        .open_file(wal.to_str().unwrap(), OpenFlags::Create, false)
        .unwrap();
    let buf = || Arc::new(Buffer::new(vec![1u8; 8]));
    for call in 0..3 {
        stash::clear();
        let r = match call {
            0 => f.pwrite(0, buf(), Completion::new_write(|_| {})),
            1 => f.pwritev(0, vec![buf()], Completion::new_write(|_| {})),
            _ => f.truncate(0, Completion::new_trunc(|_| {})),
        };
        assert!(r.is_err(), "call {call}");
        assert_eq!(
            stash::take(),
            Some(Error::Unsupported("writes on a read-only open")),
            "call {call}"
        );
    }
    ro.close().unwrap();
    assert_eq!(counts(&store), before);
}

/// An MVCC directory with no `app.db-log`. When the bucket's log image has bytes, reconcile
/// (as for any open) fills the file from the image; when the image is empty (a checkpointed
/// generation) the file is not there to open and a read-only open fails with a clear error
/// instead of creating one.
#[test]
fn read_only_mvcc_does_not_create_the_log_when_absent() {
    let (store, want) = plain_bucket();
    let d = tempdir();
    let dir = restore_into(&store, &d);
    let image = read(&dir, "app.db-log");
    std::fs::remove_file(dir.join("app.db-log")).unwrap();
    verify_open(&store, &dir, &want);
    assert_eq!(read(&dir, "app.db-log"), image, "refilled from the image");

    let store = mem();
    let t = tempdir();
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&fresh(&t)), 3);
    truncate_checkpoint(&c).unwrap();
    crash(db, vec![c]);
    let d = tempdir();
    let dir = restore_into(&store, &d);
    assert!(read(&dir, "app.db-log").is_empty());
    std::fs::remove_file(dir.join("app.db-log")).unwrap();
    let puts = counts(&store);
    let r = open_read_only_with_store(mvcc(&dir), store.clone());
    let e = r.expect_err("a read-only open without a log");
    assert!(e.to_string().contains("app.db-log is missing"), "{e}");
    assert!(
        !dir.join("app.db-log").exists(),
        "a read-only open made the log"
    );
    assert_eq!(counts(&store), puts);
}

/// A read-only open, then a writer open of the same directory (a restored directory is
/// unclaimed, so this claims): reconcile takes the bootstrap's local writes, every row is
/// there, and the writer keeps going.
fn verify_then_writer(store: &Arc<MemStore>, want: &[i64]) {
    let d = tempdir();
    let dir = restore_into(store, &d);
    verify_open(store, &dir, want);
    verify_open(store, &dir, want);
    let db = open_mvcc(store.clone(), mvcc(&dir));
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), want);
    assert_eq!(integrity_of(&c), "ok");
    let next = want.last().unwrap() + 1;
    insert_c(&c, next).unwrap();
    truncate_checkpoint(&c).unwrap();
    insert_c(&c, next + 1).unwrap();
    let mut all = want.to_vec();
    all.extend([next, next + 1]);
    assert_eq!(ids(&c), all);
    crash(db, vec![c]);
    restored_rows(store, &all);
}

#[test]
fn read_only_mvcc_open_then_writer_open_of_a_plain_restore() {
    let (store, want) = plain_bucket();
    verify_then_writer(&store, &want);
}

#[test]
fn read_only_mvcc_open_then_writer_open_of_a_mid_checkpoint_restore() {
    let (store, want) = mid_checkpoint_bucket();
    verify_then_writer(&store, &want);
}
