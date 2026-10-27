//! Every path that commits has its row in the bucket when its ack returns. A commit's sync
//! ships the pending `-wal` ops only on the thread that wrote the last of them
//! (`take_own_pending`). Each test restores the bucket at once after the ack and requires the
//! row. A sync that ships nothing for its own commit fails every test here.
use super::*;
use crate::{open_read_only_with_store, Batching};
use std::thread;

/// The row each test commits last.
const ROW: i64 = 1000;

/// The ids a restore of `store` into a new directory holds.
fn restored_ids(store: &Arc<MemStore>) -> Vec<i64> {
    let t = tempdir();
    let dir = fresh(&t);
    let cfg = config(&dir);
    restore_with_store(&cfg, store.clone(), &dir).unwrap_or_else(|e| panic!("restore: {e}"));
    let db = open_read_only_with_store(cfg, store.clone()).unwrap();
    let c = db.connect().unwrap();
    let found = ids(&c);
    c.close().unwrap();
    db.close().unwrap();
    found
}

/// Print the committed row and the restore's ids, then require the row in the restore.
fn require_row(test: &str, store: &Arc<MemStore>) {
    let restored = restored_ids(store);
    eprintln!("{test}: row {ROW}, restore holds {restored:?}");
    assert!(
        restored.contains(&ROW),
        "{test}: the commit of row {ROW} was acked and is not in the bucket"
    );
}

/// A writer on `store` with table `t` and the acked base row 1.
fn writer(store: &Arc<MemStore>, dir: &Path, b: Batching) -> (Database, Connection) {
    let db = open_with_store(config_batching(dir, b), store.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    insert(&c, 1).unwrap();
    (db, c)
}

#[test]
fn autocommit_insert_is_in_the_bucket_at_the_ack() {
    let store = mem();
    let dir = tempdir();
    let (db, c) = writer(&store, dir.path(), Batching::Off);
    insert(&c, ROW).unwrap();
    require_row("autocommit", &store);
    c.close().unwrap();
    db.close().unwrap();
}

#[test]
fn commit_on_another_thread_after_a_spill_is_in_the_bucket_at_the_ack() {
    let store = mem();
    let dir = tempdir();
    let (db, c) = writer(&store, dir.path(), Batching::Off);
    c.execute("BEGIN", &[]).unwrap();
    let wal = dir.path().join(crate::files::WAL_FILE);
    let before = std::fs::metadata(&wal).map_or(0, |m| m.len());
    // A blob of 9 MB is about 2200 pages: more than the page cache holds before it spills.
    c.execute(
        "INSERT INTO t(id, v) VALUES (?, randomblob(9000000))",
        &[Value::from_i64(ROW)],
    )
    .unwrap();
    // The spill is in the local `-wal` before the COMMIT. If the engine stops it, the test fails.
    let spilled = std::fs::metadata(&wal).map_or(0, |m| m.len());
    assert!(
        spilled > before,
        "no page spilled: -wal {before} -> {spilled} bytes"
    );
    eprintln!("commit_on_another_thread: -wal {before} -> {spilled} bytes before COMMIT");
    let spill_thread = thread::current().id();
    // The thread of the COMMIT writes its last op, so it must run the sync.
    let (c, commit_thread) = thread::spawn(move || {
        c.execute("COMMIT", &[]).unwrap();
        let id = thread::current().id();
        (c, id)
    })
    .join()
    .unwrap();
    assert_ne!(
        spill_thread, commit_thread,
        "COMMIT ran on the spill thread"
    );
    require_row("commit_on_another_thread", &store);
    c.close().unwrap();
    db.close().unwrap();
}

#[test]
fn window_commit_is_in_the_bucket_when_its_call_returns() {
    let store = mem();
    let dir = tempdir();
    let window = Batching::Window {
        max_wait: Duration::from_millis(5),
        max_commits: 64,
        max_bytes: 1 << 20,
    };
    let (db, c) = writer(&store, dir.path(), window);
    insert(&c, ROW).unwrap();
    require_row("window", &store);
    c.close().unwrap();
    db.close().unwrap();
}

#[test]
fn background_commit_is_in_the_bucket_once_durable() {
    let store = mem();
    let dir = tempdir();
    let (db, c) = writer(&store, dir.path(), Batching::Off);
    c.set_batching(Some(Batching::Background {
        max_wait: Duration::from_millis(5),
        max_commits: 64,
        max_bytes: 1 << 20,
        max_behind: 8,
    }))
    .unwrap();
    insert(&c, ROW).unwrap();
    let commit = c.last_commit().expect("the insert made a commit");
    db.wait_durable(commit, Duration::from_secs(20)).unwrap();
    assert!(db.durable_commit() >= commit);
    require_row("background", &store);
    c.close().unwrap();
    db.close().unwrap();
}
