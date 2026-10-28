//! The held WAL header (`io/wal_file.rs`). The commit that restarts the WAL sends the new
//! WAL header and its own frames in one log record. These tests cover the Off, Window and
//! background paths, a failed or in-doubt first record, crash copies of the directory, MVCC and a
//! replica. Each test prints its named lines, and each ends with a restore that holds every
//! acked row.
use super::*;
use crate::format::Op;
use crate::reconcile::Reconciled;
use crate::store::fault::HoldStore;
use crate::{open_read_only_with_store, Batching};
use std::sync::mpsc::{Receiver, Sender};

/// The size of a WAL header.
const HEADER: usize = 32;

fn window() -> Batching {
    Batching::Window {
        max_wait: Duration::from_millis(5),
        max_commits: 64,
        max_bytes: 1 << 20,
    }
}

fn background() -> Batching {
    Batching::Background {
        max_wait: Duration::from_millis(5),
        max_commits: 64,
        max_bytes: 1 << 20,
        max_behind: 8,
    }
}

/// A `config_batching` without the packer thread, so only the test writes to the bucket.
fn cfg(dir: &Path, b: Batching) -> Config {
    let mut c = config_batching(dir, b);
    c.pack = false;
    c.retry_deadline = Duration::from_millis(300);
    c
}

/// The ids a restore of `store` into a new directory holds. The restore is checked with
/// `integrity_check`.
fn restored_ids(store: &Arc<MemStore>) -> Vec<i64> {
    let t = tempdir();
    let dir = fresh(&t);
    let c = config(&dir);
    restore_with_store(&c, store.clone(), &dir).unwrap_or_else(|e| panic!("restore: {e}"));
    let db = open_read_only_with_store(c, store.clone()).unwrap();
    let conn = db.connect().unwrap();
    integrity_ok(&conn);
    let found = ids(&conn);
    conn.close().unwrap();
    db.close().unwrap();
    found
}

/// Require that a restore holds exactly `want`, and print it.
fn require_restore(test: &str, store: &Arc<MemStore>, want: &[i64]) {
    let got = restored_ids(store);
    eprintln!(
        "{test}: restore holds {} rows, last {:?}",
        got.len(),
        got.last()
    );
    assert_eq!(got, want, "{test}: the restore differs from the acked rows");
}

/// The ops of record `seq` of generation `g`, in order.
fn ops_of(store: &Arc<MemStore>, g: u64, seq: u64) -> Vec<Op> {
    match record(store, g, seq) {
        LogRecord::Extents { syncs, .. } => syncs.into_iter().flatten().collect(),
        r => panic!("log/{g}/{seq} is not an extents record: {r:?}"),
    }
}

/// An op that writes the 32 byte WAL header at offset 0.
fn is_header(op: &Op) -> bool {
    matches!(op, Op::Write { off: 0, bytes } if bytes.len() == HEADER)
}

/// The log record seqs of generation `g` and the number of header ops each holds.
fn header_counts(store: &Arc<MemStore>, g: u64) -> Vec<(u64, usize)> {
    let mut seqs = log_seqs(&**store, g);
    seqs.sort();
    seqs.into_iter()
        .filter(|&s| matches!(record(store, g, s), LogRecord::Extents { .. }))
        .map(|s| {
            (
                s,
                ops_of(store, g, s).iter().filter(|o| is_header(o)).count(),
            )
        })
        .collect()
}

/// A writer on `store` with table `t` and the acked rows 1 to 20 in generation 1. A passive
/// checkpoint then backfills the whole WAL, so the next commit restarts it.
fn ready(store: Arc<dyn ObjectStore>, dir: &Path, b: Batching) -> (Database, Connection) {
    let db = open_with_store(cfg(dir, b), store).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=20 {
        insert(&c, id).unwrap();
    }
    c.checkpoint(CheckpointMode::Passive).unwrap();
    assert_eq!(db.generation(), 1);
    (db, c)
}

/// `ready` over a `FaultStore`, whose faults reach only the log records of generation 2.
fn ready_faulty(
    mem: &Arc<MemStore>,
    dir: &Path,
    b: Batching,
) -> (Arc<FaultStore>, Database, Connection) {
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let (db, c) = ready(fault.clone(), dir, b);
    fault.only_keys_containing(&keys().log_prefix(2));
    (fault, db, c)
}

fn rows(from: i64, to: i64) -> Vec<i64> {
    (from..=to).collect()
}

/// Require that the first record of generation 2 starts with the header and holds frames.
fn first_record_starts_with_header(test: &str, store: &Arc<MemStore>) {
    let ops = ops_of(store, 2, 1);
    let header_first = is_header(&ops[0]);
    eprintln!(
        "{test}: first record ops={} header_first={header_first}",
        ops.len()
    );
    assert!(header_first && ops.len() > 1, "{test}: {ops:?}");
    assert_eq!(
        header_counts(store, 2)[0],
        (1, 1),
        "{test}: one header in record 1"
    );
}

#[test]
fn off_restart_sends_header_with_commit() {
    let store = mem();
    let t = tempdir();
    let (db, c) = ready(store.clone(), t.path(), Batching::Off);
    insert(&c, 21).unwrap();
    assert_eq!(db.generation(), 2);
    assert_eq!(
        log_seqs(&*store, 2),
        [1],
        "one record for the new generation"
    );
    first_record_starts_with_header("off_restart", &store);
    require_restore("off_restart", &store, &rows(1, 21));
    c.close().unwrap();
    db.close().unwrap();
}

#[test]
fn window_restart_carries_the_header_in_its_batch() {
    let store = mem();
    let t = tempdir();
    let (db, c) = ready(store.clone(), t.path(), window());
    insert(&c, 21).unwrap();
    assert_eq!(db.generation(), 2);
    assert_eq!(
        log_seqs(&*store, 2),
        [1],
        "one record for the new generation"
    );
    first_record_starts_with_header("window_restart", &store);
    insert(&c, 22).unwrap();
    // The header goes once.
    assert_eq!(
        header_counts(&store, 2).iter().map(|h| h.1).sum::<usize>(),
        1
    );
    require_restore("window_restart", &store, &rows(1, 22));
    c.close().unwrap();
    db.close().unwrap();
}

#[test]
fn background_restart_ships_header_record_first() {
    let store = mem();
    let t = tempdir();
    let (db, c) = ready(store.clone(), t.path(), Batching::Off);
    c.set_batching(Some(background())).unwrap();
    insert(&c, 21).unwrap();
    let commit = c.last_commit().expect("a commit");
    db.wait_durable(commit, Duration::from_secs(20)).unwrap();
    assert_eq!(db.generation(), 2);
    assert_eq!(
        log_seqs(&*store, 2),
        [1, 2],
        "the header record, then the commit"
    );
    let first = ops_of(&store, 2, 1);
    let second = ops_of(&store, 2, 2);
    eprintln!(
        "background_restart: record 1 ops={} header_only={}, record 2 ops={} header_first={}",
        first.len(),
        first.len() == 1 && is_header(&first[0]),
        second.len(),
        is_header(&second[0])
    );
    assert!(first.len() == 1 && is_header(&first[0]), "{first:?}");
    assert!(!second.iter().any(is_header), "{second:?}");
    require_restore("background_restart", &store, &rows(1, 21));
    c.close().unwrap();
    db.close().unwrap();
}

/// The first commit of generation 2 fails with its record not stored (`Off` or `Window`).
/// The next commit ships the header again. The failed row is not acked and is not in the
/// restore.
fn failed_first_commit(test: &str, b: Batching) {
    let mem = mem();
    let t = tempdir();
    let (fault, db, c) = ready_faulty(&mem, t.path(), b);
    fault.fail_next_puts_permanently(1);
    let r = insert(&c, 21);
    assert!(r.is_err(), "{test}: the first commit was acked: {r:?}");
    assert_eq!(db.generation(), 2, "{test}");
    assert!(log_seqs(&*mem, 2).is_empty(), "{test}: nothing was stored");
    insert(&c, 22).unwrap();
    let seqs = log_seqs(&*mem, 2);
    let first = *seqs.first().expect("a record");
    let ops = ops_of(&mem, 2, first);
    assert!(is_header(&ops[0]), "{test}: {ops:?}");
    eprintln!("{test}: held header resent seq={first}");
    let acked: Vec<i64> = ids(&c);
    eprintln!("{test}: local rows {} last {:?}", acked.len(), acked.last());
    // An Off failure rolls the row back. A Window failure leaves its record owed, so the
    // row stays and the next record ships it. Row 21 was never acked in both cases.
    assert_eq!(acked.contains(&21), b != Batching::Off, "{test}: row 21");
    assert!(acked.contains(&22), "{test}: row 22");
    require_restore(test, &mem, &acked);
    c.close().unwrap();
    db.close().unwrap();
}

#[test]
fn failed_first_commit_resends_header() {
    failed_first_commit("failed_first_off", Batching::Off);
    failed_first_commit("failed_first_window", window());
}

/// The degraded uploader path: a Window first commit fails and its record is owed, so the
/// uploader is degraded. A background commit then blocks like an Off commit.
#[test]
fn failed_first_commit_degraded_path_resends_header() {
    let mem = mem();
    let t = tempdir();
    let (fault, db, c) = ready_faulty(&mem, t.path(), window());
    fault.fail_puts_after(0);
    let r = insert(&c, 21);
    assert!(r.is_err(), "the Window commit was acked: {r:?}");
    fault.fail_puts_after(u64::MAX);
    eprintln!("degraded: uploader degraded");
    c.set_batching(Some(background())).unwrap();
    insert(&c, 22).unwrap();
    let commit = c.last_commit().expect("a commit");
    db.wait_durable(commit, Duration::from_secs(20)).unwrap();
    let counts = header_counts(&mem, 2);
    eprintln!("degraded: header counts per record {counts:?}");
    let first = counts.first().expect("a record");
    assert!(is_header(&ops_of(&mem, 2, first.0)[0]));
    let acked = ids(&c);
    require_restore("degraded", &mem, &acked);
    assert!(acked.contains(&22));
    c.close().unwrap();
    db.close().unwrap();
}

/// The first record of generation 2 is stored, but every attempt reports an error, so the
/// commit fails and the record is in doubt. The bucket finds the record landed at the next
/// commit. That commit's record carries the header again, because the failed commit did
/// not clear it. The restore holds every acked row.
#[test]
fn in_doubt_first_record_found_landed_resends_header() {
    for (test, b) in [
        ("in_doubt_off", Batching::Off),
        ("in_doubt_window", window()),
    ] {
        let mem = mem();
        let t = tempdir();
        let (fault, db, c) = ready_faulty(&mem, t.path(), b);
        fault.store_then_fail_next_puts(u64::MAX / 2);
        let r = insert(&c, 21);
        fault.store_then_fail_next_puts(0);
        eprintln!(
            "{test}: first commit {r:?}, records {:?}",
            log_seqs(&*mem, 2)
        );
        assert!(r.is_err(), "{test}: the first commit was acked");
        assert_eq!(log_seqs(&*mem, 2), [1], "{test}: the record landed");
        insert(&c, 22).unwrap();
        let counts = header_counts(&mem, 2);
        eprintln!("{test}: header counts per record {counts:?}");
        assert_eq!(counts.len(), 2, "{test}");
        assert!(counts.iter().all(|c| c.1 == 1), "{test}: {counts:?}");
        let (n, last) = (counts[1].0, ops_of(&mem, 2, counts[1].0));
        assert!(is_header(&last[0]), "{test}");
        eprintln!("{test}: held header resent seq={n}");
        let local = ids(&c);
        assert!(local.contains(&22), "{test}");
        require_restore(test, &mem, &local);
        c.close().unwrap();
        db.close().unwrap();
    }
}

/// The bucket held back by the test: the first PUT of a key containing `key` waits.
struct Held {
    hold: Arc<HoldStore>,
    held: Receiver<String>,
    go: Sender<()>,
}

/// A writer over `HoldStore` and the hold on the first PUT of `key`.
fn ready_held(
    mem: &Arc<MemStore>,
    dir: &Path,
    b: Batching,
    key: &str,
) -> (Held, Database, Connection) {
    let hold = Arc::new(HoldStore::new(mem.clone()));
    let (db, c) = ready(hold.clone(), dir, b);
    let (held, go) = hold.hold_next_put(key);
    (Held { hold, held, go }, db, c)
}

impl Held {
    fn wait_held(&self) {
        let k = self
            .held
            .recv_timeout(Duration::from_secs(10))
            .expect("no PUT was held");
        eprintln!("held PUT of {k}");
    }
    /// Let the PUT go on (the old process is lost, so it meets the new writer's records).
    fn release(self) {
        let _ = self.go.send(());
        drop(self.hold);
    }
}

fn local_wal_len(dir: &Path) -> u64 {
    std::fs::metadata(dir.join("app.db-wal")).map_or(0, |m| m.len())
}

/// The commit that restarts the WAL waits for its record. A copy of the directory taken then
/// has the header and the frames on disk, and the bucket's generation 2 is empty. The reopen
/// replaces the `-wal` with the empty generation (`NewGeneration`). The commit was not acked.
#[test]
fn crash_after_header_fsync_before_first_record() {
    let mem = mem();
    let t = tempdir();
    let (held, db, c) = ready_held(&mem, t.path(), Batching::Off, &keys().log_prefix(2));
    let w = std::thread::spawn(move || {
        let r = insert(&c, 21);
        (c, r)
    });
    held.wait_held();
    assert!(
        log_seqs(&*mem, 2).is_empty(),
        "the bucket's generation 2 is empty"
    );
    let before = local_wal_len(t.path());
    let copy = copy_disk(t.path());
    let db2 = open_with_store(cfg(copy.path(), Batching::Off), mem.clone()).unwrap();
    eprintln!(
        "crash_after_header_fsync: copy -wal {before} bytes, reconciled {:?}, -wal now {} bytes",
        db2.shared.reconciled,
        local_wal_len(copy.path())
    );
    assert_eq!(db2.shared.reconciled, Some(Reconciled::NewGeneration));
    assert_eq!(local_wal_len(copy.path()), 0, "the local -wal is empty");
    let c2 = db2.connect().unwrap();
    assert_eq!(ids(&c2), rows(1, 20));
    require_restore("crash_after_header_fsync", &mem, &rows(1, 20));
    // The lost process is not dead here: let its PUT go on, only to end the thread.
    held.release();
    let (c, r) = w.join().unwrap();
    eprintln!("crash_after_header_fsync: the lost writer's commit ended {r:?}");
    crash(db2, vec![c2]);
    crash(db, vec![c]);
}

/// A background commit after the restart: its header record is in the bucket and its own
/// record is held back. A copy reopens with `Ahead` and uploads the commit.
#[test]
fn crash_after_background_first_commit_before_record() {
    let mem = mem();
    let t = tempdir();
    let (held, db, c) = ready_held(&mem, t.path(), Batching::Off, &keys().log(2, 2));
    c.set_batching(Some(background())).unwrap();
    insert(&c, 21).unwrap();
    held.wait_held();
    assert_eq!(
        log_seqs(&*mem, 2),
        [1],
        "only the header record is in the bucket"
    );
    let copy = copy_disk(t.path());
    let db2 = open_with_store(cfg(copy.path(), Batching::Off), mem.clone()).unwrap();
    eprintln!(
        "crash_after_background_first: reconciled {:?}",
        db2.shared.reconciled
    );
    assert!(
        matches!(db2.shared.reconciled, Some(Reconciled::Ahead { .. })),
        "{:?}",
        db2.shared.reconciled
    );
    let c2 = db2.connect().unwrap();
    assert_eq!(ids(&c2), rows(1, 21));
    require_restore("crash_after_background_first", &mem, &rows(1, 21));
    held.release();
    crash(db2, vec![c2]);
    crash(db, vec![c]);
}

/// The Window first commit is queued, and its record is held back. The commit has not been
/// acked. A copy reopens with the empty generation, and nothing acked is lost.
#[test]
fn crash_after_window_first_commit_queued() {
    let mem = mem();
    let t = tempdir();
    let (held, db, c) = ready_held(&mem, t.path(), window(), &keys().log_prefix(2));
    let w = std::thread::spawn(move || {
        let r = insert(&c, 21);
        (c, r)
    });
    held.wait_held();
    let copy = copy_disk(t.path());
    let db2 = open_with_store(cfg(copy.path(), Batching::Off), mem.clone()).unwrap();
    eprintln!(
        "crash_after_window_first: reconciled {:?}",
        db2.shared.reconciled
    );
    assert_eq!(db2.shared.reconciled, Some(Reconciled::NewGeneration));
    let c2 = db2.connect().unwrap();
    assert_eq!(ids(&c2), rows(1, 20));
    require_restore("crash_after_window_first", &mem, &rows(1, 20));
    held.release();
    let (c, _) = w.join().unwrap();
    crash(db2, vec![c2]);
    crash(db, vec![c]);
}

/// The Window first commit is queued (its window is long), then a background commit on a
/// second connection syncs. The held header is not in the bucket yet, so the background
/// commit ships it first, alone, as an Off record, and waits. The uploader joins that
/// record with the queued Window batch (one record, seq 1). The background commit's own
/// record is seq 2, and the test store holds it back. A copy reopens with `Ahead`, and a
/// restore holds the background commit. The background commit cannot return before the
/// header is in the bucket, so no copy can lose it to `NewGeneration`.
#[test]
fn background_after_queued_window_header_crash_copy() {
    let mem = mem();
    let t = tempdir();
    let long = Batching::Window {
        max_wait: Duration::from_secs(10),
        max_commits: 64,
        max_bytes: 1 << 20,
    };
    let (held, db, c) = ready_held(&mem, t.path(), Batching::Off, &keys().log(2, 2));
    c.set_batching(Some(long)).unwrap();
    let w = std::thread::spawn(move || {
        let r = insert(&c, 21);
        (c, r)
    });
    // The window is 10 s: after this wait the commit is queued and its record is not sent.
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        log_seqs(&*mem, 2).is_empty(),
        "the Window record is not in the bucket"
    );
    let c2 = db.connect().unwrap();
    c2.set_batching(Some(background())).unwrap();
    insert(&c2, 22).unwrap();
    held.wait_held();
    let counts = header_counts(&mem, 2);
    eprintln!("background_after_window: header counts per record {counts:?}");
    assert!(counts[0].1 >= 1, "record 1 holds a header");
    assert!(is_header(&ops_of(&mem, 2, 1)[0]));
    assert_eq!(
        log_seqs(&*mem, 2),
        [1],
        "the background record is held back"
    );
    let copy = copy_disk(t.path());
    let db2 = open_with_store(cfg(copy.path(), Batching::Off), mem.clone()).unwrap();
    eprintln!(
        "background_after_window: reconciled {:?}",
        db2.shared.reconciled
    );
    assert!(matches!(
        db2.shared.reconciled,
        Some(Reconciled::Ahead { .. })
    ));
    let c2b = db2.connect().unwrap();
    assert_eq!(ids(&c2b), rows(1, 22));
    require_restore("background_after_window", &mem, &rows(1, 22));
    held.release();
    let (c, r) = w.join().unwrap();
    eprintln!("background_after_window: the Window commit ended {r:?}");
    crash(db2, vec![c2b]);
    crash(db, vec![c, c2]);
}

/// The first Off record fails, so the header stays held. The failed record is owed, so the
/// uploader is degraded, and a background commit that follows blocks like an Off commit
/// (`ship_commit`). Its record is the second of the generation, and the test store holds it
/// back. The commit has not returned, so it is not acked. A copy of the directory reopens, and
/// a restore equals the reopened database, which holds every acked row (1 to 20). The bucket's
/// generation starts with the header. After the release, the background commit
/// returns, and a restore holds it.
#[test]
fn background_after_failed_off_header_crash_copy() {
    let mem = mem();
    let t = tempdir();
    let hold = Arc::new(HoldStore::new(mem.clone()));
    let fault = Arc::new(FaultStore::new(hold.clone()));
    let (db, c) = ready(fault.clone(), t.path(), Batching::Off);
    fault.only_keys_containing(&keys().log_prefix(2));
    fault.fail_next_puts_permanently(1);
    let r = insert(&c, 21);
    assert!(r.is_err(), "{r:?}");
    assert!(log_seqs(&*mem, 2).is_empty());
    let (held_rx, go) = hold.hold_next_put(&keys().log(2, 2));
    let held = Held {
        hold: hold.clone(),
        held: held_rx,
        go,
    };
    c.set_batching(Some(background())).unwrap();
    let w = std::thread::spawn(move || {
        let r = insert(&c, 22);
        (c, r)
    });
    held.wait_held();
    let counts = header_counts(&mem, 2);
    eprintln!("background_after_failed_off: header counts per record {counts:?}");
    assert_eq!(log_seqs(&*mem, 2), [1]);
    assert!(
        is_header(&ops_of(&mem, 2, 1)[0]),
        "the generation starts with the header"
    );
    let copy = copy_disk(t.path());
    let db2 = open_with_store(cfg(copy.path(), Batching::Off), mem.clone()).unwrap();
    eprintln!(
        "background_after_failed_off: reconciled {:?}",
        db2.shared.reconciled
    );
    let c2 = db2.connect().unwrap();
    let copy_ids = ids(&c2);
    assert!(copy_ids.starts_with(&rows(1, 20)), "{copy_ids:?}");
    require_restore("background_after_failed_off", &mem, &copy_ids);
    // The old process goes on: its commit lands only if the new writer did not fence it.
    held.release();
    let (c, r) = w.join().unwrap();
    eprintln!("background_after_failed_off: the lost writer's commit ended {r:?}");
    crash(db2, vec![c2]);
    crash(db, vec![c]);
}

/// The same order without a crash: the first Off record fails, a background commit follows
/// and blocks, and its record carries the header. A restore holds every acked row.
#[test]
fn background_after_failed_off_header_restores() {
    let mem = mem();
    let t = tempdir();
    let (fault, db, c) = ready_faulty(&mem, t.path(), Batching::Off);
    fault.fail_next_puts_permanently(1);
    assert!(insert(&c, 21).is_err());
    c.set_batching(Some(background())).unwrap();
    insert(&c, 22).unwrap();
    let commit = c.last_commit().expect("a commit");
    db.wait_durable(commit, Duration::from_secs(20)).unwrap();
    let counts = header_counts(&mem, 2);
    eprintln!("background_after_failed_off_restore: header counts per record {counts:?}");
    assert!(is_header(&ops_of(&mem, 2, counts[0].0)[0]));
    let local = ids(&c);
    assert!(local.contains(&22));
    require_restore("background_after_failed_off_restore", &mem, &local);
    c.close().unwrap();
    db.close().unwrap();
}

/// Two WAL restarts with no commit between them. A transaction that spills its pages writes
/// the new WAL header and frames, and its ROLLBACK commits nothing. A TRUNCATE checkpoint
/// then switches again. The held header ships into its own generation before the switch, so
/// every generation of the bucket starts with a header, and a commit after it restores.
#[test]
fn restart_twice_before_any_commit() {
    let mem = mem();
    let t = tempdir();
    let (db, c) = ready(mem.clone(), t.path(), Batching::Off);
    c.execute("BEGIN", &[]).unwrap();
    c.execute(
        "INSERT INTO t(id, v) VALUES (?, randomblob(9000000))",
        &[Value::from_i64(900)],
    )
    .unwrap();
    c.execute("ROLLBACK", &[]).unwrap();
    let after_rollback = db.generation();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    let after_truncate = db.generation();
    eprintln!("restart_twice: generation {after_rollback} after the rollback, {after_truncate} after the TRUNCATE checkpoint");
    insert(&c, 21).unwrap();
    let last = db.generation();
    for g in 1..=last {
        let counts = header_counts(&mem, g);
        eprintln!(
            "restart_twice: generation {g}: records {}, header counts {:?}",
            counts.len(),
            counts.iter().take(3).collect::<Vec<_>>()
        );
        // The first write of a generation is its header. The TRUNCATE checkpoint's truncate to
        // 0 can come before it as a record of its own (it writes nothing).
        let first_write = counts
            .iter()
            .flat_map(|&(seq, _)| ops_of(&mem, g, seq))
            .find(|op| matches!(op, Op::Write { .. }));
        if let Some(op) = first_write {
            assert!(is_header(&op), "generation {g} starts without a header");
        }
    }
    assert!(
        after_truncate > after_rollback,
        "the TRUNCATE checkpoint did not switch"
    );
    require_restore("restart_twice", &mem, &rows(1, 21));
    c.close().unwrap();
    db.close().unwrap();
}

/// `upload_stats` counts one commit per acked Off commit, the commit that restarts the WAL
/// included. The header is not a commit of its own.
#[test]
fn upload_stats_counts_one_commit_per_off_commit() {
    let mem = mem();
    let t = tempdir();
    let (db, c) = ready(mem.clone(), t.path(), Batching::Off);
    let s0 = db.upload_stats();
    insert(&c, 21).unwrap();
    assert_eq!(db.generation(), 2);
    let s1 = db.upload_stats();
    eprintln!(
        "upload_stats: restart commit acked +{} uploaded +{} log_puts +{}",
        s1.commits_acked - s0.commits_acked,
        s1.commits_uploaded - s0.commits_uploaded,
        s1.log_puts - s0.log_puts
    );
    assert_eq!(s1.commits_acked - s0.commits_acked, 1);
    assert_eq!(s1.commits_uploaded - s0.commits_uploaded, 1);
    assert_eq!(s1.log_puts - s0.log_puts, 1);
    insert(&c, 22).unwrap();
    let s2 = db.upload_stats();
    eprintln!(
        "upload_stats: next commit acked +{} uploaded +{} log_puts +{}",
        s2.commits_acked - s1.commits_acked,
        s2.commits_uploaded - s1.commits_uploaded,
        s2.log_puts - s1.log_puts
    );
    assert_eq!(s2.commits_acked - s1.commits_acked, 1);
    assert_eq!(s2.commits_uploaded - s1.commits_uploaded, 1);
    assert_eq!(s2.log_puts - s1.log_puts, 1);
    require_restore("upload_stats", &mem, &rows(1, 22));
    c.close().unwrap();
    db.close().unwrap();
}

/// Old frames are in the `-wal`, the WAL restarts (the engine writes the header, then
/// `truncate(32)`, which is not an op) and a commit follows. A restore of the new generation
/// from the bucket gives a `-wal` equal to the local one: header, frames and length.
#[test]
fn restart_with_dropped_truncate_restores_whole_wal() {
    let mem = mem();
    let t = tempdir();
    let (db, c) = ready(mem.clone(), t.path(), Batching::Off);
    let old = local_wal_len(t.path());
    assert!(old > HEADER as u64, "old frames are in the -wal");
    insert(&c, 21).unwrap();
    insert(&c, 22).unwrap();
    assert_eq!(db.generation(), 2);
    let local = std::fs::read(t.path().join("app.db-wal")).unwrap();
    let r = tempdir();
    let dir = fresh(&r);
    restore_with_store(&config(&dir), mem.clone(), &dir).unwrap();
    let restored = std::fs::read(dir.join("app.db-wal")).unwrap();
    let page = 4096 + 24;
    let frames = (local.len() - HEADER) / page;
    let header_ok = restored.len() >= HEADER && restored[..HEADER] == local[..HEADER];
    eprintln!(
        "wal compare header={} frames={frames} len={} (old -wal {old}, restored {})",
        if header_ok { "ok" } else { "differs" },
        local.len(),
        restored.len()
    );
    assert!(header_ok, "the header differs");
    assert_eq!(
        local.len(),
        HEADER + frames * page,
        "the local -wal is whole frames"
    );
    assert!(
        local.len() < old as usize + 4 * page,
        "the local -wal restarted"
    );
    assert_eq!(restored.len(), local.len(), "the length differs");
    assert!(restored == local, "the frames differ");
    require_restore("whole_wal", &mem, &rows(1, 22));
    c.close().unwrap();
    db.close().unwrap();
}

/// An MVCC `config`: no maintenance, a short retry deadline.
fn mvcc_cfg(dir: &Path) -> Config {
    let mut c = cfg(dir, Batching::Off);
    c.journal_mode = crate::JournalMode::Mvcc;
    c.maintenance_wal_bytes = 0;
    c
}

/// Every record of every generation as one line each: the key, the file, and the ops of each
/// sync (`W<offset>+<length>` for a write, `T<length>` for a truncate).
fn signature(store: &Arc<MemStore>) -> Vec<String> {
    let mut lines = Vec::new();
    for g in 1..=head(&**store).generation {
        let mut seqs = log_seqs(&**store, g);
        seqs.sort();
        for seq in seqs {
            let line = match record(store, g, seq) {
                LogRecord::Extents { file, syncs, .. } => {
                    let syncs: Vec<String> = syncs
                        .iter()
                        .map(|ops| {
                            let ops: Vec<String> = ops
                                .iter()
                                .map(|o| match o {
                                    Op::Write { off, bytes } => format!("W{off}+{}", bytes.len()),
                                    Op::Truncate { len } => format!("T{len}"),
                                })
                                .collect();
                            format!("[{}]", ops.join(" "))
                        })
                        .collect();
                    format!("g{g}/{seq} {file:?} {}", syncs.join(" "))
                }
                r => format!(
                    "g{g}/{seq} {}",
                    format!("{r:?}").chars().take(24).collect::<String>()
                ),
            };
            lines.push(line);
        }
    }
    lines
}

/// MVCC is not changed: nothing is held. An MVCC database ships its `-wal` header writes and
/// checkpoint frames as before, in the same records and ops. The expected lines are the
/// output of the base commit (6791737, before the held header).
#[test]
fn mvcc_ships_as_before() {
    use super::mvcc::{insert_c, truncate_checkpoint};
    let store = mem();
    let t = tempdir();
    let db = open_with_store(mvcc_cfg(&fresh(&t)), store.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=3 {
        insert_c(&c, id).unwrap();
    }
    truncate_checkpoint(&c).unwrap();
    insert_c(&c, 4).unwrap();
    truncate_checkpoint(&c).unwrap();
    insert_c(&c, 5).unwrap();
    let got = signature(&store);
    for line in &got {
        eprintln!("mvcc_ships_as_before: {line}");
    }
    eprintln!("mvcc_ships_as_before: {} records", got.len());
    assert_eq!(
        got, MVCC_EXPECTED,
        "the MVCC records differ from the base commit's"
    );
    assert_eq!(ids(&c), rows(1, 5));
    c.close().unwrap();
    db.close().unwrap();
}

const MVCC_EXPECTED: &[&str] = &[
    "g2/1 Log [T0 W0+264]",
    "g2/2 Log [W264+69]",
    "g2/3 Log [W333+69]",
    "g2/4 Log [W402+69]",
    "g2/5 Wal [W0+32]",
    "g2/6 Wal [W32+4120 W4152+4120 W8272+4120]",
    "g2/7 Wal ",
    "g3/1 Log [W0+125]",
    "g3/2 Wal [W0+32]",
    "g3/3 Wal [W32+4120 W4152+4120 W8272+4120]",
    "g3/4 Wal ",
    "g4/1 Log [W0+125]",
];

/// The rows a replica connection counts.
fn replica_rows(rc: &crate::ReadConnection) -> i64 {
    rc.query("SELECT count(*) FROM t", &[]).unwrap()[0][0]
        .as_int()
        .unwrap()
}

/// Wait until the replica counts `n` rows.
fn replica_waits_for(rc: &crate::ReadConnection, n: i64) {
    let end = Instant::now() + Duration::from_secs(20);
    while replica_rows(rc) != n {
        assert!(Instant::now() < end, "the replica never reached {n} rows");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A replica polls between the head move and the joined record. The writer's commit has moved
/// the head to generation 2, and its record (header and frames) is held back. The replica
/// reads no change and stays at the last commit of generation 1. Then the record lands, and
/// the replica applies it.
#[test]
fn replica_between_head_move_and_joined_record() {
    let mem = mem();
    let t = tempdir();
    let (held, db, c) = ready_held(&mem, t.path(), Batching::Off, &keys().log_prefix(2));
    let r = tempdir();
    let mut rcfg = config(&r.path().join("r"));
    rcfg.replica_poll_interval = Duration::from_millis(100);
    let replica = crate::open_replica_with_store(rcfg, mem.clone()).unwrap();
    let rc = replica.connect().unwrap();
    replica_waits_for(&rc, 20);
    let w = std::thread::spawn(move || {
        let r = insert(&c, 21);
        (c, r)
    });
    held.wait_held();
    assert_eq!(head(&*mem).generation, 2, "the head moved");
    assert!(
        log_seqs(&*mem, 2).is_empty(),
        "the record is not in the bucket"
    );
    // About ten polls.
    std::thread::sleep(Duration::from_millis(1000));
    let s = replica.stats();
    let before = replica_rows(&rc);
    eprintln!(
        "replica_between: before the record rows={before} generation={} seq={}",
        s.generation, s.seq
    );
    assert_eq!(before, 20, "the replica read a change before the record");
    held.release();
    let (c, r) = w.join().unwrap();
    r.unwrap();
    replica_waits_for(&rc, 21);
    let s = replica.stats();
    eprintln!(
        "replica_between: after the record rows={} generation={} seq={}",
        replica_rows(&rc),
        s.generation,
        s.seq
    );
    assert_eq!(s.generation, 2);
    rc.close().unwrap();
    replica.close().unwrap();
    require_restore("replica_between", &mem, &rows(1, 21));
    c.close().unwrap();
    db.close().unwrap();
}
