use super::*;
use crate::{
    config::JournalMode,
    encryption::Encryption,
    format::Op,
    replica::position::{Position, Stage},
    store::{FaultStore, MemStore},
};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
};
use turso_core::{
    CheckpointMode, CipherMode, Connection, Database, EncryptionKey, PlatformIO, SqliteDialect,
    Value, IO,
};

const PS: usize = 4096;
const FL: usize = 24 + PS;

fn open(path: &Path) -> (Arc<Database>, Arc<Connection>) {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
    let c = db.connect().unwrap();
    (db, c)
}

fn sql(c: &Arc<Connection>, s: &str) -> Vec<Vec<Value>> {
    c.prepare(s).unwrap().run_collect_rows().unwrap()
}

fn count(c: &Arc<Connection>) -> i64 {
    match &sql(c, "SELECT count(*) FROM t")[0][0] {
        Value::Numeric(turso_core::Numeric::Integer(i)) => *i,
        v => panic!("{v:?}"),
    }
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        deadline: Duration::from_millis(200),
        first_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(4),
    }
}

fn head(keys: &Keys, generation: u64, mode: JournalMode) -> Head {
    Head {
        format: keys.new_format(mode),
        generation,
        manifest: None,
        writer_id: "w1".into(),
        epoch: 1,
        journal_mode: mode,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
    }
}

/// A real writer database, a replica copy of it, a bucket in memory, and a follower.
struct Fx {
    _dir: tempfile::TempDir,
    _wdb: Arc<Database>,
    writer: Arc<Connection>,
    wal: std::path::PathBuf,
    rdb: Arc<Database>,
    rdir: std::path::PathBuf,
    gen: u64,
    next_seq: u64,
    reader: Arc<Connection>,
    mem: Arc<MemStore>,
    fault: Arc<FaultStore>,
    keys: Keys,
    follower: Follower,
    /// WAL length already turned into records.
    shipped: usize,
}

impl Fx {
    fn new() -> Fx {
        Fx::with(Keys::new("db"), 0)
    }

    fn with(keys: Keys, offset: i64) -> Fx {
        let dir = tempfile::tempdir().unwrap();
        let (w, r) = (dir.path().join("w"), dir.path().join("r"));
        std::fs::create_dir_all(&w).unwrap();
        std::fs::create_dir_all(&r).unwrap();
        let (wdb, writer) = open(&w.join("app.db"));
        sql(&writer, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)");
        sql(&writer, "INSERT INTO t VALUES (1, zeroblob(10))");
        writer
            .checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .unwrap();
        std::fs::copy(w.join("app.db"), r.join("app.db")).unwrap();
        let (rdb, reader) = open(&r.join("app.db"));
        let mem = Arc::new(MemStore::new());
        let fault = Arc::new(FaultStore::new(mem.clone()));
        mem.put(
            &keys.head(),
            &keys.encode_head(&head(&keys, 1, JournalMode::Wal)),
        )
        .unwrap();
        let follower = Follower::new(
            fault.clone(),
            keys.clone(),
            retry(),
            Applier::new(reader.clone(), None),
            PS,
            1,
            offset,
        );
        Fx {
            wal: w.join("app.db-wal"),
            _dir: dir,
            _wdb: wdb,
            writer,
            rdb,
            rdir: r.clone(),
            gen: 1,
            next_seq: 1,
            reader,
            mem,
            fault,
            keys,
            follower,
            shipped: 0,
        }
    }

    /// Run one insert on the writer; the ops for the WAL bytes it added.
    fn commit(&mut self, id: i64) -> Vec<Op> {
        self.commit_sized(id, 100)
    }

    /// `commit` with a row of `size` bytes (a big row takes a page of its own).
    fn commit_sized(&mut self, id: i64, size: usize) -> Vec<Op> {
        sql(
            &self.writer,
            &format!("INSERT INTO t VALUES ({id}, zeroblob({size}))"),
        );
        let b = std::fs::read(&self.wal).unwrap();
        let ops = vec![Op::Write {
            off: self.shipped as u64,
            bytes: b[self.shipped..].to_vec(),
        }];
        self.shipped = b.len();
        ops
    }

    fn put(&self, seq: u64, rec: &LogRecord) {
        self.put_gen(1, seq, rec);
    }

    fn put_gen(&self, generation: u64, seq: u64, rec: &LogRecord) {
        let body = self.keys.encode_record(rec, generation, seq);
        self.mem
            .put(&self.keys.log(generation, seq), &body)
            .unwrap();
    }

    fn set_head(&self, generation: u64) {
        let h = head(&self.keys, generation, JournalMode::Wal);
        self.mem
            .put(&self.keys.head(), &self.keys.encode_head(&h))
            .unwrap();
    }

    fn pass(&mut self) -> PassResult {
        self.follower.pass(Trigger::Notification)
    }

    fn rows(&self) -> i64 {
        count(&self.reader)
    }

    /// A follower that writes the position file next to the replica database.
    fn positioned(mut self, budget: Duration) -> Fx {
        let applier = Applier::new(self.reader.clone(), None);
        self.follower = Follower::new(
            self.fault.clone(),
            self.keys.clone(),
            retry(),
            applier,
            PS,
            1,
            0,
        )
        .with_position(self.rdir.clone(), "r-test".into(), budget);
        Position {
            generation: 1,
            stage: Stage::Applying,
            offset: 0,
            new_offset: 0,
            replica_id: "r-test".into(),
        }
        .write(&self.rdir)
        .unwrap();
        self
    }

    fn put_next(&mut self, rec: &LogRecord) {
        self.put_gen(self.gen, self.next_seq, rec);
        self.next_seq += 1;
    }

    /// Commit on the writer and ship it as the next record of the current generation.
    fn ship(&mut self, id: i64) {
        let ops = self.commit(id);
        self.put_next(&ext("w1", ops));
    }

    /// `ship` of a row that takes a page of its own, so a lost record loses a row.
    fn ship_big(&mut self, id: i64) {
        let ops = self.commit_sized(id, 3000);
        self.put_next(&ext("w1", ops));
    }

    /// The writer seals: a seal-start record, a WAL restart, the head moves to `to`.
    fn seal_to(&mut self, to: u64) {
        self.put_next(&ext("w1", vec![]));
        self.writer
            .checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .unwrap();
        self.shipped = 0;
        self.set_head(to);
        self.gen = to;
        self.next_seq = 1;
    }

    fn position(&self) -> Position {
        Position::read(&self.rdir).unwrap().unwrap()
    }

    fn local_frames(&self) -> u64 {
        self.reader.wal_state().unwrap().max_frame
    }

    fn writer_rows(&self) -> i64 {
        count(&self.writer)
    }

    fn integrity_ok(&self) {
        let r = sql(&self.reader, "PRAGMA integrity_check");
        assert!(
            matches!(&r[0][0], Value::Text(t) if t.as_str() == "ok"),
            "{r:?}"
        );
    }

    /// A second read connection holding a snapshot.
    fn hold_reader(&self) -> (Arc<Connection>, i64) {
        let c = self.rdb.connect().unwrap();
        sql(&c, "BEGIN");
        let n = count(&c);
        (c, n)
    }
}

fn ext(writer: &str, ops: Vec<Op>) -> LogRecord {
    LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: writer.into(),
        syncs: vec![ops],
    }
}

fn fence(writer: &str, epoch: u64) -> LogRecord {
    LogRecord::Fence {
        writer_id: writer.into(),
        epoch,
    }
}

#[test]
fn pass_applies_new_records_in_order() {
    let mut f = Fx::new();
    let a = f.commit(2);
    let b = f.commit(3);
    f.put(1, &ext("w1", a));
    f.put(2, &ext("w1", b));
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::Applied, "{r:?}");
    assert_eq!(
        (r.objects, r.lists, r.found_new, r.skipped),
        (2, 1, true, 0)
    );
    assert_eq!(f.rows(), 3);
    assert_eq!(f.follower.last_seq(), 2);
}

#[test]
fn pass_with_nothing_new_costs_one_list_and_one_head_get() {
    let mut f = Fx::new();
    let a = f.commit(2);
    f.put(1, &ext("w1", a));
    f.pass();
    let before = f.fault.events().len();
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::Nothing);
    assert_eq!((r.lists, r.gets, r.objects), (1, 1, 0));
    let ev = f.fault.events()[before..].to_vec();
    assert_eq!(ev.len(), 2, "{ev:?}");
    assert!(
        ev[0].starts_with("list_after ") && ev[1] == "stat db/head",
        "{ev:?}"
    );
}

#[test]
fn duplicate_pass_finds_nothing_new() {
    let mut f = Fx::new();
    let a = f.commit(2);
    f.put(1, &ext("w1", a));
    assert!(f.pass().found_new);
    let r = f.pass();
    assert!(!r.found_new);
    assert_eq!(r.outcome, Outcome::Nothing);
    assert_eq!(f.rows(), 2);
}

#[test]
fn missing_seq_stops_before_n_plus_1_then_applies_both() {
    let mut f = Fx::new();
    let (a, b, c) = (f.commit(2), f.commit(3), f.commit(4));
    f.put(1, &ext("w1", a));
    f.put(3, &ext("w1", c));
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::Applied);
    assert_eq!(f.follower.last_seq(), 1);
    assert_eq!(f.rows(), 2);
    f.put(2, &ext("w1", b));
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::Applied);
    assert_eq!(f.follower.last_seq(), 3);
    assert_eq!(f.rows(), 4);
}

#[test]
fn fenced_writer_late_record_is_skipped_and_counted() {
    let mut f = Fx::new();
    let (a, b) = (f.commit(2), f.commit(3));
    f.put(1, &ext("w1", a));
    f.put(2, &fence("w2", 2));
    f.put(3, &ext("w1", b)); // the fenced writer's late record
    let r = f.pass();
    assert_eq!(r.skipped, 1, "{r:?}");
    assert_eq!(f.rows(), 2);
    assert_eq!(f.follower.last_seq(), 3);
}

#[test]
fn skipped_fenced_record_logs_info_with_generation_seq_and_writer() {
    use crate::telemetry::testing::Capture;
    let cap = Capture::new();
    let mut f = Fx::new();
    let (a, b) = (f.commit(2), f.commit(3));
    f.put(1, &ext("w1", a));
    f.put(2, &fence("w2", 2));
    f.put(3, &ext("w1", b));
    let r = tracing::subscriber::with_default(cap.subscriber(), || f.pass());
    assert_eq!(r.skipped, 1);
    let snap = cap.spans();
    let ev: Vec<_> = snap
        .events
        .iter()
        .filter(|e| e.field("message") == Some("fenced writer's record skipped"))
        .collect();
    assert_eq!(ev.len(), 1, "{:?}", snap.events);
    assert_eq!(ev[0].level, tracing::Level::INFO);
    assert_eq!(ev[0].target, "buckwal");
    assert_eq!(ev[0].field("generation"), Some("1"));
    assert_eq!(ev[0].field("seq"), Some("3"));
    assert_eq!(ev[0].field("writer_id"), Some("w1"));
}

#[test]
fn stale_fence_and_empty_records_of_a_fenced_writer_are_not_counted() {
    let mut f = Fx::new();
    let a = f.commit(2);
    f.put(1, &ext("w1", a));
    f.put(2, &fence("w2", 5));
    f.put(3, &fence("w1", 1)); // stale
    f.put(4, &ext("w1", vec![])); // seal-start of the fenced writer
    let r = f.pass();
    assert_eq!(r.skipped, 0, "{r:?}");
    assert_eq!(f.follower.last_seq(), 4);
}

#[test]
fn malformed_ops_and_non_integrity_decode_errors_are_not_integrity() {
    let mut f = Fx::new();
    // a body that is not a record at all (plain keys: sha trailer fails to verify)
    f.mem.put(&f.keys.log(1, 1), b"garbage").unwrap();
    let r = f.pass();
    assert!(
        matches!(r.outcome, Outcome::Stopped(StopReason::Corrupt(_))),
        "{r:?}"
    );
    // ops the image cannot hold
    let mut f = Fx::new();
    f.put(
        1,
        &ext(
            "w1",
            vec![Op::Write {
                off: u64::MAX - 2,
                bytes: vec![1; 8],
            }],
        ),
    );
    let r = f.pass();
    assert!(
        matches!(
            r.outcome,
            Outcome::Stopped(StopReason::Corrupt(_)) | Outcome::Failed(_)
        ),
        "{r:?}"
    );
}

#[test]
fn two_commits_in_one_record_advance_by_each_transactions_frames() {
    let mut f = Fx::new();
    let _ = f.commit(2);
    let _ = f.commit(3);
    let b = std::fs::read(&f.wal).unwrap();
    let frames = (b.len() - 32) / FL;
    assert!(frames >= 2);
    let one = vec![Op::Write { off: 0, bytes: b }];
    f.put(1, &ext("w1", one));
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::Applied);
    assert_eq!(f.rows(), 3);
    assert_eq!(f.follower.applied_frames(), frames as u64);
    // re-reading from seq 1 (a restart) skips what is local and advances the same way
    let mut again = Follower::new(
        f.fault.clone(),
        f.keys.clone(),
        retry(),
        Applier::new(f.reader.clone(), None),
        PS,
        1,
        0,
    );
    assert_eq!(again.pass(Trigger::Start).outcome, Outcome::Applied);
    assert_eq!(again.applied_frames(), frames as u64);
}

#[test]
fn fence_then_new_writer_records_applied() {
    let mut f = Fx::new();
    let (a, b) = (f.commit(2), f.commit(3));
    f.put(1, &ext("w1", a));
    f.put(2, &fence("w2", 2));
    f.put(3, &ext("w2", b));
    let r = f.pass();
    assert_eq!((r.skipped, r.outcome), (0, Outcome::Applied));
    assert_eq!(f.rows(), 3);
}

#[test]
fn second_fence_moves_rule_to_its_writer() {
    let mut f = Fx::new();
    let (a, b, c) = (f.commit(2), f.commit(3), f.commit(4));
    f.put(1, &ext("w1", a));
    f.put(2, &fence("w2", 2));
    f.put(3, &ext("w2", b));
    f.put(4, &fence("w3", 3));
    f.put(5, &ext("w2", c.clone())); // w2 is fenced now
    let r = f.pass();
    assert_eq!(r.skipped, 1, "{r:?}");
    assert_eq!(f.rows(), 3);
    f.put(6, &ext("w3", c));
    let r = f.pass();
    assert_eq!((r.skipped, r.outcome), (0, Outcome::Applied));
    assert_eq!(f.rows(), 4);
}

#[test]
fn rolled_back_large_transaction_never_visible() {
    let mut f = Fx::new();
    let a = f.commit(2);
    let a_end = f.shipped;
    let b = f.commit(3);
    f.put(1, &ext("w1", a));
    // frames of a transaction the writer later rolled back: no commit frame, other content
    let Op::Write { bytes, .. } = &b[0] else {
        panic!()
    };
    let mut junk = bytes[..FL].to_vec();
    junk[4..8].fill(0);
    let n = junk.len();
    junk[n - 1] ^= 0xff;
    f.put(
        2,
        &ext(
            "w1",
            vec![Op::Write {
                off: a_end as u64,
                bytes: junk,
            }],
        ),
    );
    let r = f.pass();
    assert_eq!(f.rows(), 2, "{r:?}");
    assert_eq!(r.outcome, Outcome::Applied); // record 1 applied; record 2 held back
                                             // the writer's next commit rewrites those frames
    f.put(3, &ext("w1", b));
    f.pass();
    assert_eq!(f.rows(), 3);
}

#[test]
fn mvcc_record_stops_with_mvcc_reason() {
    let mut f = Fx::new();
    let a = f.commit(2);
    f.put(1, &ext("w1", a));
    f.put(
        2,
        &LogRecord::Extents {
            file: LogFile::Log,
            writer_id: "w1".into(),
            syncs: vec![vec![Op::Write {
                off: 0,
                bytes: vec![1; 8],
            }]],
        },
    );
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::Stopped(StopReason::Mvcc));
    assert_eq!(f.follower.last_seq(), 1);
    assert_eq!(f.rows(), 2);
}

#[test]
fn mvcc_head_stops_with_mvcc_reason() {
    let mut f = Fx::new();
    let h = head(&f.keys, 1, JournalMode::Mvcc);
    f.mem.put(&f.keys.head(), &f.keys.encode_head(&h)).unwrap();
    assert_eq!(f.pass().outcome, Outcome::Stopped(StopReason::Mvcc));
}

#[test]
fn wrong_integrity_code_stops_and_applies_nothing() {
    let enc = Encryption::new(CipherMode::Aegis256, EncryptionKey::new_256([0x5a; 32]));
    let keys = Keys::new("db").with_integrity(Some(enc.integrity_key()));
    let mut f = Fx::with(keys, 0);
    let a = f.commit(2);
    // a valid record for seq 2, replayed at seq 1
    let body = f.keys.encode_record(&ext("w1", a), 1, 2);
    f.mem.put(&f.keys.log(1, 1), &body).unwrap();
    let r = f.pass();
    assert_eq!(
        r.outcome,
        Outcome::Stopped(StopReason::Integrity(f.keys.log(1, 1)))
    );
    assert_eq!(f.rows(), 1);
    assert_eq!(f.follower.last_seq(), 0);
}

#[test]
fn trimmed_log_object_requests_re_restore() {
    let mut f = Fx::new();
    let (a, b, c) = (f.commit(2), f.commit(3), f.commit(4));
    f.put(1, &ext("w1", a));
    f.pass();
    // seq 2 was trimmed, 3 survives, and the head has moved on
    f.put(3, &ext("w1", c));
    let _ = b;
    f.set_head(2);
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::ReRestore(Reason::Trimmed), "{r:?}");
    assert_eq!(r.outcome.re_restore(), Some(Reason::Trimmed));
}

#[test]
fn hole_with_head_not_moved_waits() {
    let mut f = Fx::new();
    let (a, _b, c) = (f.commit(2), f.commit(3), f.commit(4));
    f.put(1, &ext("w1", a));
    f.put(3, &ext("w1", c));
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::Applied);
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::Nothing);
}

#[test]
fn new_generation_is_reported_after_applying_the_rest() {
    let mut f = Fx::new();
    let a = f.commit(2);
    f.put(1, &ext("w1", a));
    f.set_head(2);
    // the pass alone reports the move; finishing it is the checkpoint stages' job
    let r = f.follower.run_once_for_test();
    assert_eq!(r.outcome, Outcome::GenerationMoved { head_generation: 2 });
    assert_eq!(f.rows(), 2);
}

#[test]
fn replica_issues_no_puts_or_deletes() {
    let mut f = Fx::new();
    let a = f.commit(2);
    f.put(1, &ext("w1", a));
    f.set_head(2);
    let start = f.fault.events().len();
    let s0 = f.mem.stats();
    f.pass();
    f.pass();
    f.set_head(1);
    f.pass();
    let ev = f.fault.events()[start..].to_vec();
    assert!(!ev.is_empty());
    // the test's own head write above is a put on the MemStore directly, not the follower's
    assert!(
        ev.iter().all(|e| e.starts_with("get ")
            || e.starts_with("list_after ")
            || e.starts_with("stat ")),
        "{ev:?}"
    );
    let d = f.mem.stats().since(&s0);
    assert_eq!(d.deletes, 0);
    assert_eq!(d.puts, 1, "only the test's own head write");
}

#[test]
fn gap_rereads_from_seq_1_then_re_restores() {
    // offset 5: every transaction starts beyond the next local frame
    let mut f = Fx::with(Keys::new("db"), 5);
    let a = f.commit(2);
    f.put(1, &ext("w1", a));
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::ReRestore(Reason::Gap), "{r:?}");
    assert_eq!(f.rows(), 1);
}

#[test]
fn redelivered_history_after_restart_is_harmless_and_conflict_stops() {
    let mut f = Fx::new();
    let a = f.commit(2);
    f.put(1, &ext("w1", a.clone()));
    assert_eq!(f.pass().outcome, Outcome::Applied);
    // a restarted follower re-reads from seq 1: the frames are already local
    let mut again = Follower::new(
        f.fault.clone(),
        f.keys.clone(),
        retry(),
        Applier::new(f.reader.clone(), None),
        PS,
        1,
        0,
    );
    let r = again.pass(Trigger::Start);
    assert_eq!(r.outcome, Outcome::Applied, "{r:?}");
    assert_eq!(f.rows(), 2);
    // the log now says something else at the same frames: conflict
    let Op::Write { off, mut bytes } = a.into_iter().next().unwrap() else {
        panic!()
    };
    let n = bytes.len();
    bytes[n - 5] ^= 0xff;
    f.put(1, &ext("w1", vec![Op::Write { off, bytes }]));
    let mut again = Follower::new(
        f.fault.clone(),
        f.keys.clone(),
        retry(),
        Applier::new(f.reader.clone(), None),
        PS,
        1,
        0,
    );
    let r = again.pass(Trigger::Start);
    assert_eq!(r.outcome, Outcome::Stopped(StopReason::Conflict), "{r:?}");
    assert_eq!(r.outcome.re_restore(), Some(Reason::Conflict));
    let d = r.detail.unwrap();
    assert!(d.contains("generation 1 frame 1"), "{d}");
}

const BUDGET: Duration = Duration::from_secs(120);

fn names(ev: &[String]) -> String {
    ev.join("\n")
}

#[test]
fn stop_reason_strings_come_from_named_constants() {
    assert_eq!(StopReason::Conflict.as_str(), reason::CONFLICT);
    assert_eq!(StopReason::Mvcc.as_str(), reason::MVCC);
    assert_eq!(
        StopReason::Integrity("k".into()).as_str(),
        reason::INTEGRITY
    );
    assert_eq!(StopReason::Corrupt("x".into()).as_str(), reason::CORRUPT);
    assert_eq!(reason::CORRUPT, "corrupt");
}

#[test]
fn seal_with_no_readers_checkpoints_and_resets_offset() {
    let mut f = Fx::new().positioned(BUDGET);
    f.ship(2);
    f.ship(3);
    f.seal_to(2);
    f.ship(4);
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::Applied, "{r:?}");
    assert_eq!(f.follower.generation(), 2);
    assert_eq!(f.follower.offset, 0);
    let p = f.position();
    assert_eq!((p.generation, p.stage, p.offset), (2, Stage::Applying, 0));
    assert_eq!(f.rows(), 4);
    assert_eq!(f.rows(), f.writer_rows());
    // the local WAL holds only generation 2's frames
    assert_eq!(f.local_frames(), f.follower.applied_frames());
    f.integrity_ok();
}

#[test]
fn seal_with_open_reader_numbers_on_and_reader_keeps_snapshot() {
    let mut f = Fx::new().positioned(BUDGET);
    f.ship(2);
    f.pass();
    let (r2, before) = f.hold_reader();
    f.ship(3);
    f.seal_to(2);
    f.ship(4);
    let last_local = {
        // frames of generation 1 applied so far plus the one the pass is about to add
        let r = f.pass();
        assert!(!matches!(r.outcome, Outcome::CheckpointDeferred), "{r:?}");
        let p = f.position();
        assert_eq!(
            (p.generation, p.stage, p.new_offset),
            (2, Stage::CkptPending, 0)
        );
        p.offset
    };
    assert!(last_local > 0);
    // the retry budget is time: many Busy passes inside it do not exhaust it, the first one
    // after it gives up
    for _ in 0..20 {
        let r = f.pass();
        assert!(!matches!(r.outcome, Outcome::CheckpointDeferred), "{r:?}");
        assert!(r.detail.is_some_and(|d| d.starts_with("checkpoint busy")));
    }
    assert_eq!(f.position().stage, Stage::CkptPending);
    f.follower.advance_clock(BUDGET);
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::CheckpointDeferred, "{r:?}");
    let p = f.position();
    assert_eq!(
        (p.generation, p.stage, p.offset),
        (2, Stage::Applying, last_local)
    );
    // generation 2 numbers on past the local last frame; the reader keeps its snapshot
    f.pass();
    assert_eq!(f.rows(), 4);
    assert_eq!(count(&r2), before);
    assert_eq!(
        f.local_frames(),
        last_local as u64 + f.follower.applied_frames()
    );
    sql(&r2, "COMMIT");
    f.integrity_ok();
}

#[test]
fn deferred_checkpoint_runs_after_reader_commits() {
    let mut f = Fx::new().positioned(BUDGET);
    f.ship(2);
    f.pass();
    let (r2, _) = f.hold_reader();
    f.ship(3);
    f.seal_to(2);
    f.ship(4);
    f.pass();
    f.follower.advance_clock(BUDGET);
    for _ in 0..3 {
        f.pass();
    }
    assert_eq!(f.follower.generation(), 2);
    assert!(f.follower.offset > 0);
    // still held: later passes keep trying and stay deferred
    f.ship(5);
    f.pass();
    assert!(f.follower.offset > 0);
    sql(&r2, "COMMIT");
    f.ship(6);
    f.pass();
    let k = f.follower.applied_frames();
    assert_eq!(f.follower.offset, -(k as i64));
    let p = f.position();
    assert_eq!(
        (p.generation, p.stage, p.offset),
        (2, Stage::Applying, -(k as i64))
    );
    assert_eq!(f.local_frames(), 0);
    // numbering continues from the new offset
    f.ship(7);
    f.pass();
    assert_eq!(f.rows(), f.writer_rows());
    assert_eq!(f.local_frames(), f.follower.applied_frames() - k);
    f.integrity_ok();
}

#[test]
fn head_skipping_a_generation_lists_the_skipped_one_once_and_jumps() {
    let mut f = Fx::new().positioned(BUDGET);
    f.ship(2);
    f.seal_to(3); // the head goes 1 -> 3 as after a takeover; log/2 never exists
    f.ship(4);
    let start = f.fault.events().len();
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::Applied, "{r:?}");
    f.pass();
    assert_eq!(f.follower.generation(), 3);
    assert_eq!(f.rows(), 3);
    let ev = f.fault.events()[start..].to_vec();
    let p2 = f.keys.log_prefix(2);
    let on2: Vec<_> = ev.iter().filter(|e| e.contains(&p2)).collect();
    assert_eq!(on2.len(), 1, "{}", names(&ev));
    assert!(on2[0].starts_with("list_after "), "{}", names(&ev));
    assert!(
        ev.iter().any(|e| e.contains(&f.keys.log_prefix(3))),
        "{}",
        names(&ev)
    );
    assert_eq!(f.position().generation, 3);
}

#[test]
fn two_seals_between_passes_follow_the_intermediate_generation() {
    let mut f = Fx::new().positioned(BUDGET);
    f.ship_big(2);
    f.seal_to(2);
    f.ship_big(3);
    f.ship_big(4);
    f.seal_to(3);
    f.ship_big(5);
    let r = f.pass();
    assert!(!matches!(r.outcome, Outcome::ReRestore(_)), "{r:?}");
    f.pass();
    assert_eq!(f.follower.generation(), 3);
    assert_eq!(f.rows(), f.writer_rows());
    assert_eq!(f.rows(), 5);
    assert_eq!(f.position().generation, 3);
    f.integrity_ok();
}

#[test]
fn three_seals_between_passes_follow_every_generation() {
    let mut f = Fx::new().positioned(BUDGET);
    f.ship_big(2);
    f.seal_to(2);
    f.ship_big(3);
    f.seal_to(3);
    f.ship_big(4);
    f.ship_big(5);
    f.seal_to(4);
    f.ship_big(6);
    let r = f.pass();
    assert!(!matches!(r.outcome, Outcome::ReRestore(_)), "{r:?}");
    f.pass();
    assert_eq!(f.follower.generation(), 4);
    assert_eq!(f.rows(), f.writer_rows());
    assert_eq!(f.rows(), 6);
    f.integrity_ok();
}

fn trimmed_intermediate_fixture() -> Fx {
    let mut f = Fx::new().positioned(BUDGET);
    f.ship_big(2);
    f.seal_to(2);
    f.ship_big(3);
    f.seal_to(3);
    f.ship_big(4);
    // GC deleted log/2 (the replica was long gone)
    f.mem.delete(&f.keys.log(2, 1)).unwrap();
    f.mem.delete(&f.keys.log(2, 2)).unwrap();
    f
}

fn assert_never_jumped(f: &Fx, start: usize) {
    assert_eq!(f.follower.generation(), 1);
    assert_eq!(f.position().generation, 1);
    let ev = f.fault.events()[start..].to_vec();
    assert!(
        ev.iter().all(|e| !e.contains(&f.keys.log_prefix(3))),
        "{}",
        names(&ev)
    );
}

#[test]
fn empty_log_after_a_long_pause_re_restores_and_never_jumps() {
    let mut f = trimmed_intermediate_fixture();
    // the last sighting of the head near us is older than trim_grace by the wall clock
    f.follower.advance_wall_clock(Duration::from_secs(601));
    let start = f.fault.events().len();
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::ReRestore(Reason::Trimmed), "{r:?}");
    assert_never_jumped(&f, start);
}

#[test]
fn empty_log_with_a_wall_clock_stepped_back_re_restores() {
    let mut f = trimmed_intermediate_fixture();
    // the sighting is in the future: a negative elapsed time is not recent
    f.follower
        .note_head_seen(1, SystemTime::now() + Duration::from_secs(3600));
    let start = f.fault.events().len();
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::ReRestore(Reason::Trimmed), "{r:?}");
    assert_never_jumped(&f, start);
}

#[test]
fn resumed_follower_with_no_sighting_re_restores_on_an_empty_log() {
    let f = trimmed_intermediate_fixture();
    let mut again = Follower::resume(
        f.fault.clone(),
        f.keys.clone(),
        retry(),
        Applier::new(f.reader.clone(), None),
        PS,
        f.rdir.clone(),
        BUDGET,
    )
    .unwrap();
    let r = again.pass(Trigger::Notification);
    assert_eq!(r.outcome, Outcome::ReRestore(Reason::Trimmed), "{r:?}");
    // a head read that named a generation past ours is no sighting near us
    again.note_head_seen(3, SystemTime::now());
    let r = again.pass(Trigger::Notification);
    assert_eq!(r.outcome, Outcome::ReRestore(Reason::Trimmed), "{r:?}");
    again.note_head_seen(2, SystemTime::now());
    let r = again.pass(Trigger::Notification);
    assert!(!matches!(r.outcome, Outcome::ReRestore(_)), "{r:?}");
}

#[test]
fn skipped_generation_logs_an_info_line_with_generation_and_reason() {
    use crate::telemetry::testing::Capture;
    let cap = Capture::new();
    let mut f = Fx::new().positioned(BUDGET);
    f.ship(2);
    f.seal_to(3);
    f.ship(4);
    let r = tracing::subscriber::with_default(cap.subscriber(), || f.pass());
    assert_eq!(r.head_generation, Some(3), "{r:?}");
    let snap = cap.spans();
    let ev: Vec<_> = snap
        .events
        .iter()
        .filter(|e| e.field("message") == Some("replica skipped a generation"))
        .collect();
    assert_eq!(ev.len(), 1, "{:?}", snap.events);
    assert_eq!(ev[0].level, tracing::Level::INFO);
    assert_eq!(ev[0].target, "buckwal");
    assert_eq!(ev[0].field("generation"), Some("2"));
    assert_eq!(ev[0].field("reason"), Some("takeover"));
}

#[test]
fn ordinary_then_skipped_generation_in_one_jump() {
    // The bucket state a writer leaves (simulated; the follower fixture has no real Bucket):
    // gen 1 sealed to 2 (ordinary), a takeover, gen 2 sealed past the skipped 3 to 4.
    let mut f = Fx::new().positioned(BUDGET);
    f.ship_big(2);
    f.seal_to(2);
    f.ship_big(3);
    f.seal_to(4);
    f.ship_big(4);
    let start = f.fault.events().len();
    let r = f.pass();
    assert!(!matches!(r.outcome, Outcome::ReRestore(_)), "{r:?}");
    f.pass();
    assert_eq!(f.follower.generation(), 4);
    assert_eq!(f.rows(), f.writer_rows());
    let ev = f.fault.events()[start..].to_vec();
    let p3 = f.keys.log_prefix(3);
    assert_eq!(
        ev.iter().filter(|e| e.contains(&p3)).count(),
        1,
        "{}",
        names(&ev)
    );
}

#[test]
fn resume_after_restart_mid_checkpoint_uses_position_stage() {
    for (case, abort_at) in [
        Step::BeforeCkptPending,
        Step::AfterCkptPending,
        Step::AfterCheckpoint,
        Step::AfterApplying,
    ]
    .into_iter()
    .enumerate()
    {
        let mut f = Fx::new().positioned(BUDGET);
        f.ship(2);
        f.ship(3);
        f.seal_to(2);
        f.ship(4);
        f.ship(5);
        f.follower.set_step_hook(move |s| {
            if s == abort_at {
                Err(format!("killed at {s:?}"))
            } else {
                Ok(())
            }
        });
        let mut seen = f.rows();
        let r = f.pass();
        assert!(
            matches!(r.outcome, Outcome::Failed(_)),
            "case {case}: {r:?}"
        );
        assert!(f.rows() >= seen);
        seen = f.rows();
        // a new follower from the same directory (the same replica database)
        let mut again = Follower::resume(
            f.fault.clone(),
            f.keys.clone(),
            retry(),
            Applier::new(f.reader.clone(), None),
            PS,
            f.rdir.clone(),
            BUDGET,
        )
        .unwrap();
        for _ in 0..6 {
            again.pass(Trigger::Start);
            let n = f.rows();
            assert!(n >= seen, "case {case}: rows went {seen} -> {n}");
            seen = n;
        }
        assert_eq!(f.rows(), f.writer_rows(), "case {case}");
        assert_eq!(again.generation(), 2, "case {case}");
        f.integrity_ok();
        let p = f.position();
        assert_eq!((p.generation, p.stage), (2, Stage::Applying), "case {case}");
    }
}

#[test]
fn sealed_generation_missing_its_tail_re_restores() {
    // the head moved but the log ends without the seal-start record: the tail is gone
    let mut f = Fx::new().positioned(BUDGET);
    f.ship(2);
    f.set_head(2);
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::ReRestore(Reason::Trimmed), "{r:?}");
}

#[test]
fn wholly_trimmed_generation_with_head_past_it_re_restores() {
    let mut f = Fx::new().positioned(BUDGET);
    f.set_head(2); // log/1 is empty
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::ReRestore(Reason::Trimmed), "{r:?}");
    // also after some records were applied and the rest was trimmed
    let mut f = Fx::new().positioned(BUDGET);
    f.ship(2);
    f.pass();
    f.mem.delete(&f.keys.log(1, 1)).unwrap();
    f.set_head(2);
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::ReRestore(Reason::Trimmed), "{r:?}");
}

#[test]
fn holed_generation_with_head_past_it_re_restores() {
    let mut f = Fx::new().positioned(BUDGET);
    f.ship(2);
    f.next_seq += 1; // seq 2 never exists
    f.ship(3);
    f.seal_to(2);
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::ReRestore(Reason::Trimmed), "{r:?}");
}

#[test]
fn fenced_writers_late_record_is_skipped_while_finishing_the_generation() {
    let mut f = Fx::new().positioned(BUDGET);
    let (a, late) = (f.commit(2), f.commit(3));
    f.put_next(&ext("w1", a));
    f.put_next(&fence("w2", 2));
    f.put_next(&ext("w1", late)); // the fenced writer's late record
    f.put_next(&ext("w2", vec![])); // the new writer's seal-start
    f.set_head(2);
    let r = f.pass();
    assert_ne!(r.outcome, Outcome::ReRestore(Reason::Trimmed), "{r:?}");
    assert_eq!(r.skipped, 1);
    assert_eq!(f.rows(), 2);
    assert_eq!(f.follower.generation(), 2);
}

/// Reader holds the replica; a seal's checkpoint is given up; generation 2 numbers on. Returns
/// the held reader. The follower has applied `ship(4)` and `ship(5)`.
fn deferred_fixture() -> (Fx, Arc<Connection>) {
    let mut f = Fx::new().positioned(BUDGET);
    f.ship(2);
    f.pass();
    let (r2, _) = f.hold_reader();
    f.ship(3);
    f.seal_to(2);
    f.ship(4);
    f.pass();
    f.follower.advance_clock(BUDGET);
    f.pass();
    f.pass();
    f.ship(5);
    f.pass(); // try_deferred wrote ckpt_pending(-k) and the checkpoint answered Busy
    assert_eq!(f.position().stage, Stage::CkptPending);
    assert!(f.position().new_offset < 0);
    assert!(f.follower.offset > 0);
    (f, r2)
}

/// Resume from the directory and pass until quiet; rows never step back; the end state is the
/// writer's, checkpointed, with numbering consistent (offset = -applied, WAL empty).
fn recover_and_check(f: &Fx, case: &str) {
    let mut seen = f.rows();
    let mut again = Follower::resume(
        f.fault.clone(),
        f.keys.clone(),
        retry(),
        Applier::new(f.reader.clone(), None),
        PS,
        f.rdir.clone(),
        BUDGET,
    )
    .unwrap();
    for _ in 0..5 {
        again.pass(Trigger::Start);
        let n = f.rows();
        assert!(n >= seen, "{case}: rows went {seen} -> {n}");
        seen = n;
    }
    assert_eq!(f.rows(), f.writer_rows(), "{case}");
    f.integrity_ok();
    assert_eq!(f.local_frames(), 0, "{case}: WAL not checkpointed");
    assert_eq!(again.offset, -(again.applied_frames() as i64), "{case}");
    let p = f.position();
    assert_eq!(
        (p.generation, p.stage, p.offset),
        (2, Stage::Applying, again.offset),
        "{case}"
    );
}

#[test]
fn kill_inside_a_deferred_checkpoint_resumes_to_the_writers_rows() {
    for abort_at in [
        Step::BeforeCkptPending,
        Step::AfterCkptPending,
        Step::AfterCheckpoint,
        Step::AfterApplying,
    ] {
        let (mut f, r2) = deferred_fixture();
        sql(&r2, "COMMIT");
        f.ship(6);
        f.follower.set_step_hook(move |s| {
            if s == abort_at {
                Err(format!("killed at {s:?}"))
            } else {
                Ok(())
            }
        });
        let seen = f.rows();
        let r = f.pass();
        assert!(
            matches!(r.outcome, Outcome::Failed(_)),
            "{abort_at:?}: {r:?}"
        );
        assert!(f.rows() >= seen);
        recover_and_check(&f, &format!("{abort_at:?}"));
    }
}

#[test]
fn conflict_frame_parses_the_message_the_applier_builds() {
    use crate::replica::apply::conflict_message;
    let e = Error::Corrupt(conflict_message(7, 42));
    assert_eq!(conflict_frame(&e), Some(42));
    assert!(is_conflict(&e));
    assert_eq!(
        conflict_frame(&Error::Corrupt("no number here".into())),
        None
    );
    assert_eq!(conflict_frame(&Error::Config("x frame 3".into())), None);
}

#[test]
fn a_hole_in_the_log_holds_back_the_deferred_checkpoint() {
    let (mut f, r2) = deferred_fixture();
    let before = f.position();
    sql(&r2, "COMMIT");
    // Record N is applied, N+1 is missing, N+2 is there: the log read ends on a hole.
    f.ship(6);
    let _lost = f.commit(7);
    f.next_seq += 1;
    f.ship(8);
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::Applied, "{r:?}");
    assert_eq!(
        f.position(),
        before,
        "the position file was rewritten past a hole"
    );
    assert!(f.follower.offset > 0);
}

#[test]
fn busy_deferral_more_frames_applied_then_killed_does_not_reuse_a_stale_offset() {
    let (mut f, r2) = deferred_fixture();
    // more frames are applied while the file still says ckpt_pending(-k); then the pass dies
    // before try_deferred can rewrite it
    f.ship(6);
    f.follower.set_step_hook(|s| {
        if s == Step::BeforeCkptPending {
            Err("killed".into())
        } else {
            Ok(())
        }
    });
    let r = f.pass();
    assert!(matches!(r.outcome, Outcome::Failed(_)), "{r:?}");
    let stale = f.position();
    assert!(stale.new_offset < 0 && -stale.new_offset < f.follower.applied_frames() as i64);
    sql(&r2, "COMMIT");
    recover_and_check(&f, "stale deferred offset");
}

#[test]
fn pass_after_n_records_lists_only_after_seq_n() {
    let mut f = Fx::new();
    let a = f.commit(2);
    let b = f.commit(3);
    f.put(1, &ext("w1", a));
    f.put(2, &ext("w1", b));
    f.pass();
    let c = f.commit(4);
    f.put(3, &ext("w1", c));
    let before = f.fault.events().len();
    let r = f.pass();
    assert_eq!(
        (r.outcome.clone(), r.objects, r.lists),
        (Outcome::Applied, 1, 1),
        "{r:?}"
    );
    let ev = f.fault.events()[before..].to_vec();
    let want = format!("list_after {} {}", f.keys.log_prefix(1), f.keys.log(1, 2));
    assert_eq!(ev[0], want, "{ev:?}");
    assert_eq!(ev[1], format!("get {}", f.keys.log(1, 3)), "{ev:?}");
    assert_eq!(f.rows(), 4);
}

#[test]
fn first_pass_and_restart_list_from_the_prefix() {
    let mut f = Fx::new();
    let a = f.commit(2);
    f.put(1, &ext("w1", a));
    f.pass();
    let ev = f.fault.events();
    let p = f.keys.log_prefix(1);
    assert_eq!(ev[0], format!("list_after {p} {p}"), "{ev:?}");
}

#[test]
fn long_generation_is_followed_in_order_and_idle_pass_lists_after_last() {
    let mut f = Fx::new();
    // One real commit, then 2,499 fence records (cheap, each read and decoded in order).
    let a = f.commit(2);
    f.put(1, &ext("w1", a));
    for i in 1..2500u64 {
        f.put(i + 1, &fence("w1", i));
    }
    let r = f.pass();
    assert_eq!(r.outcome, Outcome::Applied, "{r:?}");
    assert_eq!((r.objects, r.lists), (2500, 1), "{r:?}");
    assert_eq!(f.follower.last_seq(), 2500);
    assert_eq!(f.rows(), 2);
    let gets: Vec<String> = f
        .fault
        .events()
        .into_iter()
        .filter(|e| e.starts_with("get ") && e.contains("/log/"))
        .collect();
    let want: Vec<String> = (1..=2500)
        .map(|s| format!("get {}", f.keys.log(1, s)))
        .collect();
    assert_eq!(gets, want);
    let before = f.fault.events().len();
    let r = f.pass();
    assert_eq!((r.outcome, r.objects), (Outcome::Nothing, 0));
    let ev = f.fault.events()[before..].to_vec();
    assert_eq!(
        ev[0],
        format!(
            "list_after {} {}",
            f.keys.log_prefix(1),
            f.keys.log(1, 2500)
        )
    );
}
