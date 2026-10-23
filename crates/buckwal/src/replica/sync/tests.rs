use super::*;
use crate::{
    config::JournalMode,
    encryption::Encryption,
    format::Op,
    store::{FaultStore, MemStore},
};
use std::{path::Path, sync::Arc, time::Duration};
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
    _rdb: Arc<Database>,
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
            _rdb: rdb,
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
        sql(
            &self.writer,
            &format!("INSERT INTO t VALUES ({id}, zeroblob(100))"),
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
        ev[0].starts_with("list ") && ev[1] == "stat db/head",
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
    let r = f.pass();
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
        ev.iter()
            .all(|e| e.starts_with("get ") || e.starts_with("list ") || e.starts_with("stat ")),
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
