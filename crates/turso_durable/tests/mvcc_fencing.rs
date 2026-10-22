//! Task 9: the "argued" rows of docs/mvcc-recon.md Q7, proved against in-memory stores through
//! the public API (an MVCC bucket, `journal_mode = Mvcc`): fencing, takeover, "a sync is Ok
//! only when every op is confirmed", verify of a mid-checkpoint restore, and restore through
//! the library. GC and trim have their tests beside them (`gc/tests.rs`, `trim/tests.rs`).
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use turso_durable::store::{
    CondPut, FaultStore, MemStore, Object, ObjectStat, ObjectStore, StatsSnap, StoreError,
};
use turso_durable::{
    open_read_only_with_store, open_with_store, restore_with_store, takeover_with_store,
    CheckpointMode, Config, Connection, Database, Error, JournalMode, Value,
};

const PREFIX: &str = "db";

fn config(dir: &Path, mode: JournalMode) -> Config {
    let mut c = Config::new(dir, "http://127.0.0.1:1", "bucket", "key", "secret");
    c.prefix = PREFIX.into();
    c.retry_deadline = Duration::from_millis(300);
    c.request_timeout = Duration::from_millis(500);
    c.lease = Duration::from_secs(5);
    c.journal_mode = mode;
    // Every checkpoint is the test's.
    c.maintenance_log_objects = 0;
    c.maintenance_wal_bytes = 0;
    c.gc_after_seal = false;
    c
}

fn mvcc(dir: &Path) -> Config {
    config(dir, JournalMode::Mvcc)
}

fn tempdir() -> TempDir {
    tempfile::tempdir().unwrap()
}

fn fresh(t: &TempDir) -> PathBuf {
    t.path().join("db")
}

fn create(c: &Connection) {
    c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
}

/// One `BEGIN CONCURRENT` insert of row `id`.
fn insert_c(c: &Connection, id: i64) -> turso_durable::Result<()> {
    c.execute("BEGIN CONCURRENT", &[])?;
    let r = c
        .execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            &[Value::from_i64(id), Value::build_text(format!("row {id}"))],
        )
        .and_then(|_| c.execute("COMMIT", &[]));
    if r.is_err() {
        let _ = c.execute("ROLLBACK", &[]);
    }
    r.map(|_| ())
}

fn ids(c: &Connection) -> BTreeSet<i64> {
    c.query("SELECT id FROM t", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect()
}

fn integrity(c: &Connection) -> String {
    c.query("PRAGMA integrity_check", &[]).unwrap()[0][0]
        .to_text()
        .unwrap()
        .to_string()
}

/// A process crash: nothing runs on drop.
fn crash(db: Database, conns: Vec<Connection>) {
    for c in conns {
        std::mem::forget(c);
    }
    std::mem::forget(db);
}

/// The rows a restore of the bucket holds (a read-only open of a new directory), after an
/// `integrity_check`.
fn restored_ids(store: Arc<dyn ObjectStore>) -> BTreeSet<i64> {
    let v = tempdir();
    let db = open_read_only_with_store(mvcc(&fresh(&v)), store).unwrap();
    let c = db.connect().unwrap();
    assert_eq!(integrity(&c), "ok");
    let rows = ids(&c);
    c.close().unwrap();
    db.close().unwrap();
    rows
}

fn head_body(mem: &MemStore) -> String {
    String::from_utf8(mem.get(&format!("{PREFIX}/head")).unwrap().unwrap().body).unwrap()
}

fn head_field(mem: &MemStore, name: &str) -> String {
    let body = head_body(mem);
    let key = format!("\"{name}\":");
    let rest = &body[body.find(&key).unwrap_or_else(|| panic!("{body}")) + key.len()..];
    let end = rest.find([',', '}']).unwrap();
    rest[..end].trim_matches('"').to_string()
}

fn generation(mem: &MemStore) -> u64 {
    head_field(mem, "generation").parse().unwrap()
}

/// The record kinds (the byte after the 8-byte magic) of `log/<gen>/*` in seq order.
fn kinds(mem: &MemStore, gen: u64) -> Vec<u8> {
    mem.list(&format!("{PREFIX}/log/{gen:010}/"))
        .unwrap()
        .iter()
        .map(|k| mem.get(k).unwrap().unwrap().body[8])
        .collect()
}

/// A new MVCC writer on `store` in a new temp dir: table `t`, rows `1..=n` acked.
fn writer(store: Arc<dyn ObjectStore>, n: i64) -> (TempDir, Database, Connection) {
    let t = tempdir();
    let db = open_with_store(mvcc(&fresh(&t)), store).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=n {
        insert_c(&c, id).unwrap();
    }
    (t, db, c)
}

fn copy_dir(from: &Path) -> TempDir {
    let t = tempdir();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        if e.file_name() != "durable.lock" {
            std::fs::copy(e.path(), t.path().join(e.file_name())).unwrap();
        }
    }
    t
}

// ---------------------------------------------------------------------------------------
// Fencing and takeover
// ---------------------------------------------------------------------------------------

#[test]
fn mvcc_writer_fenced_by_takeover_fails_commits_forever() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let (_a, db_a, ca) = writer(fault.clone(), 10);
    let c2 = db_a.connect().unwrap();
    let a_epoch: u64 = head_field(&mem, "epoch").parse().unwrap();
    let a_gen = generation(&mem);

    let bt = tempdir();
    let rep = takeover_with_store(mvcc(&fresh(&bt)), mem.clone()).unwrap();
    assert_eq!(rep.epoch, a_epoch + 1);
    assert_eq!(head_field(&mem, "writer_id"), rep.writer_id);
    // B's fence sits at the seq A would write next, with B's bytes.
    let log_keys = mem.list(&format!("{PREFIX}/log/{a_gen:010}/")).unwrap();
    let fence_key = log_keys.last().unwrap().clone();
    let fence_bytes = mem.get(&fence_key).unwrap().unwrap().body;
    assert_eq!(fence_bytes[8], 2, "the last record is B's fence");

    // A's next commit, and every later one, on either connection: Fenced.
    for (n, id) in (11..=14).enumerate() {
        let c = if n % 2 == 0 { &ca } else { &c2 };
        let r = insert_c(c, id);
        assert!(matches!(r, Err(Error::Fenced(_))), "commit {id}: {r:?}");
    }
    // It requeued nothing and wrote nothing: the log is what B left.
    assert_eq!(
        mem.list(&format!("{PREFIX}/log/{a_gen:010}/")).unwrap(),
        log_keys
    );
    assert_eq!(mem.get(&fence_key).unwrap().unwrap().body, fence_bytes);
    // And a checkpoint is fenced too.
    let r = ca.checkpoint(CheckpointMode::Truncate);
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");

    // B has every row A acked, and none of the fenced ones.
    let db_b = open_with_store(mvcc(&fresh(&bt)), mem.clone()).unwrap();
    let cb = db_b.connect().unwrap();
    assert_eq!(ids(&cb), (1..=10).collect());
    assert_eq!(integrity(&cb), "ok");
    insert_c(&cb, 11).unwrap();
    cb.close().unwrap();
    db_b.close().unwrap();
    assert_eq!(restored_ids(mem.clone()), (1..=11).collect());
    crash(db_a, vec![ca, c2]);
}

/// A kind-3 PUT whose seq B holds with other bytes: Fenced, and (the log listing is
/// unchanged) the ops are not requeued under a later seq.
#[test]
fn kind3_put_on_a_seq_another_writer_holds_is_fenced() {
    let mem = Arc::new(MemStore::new());
    let (_a, db_a, ca) = writer(mem.clone(), 5);
    let gen = generation(&mem);
    let before = kinds(&mem, gen);
    assert!(before.contains(&3), "{before:?}");
    let bt = tempdir();
    takeover_with_store(mvcc(&fresh(&bt)), mem.clone()).unwrap();
    let after_takeover = kinds(&mem, gen);
    assert_eq!(after_takeover.len(), before.len() + 1);
    let r = insert_c(&ca, 6);
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
    assert_eq!(kinds(&mem, gen), after_takeover, "nothing requeued");
    crash(db_a, vec![ca]);
}

#[test]
fn takeover_of_mvcc_restores_log_claims_fences_reconciles() {
    let mem = Arc::new(MemStore::new());
    let (_a, db_a, ca) = writer(mem.clone(), 10);
    let a_epoch: u64 = head_field(&mem, "epoch").parse().unwrap();
    let a_writer = head_field(&mem, "writer_id");
    let gen = generation(&mem);

    let bt = tempdir();
    let dir = fresh(&bt);
    let rep = takeover_with_store(mvcc(&dir), mem.clone()).unwrap();
    // Both files restored; kind-3 objects counted.
    let wal = std::fs::metadata(dir.join("app.db-wal")).unwrap().len();
    let log = std::fs::metadata(dir.join("app.db-log")).unwrap().len();
    assert!(log > 56, "app.db-log holds commits: {log}");
    let _ = wal;
    assert!(rep.restore.log_objects > 0);
    // Claimed at the next epoch under a new id, then the fence record.
    assert_eq!(rep.epoch, a_epoch + 1);
    assert_ne!(rep.writer_id, a_writer);
    assert_eq!(head_field(&mem, "writer_id"), rep.writer_id);
    assert_eq!(head_field(&mem, "epoch"), rep.epoch.to_string());
    assert!(head_body(&mem).contains("\"journal_mode\":\"mvcc\""));
    let ks = kinds(&mem, gen);
    assert_eq!(*ks.last().unwrap(), 2, "{ks:?}");
    assert_eq!(rep.fence_seq as usize, ks.len());

    // B opens, continues, commits, checkpoints, commits.
    let db_b = open_with_store(mvcc(&dir), mem.clone()).unwrap();
    let cb = db_b.connect().unwrap();
    assert_eq!(ids(&cb), (1..=10).collect());
    for id in 11..=15 {
        insert_c(&cb, id).unwrap();
    }
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    assert!(generation(&mem) > gen);
    for id in 16..=18 {
        insert_c(&cb, id).unwrap();
    }
    assert_eq!(integrity(&cb), "ok");
    // A third restore has all rows.
    assert_eq!(restored_ids(mem.clone()), (1..=18).collect());
    cb.close().unwrap();
    db_b.close().unwrap();
    assert_eq!(restored_ids(mem.clone()), (1..=18).collect());
    // A is fenced.
    assert!(matches!(insert_c(&ca, 99), Err(Error::Fenced(_))));
    crash(db_a, vec![ca]);
}

/// A's kind-3 PUT is stored and its reply lost for good (the commit Errs), then a takeover
/// before the next commit: B's restore holds the record; no commit A acked is missing; A is
/// fenced. Run with one committer, then with three concurrent ones (a group commit: a
/// follower is acked only if its own record is in the bucket).
#[test]
fn takeover_after_in_doubt_kind3_record() {
    for writers in [1usize, 3] {
        let mem = Arc::new(MemStore::new());
        let sched = Arc::new(Sched::new(mem.clone()));
        let (_a, db_a, ca) = writer(sched.clone(), 5);
        let mut acked: BTreeSet<i64> = (1..=5).collect();
        sched.arm(0, Kind::StoreThenFailPermanent);
        let outcomes: Arc<Mutex<Vec<(i64, bool)>>> = Arc::default();
        let threads: Vec<_> = (0..writers)
            .map(|w| {
                let (db, outcomes) = (db_a.clone(), outcomes.clone());
                thread::spawn(move || {
                    let c = db.connect().unwrap();
                    let id = 6 + w as i64;
                    let r = insert_c(&c, id);
                    outcomes.lock().unwrap().push((id, r.is_ok()));
                    std::mem::forget(c);
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let outcomes = std::mem::take(&mut *outcomes.lock().unwrap());
        for (id, ok) in &outcomes {
            if *ok {
                acked.insert(*id);
            }
        }
        eprintln!("{writers} committers: {outcomes:?}");
        assert!(
            outcomes.iter().any(|(_, ok)| !ok),
            "the in-doubt commit must Err: {outcomes:?}"
        );
        let stored_records = kinds(&mem, generation(&mem)).len();

        let bt = tempdir();
        let dir = fresh(&bt);
        let rep = takeover_with_store(mvcc(&dir), mem.clone()).unwrap();
        assert_eq!(rep.fence_seq as usize, stored_records + 1);
        let r = insert_c(&ca, 99);
        assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
        let db_b = open_with_store(mvcc(&dir), mem.clone()).unwrap();
        let cb = db_b.connect().unwrap();
        let rows = ids(&cb);
        assert!(rows.is_superset(&acked), "{rows:?} vs {acked:?}");
        assert!(!rows.contains(&99));
        if writers == 1 {
            assert!(rows.contains(&6), "the stored record is in B's restore");
        }
        insert_c(&cb, 100).unwrap();
        cb.close().unwrap();
        db_b.close().unwrap();
        let fin = restored_ids(mem.clone());
        assert!(fin.is_superset(&acked) && fin.contains(&100));
        crash(db_a, vec![ca]);
    }
}

// ---------------------------------------------------------------------------------------
// sync is Ok only when every op is confirmed
// ---------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Kind {
    /// Not stored, reply a permanent error.
    FailPermanent,
    /// Stored, reply a permanent error (the writer must not ack; the record is in the bucket).
    StoreThenFailPermanent,
    /// Not stored, transient error (retried).
    FailTransient,
    /// Stored, transient error (the retry finds its own record).
    StoreThenFailTransient,
}

/// Fails exactly the `at`-th put_if_absent of a log key (counted from `arm`), in `kind`.
struct Sched {
    inner: Arc<dyn ObjectStore>,
    armed: Mutex<Option<(u64, Kind)>>,
    seen: AtomicU64,
}

impl Sched {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            armed: Mutex::new(None),
            seen: AtomicU64::new(0),
        }
    }
    fn arm(&self, at: u64, kind: Kind) {
        self.seen.store(0, SeqCst);
        *self.armed.lock().unwrap() = Some((at, kind));
    }
    fn verdict(&self, key: &str) -> Option<Kind> {
        if !key.contains("/log/") {
            return None;
        }
        let n = self.seen.fetch_add(1, SeqCst);
        match *self.armed.lock().unwrap() {
            Some((at, kind)) if at == n => Some(kind),
            _ => None,
        }
    }
}

fn transient() -> StoreError {
    StoreError::Transient("injected".into())
}
fn permanent() -> StoreError {
    StoreError::Permanent("injected".into())
}

impl ObjectStore for Sched {
    fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> Result<CondPut, StoreError> {
        match self.verdict(key) {
            None => self.inner.put_if_absent(key, body),
            Some(Kind::FailPermanent) => Err(permanent()),
            Some(Kind::FailTransient) => Err(transient()),
            Some(Kind::StoreThenFailPermanent) => {
                let _ = self.inner.put_if_absent(key, body);
                Err(permanent())
            }
            Some(Kind::StoreThenFailTransient) => {
                let _ = self.inner.put_if_absent(key, body);
                Err(transient())
            }
        }
    }
    fn put_if_match(&self, key: &str, body: &[u8], etag: &str) -> Result<CondPut, StoreError> {
        self.inner.put_if_match(key, body, etag)
    }
    fn get(&self, key: &str) -> Result<Option<Object>, StoreError> {
        self.inner.get(key)
    }
    fn stat(&self, key: &str) -> Result<Option<ObjectStat>, StoreError> {
        self.inner.stat(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

/// For every log-PUT index `at` of a 2-4 writer BEGIN CONCURRENT workload and every fault kind:
/// each commit that returned Ok is in a restore of the bucket; the restore is intact; a
/// reopen of the crashed local directory (which reconciles) ends equal to the bucket.
#[test]
fn sync_ok_only_when_all_ops_confirmed() {
    let kinds = [
        Kind::FailPermanent,
        Kind::StoreThenFailPermanent,
        Kind::FailTransient,
        Kind::StoreThenFailTransient,
    ];
    let mut errs = 0;
    let mut oks = 0;
    for writers in [2usize, 4] {
        for kind in kinds {
            for at in 0..8u64 {
                let mem = Arc::new(MemStore::new());
                let sched = Arc::new(Sched::new(mem.clone()));
                let t = tempdir();
                let dir = fresh(&t);
                let db = open_with_store(mvcc(&dir), sched.clone()).unwrap();
                let c0 = db.connect().unwrap();
                create(&c0);
                insert_c(&c0, 1000).unwrap();
                sched.arm(at, kind);
                let outcomes: Arc<Mutex<Vec<(i64, bool)>>> = Arc::default();
                let threads: Vec<_> = (0..writers)
                    .map(|w| {
                        let (db, outcomes) = (db.clone(), outcomes.clone());
                        thread::spawn(move || {
                            let c = db.connect().unwrap();
                            for k in 0..3 {
                                let id = (w * 10 + k) as i64 + 1;
                                let r = insert_c(&c, id);
                                outcomes.lock().unwrap().push((id, r.is_ok()));
                            }
                            std::mem::forget(c);
                        })
                    })
                    .collect();
                for th in threads {
                    th.join().unwrap();
                }
                let outcomes = std::mem::take(&mut *outcomes.lock().unwrap());
                let mut acked: BTreeSet<i64> = BTreeSet::from([1000]);
                let mut attempted = acked.clone();
                for (id, ok) in &outcomes {
                    attempted.insert(*id);
                    if *ok {
                        acked.insert(*id);
                        oks += 1;
                    } else {
                        errs += 1;
                    }
                }
                let what = format!("{writers} writers, {kind:?}, put {at}: {outcomes:?}");
                let copy = copy_dir(&dir);
                crash(db, vec![c0]);

                // Disable the fault; the bucket as it stands.
                sched.arm(u64::MAX, kind);
                let restored = restored_ids(mem.clone());
                assert!(
                    restored.is_superset(&acked),
                    "acked row lost: {what}: {restored:?}"
                );
                assert!(restored.is_subset(&attempted), "{what}: {restored:?}");

                // The crashed directory reopened: local equals the bucket afterwards.
                let db2 = open_with_store(mvcc(copy.path()), sched.clone()).unwrap();
                let c = db2.connect().unwrap();
                let local = ids(&c);
                assert_eq!(integrity(&c), "ok", "{what}");
                assert!(local.is_superset(&acked), "{what}");
                c.close().unwrap();
                db2.close().unwrap();
                assert_eq!(
                    restored_ids(mem.clone()),
                    local,
                    "local != restored: {what}"
                );
            }
        }
    }
    eprintln!("commits acked {oks}, Err {errs}");
    assert!(
        errs > 0,
        "no schedule produced an Err: the property is vacuous"
    );
    assert!(oks > 0);
}

// ---------------------------------------------------------------------------------------
// Verify of a mid-checkpoint restore; restore through the library
// ---------------------------------------------------------------------------------------

/// A seal that fails (the manifest PUT) leaves the kind-1 frames next to the whole kind-3
/// log in the bucket: the image of a restore taken mid-checkpoint. It opens read-only, every
/// acked row reads back, `integrity_check` ok, and the bucket is not touched.
#[test]
fn verify_of_mid_checkpoint_restore() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let (_a, db, c) = writer(fault.clone(), 10);
    fault.only_keys_containing("/manifest/");
    fault.fail_puts_after(0);
    assert!(c.checkpoint(CheckpointMode::Truncate).is_err());
    fault.fail_puts_after(u64::MAX);
    fault.only_keys_containing("");
    insert_c(&c, 11).unwrap();
    insert_c(&c, 12).unwrap();
    crash(db, vec![c]);
    let gen = generation(&mem);
    let ks = kinds(&mem, gen);
    assert!(ks.contains(&1) && ks.contains(&3), "mid-checkpoint: {ks:?}");

    let d = tempdir();
    let dir = fresh(&d);
    restore_with_store(&mvcc(&dir), mem.clone(), &dir).unwrap();
    assert!(
        std::fs::metadata(dir.join("app.db-wal")).unwrap().len() > 0,
        "WAL frames in the image"
    );
    let objects = mem.list("").unwrap();
    let puts = mem.stats().puts;
    let ro = open_read_only_with_store(mvcc(&dir), mem.clone()).unwrap();
    let rc = ro.connect().unwrap();
    assert_eq!(ids(&rc), (1..=12).collect());
    assert_eq!(integrity(&rc), "ok");
    rc.close().unwrap();
    ro.close().unwrap();
    assert_eq!(mem.list("").unwrap(), objects);
    assert_eq!(
        mem.stats().puts,
        puts,
        "a read-only open wrote to the bucket"
    );
}

#[test]
fn restore_through_library_format_v2() {
    let mem = Arc::new(MemStore::new());
    let (_a, db, c) = writer(mem.clone(), 6);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    for id in 7..=9 {
        insert_c(&c, id).unwrap();
    }
    crash(db, vec![c]);
    let gen = generation(&mem);
    let ks = kinds(&mem, gen);
    assert!(ks.contains(&3), "current generation holds kind-3: {ks:?}");

    let d = tempdir();
    let dir = fresh(&d);
    let rep = restore_with_store(&mvcc(&dir), mem.clone(), &dir).unwrap();
    assert_eq!(rep.generation, gen);
    assert_eq!(
        rep.log_objects as usize,
        ks.len(),
        "kind 1 and kind 3 counted"
    );
    assert!(std::fs::metadata(dir.join("app.db-log")).unwrap().len() > 56);
    let db = open_with_store(mvcc(&dir), mem.clone()).unwrap();
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), (1..=9).collect());
    assert_eq!(integrity(&c), "ok");
    c.close().unwrap();
    db.close().unwrap();

    // A WAL config refuses the MVCC bucket and writes nothing.
    let w = tempdir();
    let wd = fresh(&w);
    let before = mem.list("").unwrap();
    let r = restore_with_store(&config(&wd, JournalMode::Wal), mem.clone(), &wd);
    assert!(r.is_err(), "{r:?}");
    assert_eq!(mem.list("").unwrap(), before);
}

// ---------------------------------------------------------------------------------------
// v1 buckets stay v1
// ---------------------------------------------------------------------------------------

/// The same operations on a WAL database: head format 1 with no journal_mode key, only kind-1
/// records (and no kind 3 anywhere); the MVCC one has format 2, journal_mode, kind-3 records.
#[test]
fn wal_bucket_stays_format_1_under_the_same_operations() {
    for (mode, format) in [(JournalMode::Wal, 1), (JournalMode::Mvcc, 2)] {
        let mem = Arc::new(MemStore::new());
        let t = tempdir();
        let db = open_with_store(config(&fresh(&t), mode), mem.clone()).unwrap();
        let c = db.connect().unwrap();
        create(&c);
        for id in 1..=5 {
            match mode {
                JournalMode::Wal => {
                    c.execute(
                        "INSERT INTO t(id, v) VALUES (?, 'x')",
                        &[Value::from_i64(id)],
                    )
                    .unwrap();
                }
                JournalMode::Mvcc => insert_c(&c, id).unwrap(),
            }
        }
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        for id in 6..=8 {
            match mode {
                JournalMode::Wal => {
                    c.execute(
                        "INSERT INTO t(id, v) VALUES (?, 'x')",
                        &[Value::from_i64(id)],
                    )
                    .unwrap();
                }
                JournalMode::Mvcc => insert_c(&c, id).unwrap(),
            }
        }
        let body = head_body(&mem);
        assert!(
            body.contains(&format!("\"format\":{format}")),
            "{mode:?}: {body}"
        );
        assert_eq!(body.contains("journal_mode"), format == 2, "{body}");
        let mut seen_kinds = BTreeSet::new();
        for k in mem.list(&format!("{PREFIX}/log/")).unwrap() {
            seen_kinds.insert(mem.get(&k).unwrap().unwrap().body[8]);
        }
        match mode {
            JournalMode::Wal => assert_eq!(seen_kinds, BTreeSet::from([1]), "{seen_kinds:?}"),
            JournalMode::Mvcc => assert!(seen_kinds.contains(&3), "{seen_kinds:?}"),
        }
        c.close().unwrap();
        db.close().unwrap();
    }
}
