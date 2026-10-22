//! Task 6: restore and reconcile with two images (`app.db-wal` from kind 1, `app.db-log` from
//! kind 3). Rows are checked by opening the restored or reconciled directory with the library
//! and reading them back, with `PRAGMA integrity_check`.
use super::*;
use crate::bucket::Bucket;
use crate::format::encode_record;
use crate::reconcile::{reconcile_files, Outcomes};
use crate::restore::repair_wal_without_log;
use std::sync::Mutex;
use turso_core::mvcc::persistent_storage::{DurableStorage, Storage};

/// A copy of `from`'s files (not its lock) in a new directory: the disk of a crashed process.
fn copy_dir(from: &Path) -> TempDir {
    let to = tempdir();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_file() && e.file_name() != "durable.lock" {
            std::fs::copy(e.path(), to.path().join(e.file_name())).unwrap();
        }
    }
    to
}

fn integrity_of(c: &Connection) -> String {
    text(&c.query("PRAGMA integrity_check", &[]).unwrap()[0][0])
}

/// Open `dir` as a writer (a restore and claim when it holds nothing), read `t`, check
/// `integrity_check`, and crash (nothing runs on drop, so the bucket is left as it was).
/// Returns what the open's reconcile found.
fn check_open(store: &Arc<MemStore>, dir: &Path, want: &[i64]) -> Option<Reconciled> {
    let db = open_mvcc(store.clone(), mvcc(dir));
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), want);
    assert_eq!(integrity_of(&c), "ok");
    let r = db.shared.reconciled.clone();
    crash(db, vec![c]);
    r
}

/// The rows a restore into a new directory holds (the restore claims: do it last).
fn restored_rows(store: &Arc<MemStore>, want: &[i64]) {
    let t = tempdir();
    check_open(store, &fresh(&t), want);
}

/// `n` rows in a new MVCC database, nothing checkpointed, then a crash: the bucket and a
/// copy of the local disk.
fn crashed_with_rows(n: i64) -> (Arc<MemStore>, TempDir) {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), n);
    let copy = copy_dir(&dir);
    crash(db, vec![c]);
    (store, copy)
}

/// What each file of a crashed `dir` needs (attached as its own writer, as an open does).
fn outcomes(store: &Arc<MemStore>, dir: &Path) -> Outcomes {
    let cfg = mvcc(dir).engine(false);
    let mut state = LocalState::read(dir).unwrap().expect("local state");
    let mut b = Bucket::attach(
        store.clone(),
        keys(),
        cfg.retry.clone(),
        cfg.upload_concurrency,
        &state.writer_id,
        state.epoch,
    )
    .unwrap();
    reconcile_files(dir, &mut b, &mut state, &cfg).unwrap_or_else(|e| panic!("reconcile: {e}"))
}

/// The directory after the engine's own cut of a checkpoint's files: a crash copy at the log
/// truncate. `before`: the seal is done, the log and the `-wal` are whole. `after`: the log is
/// cut, the `-wal` is not.
struct Snaps {
    before: TempDir,
    after: TempDir,
}

struct SnapLog {
    inner: Arc<dyn File>,
    dir: PathBuf,
    snaps: Arc<Mutex<Vec<TempDir>>>,
}

impl File for SnapLog {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, b: Arc<Buffer>, c: Completion) -> turso_core::Result<Completion> {
        self.inner.pwrite(pos, b, c)
    }
    fn pwritev(
        &self,
        pos: u64,
        b: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.inner.pwritev(pos, b, c)
    }
    fn sync(&self, c: Completion, t: FileSyncType) -> turso_core::Result<Completion> {
        self.inner.sync(c, t)
    }
    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        let first = len == 0 && self.snaps.lock().unwrap().is_empty();
        if first {
            self.snaps.lock().unwrap().push(copy_dir(&self.dir));
        }
        let r = self.inner.truncate(len, c);
        if first {
            self.snaps.lock().unwrap().push(copy_dir(&self.dir));
        }
        r
    }
}

/// `n` rows, then a TRUNCATE checkpoint that succeeds, with copies of the directory taken
/// inside it at the log truncate. The database is then crashed. Per step 6 and 8 of the
/// checkpoint (docs/mvcc-recon.md Q3).
fn checkpoint_with_snapshots(store: &Arc<MemStore>, n: i64) -> Snaps {
    let t = tempdir();
    let dir = fresh(&t);
    let snaps: Arc<Mutex<Vec<TempDir>>> = Arc::default();
    let (s, d) = (snaps.clone(), dir.clone());
    WRAP_LOG_FILE.with(|w| {
        *w.borrow_mut() = Some(Arc::new(move |inner: Arc<dyn File>| -> Arc<dyn File> {
            Arc::new(SnapLog {
                inner,
                dir: d.clone(),
                snaps: s.clone(),
            })
        }))
    });
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), n);
    WRAP_LOG_FILE.with(|w| *w.borrow_mut() = None);
    truncate_checkpoint(&c).unwrap();
    assert_eq!(db.generation(), 3);
    crash(db, vec![c]);
    let mut v = std::mem::take(&mut *snaps.lock().unwrap());
    assert_eq!(v.len(), 2, "the log was cut once");
    let after = v.pop().unwrap();
    let before = v.pop().unwrap();
    Snaps { before, after }
}

/// A plain (no library) open of the files in `dir`: MVCC, `integrity_check` ok; the ids in `t`.
fn plain_rows_in(dir: &Path) -> Vec<i64> {
    use turso_core::{OpenOptions, SqliteDialect};
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = OpenOptions::new(Arc::new(SqliteDialect));
    let db = turso_core::Database::open(io, dir.join("app.db").to_str().unwrap(), opts)
        .unwrap_or_else(|e| panic!("plain open: {e}"));
    let conn = db.connect().unwrap();
    let q = |sql: &str| {
        conn.query(sql)
            .unwrap()
            .unwrap()
            .run_collect_rows()
            .unwrap()
    };
    assert_eq!(text(&q("PRAGMA integrity_check")[0][0]), "ok");
    let ids = q("SELECT id FROM t ORDER BY id")
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect();
    conn.close().unwrap();
    ids
}

/// Step 1 of the plan: does the engine's `reset_to_fresh_header` work on a `Storage` built
/// over a bare file, outside any open engine? It does: the header-only 56-byte log it leaves
/// is the engine's (`LML2` magic, version 2, header length 56, a nonzero random salt, a fresh
/// one each time), not encrypted, so the restore uses it (`repair_wal_without_log`).
#[test]
fn reset_to_fresh_header_works_standalone() {
    let t = tempdir();
    let path = t.path().join("app.db-log");
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let mut salts = Vec::new();
    for round in 0..2 {
        let file = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();
        // The leftovers of a crashed log are cut to the header.
        if round == 1 {
            std::fs::write(&path, vec![7u8; 300]).unwrap();
        }
        let storage = Storage::new(file.clone(), io.clone(), None);
        let c = storage.reset_to_fresh_header().expect("reset");
        io.wait_for_completion(c).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 56, "round {round}");
        assert_eq!(&bytes[0..4], &0x4C4D_4C32u32.to_le_bytes(), "LML2");
        assert_eq!(bytes[4], 2, "version");
        assert_eq!(&bytes[6..8], &56u16.to_le_bytes(), "header length");
        let salt = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        assert_ne!(salt, 0);
        salts.push(salt);
    }
    assert_ne!(salts[0], salts[1], "a fresh salt each time");
}

/// A bucket with both images: a failed seal leaves the `-wal` frames and the whole log in the
/// generation. Restore writes `app.db-wal` from kind 1 and `app.db-log` from kind 3, and the
/// restored directory opens with every acked row.
#[test]
fn restore_v2_bucket_writes_wal_and_log_images() {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = seal_fails_then_two_commits(&store, &dir);
    let b = bucket_image(&*mem);
    assert!(
        b.kinds.contains(&1) && b.kinds.contains(&3),
        "{:?}",
        b.kinds
    );
    assert!(!b.wal.is_empty() && !b.log.is_empty());
    crash(db, vec![c]);

    let t2 = tempdir();
    let d2 = fresh(&t2);
    let report = restore_with_store(&mvcc(&d2), mem.clone(), &d2).unwrap();
    assert_eq!(report.generation, 2);
    assert_eq!(report.log_objects, b.kinds.len() as u64);
    assert_eq!(read(&d2, "app.db-wal"), b.wal);
    assert_eq!(read(&d2, "app.db-log"), b.log);
    let want: Vec<i64> = (1..=12).collect();
    check_open(&mem, &d2, &want);
}

/// A kind-3 record cut in the middle of a frame (a torn tail) restores to the rows before it.
#[test]
fn restore_prefix_of_log_gives_prefix_rows() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 10);
    let n = bucket_image(&*store).kinds.len() as u64;
    crash(db, vec![c]);
    let key = keys().log(2, n);
    let LogRecord::Extents {
        file,
        writer_id,
        mut syncs,
    } = decode_record(&store.get(&key).unwrap().unwrap().body).unwrap()
    else {
        panic!("not extents")
    };
    assert_eq!(file, LogFile::Log);
    let Some(Op::Write { bytes, .. }) = syncs.last_mut().and_then(|s| s.last_mut()) else {
        panic!("no write")
    };
    let cut = bytes.len() - 9;
    bytes.truncate(cut);
    store
        .put(
            &key,
            &encode_record(&LogRecord::Extents {
                file,
                writer_id,
                syncs,
            }),
        )
        .unwrap();
    restored_rows(&store, &(1..=9).collect::<Vec<_>>());
}

/// P5 (d): WAL frames next to a log under 56 bytes are refused by the engine; a fresh log
/// header repairs it, and the rows are those in the pages (the log's frames are dropped).
#[test]
fn wal_frames_with_empty_log_is_repaired_and_opens() {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = seal_fails_then_two_commits(&store, &dir);
    let b = bucket_image(&*mem);
    crash(db, vec![c]);
    // The bucket's `-wal` image next to an empty log.
    let d = tempdir();
    std::fs::write(d.path().join("app.db"), &b.db).unwrap();
    std::fs::write(d.path().join("app.db-wal"), &b.wal).unwrap();
    std::fs::write(d.path().join("app.db-log"), b"").unwrap();
    // The state the engine refuses.
    let refused = {
        use turso_core::{OpenOptions, SqliteDialect};
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let probe = copy_dir(d.path());
        turso_core::Database::open(
            io,
            probe.path().join("app.db").to_str().unwrap(),
            OpenOptions::new(Arc::new(SqliteDialect)),
        )
        .map_err(|e| e.to_string())
        .and_then(|db| db.connect().map(|_| ()).map_err(|e| e.to_string()))
    };
    let e = refused.expect_err("the engine opened WAL frames with an empty log");
    assert!(e.contains("logical log header is missing"), "{e}");
    assert!(repair_wal_without_log(d.path()).unwrap());
    let log = std::fs::read(d.path().join("app.db-log")).unwrap();
    assert_eq!(log.len(), 56);
    assert_eq!(&log[0..4], &0x4C4D_4C32u32.to_le_bytes());
    assert_eq!(plain_rows_in(d.path()), (1..=10).collect::<Vec<_>>());
    // Idempotent: a log with a header is left alone.
    let d2 = tempdir();
    for f in ["app.db", "app.db-wal"] {
        std::fs::copy(d.path().join(f), d2.path().join(f)).unwrap();
    }
    std::fs::write(d2.path().join("app.db-log"), &log).unwrap();
    assert!(!repair_wal_without_log(d2.path()).unwrap());
    assert_eq!(std::fs::read(d2.path().join("app.db-log")).unwrap(), log);
    // A WAL without a frame is not repaired.
    let d3 = tempdir();
    std::fs::write(d3.path().join("app.db-wal"), vec![0u8; 32]).unwrap();
    assert!(!repair_wal_without_log(d3.path()).unwrap());
    assert!(!d3.path().join("app.db-log").exists());
}

/// The same state in the bucket: kind-1 frames and a kind-3 image that is empty. A restore
/// writes the log the fresh header, and the restored directory opens (the log's rows are
/// lost with it: 10 rows).
#[test]
fn restore_with_wal_frames_and_empty_log_image_writes_a_header() {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = seal_fails_then_two_commits(&store, &dir);
    let b = bucket_image(&*mem);
    crash(db, vec![c]);
    for (seq, kind) in b.kinds.iter().enumerate() {
        if *kind != 3 {
            continue;
        }
        let key = keys().log(2, seq as u64 + 1);
        let LogRecord::Extents {
            file, writer_id, ..
        } = decode_record(&mem.get(&key).unwrap().unwrap().body).unwrap()
        else {
            panic!("not extents")
        };
        let empty = LogRecord::Extents {
            file,
            writer_id,
            syncs: vec![vec![]],
        };
        mem.put(&key, &encode_record(&empty)).unwrap();
    }
    let t2 = tempdir();
    let d2 = fresh(&t2);
    restore_with_store(&mvcc(&d2), mem.clone(), &d2).unwrap();
    assert_eq!(read(&d2, "app.db-wal"), b.wal);
    assert_eq!(read(&d2, "app.db-log").len(), 56);
    // The same repair runs on the local directory before the engine opens it: a log cut to
    // nothing (Level with the empty image), or holding the header (NewGeneration: the image
    // is empty), is given the header again next to the `-wal` frames.
    for local in [Vec::new(), read(&d2, "app.db-log")] {
        let copy = copy_dir(&d2);
        std::fs::write(copy.path().join("app.db-log"), &local).unwrap();
        let o = outcomes(&mem, copy.path());
        assert_eq!(o.wal, Reconciled::Level);
        assert_eq!(
            o.log,
            Some(if local.is_empty() {
                Reconciled::Level
            } else {
                Reconciled::NewGeneration
            })
        );
        assert_eq!(read(copy.path(), "app.db-log").len(), 56);
    }
    check_open(&mem, &d2, &(1..=10).collect::<Vec<_>>());
}

/// A bucket made by creation (no kind-3 record yet) restores into a directory that opens.
#[test]
fn restore_of_created_mvcc_bucket_opens() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let db = open_mvcc(store.clone(), mvcc(&dir));
    crash(db, vec![]);
    assert_eq!(bucket_image(&*store).kinds, Vec::<u8>::new());
    let t2 = tempdir();
    let d2 = fresh(&t2);
    restore_with_store(&mvcc(&d2), store.clone(), &d2).unwrap();
    assert!(d2.join("app.db-log").exists(), "the restore writes the log");
    let db = open_mvcc(store.clone(), mvcc(&d2));
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=3 {
        insert_c(&c, id).unwrap();
    }
    assert_eq!(ids(&c), vec![1, 2, 3]);
    assert_eq!(integrity_of(&c), "ok");
    // A seal on the restored (empty-log) directory, then a commit after it, then another
    // restore: every row, and the sealed generation's log image starts with the commit's header.
    truncate_checkpoint(&c).unwrap();
    assert_eq!(db.generation(), 3);
    insert_c(&c, 4).unwrap();
    assert_eq!(ids(&c), vec![1, 2, 3, 4]);
    assert_eq!(integrity_of(&c), "ok");
    crash(db, vec![c]);
    restored_rows(&store, &[1, 2, 3, 4]);
}

/// Level, Behind, Ahead and NewGeneration, each for the `.db-log`.
#[test]
fn reconcile_log_level_behind_ahead_newgeneration() {
    // Level: the local log is the image.
    let (store, d) = crashed_with_rows(5);
    let b = bucket_image(&*store);
    assert_eq!(b.log, read(d.path(), "app.db-log"));
    let o = outcomes(&store, d.path());
    assert_eq!((o.wal, o.log), (Reconciled::Level, Some(Reconciled::Level)));
    assert_eq!(read(d.path(), "app.db-log"), b.log);

    // Behind: the local disk lost the log's tail.
    let (store, d) = crashed_with_rows(5);
    let b = bucket_image(&*store);
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(d.path().join("app.db-log"))
        .unwrap();
    f.set_len(b.log.len() as u64 / 2).unwrap();
    drop(f);
    let o = outcomes(&store, d.path());
    assert_eq!(o.log, Some(Reconciled::Behind));
    assert_eq!(o.wal, Reconciled::Level);
    assert_eq!(read(d.path(), "app.db-log"), b.log, "replaced by the image");
    assert!(!state(d.path()).complete);
    check_open(&store, d.path(), &(1..=5).collect::<Vec<_>>());

    // Ahead: the local log holds a tail the bucket never got; it goes up as a kind-3 record.
    let (store, d) = crashed_with_rows(5);
    let before = bucket_image(&*store);
    let mut local = before.log.clone();
    local.extend_from_slice(&[0xEE; 40]);
    std::fs::write(d.path().join("app.db-log"), &local).unwrap();
    let o = outcomes(&store, d.path());
    assert!(matches!(o.log, Some(Reconciled::Ahead { .. })), "{o:?}");
    let after = bucket_image(&*store);
    assert_eq!(after.log, local, "the image follows the local file");
    assert_eq!(after.kinds.len(), before.kinds.len() + 1);
    assert_eq!(*after.kinds.last().unwrap(), 3);
    assert_eq!(read(d.path(), "app.db-log"), local);

    // NewGeneration: the seal is done (the new generation's image is empty) and the old
    // frames are still in the local log.
    let store = mem();
    let s = checkpoint_with_snapshots(&store, 5);
    assert!(!read(s.before.path(), "app.db-log").is_empty());
    assert_eq!(state(s.before.path()).generation, 3);
    assert!(bucket_image(&*store).log.is_empty());
    let o = outcomes(&store, s.before.path());
    assert_eq!(o.log, Some(Reconciled::NewGeneration));
    assert_eq!(o.wal, Reconciled::NewGeneration);
    assert_eq!(read(s.before.path(), "app.db-log"), Vec::<u8>::new());
    assert_eq!(read(s.before.path(), "app.db-wal"), Vec::<u8>::new());
}

/// Seal done, log not yet cut (the crash between steps 6 and 7): the new generation's images
/// are empty, so reconcile replaces both local files, and every acked row is in the sealed
/// pages. On the same disk and restored onto another.
#[test]
fn crash_between_seal_and_log_truncate_reconciles_new_generation() {
    let store = mem();
    let s = checkpoint_with_snapshots(&store, 10);
    let dir = s.before.path();
    assert!(!read(dir, "app.db-log").is_empty(), "the log is whole");
    assert!(!read(dir, "app.db-wal").is_empty(), "the -wal is whole");
    assert_eq!(state(dir).generation, 3);
    let want: Vec<i64> = (1..=10).collect();
    let r = check_open(&store, dir, &want);
    assert_eq!(r, Some(Reconciled::NewGeneration));
    // The local disk lost: another machine restores.
    restored_rows(&store, &want);
}

/// Log cut, `-wal` not yet (the crash between steps 7 and 8): the log is empty next to
/// `-wal` frames, which reconcile replaces with the empty image, so the engine opens.
#[test]
fn crash_between_log_truncate_and_wal_truncate_opens() {
    let store = mem();
    let s = checkpoint_with_snapshots(&store, 10);
    let dir = s.after.path();
    assert!(read(dir, "app.db-log").is_empty(), "the log is cut");
    assert!(!read(dir, "app.db-wal").is_empty(), "the -wal is whole");
    let want: Vec<i64> = (1..=10).collect();
    let o = outcomes(&store, copy_dir(dir).path());
    assert_eq!(o.log, Some(Reconciled::Level));
    assert_eq!(o.wal, Reconciled::NewGeneration);
    let r = check_open(&store, dir, &want);
    assert_eq!(r, Some(Reconciled::NewGeneration));
    restored_rows(&store, &want);
}

/// A restore of an MVCC bucket by a WAL `Config` is refused before anything is fetched or
/// written: nothing changes in the bucket, nothing in the directory.
#[test]
fn restore_of_mvcc_bucket_by_wal_config_is_refused() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 3);
    crash(db, vec![c]);
    let objects: Vec<_> = store
        .list("")
        .unwrap()
        .into_iter()
        .map(|k| (store.get(&k).unwrap().unwrap().body, k))
        .collect();
    let puts = store.stats().puts;
    let t2 = tempdir();
    let d2 = fresh(&t2);
    let r = restore_with_store(&config(&d2), store.clone(), &d2);
    assert_eq!(unsupported(r), SWITCH_BACK);
    let after: Vec<_> = store
        .list("")
        .unwrap()
        .into_iter()
        .map(|k| (store.get(&k).unwrap().unwrap().body, k))
        .collect();
    assert_eq!(after, objects, "the bucket is unchanged");
    assert_eq!(store.stats().puts, puts);
    assert!(
        !d2.exists()
            || std::fs::read_dir(&d2)
                .unwrap()
                .all(|e| { e.unwrap().file_name() == "durable.lock" }),
        "nothing restored"
    );
}

/// A WAL bucket restores to the kind-1 replay in `app.db-wal`, the seed (or nothing) in
/// `app.db`, the local state, and no `app.db-log` (a live database; the golden bytes are
/// `restore_wal_bucket_bytes_identical_to_before`).
#[test]
fn restore_wal_bucket_files_match_kind1_replay() {
    for sealed in [false, true] {
        let store = mem();
        let t = tempdir();
        let dir = fresh(&t);
        let db = open_mem(&store, &dir);
        let c = db.connect().unwrap();
        create(&c);
        for id in 1..=5 {
            insert(&c, id).unwrap();
        }
        if sealed {
            c.checkpoint(CheckpointMode::Truncate).unwrap();
            insert(&c, 6).unwrap();
        }
        crash(db, vec![c]);
        let h = head(&*store);
        let mut wal = Vec::new();
        for seq in 1.. {
            let Some(o) = store.get(&keys().log(h.generation, seq)).unwrap() else {
                break;
            };
            if let LogRecord::Extents { file, syncs, .. } = decode_record(&o.body).unwrap() {
                assert_eq!(file, LogFile::Wal);
                for ops in syncs {
                    apply_ops(&mut wal, &ops).unwrap();
                }
            }
        }
        let t2 = tempdir();
        let d2 = fresh(&t2);
        restore_with_store(&config(&d2), store.clone(), &d2).unwrap();
        assert_eq!(read(&d2, "app.db-wal"), wal, "sealed={sealed}");
        let seed = crate::restore::wal_page_one(&wal).map(<[u8]>::to_vec);
        let want_db = if h.manifest.is_some() {
            Vec::new()
        } else {
            seed.expect("a page-1 frame")
        };
        assert_eq!(read(&d2, "app.db"), want_db, "sealed={sealed}");
        let mut names: Vec<String> = std::fs::read_dir(&d2)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n != "durable.lock")
            .collect();
        names.sort();
        assert_eq!(
            names,
            ["app.db", "app.db-durable", "app.db-wal"],
            "sealed={sealed}"
        );
    }
}

/// Commits in the current generation, a crash, a reopen of the same disk: the log is Level (or
/// Ahead of the image), the reopen no longer fails, and every acked row is there. The disk is
/// then lost and a restore onto a new one, claimed as the new writer, finds every acked row.
#[test]
fn reopen_with_commits_in_current_generation_and_restore() {
    let (store, d) = crashed_with_rows(8);
    let want: Vec<i64> = (1..=8).collect();
    let r = check_open(&store, d.path(), &want);
    assert_eq!(r, Some(Reconciled::Level));
    restored_rows(&store, &want);
}

/// Mid-adopt: the head's PUT stored and its reply was lost, the next checkpoint adopted it and
/// failed on its uploads, two more commits were acked. A restore onto a new disk has them all.
#[test]
fn restore_after_adopt_and_failed_checkpoint_opens() {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 10);
    head_stored_reply_lost(&store, &db, &c);
    store.only_keys_containing("/log/");
    store.fail_puts_after(0);
    assert!(truncate_checkpoint(&c).is_err());
    store.fail_puts_after(u64::MAX);
    store.only_keys_containing("");
    insert_c(&c, 11).unwrap();
    insert_c(&c, 12).unwrap();
    let copy = copy_dir(&dir);
    crash(db, vec![c]);
    let want: Vec<i64> = (1..=12).collect();
    check_open(&mem, copy.path(), &want);
    restored_rows(&mem, &want);
}

/// Golden bytes: a hand-built WAL bucket (two page-512 frames; records whose replay depends on
/// the order of records, of syncs inside a record, and on a truncate) restores to bytes written
/// out by hand here, not by `apply_ops`. The same test was run against the base commit
/// (ecc9f4f) and passed there.
#[test]
fn restore_wal_bucket_bytes_identical_to_before() {
    const PS: usize = 512;
    let mut header = vec![0u8; 32];
    header[0..4].copy_from_slice(&0x377f_0682u32.to_be_bytes());
    header[4..8].copy_from_slice(&3_007_000u32.to_be_bytes());
    header[8..12].copy_from_slice(&(PS as u32).to_be_bytes());
    let frame = |pgno: u32, fill: u8| {
        let mut f = vec![0u8; 24 + PS];
        f[0..4].copy_from_slice(&pgno.to_be_bytes());
        f[4..8].copy_from_slice(&pgno.to_be_bytes());
        for (i, b) in f[24..].iter_mut().enumerate() {
            *b = fill.wrapping_add((i % 251) as u8);
        }
        // Bytes 18/19 of page 1: legacy (2), not MVCC.
        f[24 + 18] = 2;
        f[24 + 19] = 2;
        f
    };
    let (f1, f2) = (frame(1, 0x10), frame(2, 0x40));
    let at = 32 + 24 + 100;
    let ext = |syncs: Vec<Vec<Op>>| LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: "w".into(),
        syncs,
    };
    let w = |off: usize, bytes: Vec<u8>| Op::Write {
        off: off as u64,
        bytes,
    };
    let records = [
        ext(vec![vec![w(0, header.clone()), w(32, f1.clone())]]),
        ext(vec![
            vec![w(at, vec![0xAA; 8])],
            vec![w(at, vec![0xBB; 4]), w(32 + 24 + PS, f2.clone())],
        ]),
        ext(vec![vec![
            w(32 + 2 * (24 + PS), vec![9; 10]),
            Op::Truncate {
                len: (32 + 2 * (24 + PS)) as u64,
            },
        ]]),
    ];
    let store = mem();
    for (i, r) in records.iter().enumerate() {
        store
            .put(&keys().log(1, i as u64 + 1), &encode_record(r))
            .unwrap();
    }
    let h = Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 1,
        manifest: None,
        writer_id: "w".into(),
        epoch: 0,
    };
    store.put(&keys().head(), &h.encode()).unwrap();

    // The expected bytes, by hand.
    let mut expect = header.clone();
    expect.extend_from_slice(&f1);
    expect.extend_from_slice(&f2);
    expect[at..at + 4].copy_from_slice(&[0xBB; 4]);
    expect[at + 4..at + 8].copy_from_slice(&[0xAA; 4]);
    let mut page1 = f1[24..].to_vec();
    page1[100..104].copy_from_slice(&[0xBB; 4]);
    page1[104..108].copy_from_slice(&[0xAA; 4]);

    let t = tempdir();
    let d = fresh(&t);
    restore_with_store(&config(&d), store.clone(), &d).unwrap();
    assert_eq!(read(&d, "app.db-wal"), expect);
    assert_eq!(read(&d, "app.db"), page1, "the page-1 seed");
    assert!(!d.join("app.db-log").exists());
}

/// A dirty record (a page checkpointed since the last seal) is not dropped by a reconcile
/// that only replaces the `.db-log`: the generation did not change, no seal covers it.
#[test]
fn log_only_new_generation_keeps_the_dirty_file() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let db = open_mvcc(store.clone(), mvcc(&dir));
    // A created database: generation 2, its log image empty, its local log the header.
    let copy = copy_dir(&dir);
    crash(db, vec![]);
    assert_eq!(read(copy.path(), "app.db-log").len(), 56);
    let dirty = [7u8, 0, 0, 0];
    std::fs::write(copy.path().join("app.db-dirty"), dirty).unwrap();
    let o = outcomes(&store, copy.path());
    assert_eq!(o.log, Some(Reconciled::NewGeneration));
    assert_eq!(o.wal, Reconciled::Level);
    assert_eq!(read(copy.path(), "app.db-dirty"), dirty, "not truncated");
}

/// `wal_frames_with_empty_log_is_repaired_and_opens`, encrypted: the repair needs no key, and
/// the engine opens the result with it.
#[test]
fn wal_frames_with_empty_log_is_repaired_and_opens_encrypted() {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let k = tempdir();
    let t = tempdir();
    let dir = fresh(&t);
    let c = mvcc_enc(&dir, &k, &key_hex());
    let enc = encryption::load(&c).unwrap().unwrap();
    let (db, conn) = mvcc_with_rows(store.clone(), c, 4);
    store.only_keys_containing("/manifest/");
    store.fail_puts_after(0);
    assert!(truncate_checkpoint(&conn).is_err());
    store.fail_puts_after(u64::MAX);
    store.only_keys_containing("");
    let b = bucket_image(&*mem);
    assert!(!b.wal.is_empty());
    crash(db, vec![conn]);
    let d = tempdir();
    std::fs::write(d.path().join("app.db-wal"), &b.wal).unwrap();
    assert!(repair_wal_without_log(d.path()).unwrap());
    let log = std::fs::read(d.path().join("app.db-log")).unwrap();
    assert_eq!(log.len(), 56);
    assert_eq!(
        plain_rows(&b.db, &b.wal, &log, Some(&enc)),
        vec![1, 2, 3, 4]
    );
}
