//! Task 5: an MVCC database opened, written and checkpointed through the library
//! (`LogDurableStorage`, the seal in its `truncate`). Each test checks the bucket by decoding
//! its objects (kind 1 into a `-wal`, kind 3 into an `app.db-log`, the head's manifest into
//! `app.db`) and opening that image with plain turso, and, where it says so, a crash copy of
//! the local directory. A restore of `app.db-log` through the library is Task 6.
use super::*;
use crate::encryption::Encryption;
use crate::format::{apply_ops, Manifest, Op};
use crate::io::log_storage::{OMIT_LOG_ENCRYPTION, WRAP_LOG_FILE};
use crate::io::stash;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use turso_core::io::FileSyncType;
use turso_core::{Buffer, Completion, File, LimboError};

/// An MVCC `config`: no maintenance (every checkpoint is the test's), a short retry deadline.
fn mvcc(dir: &Path) -> Config {
    let mut c = config(dir);
    c.journal_mode = JournalMode::Mvcc;
    c.retry_deadline = Duration::from_millis(300);
    c.maintenance_log_objects = 0;
    c.maintenance_wal_bytes = 0;
    c
}

fn open_mvcc(store: Arc<dyn ObjectStore>, c: Config) -> Database {
    open_with_store(c, store).unwrap_or_else(|e| panic!("open: {e}"))
}

/// One `BEGIN CONCURRENT` insert of row `id`.
pub(super) fn insert_c(c: &Connection, id: i64) -> Result<()> {
    c.execute("BEGIN CONCURRENT", &[])?;
    let r = c
        .execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            &[
                Value::from_i64(id),
                Value::build_text(format!("{MARKER} {id}")),
            ],
        )
        .and_then(|_| c.execute("COMMIT", &[]));
    if r.is_err() {
        // A failed COMMIT may have ended the transaction already.
        let _ = c.execute("ROLLBACK", &[]);
    }
    r.map(|_| ())
}

pub(super) fn truncate_checkpoint(c: &Connection) -> Result<()> {
    c.checkpoint(CheckpointMode::Truncate)
}

fn read(dir: &Path, name: &str) -> Vec<u8> {
    match std::fs::read(dir.join(name)) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => panic!("read {name}: {e}"),
    }
}

/// The current generation as the bucket holds it: the head, `app.db` from its manifest's
/// pages, and `log/<head.generation>` replayed: kind 1 into `wal`, kind 3 into `log`.
struct BucketImage {
    head: Head,
    db: Vec<u8>,
    wal: Vec<u8>,
    log: Vec<u8>,
    /// The kind of each record of the generation, in seq order (2: fence).
    kinds: Vec<u8>,
    /// Each record's ops.
    records: Vec<Vec<Vec<Op>>>,
}

fn bucket_image(store: &dyn ObjectStore) -> BucketImage {
    let head = head(store);
    let g = head.manifest.expect("an MVCC head names its manifest");
    let body = store.get(&keys().manifest(g)).unwrap().unwrap().body;
    let m = Manifest::decode(&as_plain(&body)).unwrap();
    let ps = m.page_size as usize;
    let mut db = vec![0u8; ps * m.page_count as usize];
    for (p, pg) in &m.pages {
        let page = store.get(&keys().page(*pg, *p)).unwrap().unwrap().body;
        let at = (*p as usize - 1) * ps;
        db[at..at + ps].copy_from_slice(page_of(&page, ps));
    }
    let (mut wal, mut log, mut kinds, mut records) = (Vec::new(), Vec::new(), vec![], vec![]);
    for seq in 1.. {
        let Some(o) = store.get(&keys().log(head.generation, seq)).unwrap() else {
            break;
        };
        match decode_record(&as_plain(&o.body)).unwrap() {
            LogRecord::Extents { file, syncs, .. } => {
                let (kind, img) = match file {
                    LogFile::Wal => (1, &mut wal),
                    LogFile::Log => (3, &mut log),
                };
                kinds.push(kind);
                for ops in &syncs {
                    apply_ops(img, ops).unwrap();
                }
                records.push(syncs);
            }
            LogRecord::Fence { .. } => {
                kinds.push(2);
                records.push(Vec::new());
            }
        }
    }
    BucketImage {
        head,
        db,
        wal,
        log,
        kinds,
        records,
    }
}

/// `app.db`, `app.db-wal` and `app.db-log` written into a new directory and opened with plain
/// turso (with `enc`'s key): MVCC, `integrity_check` ok; returns the ids in `t`.
fn plain_rows(db: &[u8], wal: &[u8], log: &[u8], enc: Option<&Encryption>) -> Vec<i64> {
    use turso_core::{DatabaseOpts, EncryptionOpts, OpenOptions, PlatformIO, SqliteDialect};
    let t = tempdir();
    for (name, bytes) in [("app.db", db), ("app.db-wal", wal), ("app.db-log", log)] {
        if !bytes.is_empty() {
            std::fs::write(t.path().join(name), bytes).unwrap();
        }
    }
    let path = t.path().join("app.db");
    let io: Arc<dyn turso_core::IO> = Arc::new(PlatformIO::new().unwrap());
    let mut opts = OpenOptions::new(Arc::new(SqliteDialect));
    if let Some(enc) = enc {
        opts = opts
            .db_opts(DatabaseOpts::new().with_encryption(true))
            .encryption(EncryptionOpts {
                cipher: enc.cipher.to_string(),
                hexkey: hex::encode(enc.key.as_slice()),
            });
    }
    let db = turso_core::Database::open(io, path.to_str().unwrap(), opts)
        .unwrap_or_else(|e| panic!("plain open: {e}"));
    assert!(db.mvcc_enabled());
    let conn = db
        .connect_with_encryption(enc.map(|e| e.key.clone()))
        .unwrap();
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

/// The rows a restore of the bucket's current generation holds (plain turso).
fn bucket_rows(store: &dyn ObjectStore, enc: Option<&Encryption>) -> Vec<i64> {
    let b = bucket_image(store);
    plain_rows(&b.db, &b.wal, &b.log, enc)
}

/// A process crash: nothing runs on drop. The rows a copy of `dir`'s files holds (plain
/// turso).
fn crash_rows(
    db: Database,
    conns: Vec<Connection>,
    dir: &Path,
    enc: Option<&Encryption>,
) -> Vec<i64> {
    crash(db, conns);
    plain_rows(
        &read(dir, "app.db"),
        &read(dir, "app.db-wal"),
        &read(dir, "app.db-log"),
        enc,
    )
}

/// A new MVCC database with table `t` and rows `1..=n`.
pub(super) fn mvcc_with_rows(
    store: Arc<dyn ObjectStore>,
    c: Config,
    n: i64,
) -> (Database, Connection) {
    let db = open_mvcc(store, c);
    let conn = db.connect().unwrap();
    create(&conn);
    for id in 1..=n {
        insert_c(&conn, id).unwrap();
    }
    (db, conn)
}

#[test]
fn mvcc_commit_then_checkpoint_seals_and_log_image_is_empty() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 3);
    assert_eq!(db.generation(), 2);
    // Every commit is a kind-3 record whose replay is the local log, header included.
    let b = bucket_image(&*store);
    assert_eq!(b.kinds, [3, 3, 3, 3], "the CREATE TABLE and three inserts");
    assert_eq!(b.log, read(&dir, "app.db-log"));
    assert_eq!(
        &b.log[0..4],
        &0x4C4D_4C32u32.to_le_bytes(),
        "starts with the log header"
    );
    assert_eq!(bucket_rows(&*store, None), vec![1, 2, 3]);

    truncate_checkpoint(&c).unwrap();
    assert_eq!(db.generation(), 3);
    let b = bucket_image(&*store);
    assert_eq!((b.head.generation, b.head.manifest), (3, Some(2)));
    assert_eq!(
        b.kinds,
        Vec::<u8>::new(),
        "nothing shipped into generation 3"
    );
    assert_eq!(read(&dir, "app.db-log"), Vec::<u8>::new());
    assert_eq!(read(&dir, "app.db-wal"), Vec::<u8>::new());
    assert_eq!(state(&dir).generation, 3);
    assert_eq!(bucket_rows(&*store, None), vec![1, 2, 3]);

    insert_c(&c, 4).unwrap();
    let b = bucket_image(&*store);
    assert_eq!(b.kinds, [3]);
    assert_eq!(b.log, read(&dir, "app.db-log"));
    assert_eq!(bucket_rows(&*store, None), vec![1, 2, 3, 4]);
    assert_eq!(ids(&c), vec![1, 2, 3, 4]);
    c.close().unwrap();
    db.close().unwrap();
}

/// A seal that fails before its head PUT (the manifest PUT fails): the checkpoint errs before
/// the engine cuts the log, the generation goes on, later commits ack, and both the bucket
/// and a crash copy hold all 12 rows (P8f).
fn seal_fails_then_two_commits(store: &Arc<FaultStore>, dir: &Path) -> (Database, Connection) {
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(dir), 10);
    let log_before = read(dir, "app.db-log");
    store.only_keys_containing("/manifest/");
    store.fail_puts_after(0);
    let r = truncate_checkpoint(&c);
    assert!(r.is_err(), "{r:?}");
    assert_eq!(db.generation(), 2);
    assert_eq!(head(&**store).generation, 2);
    assert_eq!(read(dir, "app.db-log"), log_before, "the log is whole");
    assert!(
        !read(dir, "app.db-wal").is_empty(),
        "the WAL keeps its frames"
    );
    store.fail_puts_after(u64::MAX);
    store.only_keys_containing("");
    insert_c(&c, 11).unwrap();
    insert_c(&c, 12).unwrap();
    assert_eq!(ids(&c), (1..=12).collect::<Vec<_>>());
    (db, c)
}

#[test]
fn seal_failure_leaves_log_and_wal_whole_and_later_commits_ack() {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = seal_fails_then_two_commits(&store, &dir);
    let acked = db.upload_stats().commits_acked;
    assert_eq!(acked, 13, "the CREATE TABLE and 12 inserts");
    let b = bucket_image(&*mem);
    assert_eq!(b.log, read(&dir, "app.db-log"));
    assert_eq!(bucket_rows(&*mem, None), (1..=12).collect::<Vec<_>>());
    assert_eq!(
        crash_rows(db, vec![c], &dir, None),
        (1..=12).collect::<Vec<_>>()
    );
}

#[test]
fn seal_failure_retry_crash_restore() {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = seal_fails_then_two_commits(&store, &dir);
    truncate_checkpoint(&c).unwrap();
    assert_eq!(db.generation(), 3);
    assert_eq!(read(&dir, "app.db-log"), Vec::<u8>::new());
    insert_c(&c, 13).unwrap();
    assert_eq!(bucket_rows(&*mem, None), (1..=13).collect::<Vec<_>>());
    assert_eq!(
        crash_rows(db, vec![c], &dir, None),
        (1..=13).collect::<Vec<_>>()
    );
}

/// The checkpoint's seal stores its head but the reply is lost: the checkpoint errs before
/// the engine cuts the log, and the bucket's head is a generation ahead of this writer.
fn head_stored_reply_lost(store: &Arc<FaultStore>, db: &Database, c: &Connection) {
    store.only_keys_containing("/head");
    store.store_then_fail_next_puts(1);
    store.fail_puts_after(0);
    let r = truncate_checkpoint(c);
    assert!(r.is_err(), "{r:?}");
    assert_eq!(head(&**store).generation, 3, "the head is published");
    assert_eq!(db.generation(), 2, "but not adopted");
    store.fail_puts_after(u64::MAX);
    store.store_then_fail_next_puts(0);
    store.only_keys_containing("");
}

#[test]
fn head_stored_reply_lost_next_commit_adopts_and_is_acked() {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 10);
    head_stored_reply_lost(&store, &db, &c);
    let acked = db.upload_stats().commits_acked;
    insert_c(&c, 11).unwrap();
    assert_eq!(db.upload_stats().commits_acked, acked + 1, "acked");
    assert_eq!(db.generation(), 3, "the commit adopted the published head");
    // The new generation's log image is the whole local log, header included.
    let b = bucket_image(&*mem);
    assert_eq!(b.head.generation, 3);
    assert_eq!(b.kinds, [3]);
    assert_eq!(b.log, read(&dir, "app.db-log"));
    insert_c(&c, 12).unwrap();
    let b = bucket_image(&*mem);
    assert_eq!(b.kinds, [3, 3]);
    assert_eq!(b.log, read(&dir, "app.db-log"));
    // The local disk lost: the bucket has every acked commit.
    assert_eq!(bucket_rows(&*mem, None), (1..=12).collect::<Vec<_>>());
    assert_eq!(
        crash_rows(db, vec![c], &dir, None),
        (1..=12).collect::<Vec<_>>()
    );
}

#[test]
fn head_stored_then_adopt_then_wal_upload_fails_more_commits_crash_copy_has_all() {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 10);
    head_stored_reply_lost(&store, &db, &c);
    // The next checkpoint adopts the head (`on_checkpoint_start`), then its WAL upload fails.
    store.only_keys_containing("/log/");
    store.fail_puts_after(0);
    let r = truncate_checkpoint(&c);
    assert!(r.is_err(), "{r:?}");
    assert_eq!(db.generation(), 3, "adopted before the WAL writes");
    store.fail_puts_after(u64::MAX);
    store.only_keys_containing("");
    insert_c(&c, 11).unwrap();
    insert_c(&c, 12).unwrap();
    let b = bucket_image(&*mem);
    assert_eq!(b.head.generation, 3);
    assert_eq!(
        b.log,
        read(&dir, "app.db-log"),
        "the whole local log is in generation 3"
    );
    assert_eq!(bucket_rows(&*mem, None), (1..=12).collect::<Vec<_>>());
    assert_eq!(
        crash_rows(db, vec![c], &dir, None),
        (1..=12).collect::<Vec<_>>()
    );
}

/// A local `app.db-log` whose truncate fails while `fail` is set.
struct FailTruncate {
    inner: Arc<dyn File>,
    fail: Arc<AtomicBool>,
}

impl File for FailTruncate {
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
        if self.fail.load(SeqCst) {
            return Err(LimboError::InternalError("injected: log truncate".into()));
        }
        self.inner.truncate(len, c)
    }
}

/// Wrap the next opens' local `app.db-log` (on this thread) in a `FailTruncate`.
fn fail_log_truncate(fail: &Arc<AtomicBool>) {
    let fail = fail.clone();
    WRAP_LOG_FILE.with(|w| {
        *w.borrow_mut() = Some(Arc::new(move |inner: Arc<dyn File>| -> Arc<dyn File> {
            Arc::new(FailTruncate {
                inner,
                fail: fail.clone(),
            })
        }))
    });
}

#[test]
fn local_truncate_failure_after_seal_poisons_until_reopen() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let fail = Arc::new(AtomicBool::new(false));
    fail_log_truncate(&fail);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 5);
    WRAP_LOG_FILE.with(|w| *w.borrow_mut() = None);
    fail.store(true, SeqCst);
    let r = truncate_checkpoint(&c);
    assert!(r.is_err(), "{r:?}");
    assert_eq!(db.generation(), 3, "the seal went through");
    assert_eq!(head(&*store).generation, 3);
    fail.store(false, SeqCst);
    // Poisoned: a commit is refused at its write and rolls back; checkpoints and seals too.
    let r = insert_c(&c, 6);
    assert!(
        matches!(&r, Err(Error::Io(m)) if m.contains("reopen")),
        "{r:?}"
    );
    assert_eq!(
        ids(&c),
        (1..=5).collect::<Vec<_>>(),
        "the refused commit rolled back"
    );
    let objects = store.list("").unwrap();
    let r = truncate_checkpoint(&c);
    assert!(
        matches!(&r, Err(Error::Io(m)) if m.contains("reopen")),
        "{r:?}"
    );
    assert_eq!(store.list("").unwrap(), objects, "no seal, no upload");
    assert_eq!(head(&*store).generation, 3);
    c.close().unwrap();
    db.close().unwrap();
    // A reopen clears it.
    let db = open_mvcc(store.clone(), mvcc(&dir));
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), (1..=5).collect::<Vec<_>>());
    insert_c(&c, 6).unwrap();
    assert_eq!(db.generation(), 3);
    let b = bucket_image(&*store);
    assert_eq!(b.log, read(&dir, "app.db-log"));
    assert_eq!(bucket_rows(&*store, None), (1..=6).collect::<Vec<_>>());
    truncate_checkpoint(&c).unwrap();
    assert_eq!(db.generation(), 4);
    insert_c(&c, 7).unwrap();
    assert_eq!(bucket_rows(&*store, None), (1..=7).collect::<Vec<_>>());
    assert_eq!(
        crash_rows(db, vec![c], &dir, None),
        (1..=7).collect::<Vec<_>>()
    );
}

#[test]
fn db_log_opened_outside_wrapper_is_refused() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 1);
    let io = db.engine_io();
    let log = dir.join("app.db-log");
    // Another spelling of the same file, and a link to it.
    let dotted = dir.join(".").join("app.db-log");
    let link = dir.join("other-name");
    std::os::unix::fs::symlink(&log, &link).unwrap();
    for path in [&log, &dotted, &link] {
        stash::clear();
        let r = io.open_file(path.to_str().unwrap(), OpenFlags::Create, false);
        assert!(r.is_err(), "{path:?} opened");
        assert_eq!(
            stash::take(),
            Some(Error::Unsupported(
                "MVCC: app.db-log opened outside its wrapper"
            )),
            "{path:?}"
        );
    }
    // The database still works.
    insert_c(&c, 2).unwrap();
    assert_eq!(bucket_rows(&*store, None), vec![1, 2]);
    c.close().unwrap();
    db.close().unwrap();
}

/// `enc_config` in MVCC mode.
pub(super) fn mvcc_enc(dir: &Path, keydir: &TempDir, hex: &str) -> Config {
    let mut c = enc_config(dir, keydir, hex);
    let m = mvcc(dir);
    c.journal_mode = m.journal_mode;
    c.retry_deadline = m.retry_deadline;
    c.maintenance_log_objects = 0;
    c.maintenance_wal_bytes = 0;
    c
}

fn has(bytes: &[u8], needle: &[u8]) -> bool {
    bytes.windows(needle.len()).any(|w| w == needle)
}

/// Which local files (`.db`, `.db-wal`, `.db-log`) and bucket objects hold `needle`.
fn plaintext(dir: &Path, store: &MemStore, needle: &[u8]) -> Vec<String> {
    let mut found: Vec<String> = ["app.db", "app.db-wal", "app.db-log"]
        .into_iter()
        .filter(|f| has(&read(dir, f), needle))
        .map(String::from)
        .collect();
    for key in store.list("").unwrap() {
        if has(&store.get(&key).unwrap().unwrap().body, needle) {
            found.push(key);
        }
    }
    found
}

/// Rows with the marker in the local `.db-log` and the bucket, then a checkpoint whose seal
/// fails (the `-wal` keeps its frames), then one that seals. Returns what held the marker
/// after each step, and the rows the bucket's image holds.
fn marked_rows(c: Config, enc: Option<&Encryption>) -> (Vec<Vec<String>>, Vec<i64>) {
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let dir = c.dir.clone();
    let (db, conn) = mvcc_with_rows(store.clone(), c, 4);
    let mut seen = vec![plaintext(&dir, &mem, MARKER.as_bytes())];
    store.only_keys_containing("/manifest/");
    store.fail_puts_after(0);
    assert!(truncate_checkpoint(&conn).is_err());
    assert!(!read(&dir, "app.db-wal").is_empty());
    seen.push(plaintext(&dir, &mem, MARKER.as_bytes()));
    store.fail_puts_after(u64::MAX);
    store.only_keys_containing("");
    truncate_checkpoint(&conn).unwrap();
    insert_c(&conn, 5).unwrap();
    seen.push(plaintext(&dir, &mem, MARKER.as_bytes()));
    let rows = bucket_rows(&*mem, enc);
    conn.close().unwrap();
    db.close().unwrap();
    (seen, rows)
}

#[test]
fn encrypted_mvcc_needs_context_and_hides_plaintext() {
    let store = mem();
    let k = tempdir();
    let t = tempdir();
    let dir = fresh(&t);
    // Without the context on the wrapper the engine refuses the open (P8e).
    OMIT_LOG_ENCRYPTION.with(|o| o.set(true));
    let r = open_with_store(mvcc_enc(&dir, &k, &key_hex()), store.clone());
    OMIT_LOG_ENCRYPTION.with(|o| o.set(false));
    assert!(
        matches!(&r, Err(Error::Engine(m)) if m.contains("encrypted MVCC requires")),
        "{:?}",
        r.err()
    );

    // With it: no marker in `.db-log`, `.db-wal`, `.db` or any bucket object.
    let t2 = tempdir();
    let c = mvcc_enc(&fresh(&t2), &k, &key_hex());
    let enc = encryption::load(&c).unwrap().unwrap();
    let (seen, rows) = marked_rows(c, Some(&enc));
    assert_eq!(seen, vec![Vec::<String>::new(); 3]);
    assert_eq!(rows, vec![1, 2, 3, 4, 5]);

    // The control: unencrypted, the marker is in the log, the WAL and the bucket.
    let t3 = tempdir();
    let (seen, rows) = marked_rows(mvcc(&fresh(&t3)), None);
    assert!(seen[0].contains(&"app.db-log".to_string()), "{seen:?}");
    assert!(seen[0].iter().any(|k| k.contains("/log/")), "{seen:?}");
    assert!(seen[1].contains(&"app.db-wal".to_string()), "{seen:?}");
    assert!(seen[2].contains(&"app.db".to_string()), "{seen:?}");
    assert!(seen[2].iter().any(|k| k.contains("/pages/")), "{seen:?}");
    assert_eq!(rows, vec![1, 2, 3, 4, 5]);
}

#[test]
fn wrong_key_gives_encryption_error() {
    let store = mem();
    let k = tempdir();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc_enc(&dir, &k, &key_hex()), 3);
    // Sealed, so the reopen below needs no `.db-log` reconcile (Task 6).
    truncate_checkpoint(&c).unwrap();
    c.close().unwrap();
    db.close().unwrap();
    let k2 = tempdir();
    let r = open_with_store(mvcc_enc(&dir, &k2, &"22".repeat(32)), store.clone());
    let m = encryption_err(r);
    assert!(!m.is_empty());
    // Without any key.
    let r = open_with_store(mvcc(&dir), store.clone());
    encryption_err(r);
    // The right key still opens it.
    let db = open_mvcc(store.clone(), mvcc_enc(&dir, &k, &key_hex()));
    let c = db.connect().unwrap();
    assert_eq!(ids(&c), vec![1, 2, 3]);
    c.close().unwrap();
    db.close().unwrap();
}

/// A WAL database is opened, sealed and refused exactly as before: no `app.db-log`, a
/// format-1 head, kind-1 records only, the WAL seal at `truncate(0)`, and `.db-log` opens
/// refused with `Unsupported("MVCC")`.
#[test]
fn wal_mode_open_unchanged() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let db = open_mem(&store, &dir);
    let c = db.connect().unwrap();
    create(&c);
    insert(&c, 1).unwrap();
    // A new WAL database is a format 5 one (it switches generations, no seal).
    assert_eq!(head(&*store).format, crate::format::FORMAT_V5);
    let kinds: Vec<_> = (1..=2)
        .map(|seq| record(&store, 1, seq))
        .map(|r| {
            matches!(
                r,
                LogRecord::Extents {
                    file: LogFile::Wal,
                    ..
                }
            )
        })
        .collect();
    assert_eq!(kinds, [true, true]);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    assert_eq!(db.generation(), 2);
    assert!(!dir.join("app.db-log").exists());
    let io = db.engine_io();
    stash::clear();
    let path = dir.join("app.db-log");
    assert!(io
        .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
        .is_err());
    assert_eq!(stash::take(), Some(Error::Unsupported("MVCC")));
    c.close().unwrap();
    db.close().unwrap();
}

/// One kind-3 record can carry several engine commits (group commit): every commit counts,
/// in `commits_uploaded` and in `commits_acked`, and `log_puts` counts records.
#[test]
fn stats_count_one_commit_per_record_in_group_commit() {
    const THREADS: i64 = 8;
    const EACH: i64 = 20;
    let mem = mem();
    let store = Arc::new(FaultStore::new(mem.clone()));
    let t = tempdir();
    let dir = fresh(&t);
    let mut c = mvcc(&dir);
    c.retry_deadline = Duration::from_secs(5);
    let db = open_mvcc(store.clone(), c);
    let conn = db.connect().unwrap();
    create(&conn);
    // A slow PUT, so commits queue behind the lead's and share its record.
    store.only_keys_containing("/log/");
    store.delay_puts(Duration::from_millis(15));
    std::thread::scope(|s| {
        for th in 0..THREADS {
            let c = db.connect().unwrap();
            s.spawn(move || {
                for i in 0..EACH {
                    insert_c(&c, 1 + th * EACH + i).unwrap();
                }
                c.close().unwrap();
            });
        }
    });
    store.delay_puts(Duration::ZERO);
    let commits = (1 + THREADS * EACH) as u64;
    let s = db.upload_stats();
    let records = bucket_image(&*mem).records.len() as u64;
    assert!(
        records < commits,
        "no group commit happened: {records} records"
    );
    assert_eq!(
        (s.commits_acked, s.commits_uploaded, s.log_puts),
        (commits, commits, records),
        "{s:?}"
    );
    assert!(s.log_puts < commits, "{s:?}");
    assert_eq!(ids(&conn), (1..=THREADS * EACH).collect::<Vec<_>>());
    assert_eq!(
        bucket_rows(&*mem, None),
        (1..=THREADS * EACH).collect::<Vec<_>>()
    );
    conn.close().unwrap();
    db.close().unwrap();
}

#[test]
fn passive_checkpoint_pragma_errors_in_mvcc() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 2);
    let r = c.execute("PRAGMA wal_checkpoint(PASSIVE)", &[]);
    assert!(r.is_err(), "{r:?}");
    assert_eq!(db.generation(), 2, "nothing sealed");
    insert_c(&c, 3).unwrap();
    assert_eq!(bucket_rows(&*store, None), vec![1, 2, 3]);
    c.close().unwrap();
    db.close().unwrap();
}

#[test]
fn set_batching_window_refused_in_an_open_mvcc_database() {
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 1);
    let w = Batching::Window {
        max_wait: Duration::from_millis(10),
        max_commits: 4,
        max_bytes: 1024,
    };
    assert_eq!(unsupported(db.set_batching(w)), "MVCC: Window batching");
    assert_eq!(
        unsupported(c.set_batching(Some(w))),
        "MVCC: Window batching"
    );
    assert_eq!(db.batching(), Batching::Off);
    assert_eq!(c.batching(), None);
    insert_c(&c, 2).unwrap();
    c.close().unwrap();
    db.close().unwrap();
}

#[test]
fn transaction_with_window_refused_in_mvcc_and_still_works_in_wal() {
    let w = Batching::Window {
        max_wait: Duration::from_millis(10),
        max_commits: 4,
        max_bytes: 1024,
    };
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let (db, c) = mvcc_with_rows(store.clone(), mvcc(&dir), 1);
    let mut ran = false;
    let r = c.transaction_with(w, |_| {
        ran = true;
        Ok(())
    });
    assert_eq!(unsupported(r), "MVCC: Window batching");
    assert!(!ran, "the closure ran");
    assert!(c.is_autocommit(), "BEGIN ran");
    // PRAGMA buckwal_batching inside a transaction is refused too.
    c.execute("BEGIN", &[]).unwrap();
    let r = c.execute("PRAGMA buckwal_batching = 'window 10ms 4 1KiB'", &[]);
    assert_eq!(unsupported(r), "MVCC: Window batching");
    c.execute("ROLLBACK", &[]).unwrap();
    c.transaction_with(Batching::Off, |_| Ok(())).unwrap();
    c.close().unwrap();
    db.close().unwrap();

    // WAL: unchanged.
    let store = mem();
    let t = tempdir();
    let dir = fresh(&t);
    let db = open_mem(&store, &dir);
    let c = db.connect().unwrap();
    create(&c);
    c.transaction_with(w, |tx| {
        tx.execute("INSERT INTO t(id, v) VALUES (1, 'x')", &[])
            .map(|_| ())
    })
    .unwrap();
    assert!(c.is_autocommit());
    c.close().unwrap();
    db.close().unwrap();
}

mod maintenance;
mod readonly;
mod restore;
mod run;
