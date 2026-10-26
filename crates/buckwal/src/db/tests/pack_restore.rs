//! A restore of a format 5 bucket from its pack (spec "Restore", Choice C(a)): the pointer, the
//! pack codes and the log of every generation in the chain from the pack generation. The open
//! returns before the pack is local; a page read before its block is local fetches that 1 MiB
//! block; a background fetch completes the file in 16 MiB ranges.
use super::*;
use crate::files::{BLOCKS_FILE, PACK_FILE};
use crate::io::block_store::NoBackgroundFetch;
use crate::pack::codes::BLOCK_SIZE;
use crate::pack::PackPointer;
use crate::store::{MultipartDone, Object, ObjectStat, PartSource, StatsSnap, StoreError};
use std::sync::Mutex;

/// A text of about 3.5 KB: one row fills most of a 4 KiB page, so 450 rows make a pack of
/// about 1.8 MiB (two blocks) and each leaf page holds one row.
fn big(id: i64) -> String {
    format!("{id:06}{}", "x".repeat(3500))
}

fn insert_big(c: &Connection, ids: std::ops::RangeInclusive<i64>) {
    for id in ids {
        c.execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            &[Value::from_i64(id), Value::build_text(big(id))],
        )
        .unwrap();
    }
}

/// Every row of `t`, the text cut to its first 12 bytes and its length.
fn rows(c: &Connection) -> Vec<(i64, String, i64)> {
    c.query(
        "SELECT id, substr(v, 1, 12), length(v) FROM t ORDER BY id",
        &[],
    )
    .unwrap()
    .iter()
    .map(|r| (r[0].as_int().unwrap(), text(&r[1]), r[2].as_int().unwrap()))
    .collect()
}

/// One pack run against `store`, from a working directory of its own; it must pack.
fn pack_now(store: Arc<dyn ObjectStore>, cfg: &Config) -> crate::PackReport {
    let t = tempdir();
    let mut cfg = cfg.clone();
    cfg.dir = t.path().join("packer");
    let r = crate::pack::pack_with_store(&cfg, store).unwrap();
    assert_eq!(r.outcome, crate::PackOutcome::Packed, "{r:?}");
    r
}

fn pointer(store: &dyn ObjectStore, k: &Keys) -> PackPointer {
    let k = k.clone().adopting(store);
    k.decode_pointer(&store.get(&k.pack_pointer()).unwrap().unwrap().body)
        .unwrap()
}

/// Keys that adopted the bucket's head (so a coded pointer decodes).
fn keys_of(store: &dyn ObjectStore, cfg: &Config) -> Keys {
    let enc = crate::encryption::load(cfg).unwrap();
    let k = cfg.keys_for(enc.as_deref());
    k.decode_head_open(&store.get(&k.head()).unwrap().unwrap().body)
        .unwrap();
    k
}

/// A writer with `450` big rows, switched to the next generation (the engine may restart the
/// WAL on its own before), and packed: the pack generation is the head's. The writer is still
/// open.
fn packed_writer(store: &Arc<MemStore>, cfg: Config) -> (Database, Connection) {
    let db = open_with_store(cfg.clone(), store.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    insert_big(&c, 1..=450);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    let r = pack_now(store.clone(), &cfg);
    assert_eq!(r.pack_generation, db.generation());
    (db, c)
}

/// Wait until the local state of `dir` says complete and the pack files are gone (the
/// background fetch of a writer ended).
fn wait_complete(dir: &Path) {
    let t = Instant::now();
    while !state(dir).complete || dir.join(PACK_FILE).exists() || dir.join(BLOCKS_FILE).exists() {
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "the pack never became local"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A store over a `MemStore` that records each `get_range` (key, offset, length) and can hand
/// out a saved pointer for the first pointer GET and a saved head for the first head GET.
struct Recording {
    inner: Arc<MemStore>,
    ranges: Mutex<Vec<(String, u64, u64)>>,
    pointer_key: String,
    first_pointer: Mutex<Option<Vec<u8>>>,
    head_key: String,
    first_head: Mutex<Option<Vec<u8>>>,
    /// While set, a range GET past the first block waits.
    hold: std::sync::atomic::AtomicBool,
    /// Runs once, after the first range GET returns.
    after_range: Mutex<Option<Hook>>,
    /// Runs once, after the first GET of a key that holds the text.
    after_get: Mutex<Option<(String, Hook)>>,
}

/// What a test runs between two requests of a restore.
type Hook = Box<dyn FnOnce() + Send>;

impl Recording {
    fn new(inner: Arc<MemStore>) -> Arc<Recording> {
        Arc::new(Recording {
            inner,
            ranges: Mutex::new(Vec::new()),
            pointer_key: keys().pack_pointer(),
            first_pointer: Mutex::new(None),
            head_key: keys().head(),
            first_head: Mutex::new(None),
            hold: Default::default(),
            after_range: Mutex::new(None),
            after_get: Mutex::new(None),
        })
    }
    fn take_ranges(&self) -> Vec<(String, u64, u64)> {
        std::mem::take(&mut *self.ranges.lock().unwrap())
    }
}

impl ObjectStore for Recording {
    fn put(&self, k: &str, b: &[u8]) -> std::result::Result<String, StoreError> {
        self.inner.put(k, b)
    }
    fn put_if_absent(&self, k: &str, b: &[u8]) -> std::result::Result<CondPut, StoreError> {
        self.inner.put_if_absent(k, b)
    }
    fn put_if_match(&self, k: &str, b: &[u8], e: &str) -> std::result::Result<CondPut, StoreError> {
        self.inner.put_if_match(k, b, e)
    }
    fn get(&self, k: &str) -> std::result::Result<Option<Object>, StoreError> {
        if k == self.pointer_key {
            if let Some(body) = self.first_pointer.lock().unwrap().take() {
                return Ok(Some(Object {
                    body,
                    etag: "\"saved\"".into(),
                }));
            }
        }
        if k == self.head_key {
            if let Some(body) = self.first_head.lock().unwrap().take() {
                return Ok(Some(Object {
                    body,
                    etag: "\"saved-head\"".into(),
                }));
            }
        }
        let got = self.inner.get(k);
        let hook = {
            let mut armed = self.after_get.lock().unwrap();
            match &*armed {
                Some((text, _)) if k.contains(text.as_str()) => armed.take().map(|(_, h)| h),
                _ => None,
            }
        };
        if let Some(h) = hook {
            h();
        }
        got
    }
    fn stat(&self, k: &str) -> std::result::Result<Option<ObjectStat>, StoreError> {
        self.inner.stat(k)
    }
    fn list(&self, p: &str) -> std::result::Result<Vec<String>, StoreError> {
        self.inner.list(p)
    }
    fn list_after(&self, p: &str, a: &str) -> std::result::Result<Vec<String>, StoreError> {
        self.inner.list_after(p, a)
    }
    fn list_sized(&self, p: &str) -> std::result::Result<Vec<crate::store::Listed>, StoreError> {
        self.inner.list_sized(p)
    }
    fn put_multipart(
        &self,
        k: &str,
        s: &mut dyn PartSource,
        size: usize,
        n: usize,
    ) -> std::result::Result<MultipartDone, StoreError> {
        self.inner.put_multipart(k, s, size, n)
    }
    fn get_range(
        &self,
        k: &str,
        offset: u64,
        len: u64,
    ) -> std::result::Result<Option<Vec<u8>>, StoreError> {
        self.ranges
            .lock()
            .unwrap()
            .push((k.to_string(), offset, len));
        while offset >= BLOCK_SIZE && self.hold.load(std::sync::atomic::Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(5));
        }
        let got = self.inner.get_range(k, offset, len);
        let hook = self.after_range.lock().unwrap().take();
        if let Some(h) = hook {
            h();
        }
        got
    }
    fn abort_incomplete(&self, p: &str, d: Duration) -> std::result::Result<u64, StoreError> {
        self.inner.abort_incomplete(p, d)
    }
    fn delete(&self, k: &str) -> std::result::Result<(), StoreError> {
        self.inner.delete(k)
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

/// A pack whose pack generation is the head's: the restore reads the pointer, the codes, the
/// pack's first block and no log below the pack generation (deleted here, as GC would). It
/// equals the writer's database.
#[test]
fn restore_with_pack_and_no_log_equals_writer_db() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    let want = rows(&c);
    let p = pointer(&*store, &keys());
    assert_eq!(p.pack_generation, db.generation());
    assert!(p.size > BLOCK_SIZE, "{p:?}");
    for g in 1..p.pack_generation {
        for seq in log_seqs(&*store, g) {
            store.delete(&keys().log(g, seq)).unwrap();
        }
    }
    let r = tempdir();
    let rep = restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r)).unwrap();
    assert_eq!(rep.generation, p.pack_generation);
    assert_eq!(rep.pack_key.as_deref(), Some(p.pack_key.as_str()));
    assert_eq!(
        rep.bytes_fetched, BLOCK_SIZE,
        "only the first block: {rep:?}"
    );
    assert!(fresh(&r).join(PACK_FILE).exists());
    assert!(!state(&fresh(&r)).complete);
    let v = open_read_only_with_store(config(&fresh(&r)), store.clone()).unwrap();
    let vc = v.connect().unwrap();
    integrity_ok(&vc);
    assert_eq!(rows(&vc), want);
    drop(vc);
    drop(v);
    crash(db, vec![c]);
}

/// No pack yet: the restore reads the log from generation 1 on, fetches no pack and reports
/// none.
#[test]
fn restore_without_pack_reads_log_from_first_generation() {
    let store = mem();
    let a = tempdir();
    let db = open_mem(&store, a.path());
    let c = db.connect().unwrap();
    create(&c);
    insert_big(&c, 1..=20);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert_big(&c, 21..=40);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert_big(&c, 41..=45);
    assert_eq!(db.generation(), 3);
    let want = rows(&c);
    let fault = Arc::new(FaultStore::new(store.clone()));
    let r = tempdir();
    let rep = restore_with_store(&config(&fresh(&r)), fault.clone(), &fresh(&r)).unwrap();
    assert_eq!(
        (rep.pack_key.clone(), rep.bytes_fetched),
        (None, 0),
        "{rep:?}"
    );
    let ev = fault.events();
    assert!(
        ev.iter().any(|e| e == &format!("get {}", keys().log(1, 1))),
        "{ev:?}"
    );
    assert!(!ev.iter().any(|e| e.starts_with("get_range")), "{ev:?}");
    assert!(state(&fresh(&r)).complete);
    assert!(!fresh(&r).join(PACK_FILE).exists());
    let v = open_read_only_with_store(config(&fresh(&r)), store.clone()).unwrap();
    let vc = v.connect().unwrap();
    assert_eq!(rows(&vc), want);
    drop(vc);
    drop(v);
    crash(db, vec![c]);
}

/// Choice C(a): a writer open of an empty directory restores and returns while the pack's
/// blocks past the first are still in the bucket (their range GETs wait here). The database
/// answers from the local block at once; once the range GETs go on, the background fetch
/// completes the file, records it complete and removes the pack files.
#[test]
fn open_returns_before_pack_is_local() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    insert_big(&c, 451..=460);
    let want = rows(&c);
    crash(db, vec![c]);
    let rec = Recording::new(store.clone());
    rec.hold.store(true, std::sync::atomic::Ordering::SeqCst);
    let b = tempdir();
    let db = open_with_store(config(&fresh(&b)), rec.clone()).unwrap();
    assert!(!state(&fresh(&b)).complete);
    assert!(fresh(&b).join(BLOCKS_FILE).exists());
    let cb = db.connect().unwrap();
    let first = cb
        .query("SELECT substr(v, 1, 6) FROM t WHERE id = 1", &[])
        .unwrap();
    assert_eq!(text(&first[0][0]), "000001");
    assert!(!state(&fresh(&b)).complete);
    rec.hold.store(false, std::sync::atomic::Ordering::SeqCst);
    wait_complete(&fresh(&b));
    assert_eq!(rows(&cb), want);
    insert_big(&cb, 461..=461);
    let want = rows(&cb);
    cb.close().unwrap();
    db.close().unwrap();
    let v = open_read_only_with_store(config(&fresh(&b)), store.clone()).unwrap();
    let vc = v.connect().unwrap();
    integrity_ok(&vc);
    assert_eq!(rows(&vc), want);
}

/// With the background fetch off, a read of a page whose block is not local fetches its 1 MiB
/// block in one range GET, checks it, and the next read of that block fetches nothing.
#[test]
fn page_read_before_block_is_local_fetches_one_block() {
    let _off = NoBackgroundFetch::on();
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    let p = pointer(&*store, &keys());
    let rec = Recording::new(store.clone());
    let r = tempdir();
    restore_with_store(&config(&fresh(&r)), rec.clone(), &fresh(&r)).unwrap();
    let v = open_read_only_with_store(config(&fresh(&r)), rec.clone()).unwrap();
    let vc = v.connect().unwrap();
    rec.take_ranges();
    let one = |id: i64| {
        vc.query(
            "SELECT substr(v, 1, 6) FROM t WHERE id = ?",
            &[Value::from_i64(id)],
        )
        .unwrap()
    };
    assert_eq!(text(&one(400)[0][0]), "000400");
    let got = rec.take_ranges();
    assert_eq!(got.len(), 1, "{got:?}");
    let (key, offset, len) = &got[0];
    assert_eq!(key, &p.pack_key);
    assert_eq!(
        (offset % BLOCK_SIZE, *offset >= BLOCK_SIZE),
        (0, true),
        "{got:?}"
    );
    assert_eq!(*len, BLOCK_SIZE.min(p.size - offset), "{got:?}");
    assert_eq!(text(&one(401)[0][0]), "000401");
    assert_eq!(rec.take_ranges(), []);
    assert!(!state(&fresh(&r)).complete);
    drop(vc);
    drop(v);
    crash(db, vec![c]);
}

/// A change to a pack's bytes.
type Edit<'a> = Box<dyn Fn(&mut Vec<u8>) + 'a>;

/// The pack's bytes, edited by `edit`, put back in place of the pack.
fn edit_pack(store: &MemStore, p: &PackPointer, edit: impl FnOnce(&mut Vec<u8>)) {
    let mut body = store.get(&p.pack_key).unwrap().unwrap().body;
    edit(&mut body);
    store.put(&p.pack_key, &body).unwrap();
}

/// The error of a restore, else of reading every row of the restored copy (background fetch
/// off). A failed restore leaves no database in the directory.
fn read_all_err(store: &Arc<MemStore>, cfg: &Config) -> Error {
    let dir = cfg.dir.clone();
    if let Err(e) = restore_with_store(cfg, store.clone(), &dir) {
        assert!(!dir.join("app.db").exists());
        return e;
    }
    let v = open_read_only_with_store(cfg.clone(), store.clone()).unwrap();
    let vc = v.connect().unwrap();
    let r = vc.query("SELECT sum(length(v)) FROM t", &[]);
    r.expect_err("a read over a bad block")
}

/// A block moved to another place, a block from another pack, and a pack cut short each fail
/// the block's code (plaintext: the SHA-256 error naming the pack).
#[test]
fn block_moved_swapped_or_cut_short_is_corrupt() {
    let _off = NoBackgroundFetch::on();
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    let p = pointer(&*store, &keys());
    let original = store.get(&p.pack_key).unwrap().unwrap().body;
    // A second pack of the same database, after more rows, for a block from another pack.
    insert_big(&c, 451..=470);
    c.execute("UPDATE t SET v = 'y' WHERE id > 300", &[])
        .unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    pack_now(store.clone(), &config(a.path()));
    let other = pointer(&*store, &keys());
    let other_body = store.get(&other.pack_key).unwrap().unwrap().body;
    // The pointer names the first pack again, for each case.
    let first_pointer = keys().encode_pointer(&p);
    let bs = BLOCK_SIZE as usize;
    let cases: Vec<(&str, Edit)> = vec![
        (
            "moved",
            Box::new(|b: &mut Vec<u8>| {
                // Block 0's bytes in block 1's place (the last block is shorter).
                let n = b.len() - bs;
                let block0 = b[..n].to_vec();
                b[bs..].copy_from_slice(&block0);
            }),
        ),
        (
            "swapped",
            Box::new(|b: &mut Vec<u8>| {
                // Block 1 of the other pack, which holds rows the update changed.
                let n = b.len() - bs;
                assert_ne!(b[bs..], other_body[bs..bs + n]);
                b[bs..].copy_from_slice(&other_body[bs..bs + n]);
            }),
        ),
        (
            "cut short",
            Box::new(|b: &mut Vec<u8>| b.truncate(b.len() - 4096)),
        ),
    ];
    for (what, edit) in cases {
        store.put(&p.pack_key, &original).unwrap();
        store.put(&keys().pack_pointer(), &first_pointer).unwrap();
        edit_pack(&store, &p, |b| edit(b));
        let r = tempdir();
        let e = read_all_err(&store, &config(&fresh(&r)));
        assert_eq!(
            e,
            Error::Corrupt(format!("checksum mismatch: {}", p.pack_key)),
            "{what}"
        );
    }
    // The first block swapped: the restore itself fails, and leaves nothing.
    store.put(&p.pack_key, &original).unwrap();
    edit_pack(&store, &p, |b| {
        b[..4096].copy_from_slice(&other_body[..4096])
    });
    let r = tempdir();
    let e = restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r)).unwrap_err();
    assert_eq!(
        e,
        Error::Corrupt(format!("checksum mismatch: {}", p.pack_key))
    );
    assert!(!fresh(&r).join("app.db").exists());
    crash(db, vec![c]);
}

/// The `integrity check failed` records emitted on this thread while `f` runs.
fn integrity_records<T>(f: impl FnOnce() -> T) -> (T, usize) {
    use crate::telemetry::testing::Capture;
    let cap = Capture::new();
    let out = tracing::subscriber::with_default(cap.subscriber(), f);
    let n = cap
        .spans()
        .events
        .iter()
        .filter(|e| e.field("message") == Some("integrity check failed"))
        .count();
    (out, n)
}

/// An encrypted bucket: a block whose bytes changed fails its integrity code, with the
/// `integrity check failed` record; so does a pack codes object whose code changed.
#[test]
fn changed_block_is_corrupt_with_integrity_record_when_encrypted() {
    let _off = NoBackgroundFetch::on();
    let store = mem();
    let k = tempdir();
    let a = tempdir();
    let (db, c) = packed_writer(&store, enc_config(a.path(), &k, &key_hex()));
    let kk = keys_of(&*store, &enc_config(a.path(), &k, &key_hex()));
    let p = pointer(&*store, &kk);
    assert!(p.database_id.is_some());
    edit_pack(&store, &p, |b| b[BLOCK_SIZE as usize + 100] ^= 1);
    let r = tempdir();
    let (e, n) =
        integrity_records(|| read_all_err(&store, &enc_config(&fresh(&r), &k, &key_hex())));
    assert_eq!(e, Error::Corrupt(format!("integrity: {}", p.pack_key)));
    assert!(n >= 1, "no integrity record");
    // A changed code in the codes object.
    edit_pack(&store, &p, |b| b[BLOCK_SIZE as usize + 100] ^= 1);
    let mut codes = store.get(&p.codes_key).unwrap().unwrap().body;
    codes[40] ^= 1;
    store.put(&p.codes_key, &codes).unwrap();
    let r = tempdir();
    let cfg = enc_config(&fresh(&r), &k, &key_hex());
    let (e, n) = integrity_records(|| restore_with_store(&cfg, store.clone(), &fresh(&r)));
    assert_eq!(
        e.unwrap_err(),
        Error::Corrupt(format!("integrity: {}", p.codes_key))
    );
    assert!(n >= 1, "no integrity record");
    crash(db, vec![c]);
}

/// A plaintext bucket: a block whose bytes changed is the SHA-256 error, with no integrity
/// record.
#[test]
fn changed_block_is_corrupt_with_sha256_error_when_plain() {
    let _off = NoBackgroundFetch::on();
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    let p = pointer(&*store, &keys());
    edit_pack(&store, &p, |b| b[BLOCK_SIZE as usize + 100] ^= 1);
    let r = tempdir();
    let (e, n) = integrity_records(|| read_all_err(&store, &config(&fresh(&r))));
    assert_eq!(
        e,
        Error::Corrupt(format!("checksum mismatch: {}", p.pack_key))
    );
    assert_eq!(n, 0);
    crash(db, vec![c]);
}

/// The restore read a pointer that names a pack GC deleted after a newer pack won: its range GET
/// finds nothing, it reads the pointer again and starts again from the newer pack.
#[test]
fn restore_whose_pack_was_deleted_restarts_from_newer_pack() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    let old = pointer(&*store, &keys());
    let old_body = store.get(&keys().pack_pointer()).unwrap().unwrap().body;
    c.execute("UPDATE t SET v = 'z' WHERE id > 400", &[])
        .unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert_big(&c, 451..=455);
    pack_now(store.clone(), &config(a.path()));
    let new = pointer(&*store, &keys());
    assert!(new.pack_generation > old.pack_generation);
    // GC deleted the old pack (its codes are still there, so the range GET is what fails).
    store.delete(&old.pack_key).unwrap();
    let want = rows(&c);
    let rec = Recording::new(store.clone());
    *rec.first_pointer.lock().unwrap() = Some(old_body);
    let r = tempdir();
    let rep = restore_with_store(&config(&fresh(&r)), rec.clone(), &fresh(&r)).unwrap();
    assert_eq!(rep.pack_key.as_deref(), Some(new.pack_key.as_str()));
    let ranges = rec.take_ranges();
    assert!(
        ranges.iter().any(|(k, _, _)| k == &old.pack_key),
        "{ranges:?}"
    );
    let v = open_read_only_with_store(config(&fresh(&r)), store.clone()).unwrap();
    let vc = v.connect().unwrap();
    integrity_ok(&vc);
    assert_eq!(rows(&vc), want);
    drop(vc);
    drop(v);
    crash(db, vec![c]);
}

/// A takeover of a packed bucket is a restore from the pack (and the log since it), then the
/// head write at the next epoch and the fence record: the old writer is fenced, and the new
/// writer continues with every acked row.
#[test]
fn takeover_is_restore_then_head_write_and_fence() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    insert_big(&c, 451..=455);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert_big(&c, 456..=460);
    let want = rows(&c);
    let before = head(&*store);
    let p = pointer(&*store, &keys());
    let b = tempdir();
    let rep = crate::takeover::takeover_with_store(config(&fresh(&b)), store.clone()).unwrap();
    assert_eq!(rep.restore.pack_key.as_deref(), Some(p.pack_key.as_str()));
    let after = head(&*store);
    assert_eq!(
        (after.generation, after.epoch),
        (before.generation, before.epoch + 1)
    );
    assert_ne!(after.writer_id, before.writer_id);
    let last = *log_seqs(&*store, after.generation).iter().max().unwrap();
    assert_eq!(last, rep.fence_seq);
    assert!(matches!(
        record(&store, after.generation, last),
        LogRecord::Fence { .. }
    ));
    assert!(matches!(insert(&c, 9999), Err(Error::Fenced(_))));
    crash(db, vec![c]);
    let db = open_mem(&store, &fresh(&b));
    let cb = db.connect().unwrap();
    assert_eq!(rows(&cb), want);
    insert_big(&cb, 461..=461);
    let want = rows(&cb);
    cb.close().unwrap();
    db.close().unwrap();
    let r = tempdir();
    restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r)).unwrap();
    let v = open_read_only_with_store(config(&fresh(&r)), store.clone()).unwrap();
    assert_eq!(rows(&v.connect().unwrap()), want);
}

/// The writer of `cfg` after its pack: updates, a delete and new rows over three more
/// generation switches, then rows in the head's generation. Returns the writer.
fn switched_after_the_pack(store: &Arc<MemStore>, cfg: Config) -> (Database, Connection) {
    let (db, c) = packed_writer(store, cfg);
    let packed = db.generation();
    c.execute("UPDATE t SET v = 'u' || id WHERE id % 7 = 0", &[])
        .unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    c.execute("DELETE FROM t WHERE id BETWEEN 100 AND 150", &[])
        .unwrap();
    insert_big(&c, 451..=520);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert_big(&c, 521..=530);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert_big(&c, 531..=535);
    assert!(db.generation() >= packed + 3);
    (db, c)
}

/// The pack, then three generations the writer switched after it, then its current one: the
/// restore applies the three with a local checkpoint over the pack and keeps the current one as
/// the WAL. It equals the writer's database, and a writer open of it carries on.
#[test]
fn restore_with_pack_and_switches_after_it_equals_writer_db() {
    let store = mem();
    let a = tempdir();
    let (db, c) = switched_after_the_pack(&store, config(a.path()));
    let want = rows(&c);
    let p = pointer(&*store, &keys());
    assert!(p.pack_generation + 3 <= db.generation());
    let r = tempdir();
    let rep = restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r)).unwrap();
    assert_eq!(rep.generation, db.generation());
    assert_eq!(rep.pack_key.as_deref(), Some(p.pack_key.as_str()));
    assert!(rep.bytes_fetched >= BLOCK_SIZE, "{rep:?}");
    let v = open_read_only_with_store(config(&fresh(&r)), store.clone()).unwrap();
    let vc = v.connect().unwrap();
    integrity_ok(&vc);
    assert_eq!(rows(&vc), want);
    drop(vc);
    drop(v);
    crash(db, vec![c]);
}

/// `restore_with_pack_and_switches_after_it_equals_writer_db` in an encrypted bucket: no
/// plaintext in the pack, and the restore with the key equals the writer.
#[test]
fn encrypted_restore_with_pack_and_switches_after_it_equals_writer_db() {
    let store = mem();
    let k = tempdir();
    let a = tempdir();
    let (db, c) = switched_after_the_pack(&store, enc_config(a.path(), &k, &key_hex()));
    let want = rows(&c);
    let kk = keys_of(&*store, &enc_config(a.path(), &k, &key_hex()));
    let p = pointer(&*store, &kk);
    let body = store.get(&p.pack_key).unwrap().unwrap().body;
    assert!(
        !body.windows(12).any(|w| w == b"000400xxxxxx"),
        "plaintext in the pack"
    );
    // Without the key: refused before anything is written into the directory.
    let r = tempdir();
    let got = restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r));
    assert!(matches!(got, Err(Error::Encryption(_))), "{got:?}");
    assert!(!fresh(&r).join("app.db").exists());
    let r = tempdir();
    let cfg = enc_config(&fresh(&r), &k, &key_hex());
    let rep = restore_with_store(&cfg, store.clone(), &fresh(&r)).unwrap();
    assert_eq!(rep.pack_key.as_deref(), Some(p.pack_key.as_str()));
    let v = open_read_only_with_store(cfg, store.clone()).unwrap();
    let vc = v.connect().unwrap();
    integrity_ok(&vc);
    assert_eq!(rows(&vc), want);
    drop(vc);
    drop(v);
    crash(db, vec![c]);
}

/// The pointer's page size must be the one the database header gives.
#[test]
fn pointer_page_size_other_than_the_header_is_corrupt() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    let mut p = pointer(&*store, &keys());
    p.page_size = 8192;
    store
        .put(&keys().pack_pointer(), &keys().encode_pointer(&p))
        .unwrap();
    let r = tempdir();
    let e = restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r)).unwrap_err();
    assert!(
        matches!(&e, Error::Corrupt(m) if m.contains("page_size")),
        "{e:?}"
    );
    crash(db, vec![c]);
}

/// The block flags of `dir`: one per block, after the 8-byte limit.
fn block_flags(dir: &Path) -> Vec<u8> {
    std::fs::read(dir.join(BLOCKS_FILE)).unwrap()[8..].to_vec()
}

/// A block the writer wrote into is fetched before the write and its flag is synced: after a
/// crash, the next open's background fetch never fetches it again over the newer pages, and the
/// copy holds every acked row.
#[test]
fn written_block_is_never_fetched_again_after_a_crash() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    insert_big(&c, 451..=700);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    pack_now(store.clone(), &config(a.path()));
    let p = pointer(&*store, &keys());
    assert!(p.size > 2 * BLOCK_SIZE, "{p:?}");
    crash(db, vec![c]);
    let b = tempdir();
    let off = NoBackgroundFetch::on();
    let db = open_mem(&store, &fresh(&b));
    let cb = db.connect().unwrap();
    // Rows on the last leaf pages: their block is fetched before the checkpoint writes it.
    cb.execute("UPDATE t SET v = 'w' || id WHERE id > 690", &[])
        .unwrap();
    insert_big(&cb, 701..=702);
    // TRUNCATE: the WAL restarts (a generation switch), so the pages are only in `app.db`.
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    // Before a read of every row, which would fetch every block.
    let flags = block_flags(&fresh(&b));
    assert_eq!(flags[0], 1);
    assert!(
        flags.contains(&0),
        "every block is local already: {flags:?}"
    );
    assert_eq!(*flags.last().unwrap(), 1, "{flags:?}");
    // The rows as acked, not read (a read would fetch every block now).
    let want: Vec<(i64, String, i64)> = (1..=702)
        .map(|id| match id {
            691..=700 => (id, format!("w{id}"), 4),
            _ => (id, big(id)[..12].to_string(), big(id).len() as i64),
        })
        .collect();
    crash(db, vec![cb]);
    drop(off);
    // The crashed directory stays open in this process: its files, in a directory of their own.
    let copy = copy_db_files(&fresh(&b));
    assert_eq!(block_flags(copy.path()), block_flags(&fresh(&b)));
    let db = open_mem(&store, copy.path());
    wait_complete(copy.path());
    let cb = db.connect().unwrap();
    integrity_ok(&cb);
    assert_eq!(rows(&cb), want);
    cb.close().unwrap();
    db.close().unwrap();
}

/// After the open, the pack a copy reads from is replaced by a newer pack and GC deletes it: a
/// read of a block that is not local takes the newer pack (its pack generation is not above the
/// copy's generation), and `app.db-pack` names it from then on.
#[test]
fn block_read_after_its_pack_was_deleted_takes_the_newer_pack() {
    let _off = NoBackgroundFetch::on();
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    insert_big(&c, 451..=700);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    pack_now(store.clone(), &config(a.path()));
    let old = pointer(&*store, &keys());
    crash(db, vec![c]);
    let b = tempdir();
    let db = open_mem(&store, &fresh(&b));
    let cb = db.connect().unwrap();
    // The new writer adds a row (the last block) and switches; a pack run packs that.
    insert_big(&cb, 701..=701);
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    pack_now(store.clone(), &config(a.path()));
    let new = pointer(&*store, &keys());
    assert!(new.pack_generation > old.pack_generation);
    assert!(new.pack_generation <= db.generation());
    store.delete(&old.pack_key).unwrap();
    store.delete(&old.codes_key).unwrap();
    assert_eq!(block_flags(&fresh(&b))[1], 0, "block 1 is local already");
    let one = cb
        .query("SELECT substr(v, 1, 6) FROM t WHERE id = 300", &[])
        .unwrap();
    assert_eq!(text(&one[0][0]), "000300");
    assert_eq!(block_flags(&fresh(&b))[1], 1);
    let pack_file = std::fs::read(fresh(&b).join(PACK_FILE)).unwrap();
    // The pointer bytes hold the key relative to the prefix.
    let rel = keys().relative(&new.pack_key).to_string();
    assert!(
        pack_file.windows(rel.len()).any(|w| w == rel.as_bytes()),
        "app.db-pack does not name the newer pack"
    );
    let want = rows(&cb);
    integrity_ok(&cb);
    cb.close().unwrap();
    db.close().unwrap();
    let r = tempdir();
    restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r)).unwrap();
    let v = open_read_only_with_store(config(&fresh(&r)), store.clone()).unwrap();
    assert_eq!(rows(&v.connect().unwrap()), want);
}

/// The pack grows across a 1 MiB block boundary after the open, and the old pack is gone: a read
/// takes the newer pack. The database is closed before the fetch completes; the directory
/// reopens, reads, and the background fetch completes it.
#[test]
fn pack_switch_across_a_block_boundary_then_reopen_reads_and_completes() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    insert_big(&c, 451..=700);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    pack_now(store.clone(), &config(a.path()));
    let old = pointer(&*store, &keys());
    crash(db, vec![c]);
    let b = tempdir();
    let off = NoBackgroundFetch::on();
    let db = open_mem(&store, &fresh(&b));
    let cb = db.connect().unwrap();
    insert_big(&cb, 701..=1000);
    cb.checkpoint(CheckpointMode::Truncate).unwrap();
    pack_now(store.clone(), &config(a.path()));
    let new = pointer(&*store, &keys());
    assert!(
        new.size.div_ceil(BLOCK_SIZE) > old.size.div_ceil(BLOCK_SIZE),
        "{old:?} {new:?}"
    );
    store.delete(&old.pack_key).unwrap();
    store.delete(&old.codes_key).unwrap();
    assert_eq!(block_flags(&fresh(&b))[1], 0, "block 1 is local already");
    let one = cb
        .query("SELECT substr(v, 1, 6) FROM t WHERE id = 300", &[])
        .unwrap();
    assert_eq!(text(&one[0][0]), "000300");
    assert_eq!(block_flags(&fresh(&b))[1], 1);
    cb.close().unwrap();
    db.close().unwrap();
    assert!(!state(&fresh(&b)).complete);
    let db = open_mem(&store, &fresh(&b));
    let cb = db.connect().unwrap();
    let one = cb
        .query("SELECT substr(v, 1, 6) FROM t WHERE id = 200", &[])
        .unwrap();
    assert_eq!(text(&one[0][0]), "000200");
    cb.close().unwrap();
    db.close().unwrap();
    drop(off);
    let db = open_mem(&store, &fresh(&b));
    wait_complete(&fresh(&b));
    let cb = db.connect().unwrap();
    integrity_ok(&cb);
    let want: Vec<(i64, String, i64)> = (1..=1000)
        .map(|id| (id, big(id)[..12].to_string(), big(id).len() as i64))
        .collect();
    assert_eq!(rows(&cb), want);
    cb.close().unwrap();
    db.close().unwrap();
}

/// GC runs between the restore's first block and its log listing: it deletes the pack the
/// restore read (a newer pack won) and the log below the newer pack's generation. The restore
/// reads the pointer again and starts again from the newer pack.
#[test]
fn restore_restarts_when_gc_deletes_its_pack_and_log_between_steps() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    let old = pointer(&*store, &keys());
    let old_body = store.get(&keys().pack_pointer()).unwrap().unwrap().body;
    c.execute("UPDATE t SET v = 'g' WHERE id > 420", &[])
        .unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert_big(&c, 451..=452);
    pack_now(store.clone(), &config(a.path()));
    let new = pointer(&*store, &keys());
    let want = rows(&c);
    let rec = Recording::new(store.clone());
    *rec.first_pointer.lock().unwrap() = Some(old_body);
    let gc = store.clone();
    let (old_key, old_codes, below) = (
        old.pack_key.clone(),
        old.codes_key.clone(),
        new.pack_generation,
    );
    *rec.after_range.lock().unwrap() = Some(Box::new(move || {
        gc.delete(&old_key).unwrap();
        gc.delete(&old_codes).unwrap();
        for g in 1..below {
            for seq in log_seqs(&*gc, g) {
                gc.delete(&keys().log(g, seq)).unwrap();
            }
        }
    }));
    let r = tempdir();
    let rep = restore_with_store(&config(&fresh(&r)), rec.clone(), &fresh(&r)).unwrap();
    assert_eq!(rep.pack_key.as_deref(), Some(new.pack_key.as_str()));
    let v = open_read_only_with_store(config(&fresh(&r)), store.clone()).unwrap();
    let vc = v.connect().unwrap();
    integrity_ok(&vc);
    assert_eq!(rows(&vc), want);
    drop(vc);
    drop(v);
    crash(db, vec![c]);
}

/// The log lines and the `buckwal.restore` span of a restore run under a capture.
struct Seen {
    restarted: Vec<crate::telemetry::testing::EventRec>,
    gave_up: Vec<crate::telemetry::testing::EventRec>,
    errors: usize,
    restarts: Option<String>,
    outcome: Option<String>,
}

fn seen<T>(f: impl FnOnce() -> T) -> (T, Seen) {
    use crate::telemetry::testing::Capture;
    let cap = Capture::new();
    let out = tracing::subscriber::with_default(cap.subscriber(), f);
    let spans = cap.spans();
    let msg = |m: &str| -> Vec<_> {
        spans
            .events
            .iter()
            .filter(|e| e.field("message") == Some(m))
            .cloned()
            .collect()
    };
    let restore = spans.named("buckwal.restore");
    let last = restore.last().expect("a buckwal.restore span");
    let seen = Seen {
        restarted: msg("restore restarted"),
        gave_up: msg("restore gave up"),
        errors: spans
            .events
            .iter()
            .filter(|e| e.level == tracing::Level::ERROR)
            .count(),
        restarts: last.field("buckwal.restore.restarts").map(str::to_string),
        outcome: last.field("buckwal.restore.outcome").map(str::to_string),
    };
    (out, seen)
}

/// A pointer that keeps naming a pack that is gone: the restore starts again at most 3 times,
/// one WARN per restart with its attempt number, then stops with the `Timeout` of spec rev 14,
/// one ERROR and the span outcome `gave_up`, and leaves no database.
#[test]
fn restore_of_a_pack_that_stays_gone_stops_after_its_restarts() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    let p = pointer(&*store, &keys());
    store.delete(&p.pack_key).unwrap();
    let rec = Recording::new(store.clone());
    let r = tempdir();
    let (e, seen) =
        seen(|| restore_with_store(&config(&fresh(&r)), rec.clone(), &fresh(&r)).unwrap_err());
    assert_eq!(
        e,
        Error::Timeout(
            "restore: the pack changed 3 times while it was read; raise BUCKWAL_PACK_KEEP_SECS"
                .into()
        )
    );
    let tries = rec
        .take_ranges()
        .iter()
        .filter(|(k, _, _)| k == &p.pack_key)
        .count();
    assert_eq!(tries, 4, "one attempt and 3 restarts");
    assert!(!fresh(&r).join("app.db").exists());
    assert_eq!(seen.restarted.len(), 3);
    for (i, w) in seen.restarted.iter().enumerate() {
        assert_eq!(w.level, tracing::Level::WARN);
        assert_eq!(
            w.field("attempt"),
            Some((i + 1).to_string().as_str()),
            "{w:?}"
        );
        assert_eq!(w.field("reason"), Some("pack_missing"), "{w:?}");
        assert_eq!(w.field("pack_key"), Some(p.pack_key.as_str()), "{w:?}");
    }
    assert_eq!(seen.gave_up.len(), 1);
    assert_eq!(seen.gave_up[0].field("attempt"), Some("4"));
    assert_eq!(seen.errors, 1, "one ERROR");
    assert_eq!(seen.restarts.as_deref(), Some("3"));
    assert_eq!(seen.outcome.as_deref(), Some("gave_up"));
    crash(db, vec![c]);
}

/// A restore of the log alone (no pack yet) meets a log object that GC deleted: a pack run and
/// GC ran between its first log GET and the rest. It reads the pointer again, starts again from
/// the new pack, logs one WARN `log_missing`, and records 1 restart and outcome `restored`.
#[test]
fn log_only_restore_whose_log_was_deleted_restarts_from_the_pointer() {
    let store = mem();
    let a = tempdir();
    let cfg = config(a.path());
    let db = open_with_store(cfg.clone(), store.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    insert_big(&c, 1..=20);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert_big(&c, 21..=40);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert_big(&c, 41..=45);
    assert!(
        store.get(&keys().pack_pointer()).unwrap().is_none(),
        "no pack yet"
    );
    let want = rows(&c);
    let rec = Recording::new(store.clone());
    let (gc, packer) = (store.clone(), cfg.clone());
    *rec.after_get.lock().unwrap() = Some((
        keys().log_prefix(1),
        Box::new(move || {
            let r = pack_now(gc.clone(), &packer);
            for g in 1..r.pack_generation {
                for seq in log_seqs(&*gc, g) {
                    gc.delete(&keys().log(g, seq)).unwrap();
                }
            }
        }),
    ));
    let r = tempdir();
    let (rep, seen) =
        seen(|| restore_with_store(&config(&fresh(&r)), rec.clone(), &fresh(&r)).unwrap());
    let p = pointer(&*store, &keys());
    assert_eq!(rep.pack_key.as_deref(), Some(p.pack_key.as_str()));
    assert_eq!(seen.restarted.len(), 1, "one restart");
    let w = &seen.restarted[0];
    assert_eq!(
        (w.field("attempt"), w.field("reason"), w.field("pack_key")),
        (Some("1"), Some("log_missing"), Some(""))
    );
    assert_eq!(seen.errors, 0);
    assert_eq!(seen.restarts.as_deref(), Some("1"));
    assert_eq!(seen.outcome.as_deref(), Some("restored"));
    let v = open_read_only_with_store(config(&fresh(&r)), store.clone()).unwrap();
    let vc = v.connect().unwrap();
    integrity_ok(&vc);
    assert_eq!(rows(&vc), want);
    drop(vc);
    drop(v);
    crash(db, vec![c]);
}

/// A restore from a pack meets a log object that GC deleted below a newer pack (the older pack
/// is still there): it starts again from the newer pack with the reason `log_missing`.
#[test]
fn restore_whose_log_was_deleted_restarts_from_the_newer_pack() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    let old = pointer(&*store, &keys());
    let old_body = store.get(&keys().pack_pointer()).unwrap().unwrap().body;
    c.execute("UPDATE t SET v = 'q' WHERE id > 440", &[])
        .unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert_big(&c, 451..=453);
    pack_now(store.clone(), &config(a.path()));
    let new = pointer(&*store, &keys());
    let want = rows(&c);
    let rec = Recording::new(store.clone());
    *rec.first_pointer.lock().unwrap() = Some(old_body);
    let (gc, below) = (store.clone(), new.pack_generation);
    *rec.after_range.lock().unwrap() = Some(Box::new(move || {
        for g in 1..below {
            for seq in log_seqs(&*gc, g) {
                gc.delete(&keys().log(g, seq)).unwrap();
            }
        }
    }));
    let r = tempdir();
    let (rep, seen) =
        seen(|| restore_with_store(&config(&fresh(&r)), rec.clone(), &fresh(&r)).unwrap());
    assert!(
        store.get(&old.pack_key).unwrap().is_some(),
        "the old pack stays"
    );
    assert_eq!(rep.pack_key.as_deref(), Some(new.pack_key.as_str()));
    assert_eq!(seen.restarted.len(), 1);
    let w = &seen.restarted[0];
    assert_eq!(
        (w.field("attempt"), w.field("reason"), w.field("pack_key")),
        (Some("1"), Some("log_missing"), Some(old.pack_key.as_str()))
    );
    assert_eq!(seen.outcome.as_deref(), Some("restored"));
    let v = open_read_only_with_store(config(&fresh(&r)), store.clone()).unwrap();
    let vc = v.connect().unwrap();
    integrity_ok(&vc);
    assert_eq!(rows(&vc), want);
    drop(vc);
    drop(v);
    crash(db, vec![c]);
}

/// A restore reads the head at generation H, then a pack run moves the pointer to a pack above
/// H before the restore reads the pointer. The log of H must not be replayed over that newer
/// pack: the restore starts again (reason `pointer_ahead`), reads the head and the pointer
/// again, and equals the writer.
#[test]
fn restore_whose_pointer_is_above_its_head_restarts() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    let old_head = store.get(&keys().head()).unwrap().unwrap().body;
    let read_generation = db.generation();
    c.execute("UPDATE t SET v = 'h' WHERE id > 430", &[])
        .unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert_big(&c, 451..=453);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    pack_now(store.clone(), &config(a.path()));
    let new = pointer(&*store, &keys());
    assert!(new.pack_generation > read_generation, "{new:?}");
    c.execute("DELETE FROM t WHERE id BETWEEN 10 AND 20", &[])
        .unwrap();
    insert_big(&c, 454..=455);
    let want = rows(&c);
    let rec = Recording::new(store.clone());
    *rec.first_head.lock().unwrap() = Some(old_head);
    let r = tempdir();
    let (rep, seen) =
        seen(|| restore_with_store(&config(&fresh(&r)), rec.clone(), &fresh(&r)).unwrap());
    assert_eq!(rep.generation, db.generation());
    assert_eq!(rep.pack_key.as_deref(), Some(new.pack_key.as_str()));
    assert_eq!(seen.restarted.len(), 1, "one restart");
    let w = &seen.restarted[0];
    assert_eq!(w.level, tracing::Level::WARN);
    assert_eq!(
        (w.field("attempt"), w.field("reason"), w.field("pack_key")),
        (
            Some("1"),
            Some("pointer_ahead"),
            Some(new.pack_key.as_str())
        )
    );
    assert_eq!(seen.restarts.as_deref(), Some("1"));
    assert_eq!(seen.outcome.as_deref(), Some("restored"));
    let v = open_read_only_with_store(config(&fresh(&r)), store.clone()).unwrap();
    let vc = v.connect().unwrap();
    integrity_ok(&vc);
    assert_eq!(rows(&vc), want);
    drop(vc);
    drop(v);
    crash(db, vec![c]);
}

/// A restore that meets no deletion records 0 restarts and the outcome `restored`.
#[test]
fn restore_without_restart_records_restored() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    let r = tempdir();
    let (_, seen) =
        seen(|| restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r)).unwrap());
    assert!(seen.restarted.is_empty());
    assert_eq!(seen.restarts.as_deref(), Some("0"));
    assert_eq!(seen.outcome.as_deref(), Some("restored"));
    crash(db, vec![c]);
}

/// A read-only open of an empty directory restores, and the writer switches generation during
/// the restore: the copy is behind the head before it ever opened. The open removes it and
/// restores again, and answers with the writer's rows.
#[test]
fn read_only_open_restores_again_when_the_head_moves_during_its_restore() {
    let store = mem();
    let a = tempdir();
    let db = open_mem(&store, a.path());
    let c = db.connect().unwrap();
    create(&c);
    insert_big(&c, 1..=10);
    let writer = db.clone();
    let rec = Recording::new(store.clone());
    *rec.after_get.lock().unwrap() = Some((
        "/log/".to_string(),
        Box::new(move || {
            let w = writer.connect().unwrap();
            insert_big(&w, 11..=12);
            w.checkpoint(CheckpointMode::Truncate).unwrap();
            w.close().unwrap();
        }),
    ));
    let g = db.generation();
    let r = tempdir();
    let v = open_read_only_with_store(config(&fresh(&r)), rec.clone()).unwrap();
    assert!(db.generation() > g, "the hook never ran");
    let vc = v.connect().unwrap();
    assert_eq!(rows(&vc), rows(&c));
    drop(vc);
    drop(v);
    crash(db, vec![c]);
}

/// A copy on another machine than the writer (a read-only open) whose background fetch still
/// runs at half the pack keep time logs one WARN `background fetch slower than the pack keep
/// time` with the bytes left, and its `buckwal.prefetch` span says `past_half_keep`. The
/// writer's own fetch records the attribute but logs no WARN (its GC keeps its pack).
#[test]
fn background_fetch_past_half_the_keep_time_warns_on_a_reader() {
    for writer in [false, true] {
        let store = mem();
        let a = tempdir();
        let (db, c) = packed_writer(&store, config(a.path()));
        crash(db, vec![c]);
        let rec = Recording::new(store.clone());
        rec.hold.store(true, std::sync::atomic::Ordering::SeqCst);
        let b = tempdir();
        // Trim grace 1 s: the pack keep time is 1 s, half of it 0.5 s.
        let cfg = config_with_lease(&fresh(&b), Duration::from_millis(300));
        let cap = crate::telemetry::testing::Capture::new();
        let db = tracing::subscriber::with_default(cap.subscriber(), || {
            if writer {
                open_with_store(cfg.clone(), rec.clone()).unwrap()
            } else {
                open_read_only_with_store(cfg.clone(), rec.clone()).unwrap()
            }
        });
        std::thread::sleep(Duration::from_millis(900));
        rec.hold.store(false, std::sync::atomic::Ordering::SeqCst);
        let t = Instant::now();
        let span = loop {
            let spans = cap.spans();
            if let Some(s) = spans
                .named("buckwal.prefetch")
                .into_iter()
                .find(|s| s.closed.is_some())
            {
                break s.clone();
            }
            assert!(
                t.elapsed() < Duration::from_secs(20),
                "the fetch never ended"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(span.field("buckwal.prefetch.past_half_keep"), Some("true"));
        let warns: Vec<_> = cap
            .spans()
            .events
            .into_iter()
            .filter(|e| {
                e.field("message") == Some("background fetch slower than the pack keep time")
            })
            .collect();
        if writer {
            assert!(warns.is_empty(), "{warns:?}");
        } else {
            assert_eq!(warns.len(), 1, "{warns:?}");
            assert_eq!(warns[0].level, tracing::Level::WARN);
            let left: u64 = warns[0].field("bytes_left").unwrap().parse().unwrap();
            assert!(left > 0, "{warns:?}");
        }
        db.close().unwrap();
    }
}

/// A fetch that ends well within half the keep time records `past_half_keep` false.
#[test]
fn background_fetch_within_the_keep_time_records_false() {
    let store = mem();
    let a = tempdir();
    let (db, c) = packed_writer(&store, config(a.path()));
    crash(db, vec![c]);
    let b = tempdir();
    let cap = crate::telemetry::testing::Capture::new();
    let db = tracing::subscriber::with_default(cap.subscriber(), || {
        open_read_only_with_store(config(&fresh(&b)), store.clone()).unwrap()
    });
    let t = Instant::now();
    let span = loop {
        if let Some(s) = cap
            .spans()
            .named("buckwal.prefetch")
            .into_iter()
            .find(|s| s.closed.is_some())
        {
            break s.clone();
        }
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "the fetch never ended"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(span.field("buckwal.prefetch.past_half_keep"), Some("false"));
    db.close().unwrap();
}

/// The writer's GC never deletes the pack that the writer still reads blocks from, past the
/// pack keep time, while it deletes the log below the newer pack. (No background fetch here:
/// the blocks past the first stay remote.)
#[test]
fn writer_gc_keeps_the_pack_its_own_copy_still_reads() {
    let store = mem();
    let a = tempdir();
    let cfg_a = config(a.path());
    let dba = open_with_store(cfg_a.clone(), store.clone()).unwrap();
    let ca = dba.connect().unwrap();
    create(&ca);
    insert_big(&ca, 1..=1000);
    ca.checkpoint(CheckpointMode::Truncate).unwrap();
    pack_now(store.clone(), &cfg_a);
    let first = pointer(&*store, &keys());
    crash(dba, vec![ca]);

    let _no_fetch = NoBackgroundFetch::on();
    let trimmed: Arc<Mutex<Vec<u64>>> = Arc::default();
    let seen = trimmed.clone();
    let b = tempdir();
    let mut cfg_b = config(&fresh(&b));
    cfg_b.on_event = Some(Arc::new(move |e: &crate::config::Event| {
        if let crate::config::Event::Trimmed { deleted } = e {
            seen.lock().unwrap().push(*deleted);
        }
    }));
    let db = open_with_store(cfg_b.clone(), store.clone()).unwrap();
    let c = db.connect().unwrap();
    assert!(fresh(&b).join(BLOCKS_FILE).exists(), "blocks still remote");
    insert_big(&c, 1001..=1002);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    pack_now(store.clone(), &cfg_a);
    let second = pointer(&*store, &keys());
    assert!(second.pack_generation > first.pack_generation);
    store.advance_clock(Duration::from_secs(3600));
    // A switch wakes the writer's GC.
    insert_big(&c, 1003..=1003);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    let t = Instant::now();
    while trimmed.lock().unwrap().is_empty() {
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "the writer's GC never trimmed"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(fresh(&b).join(BLOCKS_FILE).exists(), "blocks still remote");
    assert!(
        store.get(&first.pack_key).unwrap().is_some(),
        "the pack it reads stays"
    );
    assert!(store.get(&first.codes_key).unwrap().is_some());
    for g in 1..second.pack_generation {
        assert!(
            log_seqs(&*store, g).is_empty(),
            "log/{g} below the GC base goes"
        );
    }
    // Every row still reads, from the pack it kept.
    assert_eq!(rows(&c).len(), 1003);
    // `gc` of another process does not know the writer's copy: it deletes the pack.
    let r = crate::gc::gc_with_store(&config(a.path()), store.clone()).unwrap();
    assert_eq!(r.packs_deleted, 1, "{r:?}");
    drop(c);
    db.close().unwrap();
}

/// With batching Off and with a Window, a writer commits (a pack, then acked commits in a
/// generation it switched after the pack and in its current one, from several connections at
/// once), its directory is deleted, and a claim from an empty directory restores every acked
/// commit (spec "Tests"). Background commits that were not yet acked are not part of it.
#[test]
fn claim_from_an_empty_directory_restores_every_acked_commit() {
    let window = crate::Batching::Window {
        max_wait: Duration::from_millis(20),
        max_commits: 8,
        max_bytes: 1 << 20,
    };
    for batching in [crate::Batching::Off, window] {
        let store = mem();
        let a = tempdir();
        let mut cfg = config(&fresh(&a));
        cfg.batching = batching;
        let (db, c) = packed_writer(&store, cfg.clone());
        let packed_at = db.generation();
        insert_big(&c, 451..=455);
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        assert!(db.generation() > packed_at);
        // Four connections at once: with a Window their commits share uploads.
        let acked: Vec<i64> = std::thread::scope(|s| {
            let hs: Vec<_> = (0..4)
                .map(|n| {
                    let conn = db.connect().unwrap();
                    s.spawn(move || {
                        let ids: Vec<i64> = (0..10).map(|i| 1000 + n * 100 + i).collect();
                        for id in &ids {
                            conn.execute(
                                "INSERT INTO t(id, v) VALUES (?, ?)",
                                &[Value::from_i64(*id), Value::build_text(big(*id))],
                            )
                            .unwrap();
                        }
                        ids
                    })
                })
                .collect();
            hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(acked.len(), 40);
        let want = rows(&c);
        assert_eq!(want.len(), 450 + 5 + 40, "{batching:?}");
        let before = head(&*store);
        crash(db, vec![c]);
        std::fs::remove_dir_all(fresh(&a)).unwrap();

        let b = tempdir();
        let mut cfg_b = config(&fresh(&b));
        cfg_b.batching = batching;
        let db = open_with_store(cfg_b, store.clone()).unwrap();
        let after = head(&*store);
        assert_eq!(
            after.epoch,
            before.epoch + 1,
            "{batching:?}: the open claims"
        );
        assert_eq!(after.writer_id, state(&fresh(&b)).writer_id);
        let cb = db.connect().unwrap();
        integrity_ok(&cb);
        assert_eq!(rows(&cb), want, "{batching:?}");
        // The claimed writer carries on, and a restore equals it.
        insert_big(&cb, 461..=461);
        let want = rows(&cb);
        cb.close().unwrap();
        db.close().unwrap();
        let r = tempdir();
        restore_with_store(&config(&fresh(&r)), store.clone(), &fresh(&r)).unwrap();
        let v = open_read_only_with_store(config(&fresh(&r)), store.clone()).unwrap();
        assert_eq!(rows(&v.connect().unwrap()), want, "{batching:?}");
    }
}
