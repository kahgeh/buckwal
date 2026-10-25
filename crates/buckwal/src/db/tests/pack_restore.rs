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
/// out a saved pointer for the first pointer GET.
struct Recording {
    inner: Arc<MemStore>,
    ranges: Mutex<Vec<(String, u64, u64)>>,
    pointer_key: String,
    first_pointer: Mutex<Option<Vec<u8>>>,
    /// While set, a range GET past the first block waits.
    hold: std::sync::atomic::AtomicBool,
}

impl Recording {
    fn new(inner: Arc<MemStore>) -> Arc<Recording> {
        Arc::new(Recording {
            inner,
            ranges: Mutex::new(Vec::new()),
            pointer_key: keys().pack_pointer(),
            first_pointer: Mutex::new(None),
            hold: Default::default(),
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
        self.inner.get(k)
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
    fn list_sized(&self, p: &str) -> std::result::Result<Vec<(String, u64)>, StoreError> {
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
        self.inner.get_range(k, offset, len)
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
    assert!(
        pack_file
            .windows(new.pack_key.len())
            .any(|w| w == new.pack_key.as_bytes()),
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
