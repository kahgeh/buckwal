//! Pack runs against the in-memory store, with a store that injects replies (`HookStore`).
use super::*;
use crate::config::Config;
use crate::db::{open_with_store, Database};
use crate::format::{Head, Keys, LogRecord};
use crate::store::{
    MemStore, MultipartDone, Object, ObjectStat, ObjectStore, PartSource, StatsSnap, StoreError,
};
use crate::{CheckpointMode, Connection, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;
use tempfile::TempDir;

const PREFIX: &str = "t";

fn config(dir: &Path) -> Config {
    let mut c = Config::new(dir, "http://127.0.0.1:1", "bucket", "key", "secret");
    c.prefix = PREFIX.to_string();
    c.retry_deadline = Duration::from_secs(2);
    c
}

fn keys() -> Keys {
    Keys::new(PREFIX)
}

fn tempdir() -> TempDir {
    tempfile::tempdir().unwrap()
}

/// The test key, as hex, in `keydir/key`.
fn enc_config(dir: &Path, keydir: &TempDir) -> Config {
    let path = keydir.path().join("key");
    fs::write(&path, "11".repeat(32)).unwrap();
    let mut c = config(dir);
    c.encryption_key_file = Some(path);
    c
}

/// A clock the test moves by hand, in ms.
fn fake_clock() -> (Arc<AtomicU64>, Clock) {
    let ms = Arc::new(AtomicU64::new(0));
    let m = ms.clone();
    (ms, Arc::new(move || Duration::from_millis(m.load(SeqCst))))
}

/// What the hook makes a call answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reply {
    Pass,
    /// A conditional put answers 412 and stores nothing.
    Fail412,
    /// The call fails as a timeout would, before it reaches the store.
    Transient,
    /// The call reaches the store, then fails as a dropped connection would.
    StoreThenTransient,
    /// A conditional put is stored, then answers 412.
    StoreThen412,
}

type Hook = Box<dyn FnMut(&str, &str, &MemStore) -> Reply + Send>;

/// A store over a `MemStore` that records each call ("op key") and lets a hook answer it.
struct HookStore {
    inner: Arc<MemStore>,
    hook: Mutex<Hook>,
    calls: Mutex<Vec<String>>,
    /// The HEAD of a pack (not its codes) answers one byte more.
    size_lie: AtomicBool,
    /// A multipart upload answers another checksum for its first part.
    sha_lie: AtomicBool,
    /// Each multipart upload waits here first (outside the hook's lock).
    barrier: Mutex<Option<Arc<Barrier>>>,
}

impl HookStore {
    fn new(inner: Arc<MemStore>) -> Arc<HookStore> {
        Arc::new(HookStore {
            inner,
            hook: Mutex::new(Box::new(|_, _, _| Reply::Pass)),
            calls: Mutex::new(Vec::new()),
            size_lie: AtomicBool::new(false),
            sha_lie: AtomicBool::new(false),
            barrier: Mutex::new(None),
        })
    }

    fn set_hook(&self, h: impl FnMut(&str, &str, &MemStore) -> Reply + Send + 'static) {
        *self.hook.lock().unwrap() = Box::new(h);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn reply(&self, op: &str, key: &str) -> Reply {
        self.calls.lock().unwrap().push(format!("{op} {key}"));
        (self.hook.lock().unwrap())(op, key, &self.inner)
    }

    fn transient(key: &str) -> StoreError {
        StoreError::Transient(format!("injected for {key}"))
    }

    fn cond(
        &self,
        op: &str,
        key: &str,
        f: impl FnOnce() -> std::result::Result<CondPut, StoreError>,
    ) -> std::result::Result<CondPut, StoreError> {
        match self.reply(op, key) {
            Reply::Pass => f(),
            Reply::Fail412 => Ok(CondPut::PreconditionFailed),
            Reply::Transient => Err(Self::transient(key)),
            Reply::StoreThenTransient => f().and(Err(Self::transient(key))),
            Reply::StoreThen412 => f().map(|_| CondPut::PreconditionFailed),
        }
    }

    fn plain<T>(
        &self,
        op: &str,
        key: &str,
        f: impl FnOnce() -> std::result::Result<T, StoreError>,
    ) -> std::result::Result<T, StoreError> {
        match self.reply(op, key) {
            Reply::Transient => Err(Self::transient(key)),
            Reply::StoreThenTransient => f().and(Err(Self::transient(key))),
            _ => f(),
        }
    }
}

fn is_pack(key: &str) -> bool {
    key.contains("/pack/") && !key.ends_with(".codes")
}

impl ObjectStore for HookStore {
    fn put(&self, key: &str, body: &[u8]) -> std::result::Result<String, StoreError> {
        self.plain("put", key, || self.inner.put(key, body))
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> std::result::Result<CondPut, StoreError> {
        self.cond("put_if_absent", key, || self.inner.put_if_absent(key, body))
    }
    fn put_if_match(
        &self,
        key: &str,
        body: &[u8],
        etag: &str,
    ) -> std::result::Result<CondPut, StoreError> {
        self.cond("put_if_match", key, || {
            self.inner.put_if_match(key, body, etag)
        })
    }
    fn get(&self, key: &str) -> std::result::Result<Option<Object>, StoreError> {
        self.plain("get", key, || self.inner.get(key))
    }
    fn stat(&self, key: &str) -> std::result::Result<Option<ObjectStat>, StoreError> {
        let lie = self.size_lie.load(SeqCst) && is_pack(key);
        self.plain("stat", key, || self.inner.stat(key)).map(|st| {
            st.map(|mut s| {
                if lie {
                    s.size += 1;
                }
                s
            })
        })
    }
    fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StoreError> {
        self.plain("list", prefix, || self.inner.list(prefix))
    }
    fn list_after(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> std::result::Result<Vec<String>, StoreError> {
        self.plain("list_after", prefix, || {
            self.inner.list_after(prefix, start_after)
        })
    }
    fn put_multipart(
        &self,
        key: &str,
        source: &mut dyn PartSource,
        part_size: usize,
        concurrency: usize,
    ) -> std::result::Result<MultipartDone, StoreError> {
        let barrier = self.barrier.lock().unwrap().clone();
        if let Some(b) = barrier {
            b.wait();
        }
        let mut done = self.plain("put_multipart", key, || {
            self.inner
                .put_multipart(key, source, part_size, concurrency)
        })?;
        if self.sha_lie.load(SeqCst) {
            done.part_sha256[0][0] ^= 1;
        }
        Ok(done)
    }
    fn get_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> std::result::Result<Option<Vec<u8>>, StoreError> {
        self.plain("get_range", key, || self.inner.get_range(key, offset, len))
    }
    fn abort_incomplete(
        &self,
        prefix: &str,
        older_than: Duration,
    ) -> std::result::Result<u64, StoreError> {
        self.plain("abort_incomplete", prefix, || {
            self.inner.abort_incomplete(prefix, older_than)
        })
    }
    fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
        self.plain("delete", key, || self.inner.delete(key))
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

/// A writer over `store` in its own directory, with table `t`.
struct Writer {
    db: Database,
    c: Connection,
    _dir: TempDir,
}

fn writer_with(store: &Arc<MemStore>, cfg: impl FnOnce(&Path) -> Config) -> Writer {
    let dir = tempdir();
    let db = open_with_store(cfg(dir.path()), store.clone()).unwrap();
    let c = db.connect().unwrap();
    c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    Writer { db, c, _dir: dir }
}

fn writer(store: &Arc<MemStore>) -> Writer {
    writer_with(store, config)
}

impl Writer {
    fn insert(&self, ids: std::ops::RangeInclusive<i64>) {
        for id in ids {
            self.c
                .execute(
                    "INSERT INTO t(id, v) VALUES (?, ?)",
                    &[Value::from_i64(id), Value::build_text(format!("row {id}"))],
                )
                .unwrap();
        }
    }

    /// A TRUNCATE checkpoint: the generation ends (a generation switch; a seal in a bucket of
    /// format 1 to 4).
    fn end_generation(&self) {
        let g = self.db.generation();
        self.c.checkpoint(CheckpointMode::Truncate).unwrap();
        assert!(self.db.generation() > g, "the checkpoint moved the head");
    }

    fn ids(&self) -> Vec<i64> {
        query_ids(&self.c)
    }

    /// A crash: nothing runs on drop.
    fn crash(self) {
        std::mem::forget(self.c);
        std::mem::forget(self.db);
    }
}

fn query_ids(c: &Connection) -> Vec<i64> {
    c.query("SELECT id FROM t ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect()
}

/// A pack run with a real clock, and the working directory gone after it.
fn run(cfg: &Config, store: Arc<dyn ObjectStore>) -> Result<PackReport> {
    let (_, clock) = fake_clock();
    run_clocked(cfg, store, clock)
}

fn run_clocked(cfg: &Config, store: Arc<dyn ObjectStore>, clock: Clock) -> Result<PackReport> {
    let r = pack_run(cfg, store, clock);
    if let Ok(entries) = fs::read_dir(&cfg.dir) {
        for e in entries {
            let name = e.unwrap().file_name().to_string_lossy().to_string();
            assert!(!name.starts_with("pack-work-"), "{name} is left");
        }
    }
    r
}

fn packer(t: &TempDir) -> Config {
    config(&t.path().join("packer"))
}

fn head_of(store: &dyn ObjectStore) -> Head {
    keys()
        .decode_head(&store.get(&keys().head()).unwrap().unwrap().body)
        .unwrap()
}

fn pointer_of(store: &dyn ObjectStore) -> Option<PackPointer> {
    let body = store.get(&keys().pack_pointer()).unwrap()?.body;
    Some(keys().adopting(store).decode_pointer(&body).unwrap())
}

/// Every key under `pack/`.
fn packs(store: &dyn ObjectStore) -> Vec<String> {
    store.list(&keys().pack_root()).unwrap()
}

fn log_keys(store: &dyn ObjectStore) -> Vec<String> {
    store.list(&keys().log_root()).unwrap()
}

/// Open a database file (and its `-wal`, when given) in a new directory and read table `t`.
fn rows_of(db: &[u8], wal: Option<&[u8]>, enc: Option<&Config>) -> Vec<i64> {
    let t = tempdir();
    fs::write(t.path().join("app.db"), db).unwrap();
    if let Some(w) = wal {
        fs::write(t.path().join("app.db-wal"), w).unwrap();
    }
    let enc = enc.map(|c| encryption::load(c).unwrap().unwrap());
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let mut opts = turso_core::OpenOptions::new(Arc::new(turso_core::SqliteDialect));
    if let Some(e) = &enc {
        opts = opts
            .db_opts(turso_core::DatabaseOpts::new().with_encryption(true))
            .encryption(turso_core::EncryptionOpts {
                cipher: e.cipher.to_string(),
                hexkey: hex::encode(e.key.as_slice()),
            });
    }
    let path = t.path().join("app.db");
    let db = turso_core::Database::open(io, path.to_str().unwrap(), opts).unwrap();
    let c = db
        .connect_with_encryption(enc.as_ref().map(|e| e.key.clone()))
        .unwrap();
    let rows = c
        .query("SELECT id FROM t ORDER BY id")
        .unwrap()
        .unwrap()
        .run_collect_rows()
        .unwrap();
    rows.iter()
        .map(|r| match &r[0] {
            Value::Numeric(turso_core::Numeric::Integer(i)) => *i,
            v => panic!("{v:?}"),
        })
        .collect()
}

/// The pointer's pack, every block checked against its codes.
fn pack_bytes(store: &dyn ObjectStore, k: &Keys) -> (PackPointer, Vec<u8>) {
    let k = &k.clone().adopting(store);
    let p = k
        .decode_pointer(&store.get(&k.pack_pointer()).unwrap().unwrap().body)
        .unwrap();
    let body = store.get(&p.pack_key).unwrap().unwrap().body;
    assert_eq!(body.len() as u64, p.size);
    let codes = store.get(&p.codes_key).unwrap().unwrap().body;
    let codes = PackCodes::decode(k, &codes, &p.pack_key, p.pack_generation, p.size).unwrap();
    for (n, b) in body.chunks(BLOCK_SIZE as usize).enumerate() {
        codes
            .verify_block(k, &p.pack_key, p.pack_generation, p.size, n as u64, b)
            .unwrap();
    }
    (p, body)
}

fn pack_rows(store: &dyn ObjectStore) -> Vec<i64> {
    rows_of(&pack_bytes(store, &keys()).1, None, None)
}

fn packed(r: &PackReport) {
    assert_eq!(r.outcome, PackOutcome::Packed, "{r:?}");
}

fn failed_at(r: &PackReport, step: &str) -> Option<String> {
    match &r.outcome {
        PackOutcome::Failed { step: s, key } if s == step => key.clone(),
        o => panic!("expected Failed at {step}, got {o:?}"),
    }
}

/// Put an encoded record at `log/<g>/<seq>`.
fn put_record(store: &dyn ObjectStore, g: u64, seq: u64, rec: &LogRecord) {
    let k = keys();
    store
        .put(&k.log(g, seq), &k.encode_record(rec, g, seq))
        .unwrap();
}

fn record_at(store: &dyn ObjectStore, g: u64, seq: u64) -> LogRecord {
    let k = keys();
    k.decode_record(&store.get(&k.log(g, seq)).unwrap().unwrap().body, g, seq)
        .unwrap()
}

/// The head as a takeover's new writer `b` leaves it once it writes in a later generation:
/// generation `g`, the next epoch, the floor at `g`.
fn move_head(store: &dyn ObjectStore, g: u64) {
    let old = head_of(store);
    let new = Head {
        generation: g,
        writer_id: "b".into(),
        epoch: old.epoch + 1,
        next_generation_floor: Some(g),
        ..old
    };
    store
        .put(&keys().head(), &keys().encode_head(&new))
        .unwrap();
}

fn fence(epoch: u64) -> LogRecord {
    LogRecord::Fence {
        writer_id: "b".into(),
        epoch,
    }
}

/// The highest seq of generation `g`.
fn last_seq(store: &dyn ObjectStore, g: u64) -> u64 {
    store
        .list(&keys().log_prefix(g))
        .unwrap()
        .iter()
        .filter_map(|k| keys().parse_log(k))
        .map(|(_, s)| s)
        .max()
        .unwrap_or(0)
}

#[test]
fn pack_applies_one_generation() {
    for encrypted in [false, true] {
        let mem = Arc::new(MemStore::new());
        let keydir = tempdir();
        let w = if encrypted {
            writer_with(&mem, |d| enc_config(d, &keydir))
        } else {
            writer(&mem)
        };
        w.insert(1..=5);
        w.end_generation();
        w.insert(6..=6);
        let t = tempdir();
        let cfg = if encrypted {
            enc_config(&t.path().join("packer"), &keydir)
        } else {
            packer(&t)
        };
        let r = run(&cfg, mem.clone()).unwrap();
        packed(&r);
        assert_eq!(r.pack_generation, 2);
        assert!(r.bytes_up > 0 && r.bytes_down > 0, "{r:?}");
        let k = cfg.keys().unwrap();
        k.decode_head(&mem.get(&k.head()).unwrap().unwrap().body)
            .unwrap();
        let (p, body) = pack_bytes(&*mem, &k);
        assert_eq!(Some(p.pack_key.clone()), r.pack_key);
        assert!(
            p.pack_key.starts_with("t/pack/0000000002/"),
            "{}",
            p.pack_key
        );
        // Every format 5 pointer carries the head's id (spec rev 11, rule 3).
        assert_eq!(p.database_id, k.known_database_id());
        assert!(p.database_id.is_some());
        assert_eq!(
            rows_of(&body, None, encrypted.then_some(&cfg)),
            vec![1, 2, 3, 4, 5]
        );
        if encrypted {
            assert!(
                !body.windows(5).any(|w| w == b"row 1"),
                "plaintext in the pack"
            );
        }
        // Nothing ended since.
        let again = run(&cfg, mem.clone()).unwrap();
        assert_eq!(again.outcome, PackOutcome::NothingToDo);
        assert_eq!(again.pack_generation, 2);
        w.crash();
    }
}

#[test]
fn pack_applies_several_generations_and_restore_equals_writer_db() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    let t = tempdir();
    let cfg = packer(&t);
    w.insert(1..=3);
    w.end_generation();
    w.insert(4..=6);
    w.end_generation();
    let r = run(&cfg, mem.clone()).unwrap();
    packed(&r);
    assert_eq!(r.pack_generation, 3);
    assert_eq!(pack_rows(&*mem), (1..=6).collect::<Vec<_>>());
    w.c.execute("UPDATE t SET v = 'x' WHERE id = 2", &[])
        .unwrap();
    w.c.execute("DELETE FROM t WHERE id = 5", &[]).unwrap();
    w.insert(7..=9);
    w.end_generation();
    w.insert(10..=40);
    w.end_generation();
    w.insert(41..=42);
    let r = run(&cfg, mem.clone()).unwrap();
    packed(&r);
    assert_eq!(r.pack_generation, 5);
    // A restore: the pack, then the head's generation as the WAL.
    let head = head_of(&*mem);
    assert_eq!(head.generation, 5);
    let images = crate::restore::generation_image(
        &*mem,
        &keys(),
        &head,
        &cfg.engine(crate::io::IoMode::Writer),
    )
    .unwrap();
    let (_, body) = pack_bytes(&*mem, &keys());
    assert_eq!(rows_of(&body, Some(&images.wal), None), w.ids());
    assert!(r.free_page_share >= 0.0 && r.free_page_share < 1.0);
    let names: Vec<_> = r.step_times.iter().map(|(n, _)| *n).collect();
    assert_eq!(names, ["read", "apply", "upload", "verify", "publish"]);
    w.crash();
}

#[test]
fn generation_without_terminal_record_waits() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=3);
    let t = tempdir();
    let cfg = packer(&t);
    // The head's generation never counts as ended.
    let r = run(&cfg, mem.clone()).unwrap();
    assert_eq!(r.outcome, PackOutcome::NothingToDo);
    assert_eq!(r.pack_generation, 0);
    assert!(packs(&*mem).is_empty() && pointer_of(&*mem).is_none());
    w.crash();
}

/// The ended generations a scan of `store` from `from` finds (`ended_generations`).
fn ended(cfg: &Config, store: &MemStore, from: u64) -> Vec<EndedGeneration> {
    let k = keys();
    let retry = cfg.engine(crate::io::IoMode::Writer).retry;
    let r = LogReader {
        store,
        keys: &k,
        retry: &retry,
        concurrency: 4,
    };
    ended_generations(&r, from, &head_of(store)).unwrap().ended
}

/// A seal-start record of `writer`; in format 5 it names the switch's next generation.
fn seal_start(writer: &str, next: Option<u64>) -> LogRecord {
    LogRecord::Extents {
        file: crate::format::LogFile::Wal,
        writer_id: writer.into(),
        syncs: Vec::new(),
        next_generation: next,
    }
}

/// `rec`, an Extents record, as writer `writer` wrote it.
fn as_writer(rec: LogRecord, writer: &str) -> LogRecord {
    match rec {
        LogRecord::Extents { file, syncs, .. } => LogRecord::Extents {
            file,
            writer_id: writer.into(),
            syncs,
            next_generation: None,
        },
        r => panic!("not an Extents record: {r:?}"),
    }
}

/// Make the last record of generation `g`, a seal-start record the writer of today wrote,
/// the format 5 one: it names `next`, the generation its switch moved the head to.
fn link(store: &dyn ObjectStore, g: u64, next: u64) {
    let s = last_seq(store, g);
    let LogRecord::Extents {
        writer_id, syncs, ..
    } = record_at(store, g, s)
    else {
        panic!("log/{g} ends with a fence");
    };
    assert!(
        syncs.is_empty(),
        "log/{g} does not end with a seal-start record"
    );
    put_record(store, g, s, &seal_start(&writer_id, Some(next)));
}

/// The rows a restore of generation 1 (no pack, no seal) gives, with `writer` the writer the
/// head named at generation 1.
fn restore_rows_of_generation_1(store: &dyn ObjectStore, writer: &str) -> Vec<i64> {
    let head = Head {
        generation: 1,
        writer_id: writer.into(),
        ..head_of(store)
    };
    let cfg = config(Path::new("/x")).engine(crate::io::IoMode::Writer);
    let images = crate::restore::generation_image(store, &keys(), &head, &cfg).unwrap();
    let seed = crate::restore::page_one_seed(&images.wal, &head, JournalMode::Wal).unwrap();
    rows_of(&seed, Some(&images.wal), None)
}

fn no_upload_and_no_pointer_write(hs: &HookStore, mem: &MemStore) {
    assert!(
        !hs.calls().iter().any(|c| c.starts_with("put_multipart")
            || (c.starts_with("put") && c.ends_with("pack-latest"))),
        "{:?}",
        hs.calls()
    );
    assert!(packs(mem).is_empty() && pointer_of(mem).is_none());
}

/// A run through a recording store that must end `log_incomplete` at `key`, having uploaded
/// nothing.
fn assert_log_incomplete(mem: &Arc<MemStore>, t: &TempDir, key: String) {
    let hs = HookStore::new(mem.clone());
    let r = run(&packer(t), hs.clone()).unwrap();
    assert_eq!(failed_at(&r, "log_incomplete"), Some(key));
    no_upload_and_no_pointer_write(&hs, mem);
}

#[test]
fn head_past_generation_without_seal_start_last_is_log_incomplete() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=3);
    w.end_generation();
    let s = last_seq(&*mem, 1);
    assert_eq!(
        record_at(&*mem, 1, s),
        seal_start(&head_of(&*mem).writer_id, Some(2))
    );
    mem.delete(&keys().log(1, s)).unwrap();
    let t = tempdir();
    assert_log_incomplete(&mem, &t, keys().log(1, s - 1));
    w.crash();
}

#[test]
fn fence_last_with_head_at_generation_is_not_ended() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=2);
    w.crash();
    let retry = config(Path::new("/x"))
        .engine(crate::io::IoMode::Writer)
        .retry;
    let (taken, _) = crate::bucket::take_over(&*mem, &keys(), &retry, 1, "b").unwrap();
    put_record(&*mem, 1, last_seq(&*mem, 1) + 1, &fence(taken.epoch));
    let t = tempdir();
    let cfg = packer(&t);
    assert!(ended(&cfg, &mem, 1).is_empty());
    let r = run(&cfg, mem.clone()).unwrap();
    assert_eq!(r.outcome, PackOutcome::NothingToDo);
    assert!(packs(&*mem).is_empty() && pointer_of(&*mem).is_none());
}

#[test]
fn pack_between_head_write_and_fence_record_does_not_apply_that_generation() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=2);
    let retry = config(Path::new("/x"))
        .engine(crate::io::IoMode::Writer)
        .retry;
    // The takeover's head write; its fence record has not landed.
    crate::bucket::take_over(&*mem, &keys(), &retry, 1, "b").unwrap();
    // Not applied because the head still names generation 1.
    assert_eq!(head_of(&*mem).generation, 1);
    let t = tempdir();
    let cfg = packer(&t);
    assert!(ended(&cfg, &mem, 1).is_empty());
    let r = run(&cfg, mem.clone()).unwrap();
    assert_eq!(r.outcome, PackOutcome::NothingToDo);
    assert!(packs(&*mem).is_empty() && pointer_of(&*mem).is_none());
    w.crash();
}

/// The old writer acks commits between the takeover's head write and its fence record; the new
/// writer commits after the fence in the same generation, then switches. The pack holds all of
/// them, as a restore does.
#[test]
fn later_run_holds_acked_commits_before_fence_and_new_writers_after() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=2);
    let retry = config(Path::new("/x"))
        .engine(crate::io::IoMode::Writer)
        .retry;
    let (taken, _) = crate::bucket::take_over(&*mem, &keys(), &retry, 1, "b").unwrap();
    w.insert(3..=3);
    // Row 4's record becomes the new writer's first commit after the fence.
    w.insert(4..=4);
    w.crash();
    let s = last_seq(&*mem, 1);
    let r4 = record_at(&*mem, 1, s);
    mem.delete(&keys().log(1, s)).unwrap();
    put_record(&*mem, 1, s, &fence(taken.epoch));
    put_record(&*mem, 1, s + 1, &as_writer(r4, "b"));
    put_record(&*mem, 1, s + 2, &seal_start("b", Some(3)));
    move_head(&*mem, 3);
    let want = restore_rows_of_generation_1(&*mem, "b");
    assert_eq!(want, vec![1, 2, 3, 4]);
    let t = tempdir();
    let r = run(&packer(&t), mem.clone()).unwrap();
    packed(&r);
    assert_eq!(r.pack_generation, 3);
    assert_eq!(pack_rows(&*mem), want);
}

/// An older writer's seal-start record before a fence holds no pages and changes nothing.
#[test]
fn earlier_seal_start_before_fence_is_harmless() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=2);
    w.insert(3..=3);
    let a = head_of(&*mem).writer_id;
    w.crash();
    let s = last_seq(&*mem, 1);
    let r3 = record_at(&*mem, 1, s);
    mem.delete(&keys().log(1, s)).unwrap();
    let epoch = head_of(&*mem).epoch + 1;
    put_record(&*mem, 1, s, &seal_start(&a, Some(2)));
    put_record(&*mem, 1, s + 1, &fence(epoch));
    put_record(&*mem, 1, s + 2, &as_writer(r3, "b"));
    put_record(&*mem, 1, s + 3, &seal_start("b", Some(3)));
    move_head(&*mem, 3);
    let want = restore_rows_of_generation_1(&*mem, "b");
    assert_eq!(want, vec![1, 2, 3]);
    let t = tempdir();
    let r = run(&packer(&t), mem.clone()).unwrap();
    packed(&r);
    assert_eq!(r.pack_generation, 3);
    assert_eq!(pack_rows(&*mem), want);
}

/// A takeover through the library, in its own directory: the head write and the fence record,
/// then the new writer's open.
fn take_over_into(mem: &Arc<MemStore>, dir: &Path) -> (Database, Connection) {
    crate::takeover::takeover_with_store(config(dir), mem.clone()).unwrap();
    let db = open_with_store(config(dir), mem.clone()).unwrap();
    let c = db.connect().unwrap();
    (db, c)
}

fn insert_on(c: &Connection, ids: std::ops::RangeInclusive<i64>) {
    for id in ids {
        c.execute(
            "INSERT INTO t(id, v) VALUES (?, 'n')",
            &[Value::from_i64(id)],
        )
        .unwrap();
    }
}

/// A generation switch of `db` (a TRUNCATE checkpoint): its seal-start record, the last of the
/// generation, names the generation the head moved to. Returns that generation.
fn switch(mem: &MemStore, db: &Database, c: &Connection) -> u64 {
    let from = db.generation();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    let to = db.generation();
    assert!(to > from);
    let LogRecord::Extents {
        syncs,
        next_generation,
        ..
    } = record_at(mem, from, last_seq(mem, from))
    else {
        panic!("log/{from} ends with a fence");
    };
    assert_eq!((syncs.len(), next_generation), (0, Some(to)));
    to
}

/// The pack, then the head's generation as the WAL: a restore's rows.
fn restore_rows(mem: &MemStore) -> Vec<i64> {
    let head = head_of(mem);
    let cfg = config(Path::new("/x")).engine(crate::io::IoMode::Writer);
    let images = crate::restore::generation_image(mem, &keys(), &head, &cfg).unwrap();
    rows_of(&pack_bytes(mem, &keys()).1, Some(&images.wal), None)
}

/// A takeover, the new writer's commits after the fence in the same generation, its switch
/// (which skips a generation): the pack equals a restore. With format 5 seal-start records the
/// chain names the skip; with the old ones the head's floor does.
#[test]
fn skipped_generation_is_passed_over() {
    for format5 in [true, false] {
        let mem = Arc::new(MemStore::new());
        // The seal formats write seal-start records that name no next generation.
        let legacy = (!format5).then(crate::format::LegacySeal::on);
        let a = writer(&mem);
        drop(legacy);
        a.insert(1..=3);
        if format5 {
            switch(&mem, &a.db, &a.c);
        } else {
            a.end_generation();
        }
        a.insert(4..=5);
        a.crash();
        let t = tempdir();
        let (b, c) = take_over_into(&mem, &t.path().join("b"));
        insert_on(&c, 6..=7);
        if format5 {
            assert_eq!(switch(&mem, &b, &c), 4);
        } else {
            c.checkpoint(CheckpointMode::Truncate).unwrap();
            assert_eq!(b.generation(), 4);
        }
        assert!(log_keys(&*mem)
            .iter()
            .all(|k| keys().parse_log(k).unwrap().0 != 3));
        insert_on(&c, 8..=8);
        let cfg = packer(&t);
        let gens: Vec<_> = ended(&cfg, &mem, 1)
            .iter()
            .map(|e| (e.generation, e.last_seq_to_apply > 0))
            .collect();
        assert_eq!(
            gens,
            [(1, true), (2, true), (3, false)],
            "format 5: {format5}"
        );
        let r = run(&cfg, mem.clone()).unwrap();
        packed(&r);
        assert_eq!(r.pack_generation, 4);
        assert_eq!(pack_rows(&*mem), (1..=7).collect::<Vec<_>>());
        assert_eq!(restore_rows(&mem), query_ids(&c));
        std::mem::forget(c);
        std::mem::forget(b);
    }
}

/// Two takeovers in one generation, a switch, a third takeover and a switch, no pack between:
/// the gaps (two generations, then one) follow from the chain, and the pack equals a restore.
/// The head's floor alone names only the newest gap.
#[test]
fn two_takeovers_a_switch_a_third_takeover_and_a_switch_pack_equals_restore() {
    let mem = Arc::new(MemStore::new());
    let a = writer(&mem);
    a.insert(1..=3);
    switch(&mem, &a.db, &a.c);
    a.insert(4..=5);
    a.crash();
    let t = tempdir();
    // Two takeovers of generation 2: floors 4, then 5. The second writer commits and switches.
    crate::takeover::takeover_with_store(config(&t.path().join("b")), mem.clone()).unwrap();
    let (c, cc) = take_over_into(&mem, &t.path().join("c"));
    insert_on(&cc, 6..=7);
    assert_eq!(switch(&mem, &c, &cc), 5);
    insert_on(&cc, 8..=8);
    std::mem::forget(cc);
    std::mem::forget(c);
    // A third takeover, of generation 5 (floor 7), and its switch.
    let (d, cd) = take_over_into(&mem, &t.path().join("d"));
    insert_on(&cd, 9..=10);
    assert_eq!(switch(&mem, &d, &cd), 7);
    insert_on(&cd, 11..=11);
    let r = run(&packer(&t), mem.clone()).unwrap();
    packed(&r);
    assert_eq!(r.pack_generation, 7);
    assert_eq!(pack_rows(&*mem), (1..=10).collect::<Vec<_>>());
    assert_eq!(restore_rows(&mem), query_ids(&cd));
    std::mem::forget(cd);
    std::mem::forget(d);
}

/// The chain breaks when a link names a generation with no log that is not the head's, or a
/// generation that is not above its own.
#[test]
fn broken_chain_is_log_incomplete() {
    // The next link has no log.
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=2);
    switch(&mem, &w.db, &w.c);
    w.insert(3..=4);
    switch(&mem, &w.db, &w.c);
    w.insert(5..=5);
    for k in mem.list(&keys().log_prefix(2)).unwrap() {
        mem.delete(&k).unwrap();
    }
    let t = tempdir();
    assert_log_incomplete(&mem, &t, keys().log_prefix(2));
    w.crash();
    // The next link is not above the one before it.
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=2);
    switch(&mem, &w.db, &w.c);
    w.insert(3..=3);
    link(&*mem, 1, 1);
    let t = tempdir();
    assert_log_incomplete(&mem, &t, keys().log(1, last_seq(&*mem, 1)));
    w.crash();
}

/// A gap of two generations after a generation with no fence record is fine when the chain
/// names it.
#[test]
fn chain_names_a_gap_longer_than_the_fence_count() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=3);
    w.end_generation();
    w.crash();
    move_head(&*mem, 4);
    link(&*mem, 1, 4);
    let t = tempdir();
    let r = run(&packer(&t), mem.clone()).unwrap();
    packed(&r);
    assert_eq!(r.pack_generation, 4);
    assert_eq!(pack_rows(&*mem), vec![1, 2, 3]);
}

/// Old seal-start records name no next generation: a gap after a generation with no floor
/// that names it means the log is gone.
#[test]
fn missing_log_without_floor_is_an_error() {
    let mem = Arc::new(MemStore::new());
    let w = {
        let _legacy = crate::format::LegacySeal::on();
        writer(&mem)
    };
    w.insert(1..=2);
    w.end_generation();
    w.insert(3..=4);
    w.end_generation();
    w.insert(5..=5);
    for k in mem.list(&keys().log_prefix(2)).unwrap() {
        mem.delete(&k).unwrap();
    }
    let t = tempdir();
    assert_log_incomplete(&mem, &t, keys().log_prefix(2));
    w.crash();
}

/// A bucket with a first pack (pack generation 2) and one more ended generation.
fn two_generations(mem: &Arc<MemStore>, cfg: &Config) -> Writer {
    let w = writer(mem);
    w.insert(1..=3);
    w.end_generation();
    packed(&run(cfg, mem.clone()).unwrap());
    w.insert(4..=6);
    w.end_generation();
    w
}

#[test]
fn size_mismatch_moves_no_pointer_and_deletes_no_log() {
    let mem = Arc::new(MemStore::new());
    let t = tempdir();
    let cfg = packer(&t);
    let w = two_generations(&mem, &cfg);
    let pointer = mem.get(&keys().pack_pointer()).unwrap().unwrap();
    let logs = log_keys(&*mem);
    let hs = HookStore::new(mem.clone());
    hs.size_lie.store(true, SeqCst);
    let r = run(&cfg, hs.clone()).unwrap();
    let key = failed_at(&r, "verify size").unwrap();
    assert!(key.starts_with("t/pack/0000000003/"), "{key}");
    assert_eq!(mem.get(&keys().pack_pointer()).unwrap().unwrap(), pointer);
    assert_eq!(log_keys(&*mem), logs);
    assert!(packs(&*mem)
        .iter()
        .all(|k| !k.starts_with("t/pack/0000000003/")));
    w.crash();
}

#[test]
fn part_checksum_mismatch_deletes_new_pack_and_codes() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=3);
    w.end_generation();
    let hs = HookStore::new(mem.clone());
    hs.sha_lie.store(true, SeqCst);
    let t = tempdir();
    let r = run(&packer(&t), hs.clone()).unwrap();
    let key = failed_at(&r, "verify checksum").unwrap();
    assert!(hs.calls().contains(&format!("put_multipart {key}")));
    assert!(packs(&*mem).is_empty(), "{:?}", packs(&*mem));
    assert!(pointer_of(&*mem).is_none());
    assert!(!hs
        .calls()
        .iter()
        .any(|c| c.ends_with("pack-latest") && c.starts_with("put")));
    w.crash();
}

#[test]
fn missing_pack_before_pointer_write_is_failed() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=3);
    w.end_generation();
    let hs = HookStore::new(mem.clone());
    // The pack goes once its codes land, before the HEAD of step 7.
    hs.set_hook(|op, key, inner| {
        if op == "put" && key.ends_with(".codes") {
            let pack = key.trim_end_matches(".codes");
            inner.delete(pack).unwrap();
        }
        Reply::Pass
    });
    let t = tempdir();
    let r = run(&packer(&t), hs.clone()).unwrap();
    let key = failed_at(&r, "head").unwrap();
    assert_eq!(Some(key), r.pack_key);
    assert!(packs(&*mem).is_empty(), "the codes are deleted too");
    assert!(pointer_of(&*mem).is_none());
    w.crash();
}

#[test]
fn first_pointer_uses_if_none_match() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=3);
    w.end_generation();
    let hs = HookStore::new(mem.clone());
    let t = tempdir();
    let cfg = packer(&t);
    packed(&run(&cfg, hs.clone()).unwrap());
    let pointer_puts = |hs: &HookStore| -> Vec<String> {
        hs.calls()
            .into_iter()
            .filter(|c| c.starts_with("put") && c.ends_with(" t/pack-latest"))
            .collect()
    };
    assert_eq!(pointer_puts(&hs), ["put_if_absent t/pack-latest"]);
    w.insert(4..=4);
    w.end_generation();
    let hs = HookStore::new(mem.clone());
    packed(&run(&cfg, hs.clone()).unwrap());
    assert_eq!(pointer_puts(&hs), ["put_if_match t/pack-latest"]);
    w.crash();
}

/// Answer the first pointer write with `reply`, after `before` ran against the store.
fn on_first_pointer_write(
    hs: &HookStore,
    reply: Reply,
    before: impl FnOnce(&MemStore) + Send + 'static,
) {
    let mut before = Some(before);
    hs.set_hook(move |op, key, inner| {
        if op.starts_with("put_if") && key.ends_with("pack-latest") {
            if let Some(f) = before.take() {
                f(inner);
                return reply;
            }
        }
        Reply::Pass
    });
}

fn pointer_writes(hs: &HookStore) -> usize {
    hs.calls()
        .iter()
        .filter(|c| c.starts_with("put_if") && c.ends_with("pack-latest"))
        .count()
}

#[test]
fn pointer_412_names_this_run_is_packed() {
    let mem = Arc::new(MemStore::new());
    let t = tempdir();
    let cfg = packer(&t);
    let w = two_generations(&mem, &cfg);
    let hs = HookStore::new(mem.clone());
    on_first_pointer_write(&hs, Reply::StoreThen412, |_| {});
    let r = run(&cfg, hs.clone()).unwrap();
    packed(&r);
    assert_eq!(pointer_writes(&hs), 1);
    let p = pointer_of(&*mem).unwrap();
    assert_eq!(Some(p.pack_key.clone()), r.pack_key);
    assert!(mem.get(&p.pack_key).unwrap().is_some() && mem.get(&p.codes_key).unwrap().is_some());
    w.crash();
}

/// A pointer of this database (the head's `database_id`) naming another run's pack of
/// `generation`, without the pack.
fn other_pointer(store: &dyn ObjectStore, generation: u64) -> Vec<u8> {
    let k = keys();
    k.encode_pointer(&PackPointer {
        format: POINTER_FORMAT,
        write_id: "w1".into(),
        pack_key: k.pack(generation, "other"),
        codes_key: k.pack_codes(generation, "other"),
        pack_generation: generation,
        size: 4096,
        page_size: 4096,
        part_size: PART_SIZE as u64,
        block_size: BLOCK_SIZE,
        database_id: head_of(store).database_id,
    })
}

#[test]
fn pointer_412_names_higher_generation_is_superseded_and_deletes_own_pack() {
    let mem = Arc::new(MemStore::new());
    let t = tempdir();
    let cfg = packer(&t);
    let w = two_generations(&mem, &cfg);
    let hs = HookStore::new(mem.clone());
    on_first_pointer_write(&hs, Reply::Pass, |inner| {
        inner
            .put(&keys().pack_pointer(), &other_pointer(inner, 9))
            .unwrap();
    });
    let r = run(&cfg, hs.clone()).unwrap();
    assert_eq!(r.outcome, PackOutcome::Superseded);
    assert_eq!(pointer_of(&*mem).unwrap().pack_generation, 9);
    let own = r.pack_key.unwrap();
    assert!(mem.get(&own).unwrap().is_none());
    assert!(mem.get(&format!("{own}.codes")).unwrap().is_none());
    w.crash();
}

#[test]
fn pointer_412_still_old_retries() {
    let mem = Arc::new(MemStore::new());
    let t = tempdir();
    let cfg = packer(&t);
    let w = two_generations(&mem, &cfg);
    let hs = HookStore::new(mem.clone());
    on_first_pointer_write(&hs, Reply::Fail412, |_| {});
    let r = run(&cfg, hs.clone()).unwrap();
    packed(&r);
    assert_eq!(pointer_writes(&hs), 2);
    assert_eq!(Some(pointer_of(&*mem).unwrap().pack_key), r.pack_key);
    // An unknown result that did not land is the same: a retry, not `superseded`.
    w.insert(7..=7);
    w.end_generation();
    let hs = HookStore::new(mem.clone());
    on_first_pointer_write(&hs, Reply::Transient, |_| {});
    let r = run(&cfg, hs.clone()).unwrap();
    packed(&r);
    assert_eq!(Some(pointer_of(&*mem).unwrap().pack_key), r.pack_key);
    w.crash();
}

#[test]
fn pointer_412_lower_generation_retries_with_new_etag() {
    let mem = Arc::new(MemStore::new());
    let t = tempdir();
    let cfg = packer(&t);
    let w = two_generations(&mem, &cfg);
    let hs = HookStore::new(mem.clone());
    // A slower run moves the pointer to another pack of generation 2.
    on_first_pointer_write(&hs, Reply::Pass, |inner| {
        inner
            .put(&keys().pack_pointer(), &other_pointer(inner, 2))
            .unwrap();
    });
    let r = run(&cfg, hs.clone()).unwrap();
    packed(&r);
    assert_eq!(pointer_writes(&hs), 2);
    let p = pointer_of(&*mem).unwrap();
    assert_eq!(Some(p.pack_key), r.pack_key);
    assert_eq!(p.pack_generation, 3);
    w.crash();
}

#[test]
fn unknown_result_that_landed_keeps_pack() {
    let mem = Arc::new(MemStore::new());
    let t = tempdir();
    let cfg = packer(&t);
    let w = two_generations(&mem, &cfg);
    let hs = HookStore::new(mem.clone());
    on_first_pointer_write(&hs, Reply::StoreThenTransient, |_| {});
    let r = run(&cfg, hs.clone()).unwrap();
    packed(&r);
    let p = pointer_of(&*mem).unwrap();
    assert_eq!(Some(p.pack_key.clone()), r.pack_key);
    assert!(mem.get(&p.pack_key).unwrap().is_some() && mem.get(&p.codes_key).unwrap().is_some());
    w.crash();
}

#[test]
fn run_past_half_orphan_grace_is_failed_with_no_pointer_write() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=3);
    w.end_generation();
    let t = tempdir();
    let cfg = packer(&t);
    let (ms, clock) = fake_clock();
    let hs = HookStore::new(mem.clone());
    let half = cfg.pack_orphan_grace / 2;
    // The upload takes half the orphan grace.
    hs.set_hook(move |op, _, _| {
        if op == "put_multipart" {
            ms.fetch_add(half.as_millis() as u64, SeqCst);
        }
        Reply::Pass
    });
    let r = run_clocked(&cfg, hs.clone(), clock).unwrap();
    let key = failed_at(&r, "time limit").unwrap();
    assert_eq!(Some(key), r.pack_key);
    assert_eq!(pointer_writes(&hs), 0);
    assert!(packs(&*mem).is_empty() && pointer_of(&*mem).is_none());
    // Just inside the limit, the run moves the pointer.
    let (ms, clock) = fake_clock();
    let hs = HookStore::new(mem.clone());
    hs.set_hook(move |op, _, _| {
        if op == "put_multipart" {
            ms.fetch_add(half.as_millis() as u64 - 1, SeqCst);
        }
        Reply::Pass
    });
    packed(&run_clocked(&cfg, hs, clock).unwrap());
    w.crash();
}

#[test]
fn two_runs_at_once_leave_one_newest_pack() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=3);
    w.end_generation();
    let hs = HookStore::new(mem.clone());
    // Both runs upload before either moves the pointer.
    *hs.barrier.lock().unwrap() = Some(Arc::new(Barrier::new(2)));
    let t = tempdir();
    // Each run has its own directory: a run checks that its own working directory is gone.
    let cfgs = [config(&t.path().join("a")), config(&t.path().join("b"))];
    let outcomes: Vec<PackOutcome> = std::thread::scope(|s| {
        let runs: Vec<_> = cfgs
            .iter()
            .map(|cfg| s.spawn(|| run(cfg, hs.clone()).unwrap().outcome))
            .collect();
        runs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut sorted = outcomes.clone();
    sorted.sort_by_key(|o| format!("{o:?}"));
    assert_eq!(sorted, [PackOutcome::Packed, PackOutcome::Superseded]);
    let p = pointer_of(&*mem).unwrap();
    assert_eq!(packs(&*mem), [p.pack_key.clone(), p.codes_key.clone()]);
    assert_eq!(pack_rows(&*mem), vec![1, 2, 3]);
    w.crash();
}

#[test]
fn run_never_deletes_the_named_pack() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=3);
    w.end_generation();
    let t = tempdir();
    let mut cfg = packer(&t);
    cfg.retry_deadline = Duration::from_millis(200);
    let hs = HookStore::new(mem.clone());
    // The pointer write lands but its reply is lost, and the pointer cannot be read after.
    let mut landed = false;
    hs.set_hook(move |op, key, _| {
        if !key.ends_with("pack-latest") {
            return Reply::Pass;
        }
        match op {
            "put_if_absent" if !landed => {
                landed = true;
                Reply::StoreThenTransient
            }
            "get" if landed => Reply::Transient,
            _ => Reply::Pass,
        }
    });
    let r = run(&cfg, hs.clone()).unwrap();
    failed_at(&r, "pointer read");
    let p = pointer_of(&*mem).unwrap();
    assert_eq!(Some(p.pack_key.clone()), r.pack_key);
    assert!(mem.get(&p.pack_key).unwrap().is_some() && mem.get(&p.codes_key).unwrap().is_some());
    assert!(
        !hs.calls().iter().any(|c| c.starts_with("delete")),
        "{:?}",
        hs.calls()
    );
    w.crash();
}

/// The page size of a downloaded pack must match its pointer's (the database header's).
#[test]
fn pointer_page_size_must_match_the_pack() {
    let mem = Arc::new(MemStore::new());
    let t = tempdir();
    let cfg = packer(&t);
    let w = two_generations(&mem, &cfg);
    let mut p = pointer_of(&*mem).unwrap();
    p.page_size = 8192;
    mem.put(&keys().pack_pointer(), &keys().encode_pointer(&p))
        .unwrap();
    match run(&cfg, mem.clone()) {
        Err(Error::Corrupt(m)) => assert!(m.contains("page_size 8192"), "{m}"),
        r => panic!("expected Corrupt, got {r:?}"),
    }
    w.crash();
}

/// A log without a fence record (the rule of logs from before stage 3): another writer's run
/// of records, then the switcher's. A restore of that generation read it under the head of
/// then, which named the switcher; the current head names a later writer.
#[test]
fn replay_of_an_ended_generation_uses_the_writer_of_its_terminal_record() {
    let ext = |w: &str| LogRecord::Extents {
        file: crate::format::LogFile::Wal,
        writer_id: w.into(),
        syncs: vec![vec![]],
        next_generation: None,
    };
    let records = [ext("c"), ext("a"), seal_start("a", Some(3))];
    let now = Head {
        format: crate::format::FORMAT,
        generation: 5,
        manifest: None,
        writer_id: "c".into(),
        epoch: 4,
        journal_mode: JournalMode::Wal,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
        database_id: None,
    };
    let then = Head {
        generation: 2,
        ..now.clone()
    };
    assert!(matches!(replay(&records, &then), Err(Error::Fenced(_))));
    replay_ended(&records, &now, 2).unwrap();
}

/// `discard` keeps the pack the pointer names, and its codes.
#[test]
fn discard_keeps_the_pack_the_pointer_names() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=3);
    w.end_generation();
    let t = tempdir();
    let cfg = packer(&t);
    packed(&run(&cfg, mem.clone()).unwrap());
    let p = pointer_of(&*mem).unwrap();
    let hs = HookStore::new(mem.clone());
    let (_, clock) = fake_clock();
    let r = Run {
        store: &*hs,
        keys: keys().adopting(&*mem),
        retry: cfg.engine(crate::io::IoMode::Writer).retry,
        enc: None,
        clock,
        limit: Duration::from_secs(1),
        run_id: "x".into(),
        work: t.path().join("w"),
        steps: Vec::new(),
        step_start: Instant::now(),
    };
    r.discard(&p.pack_key, &p.codes_key);
    assert!(mem.get(&p.pack_key).unwrap().is_some() && mem.get(&p.codes_key).unwrap().is_some());
    assert!(!hs.calls().iter().any(|c| c.starts_with("delete")));
    // A pack the pointer does not name goes, with its codes.
    mem.put(&keys().pack(9, "y"), b"p").unwrap();
    mem.put(&keys().pack_codes(9, "y"), b"c").unwrap();
    r.discard(&keys().pack(9, "y"), &keys().pack_codes(9, "y"));
    assert!(mem.get(&keys().pack(9, "y")).unwrap().is_none());
    assert!(mem.get(&keys().pack_codes(9, "y")).unwrap().is_none());
    w.crash();
}

/// A real format 5 writer switches generations itself (no record is rewritten by the test), and
/// `pack_with_store` packs them: the pack equals the writer's database at the end of the last
/// switched generation, and the pack plus the head's generation equals the writer's database.
#[test]
fn pack_run_on_a_real_writer_bucket_equals_restore() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    assert_eq!(head_of(&*mem).format, crate::format::FORMAT_V5);
    w.insert(1..=10);
    w.end_generation();
    w.c.execute("UPDATE t SET v = 'x' WHERE id < 4", &[])
        .unwrap();
    w.insert(11..=50);
    w.end_generation();
    w.c.execute("DELETE FROM t WHERE id > 45", &[]).unwrap();
    w.end_generation();
    let at_end = w.ids();
    w.insert(100..=105);
    for g in 1..=3 {
        match record_at(&*mem, g, last_seq(&*mem, g)) {
            LogRecord::Extents {
                syncs,
                next_generation,
                ..
            } => assert_eq!((syncs.len(), next_generation), (0, Some(g + 1))),
            r => panic!("log/{g}: {r:?}"),
        }
    }
    let t = tempdir();
    let cfg = packer(&t);
    let r = pack_with_store(&cfg, mem.clone()).unwrap();
    packed(&r);
    assert_eq!(r.pack_generation, 4);
    assert_eq!(pack_rows(&*mem), at_end);
    let head = head_of(&*mem);
    assert_eq!(head.generation, 4);
    let images = crate::restore::generation_image(
        &*mem,
        &keys(),
        &head,
        &cfg.engine(crate::io::IoMode::Writer),
    )
    .unwrap();
    let (_, body) = pack_bytes(&*mem, &keys());
    assert_eq!(rows_of(&body, Some(&images.wal), None), w.ids());
    w.crash();
}

/// Make `path` look last changed `age` ago.
fn age_dir(path: &Path, age: Duration) {
    let when = std::time::SystemTime::now() - age;
    File::open(path).unwrap().set_modified(when).unwrap();
}

/// A crashed pack run leaves `pack-work-<run id>` behind. A directory older than half the
/// orphan grace has no live run (a run gives up at half the orphan grace): a pack run's start
/// and a writer's open remove it, and keep a newer one and every other entry. A newer one does
/// not stop a takeover or a restore into the directory.
#[test]
fn stale_pack_work_directory_is_removed() {
    let mem = Arc::new(MemStore::new());
    let w = writer(&mem);
    w.insert(1..=3);
    w.end_generation();
    let half = config(Path::new("/x")).pack_orphan_grace / 2;
    let stale = |dir: &Path, name: &str| {
        let p = dir.join(name);
        fs::create_dir_all(&p).unwrap();
        fs::write(p.join(DB_FILE), b"copy").unwrap();
        age_dir(&p, half + Duration::from_secs(60));
        p
    };
    let fresh = |dir: &Path, name: &str| {
        let p = dir.join(name);
        fs::create_dir_all(&p).unwrap();
        p
    };

    // The rule alone.
    let t = tempdir();
    let old = stale(t.path(), "pack-work-old");
    let new = fresh(t.path(), "pack-work-new");
    let other = stale(t.path(), "other");
    remove_stale_work_dirs(t.path(), half).unwrap();
    assert!(!old.exists() && new.exists() && other.exists());
    // A missing directory has nothing to remove.
    remove_stale_work_dirs(&t.path().join("missing"), half).unwrap();

    // A pack run's start.
    let t = tempdir();
    let cfg = packer(&t);
    let old = stale(&cfg.dir, "pack-work-old");
    let new = fresh(&cfg.dir, "pack-work-new");
    packed(&run_dirty(&cfg, mem.clone()));
    assert!(!old.exists() && new.exists());

    // A takeover into a directory that holds only a newer one, then its open.
    let t = tempdir();
    let cfg = config(&t.path().join("b"));
    let new = fresh(&cfg.dir, "pack-work-new");
    crate::takeover_with_store(cfg.clone(), mem.clone()).unwrap();
    let old = stale(&cfg.dir, "pack-work-old");
    let db = open_with_store(cfg.clone(), mem.clone()).unwrap();
    assert!(!old.exists() && new.exists());
    assert_eq!(query_ids(&db.connect().unwrap()), vec![1, 2, 3]);
    drop(db);

    // A writer's open that restores into a directory that holds only a newer one.
    let t = tempdir();
    let cfg = config(&t.path().join("c"));
    fresh(&cfg.dir, "pack-work-new");
    let db = open_with_store(cfg, mem.clone()).unwrap();
    assert_eq!(query_ids(&db.connect().unwrap()), vec![1, 2, 3]);
    drop(db);
    drop(w);
}

/// A pack run that does not check for left-over `pack-work-*` directories (other than its own).
fn run_dirty(cfg: &Config, store: Arc<dyn ObjectStore>) -> PackReport {
    let (_, clock) = fake_clock();
    pack_run(cfg, store, clock).unwrap()
}
