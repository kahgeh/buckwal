//! Shared helpers for the replica-recon probes: a writer on a MemStore, a hand-written decoder
//! for the writer's log records (the library keeps its decoder crate-private), and WAL image
//! helpers.
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use buckwal::store::{MemStore, ObjectStore};
use buckwal::{Batching, Config};

pub const PREFIX: &str = "db";

#[derive(Debug, Clone)]
pub enum Op {
    Write { off: u64, bytes: Vec<u8> },
    Truncate { len: u64 },
}

#[derive(Debug, Clone)]
pub enum Rec {
    Extents { syncs: Vec<Vec<Op>> },
    Fence { epoch: u64 },
}

pub struct LogObj {
    pub gen: u64,
    pub seq: u64,
    pub rec: Rec,
}

/// A fresh, empty directory under the probe's work dir (never removed by the probes).
pub fn fresh_dir(name: &str) -> PathBuf {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("work");
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let d = base.join(format!("{name}-{}-{t}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

pub fn config(dir: &Path, b: Batching) -> Config {
    let mut c = Config::new(dir, "http://127.0.0.1:1", "bucket", "key", "secret");
    c.prefix = PREFIX.into();
    c.retry_deadline = Duration::from_secs(2);
    c.request_timeout = Duration::from_millis(500);
    c.lease = Duration::from_secs(5);
    c.batching = b;
    c
}

pub fn mem() -> Arc<MemStore> {
    Arc::new(MemStore::new())
}

struct R<'a>(&'a [u8]);
impl<'a> R<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        a
    }
    fn u8(&mut self) -> u8 {
        self.take(1)[0]
    }
    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.take(4).try_into().unwrap())
    }
    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take(8).try_into().unwrap())
    }
}

/// Decode a log object (layout: MAGIC 8, kind, id len, id, body, sha256 trailer 32).
pub fn decode(b: &[u8]) -> Rec {
    let body = &b[..b.len() - 32];
    assert_eq!(&body[..8], b"TDLOG\0\0\x01");
    let mut r = R(&body[8..]);
    let kind = r.u8();
    let n = r.u8() as usize;
    r.take(n);
    match kind {
        2 => {
            assert_eq!(r.u32(), 0);
            Rec::Fence { epoch: r.u64() }
        }
        1 => {
            let mut syncs = Vec::new();
            for _ in 0..r.u32() {
                let mut ops = Vec::new();
                for _ in 0..r.u32() {
                    match r.u8() {
                        1 => {
                            let off = r.u64();
                            let len = r.u32() as usize;
                            ops.push(Op::Write { off, bytes: r.take(len).to_vec() });
                        }
                        2 => ops.push(Op::Truncate { len: r.u64() }),
                        t => panic!("op tag {t}"),
                    }
                }
                syncs.push(ops);
            }
            Rec::Extents { syncs }
        }
        k => panic!("kind {k}"),
    }
}

/// Every log object under `log/`, in (generation, seq) order.
pub fn read_log(store: &dyn ObjectStore) -> Vec<LogObj> {
    let mut keys = store.list(&format!("{PREFIX}/log/")).unwrap();
    keys.sort();
    keys.iter()
        .map(|k| {
            let rest = k.strip_prefix(&format!("{PREFIX}/log/")).unwrap();
            let (g, s) = rest.split_once('/').unwrap();
            let o = store.get(k).unwrap().unwrap();
            LogObj { gen: g.parse().unwrap(), seq: s.parse().unwrap(), rec: decode(&o.body) }
        })
        .collect()
}

/// File semantics: a write past the end zero-fills, truncate resizes.
pub fn apply_ops(buf: &mut Vec<u8>, ops: &[Op]) {
    for op in ops {
        match op {
            Op::Write { off, bytes } => {
                let off = *off as usize;
                let end = off + bytes.len();
                if buf.len() < end {
                    buf.resize(end, 0);
                }
                buf[off..end].copy_from_slice(bytes);
            }
            Op::Truncate { len } => buf.resize(*len as usize, 0),
        }
    }
}

pub const WAL_HDR: usize = 32;
pub const FRAME_HDR: usize = 24;

/// Page size from the WAL header (bytes 8..12, big endian), when the header is there.
pub fn wal_page_size(img: &[u8]) -> Option<usize> {
    if img.len() < WAL_HDR {
        return None;
    }
    let ps = u32::from_be_bytes(img[8..12].try_into().unwrap()) as usize;
    (ps.is_power_of_two() && ps >= 512).then_some(ps)
}

/// Whole frames in an image: (page_no, db_size, bytes).
pub fn whole_frames(img: &[u8]) -> Vec<(u32, u32, &[u8])> {
    let Some(ps) = wal_page_size(img) else { return vec![] };
    let fs = FRAME_HDR + ps;
    img[WAL_HDR..]
        .chunks_exact(fs)
        .map(|f| {
            (
                u32::from_be_bytes(f[0..4].try_into().unwrap()),
                u32::from_be_bytes(f[4..8].try_into().unwrap()),
                f,
            )
        })
        .collect()
}

// ---- turso_core side ----
use turso_core::{CheckpointMode as CoreCkpt, Connection as CoreConn, Database as CoreDb, PlatformIO, SqliteDialect, Value as CoreValue, Numeric};

/// Open a plain-file database with turso_core directly (no encryption).
pub fn open_core(db_path: &Path) -> (Arc<CoreDb>, Arc<CoreConn>) {
    let io: Arc<dyn turso_core::IO> = Arc::new(PlatformIO::new().unwrap());
    let db = CoreDb::open_file(io, db_path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
    let c = db.connect().unwrap();
    (db, c)
}

/// `SELECT count(*) FROM t`.
pub fn count(c: &Arc<CoreConn>) -> i64 {
    let mut st = c.query("SELECT count(*) FROM t").unwrap().unwrap();
    let rows = st.run_collect_rows().unwrap();
    match &rows[0][0] {
        CoreValue::Numeric(Numeric::Integer(i)) => *i,
        v => panic!("{v:?}"),
    }
}

pub fn integrity(c: &Arc<CoreConn>) -> String {
    let mut st = c.query("PRAGMA integrity_check").unwrap().unwrap();
    format!("{:?}", st.run_collect_rows().unwrap())
}

pub fn ckpt_truncate(c: &Arc<CoreConn>) -> turso_core::Result<turso_core::CheckpointResult> {
    c.checkpoint(CoreCkpt::Truncate { upper_bound_inclusive: None })
}

/// Rebuilds a generation's WAL image from its log, one sync at a time, and yields the whole
/// frames each sync appended. Header writes and truncates are applied to the image only.
#[derive(Default)]
pub struct Rebuild {
    pub img: Vec<u8>,
}

impl Rebuild {
    pub fn reset(&mut self) {
        self.img.clear();
    }
    /// Apply one sync, returning the frames it appended (page_no, db_size, full frame bytes).
    pub fn feed(&mut self, ops: &[Op]) -> Vec<(u32, u32, Vec<u8>)> {
        let before = whole_frames(&self.img).len();
        apply_ops(&mut self.img, ops);
        let all = whole_frames(&self.img);
        all[before..].iter().map(|f| (f.0, f.1, f.2.to_vec())).collect()
    }
}

/// A writer that has made some rows, checkpointed (so its `app.db` is whole and its WAL
/// empty) and handed a copy of that `app.db` to a replica directory.
pub struct Baseline {
    pub store: Arc<MemStore>,
    pub wdb: buckwal::Database,
    pub w: buckwal::Connection,
    pub replica_db_path: PathBuf,
    pub gen: u64,
}

pub fn baseline(name: &str, batching: Batching) -> Baseline {
    baseline_with(name, batching, None)
}

/// Same, with the writer's `encryption_key_file` set (aegis256 default cipher).
pub fn baseline_with(name: &str, batching: Batching, key_file: Option<PathBuf>) -> Baseline {
    let store = mem();
    let root = fresh_dir(name);
    let wdir = root.join("writer");
    let rdir = root.join("replica");
    std::fs::create_dir_all(&rdir).unwrap();
    let mut cfg = config(&wdir, batching);
    cfg.encryption_key_file = key_file;
    let wdb = buckwal::open_with_store(cfg, store.clone()).unwrap();
    let w = wdb.connect().unwrap();
    w.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    for i in 1..=10 {
        w.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(100))", &[buckwal::Value::from_i64(i)]).unwrap();
    }
    w.checkpoint(buckwal::CheckpointMode::Truncate).unwrap();
    let gen = wdb.generation();
    std::fs::copy(wdir.join("app.db"), rdir.join("app.db")).unwrap();
    Baseline { store, wdb, w, replica_db_path: rdir.join("app.db"), gen }
}

/// Every Extents sync of generation `gen`, in log order, as (seq, ops); fences as (seq, None).
pub fn gen_syncs(store: &dyn ObjectStore, gen: u64) -> Vec<(u64, Option<Vec<Op>>)> {
    let mut out = Vec::new();
    for o in read_log(store).into_iter().filter(|o| o.gen == gen) {
        match o.rec {
            Rec::Fence { .. } => out.push((o.seq, None)),
            Rec::Extents { syncs } => {
                for s in syncs {
                    out.push((o.seq, Some(s)));
                }
            }
        }
    }
    out
}

/// TRUNCATE checkpoint that retries `Busy` (a reader is mid-transaction), up to 5 s. Returns the
/// number of Busy answers seen.
pub fn ckpt_retry(c: &Arc<CoreConn>) -> u32 {
    let start = std::time::Instant::now();
    let mut busy = 0;
    loop {
        match ckpt_truncate(c) {
            Ok(_) => return busy,
            Err(turso_core::LimboError::Busy) => {
                busy += 1;
                assert!(start.elapsed() < Duration::from_secs(5), "checkpoint stayed Busy for 5 s");
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(e) => panic!("checkpoint: {e:?}"),
        }
    }
}

/// Every `id` in table t, sorted.
pub fn ids(c: &Arc<CoreConn>) -> Vec<i64> {
    let mut st = c.query("SELECT id FROM t ORDER BY id").unwrap().unwrap();
    st.run_collect_rows()
        .unwrap()
        .iter()
        .map(|r| match &r[0] {
            CoreValue::Numeric(Numeric::Integer(i)) => *i,
            v => panic!("{v:?}"),
        })
        .collect()
}

/// Open an encrypted (aegis256) plain-file database with turso_core directly, as the library's
/// engine does (engine.rs: db option, cipher and hex key at open, key again on the connection).
pub fn open_core_enc(db_path: &Path, key: [u8; 32]) -> (Arc<CoreDb>, Arc<CoreConn>) {
    use turso_core::{DatabaseOpts, EncryptionKey, EncryptionOpts, OpenOptions};
    let io: Arc<dyn turso_core::IO> = Arc::new(PlatformIO::new().unwrap());
    let hexkey: String = key.iter().map(|b| format!("{b:02x}")).collect();
    let opts = OpenOptions::new(Arc::new(SqliteDialect))
        .db_opts(DatabaseOpts::new().with_encryption(true))
        .encryption(EncryptionOpts { cipher: "aegis256".to_string(), hexkey });
    let db = CoreDb::open(io, db_path.to_str().unwrap(), opts).unwrap();
    let c = db.connect_with_encryption(Some(EncryptionKey::new_256(key))).unwrap();
    (db, c)
}

/// A hex key file (64 hex digits) for the writer, returning its path.
pub fn write_key_file(dir: &Path, key: [u8; 32]) -> PathBuf {
    let p = dir.join("db.key");
    let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
    std::fs::write(&p, hex).unwrap();
    p
}
