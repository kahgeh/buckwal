//! Inserts committed WAL frames into the replica's local database through turso's frame API
//! (`wal_insert_begin`, `wal_insert_frame`, `wal_insert_end`).
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use turso_core::{CheckpointMode, CipherMode, Connection, EncryptionContext, LimboError};

use crate::{
    encryption::Encryption,
    error::{Error, Result},
    replica::image::Frame,
};

/// The start of the message of a `Conflict` (the follower stops and re-restores on it).
pub(crate) const CONFLICT_PREFIX: &str = "replica conflict: ";

/// The message of a conflict at `frame` of `generation`; `sync::conflict_frame` parses it.
pub(crate) fn conflict_message(generation: u64, frame: u64) -> String {
    format!("{CONFLICT_PREFIX}generation {generation} frame {frame}")
}

/// True for the error `apply_commit` returns when a frame already in the local WAL differs.
pub(crate) fn is_conflict(e: &Error) -> bool {
    matches!(e, Error::Corrupt(m) if m.starts_with(CONFLICT_PREFIX))
}

/// Decrypts one page body given its page number.
type DecryptFn = Box<dyn Fn(u32, &[u8]) -> Result<Vec<u8>> + Send + Sync>;

pub(crate) struct PageDecrypt(DecryptFn);

impl PageDecrypt {
    pub(crate) fn new(f: impl Fn(u32, &[u8]) -> Result<Vec<u8>> + Send + Sync + 'static) -> Self {
        PageDecrypt(Box::new(f))
    }

    /// With the library's key and cipher; `page_size` comes from the WAL header.
    pub(crate) fn from_encryption(enc: &Encryption, page_size: usize) -> Result<Self> {
        let cipher: CipherMode = enc.cipher;
        let ctx = EncryptionContext::new(cipher, &enc.key, page_size)
            .map_err(|e| Error::Encryption(format!("page decryption setup: {e}")))?;
        Ok(PageDecrypt::new(move |page_no, body| {
            ctx.decrypt_page(body, page_no as usize)
                .map_err(|e| Error::Encryption(format!("decrypting page {page_no}: {e}")))
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyOutcome {
    /// The transaction is committed; `frames` counts frames appended or matched (frames at
    /// local number 0 or below are skipped and not counted).
    Applied {
        frames: u64,
    },
    Gap,
    /// The frames do not end on a commit frame. Nothing was inserted.
    Incomplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointResult {
    Done,
    Busy,
}

pub(crate) struct Applier {
    conn: Arc<Connection>,
    decrypt: Option<PageDecrypt>,
    /// Only for the conflict message.
    generation: AtomicU64,
}

fn engine(e: LimboError) -> Error {
    Error::Engine(e.to_string())
}

impl Applier {
    pub(crate) fn new(conn: Arc<Connection>, decrypt: Option<PageDecrypt>) -> Applier {
        Applier {
            conn,
            decrypt,
            generation: AtomicU64::new(0),
        }
    }

    pub(crate) fn set_generation(&self, generation: u64) {
        self.generation.store(generation, Ordering::Relaxed);
    }

    fn last_frame(&self) -> Result<u64> {
        Ok(self.conn.wal_state().map_err(engine)?.max_frame)
    }

    /// The local WAL's last frame number.
    pub(crate) fn local_last_frame(&self) -> Result<u64> {
        self.last_frame()
    }

    pub(crate) fn local_wal_empty(&self) -> Result<bool> {
        Ok(self.last_frame()? == 0)
    }

    /// Insert exactly one committed transaction: `frames` must end on a commit frame (header
    /// `db_size != 0`) and hold no commit frame before the last. Local number is
    /// `frame.no + offset`. Frames that do not end on a commit frame give `Incomplete` and
    /// nothing is inserted; a commit frame before the last is an error (the caller passes one
    /// transaction at a time). On any failure the session is rolled back.
    pub(crate) fn apply_commit(&self, frames: &[Frame], offset: i64) -> Result<ApplyOutcome> {
        let is_commit = |f: &Frame| f.header[4..8] != [0; 4];
        match frames.split_last() {
            Some((last, rest)) if is_commit(last) => {
                if rest.iter().any(is_commit) {
                    return Err(Error::Corrupt(
                        "replica apply: a commit frame before the last frame".into(),
                    ));
                }
            }
            _ => return Ok(ApplyOutcome::Incomplete),
        }
        let mut last = self.last_frame()?;
        self.conn.wal_insert_begin().map_err(engine)?;
        match self.insert_all(frames, offset, &mut last) {
            Ok(Some(applied)) => match self.conn.wal_insert_end(true) {
                Ok(()) => Ok(ApplyOutcome::Applied { frames: applied }),
                Err(e) => {
                    let _ = self.conn.wal_insert_end(false);
                    Err(engine(e))
                }
            },
            Ok(None) => {
                self.conn.wal_insert_end(false).map_err(engine)?;
                Ok(ApplyOutcome::Gap)
            }
            Err(e) => {
                let _ = self.conn.wal_insert_end(false);
                Err(e)
            }
        }
    }

    /// `Some(frames appended or matched)`, or `None` on a gap.
    fn insert_all(&self, frames: &[Frame], offset: i64, last: &mut u64) -> Result<Option<u64>> {
        let mut applied = 0;
        for f in frames {
            let n = f.no as i64 + offset;
            if n <= 0 {
                continue;
            }
            let n = n as u64;
            if n > *last + 1 {
                return Ok(None);
            }
            let mut bytes = Vec::with_capacity(f.header.len() + f.page.len());
            bytes.extend_from_slice(&f.header);
            match &self.decrypt {
                Some(d) => {
                    let page_no =
                        u32::from_be_bytes([f.header[0], f.header[1], f.header[2], f.header[3]]);
                    bytes.extend_from_slice(&(d.0)(page_no, &f.page)?);
                }
                None => bytes.extend_from_slice(&f.page),
            }
            match self.conn.wal_insert_frame(n, &bytes) {
                Ok(_) => {}
                Err(LimboError::Conflict(_)) => {
                    return Err(Error::Corrupt(conflict_message(
                        self.generation.load(Ordering::Relaxed),
                        f.no,
                    )))
                }
                Err(e) => return Err(engine(e)),
            }
            if n > *last {
                *last = n;
            }
            applied += 1;
        }
        Ok(Some(applied))
    }

    /// A TRUNCATE checkpoint of the local database. `Busy` when a reader holds it back.
    pub(crate) fn checkpoint_truncate(&self) -> Result<CheckpointResult> {
        match self.conn.checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        }) {
            Ok(_) => Ok(CheckpointResult::Done),
            Err(LimboError::Busy | LimboError::BusySnapshot) => Ok(CheckpointResult::Busy),
            Err(e) => Err(engine(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use turso_core::{
        Database, DatabaseOpts, EncryptionKey, EncryptionOpts, OpenOptions, PlatformIO,
        SqliteDialect, Value, IO,
    };

    const KEY: [u8; 32] = [0x5a; 32];
    const WAL_HEADER: usize = 32;

    fn open(path: &Path, key: bool) -> (Arc<Database>, Arc<Connection>) {
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
        let p = path.to_str().unwrap();
        if !key {
            let db = Database::open_file(io, p, Arc::new(SqliteDialect)).unwrap();
            let c = db.connect().unwrap();
            return (db, c);
        }
        let hexkey: String = KEY.iter().map(|b| format!("{b:02x}")).collect();
        let opts = OpenOptions::new(Arc::new(SqliteDialect))
            .db_opts(DatabaseOpts::new().with_encryption(true))
            .encryption(EncryptionOpts {
                cipher: "aegis256".to_string(),
                hexkey,
            });
        let db = Database::open(io, p, opts).unwrap();
        let c = db
            .connect_with_encryption(Some(EncryptionKey::new_256(KEY)))
            .unwrap();
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

    /// The whole frames of a `-wal` file.
    fn read_frames(wal: &Path) -> Vec<Frame> {
        let b = std::fs::read(wal).unwrap();
        let ps = u32::from_be_bytes(b[8..12].try_into().unwrap()) as usize;
        let fl = 24 + ps;
        (0..(b.len() - WAL_HEADER) / fl)
            .map(|i| {
                let s = WAL_HEADER + i * fl;
                Frame {
                    no: i as u64 + 1,
                    header: b[s..s + 24].try_into().unwrap(),
                    page: b[s + 24..s + fl].to_vec(),
                }
            })
            .collect()
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        _wdb: Arc<Database>,
        writer: Arc<Connection>,
        wal: std::path::PathBuf,
        _rdb: Arc<Database>,
        reader: Arc<Connection>,
        applier: Applier,
    }

    /// A writer with table `t` checkpointed into `app.db` (copied to the replica), WAL empty.
    fn fixture(encrypted: bool) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let (w, r) = (dir.path().join("w"), dir.path().join("r"));
        std::fs::create_dir_all(&w).unwrap();
        std::fs::create_dir_all(&r).unwrap();
        let (wdb, writer) = open(&w.join("app.db"), encrypted);
        sql(&writer, "CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)");
        sql(&writer, "INSERT INTO t VALUES (1, zeroblob(10))");
        writer
            .checkpoint(CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            })
            .unwrap();
        std::fs::copy(w.join("app.db"), r.join("app.db")).unwrap();
        let (rdb, reader) = open(&r.join("app.db"), encrypted);
        let decrypt = encrypted.then(|| {
            let enc = Encryption::new(CipherMode::Aegis256, EncryptionKey::new_256(KEY));
            PageDecrypt::from_encryption(&enc, 4096).unwrap()
        });
        let applier = Applier::new(reader.clone(), decrypt);
        Fixture {
            wal: w.join("app.db-wal"),
            _dir: dir,
            _wdb: wdb,
            writer,
            _rdb: rdb,
            reader,
            applier,
        }
    }

    fn commit_frames(f: &Fixture) -> Vec<Frame> {
        let frames = read_frames(&f.wal);
        assert!(!frames.is_empty());
        assert_ne!(frames.last().unwrap().header[4..8], [0; 4]);
        frames
    }

    /// Split frames into transactions, each ending on a commit frame.
    fn txs(frames: &[Frame]) -> Vec<Vec<Frame>> {
        let mut out = vec![];
        let mut cur = vec![];
        for f in frames {
            cur.push(f.clone());
            if f.header[4..8] != [0; 4] {
                out.push(std::mem::take(&mut cur));
            }
        }
        assert!(cur.is_empty());
        out
    }

    #[test]
    fn applies_committed_frames_and_reader_sees_rows_at_commit_frame() {
        let f = fixture(false);
        sql(&f.writer, "INSERT INTO t VALUES (2, zeroblob(100))");
        sql(&f.writer, "INSERT INTO t VALUES (3, zeroblob(100))");
        let frames = commit_frames(&f);
        assert_eq!(count(&f.reader), 1);
        let t = txs(&frames);
        assert_eq!(t.len(), 2);
        let out = f.applier.apply_commit(&t[0], 0).unwrap();
        assert_eq!(
            out,
            ApplyOutcome::Applied {
                frames: t[0].len() as u64
            }
        );
        assert_eq!(count(&f.reader), 2);
        f.applier.apply_commit(&t[1], 0).unwrap();
        assert_eq!(count(&f.reader), 3);
        assert!(!f.applier.local_wal_empty().unwrap());
    }

    #[test]
    fn reapplying_same_frames_is_ok() {
        let f = fixture(false);
        sql(&f.writer, "INSERT INTO t VALUES (2, zeroblob(100))");
        let frames = commit_frames(&f);
        f.applier.apply_commit(&frames, 0).unwrap();
        let out = f.applier.apply_commit(&frames, 0).unwrap();
        assert_eq!(
            out,
            ApplyOutcome::Applied {
                frames: frames.len() as u64
            }
        );
        assert_eq!(count(&f.reader), 2);
    }

    #[test]
    fn different_content_at_same_frame_is_conflict() {
        let f = fixture(false);
        sql(&f.writer, "INSERT INTO t VALUES (2, zeroblob(100))");
        let frames = commit_frames(&f);
        f.applier.set_generation(7);
        f.applier.apply_commit(&frames, 0).unwrap();
        let mut bad = frames.clone();
        let l = bad[0].page.len();
        bad[0].page[l - 10] ^= 0xff;
        let e = f.applier.apply_commit(&bad, 0).unwrap_err();
        assert!(is_conflict(&e), "{e}");
        assert_eq!(
            e,
            Error::Corrupt("replica conflict: generation 7 frame 1".into())
        );
        assert_eq!(count(&f.reader), 2);
    }

    #[test]
    fn negative_numbering_skips_frames_already_in_app_db() {
        let f = fixture(false);
        sql(&f.writer, "INSERT INTO t VALUES (2, zeroblob(100))");
        sql(&f.writer, "INSERT INTO t VALUES (3, zeroblob(100))");
        let frames = commit_frames(&f);
        let t = txs(&frames);
        let k = t[0].len() as i64;
        // the first transaction is assumed already in app.db: all skipped, none counted
        let out = f.applier.apply_commit(&t[0], -k).unwrap();
        assert_eq!(out, ApplyOutcome::Applied { frames: 0 });
        assert!(f.applier.local_wal_empty().unwrap());
        // the second lands from local frame 1
        let out = f.applier.apply_commit(&t[1], -k).unwrap();
        assert_eq!(
            out,
            ApplyOutcome::Applied {
                frames: t[1].len() as u64
            }
        );
        assert_eq!(f.applier.last_frame().unwrap(), t[1].len() as u64);
    }

    #[test]
    fn gap_is_reported_not_applied() {
        let f = fixture(false);
        sql(&f.writer, "INSERT INTO t VALUES (2, zeroblob(100))");
        let frames = commit_frames(&f);
        let out = f.applier.apply_commit(&frames, 5).unwrap();
        assert_eq!(out, ApplyOutcome::Gap);
        assert!(f.applier.local_wal_empty().unwrap());
        assert_eq!(count(&f.reader), 1);
    }

    #[test]
    fn tail_only_is_incomplete_with_nothing_visible() {
        let f = fixture(false);
        sql(&f.writer, "INSERT INTO t VALUES (2, zeroblob(100))");
        let mut tail = commit_frames(&f);
        for fr in &mut tail {
            fr.header[4..8].fill(0);
        }
        let out = f.applier.apply_commit(&tail, 0).unwrap();
        assert_eq!(out, ApplyOutcome::Incomplete);
        assert!(f.applier.local_wal_empty().unwrap());
        assert_eq!(count(&f.reader), 1);
        assert_eq!(
            f.applier.apply_commit(&[], 0).unwrap(),
            ApplyOutcome::Incomplete
        );
        // the real commit then applies cleanly
        f.applier.apply_commit(&commit_frames(&f), 0).unwrap();
        assert_eq!(count(&f.reader), 2);
    }

    #[test]
    fn commit_followed_by_uncommitted_tail_is_incomplete_and_nothing_applied() {
        let f = fixture(false);
        sql(&f.writer, "INSERT INTO t VALUES (2, zeroblob(100))");
        let frames = commit_frames(&f);
        let mut mixed = frames.clone();
        let mut extra = frames.last().unwrap().clone();
        extra.no += 1;
        extra.header[4..8].fill(0);
        mixed.push(extra);
        // ends on a non-commit frame: Incomplete, the committed part is not applied either
        let out = f.applier.apply_commit(&mixed, 0).unwrap();
        assert_eq!(out, ApplyOutcome::Incomplete);
        assert!(f.applier.local_wal_empty().unwrap());
        assert_eq!(count(&f.reader), 1);
    }

    #[test]
    fn two_transactions_in_one_call_are_rejected() {
        let f = fixture(false);
        sql(&f.writer, "INSERT INTO t VALUES (2, zeroblob(100))");
        sql(&f.writer, "INSERT INTO t VALUES (3, zeroblob(100))");
        let frames = commit_frames(&f);
        assert!(frames.len() >= 2);
        assert!(f.applier.apply_commit(&frames, 0).is_err());
        assert!(f.applier.local_wal_empty().unwrap());
    }

    #[test]
    fn encrypted_frames_are_decrypted_before_insert() {
        let f = fixture(true);
        sql(&f.writer, "INSERT INTO t VALUES (2, zeroblob(100))");
        sql(&f.writer, "INSERT INTO t VALUES (3, zeroblob(100))");
        let frames = commit_frames(&f);
        let t = txs(&frames);
        for x in &t {
            f.applier.apply_commit(x, 0).unwrap();
        }
        assert_eq!(count(&f.reader), 3);
        // re-delivery compares plaintext against plaintext
        for x in &t {
            f.applier.apply_commit(x, 0).unwrap();
        }
    }

    #[test]
    fn checkpoint_truncate_empties_the_local_wal() {
        let f = fixture(false);
        sql(&f.writer, "INSERT INTO t VALUES (2, zeroblob(100))");
        f.applier.apply_commit(&commit_frames(&f), 0).unwrap();
        assert_eq!(
            f.applier.checkpoint_truncate().unwrap(),
            CheckpointResult::Done
        );
        assert!(f.applier.local_wal_empty().unwrap());
        assert_eq!(count(&f.reader), 2);
    }
}
