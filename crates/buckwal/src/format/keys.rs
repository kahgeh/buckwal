//! Object key layout. Generation is `{:010}`, seq `{:020}`, page `{:010}`.
//!
//! `Keys` also carries the integrity key of an encrypted database, and is where every object
//! of the bucket is encoded and decoded: with a key, pages, log records, manifests and the head
//! carry integrity codes (`integrity.rs`) and are checked on every read; without one, the bytes
//! are exactly those of a plaintext bucket.
use super::integrity::{self, Check};
use super::integrity::{integrity_failed, integrity_failed_as, Kind};
use super::{
    Head, JournalMode, LogRecord, Manifest, FORMAT, FORMAT_V2, FORMAT_V3, FORMAT_V4, FORMAT_V5,
};
use crate::encryption::IntegrityKey;
use crate::error::Result;
use crate::pack::PackPointer;
use std::borrow::Cow;
use std::sync::{Arc, Mutex};

#[cfg(test)]
thread_local! {
    static LEGACY_FORMAT3: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Tests only: while alive, a database created on this thread is a format 3 one (no
/// `database_id`, objects coded under the integrity key), as every encrypted bucket made before
/// format 4 is.
#[cfg(test)]
pub(crate) struct LegacyFormat3;

#[cfg(test)]
impl LegacyFormat3 {
    pub(crate) fn on() -> Self {
        LEGACY_FORMAT3.with(|f| f.set(true));
        LegacyFormat3
    }
}

#[cfg(test)]
impl Drop for LegacyFormat3 {
    fn drop(&mut self) {
        LEGACY_FORMAT3.with(|f| f.set(false));
    }
}

/// What these keys know of the database's format, learned from its head (shared by every clone
/// of the keys): the objects of a format 4 database are coded under its database integrity key.
#[derive(Debug)]
enum Identity {
    /// No head read or written yet.
    Unknown,
    /// A format 3 head: the objects are coded under the integrity key.
    Format3,
    /// A format 4 head: its `database_id`, and the database integrity key made from it.
    Format4 { id: String, key: Arc<IntegrityKey> },
}

#[derive(Debug, Clone)]
pub(crate) struct Keys {
    prefix: String,
    integrity: Option<Arc<IntegrityKey>>,
    identity: Arc<Mutex<Identity>>,
    /// Whether `decode_head` is an open's first read of the bucket (`for_open`).
    opening: bool,
}

/// What a coded decode or encode needs, when there is a key.
fn check<'a>(
    ik: Option<&'a IntegrityKey>,
    key: &'a str,
    generation: u64,
    seq: u64,
) -> Option<Check<'a>> {
    ik.map(|ik| Check {
        ik,
        key,
        generation,
        seq,
    })
}

/// Whether `run_id` is a pack run id: one or more of `[A-Za-z0-9_-]`.
fn valid_run_id(run_id: &str) -> bool {
    !run_id.is_empty()
        && run_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Parse exactly `width` ASCII digits.
fn digits(s: &str, width: usize) -> Option<u64> {
    if s.len() != width || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

impl Keys {
    pub(crate) fn new(prefix: &str) -> Self {
        Keys {
            prefix: prefix.to_string(),
            integrity: None,
            identity: Arc::new(Mutex::new(Identity::Unknown)),
            opening: false,
        }
    }

    /// These keys for an encrypted database (`Some`): its objects carry integrity codes.
    pub(crate) fn with_integrity(mut self, ik: Option<IntegrityKey>) -> Self {
        self.integrity = ik.map(Arc::new);
        self.identity = Arc::new(Mutex::new(Identity::Unknown));
        self
    }

    /// The key page, log record and manifest codes are made under: the database integrity key
    /// once a format 4 head has been read or made, else the integrity key; `None` for a
    /// plaintext database.
    pub(crate) fn object_key(&self) -> Option<Arc<IntegrityKey>> {
        let base = self.integrity.as_ref()?;
        match &*self.identity.lock().unwrap_or_else(|e| e.into_inner()) {
            Identity::Format4 { key, .. } => Some(key.clone()),
            _ => Some(base.clone()),
        }
    }

    /// A new encrypted database: make its database id (16 random bytes, hex) and take it as
    /// this database's, so every object written after is coded under its database integrity
    /// key. `None` for a plaintext database.
    pub(crate) fn init_database_id(&self) -> Result<Option<String>> {
        let Some(base) = &self.integrity else {
            return Ok(None);
        };
        #[cfg(test)]
        if LEGACY_FORMAT3.with(|f| f.get()) {
            return Ok(None);
        }
        let id = crate::db::new_writer_id()?;
        let bytes = hex::decode(&id).expect("new_writer_id is hex");
        let key = Arc::new(base.database_key(&bytes));
        *self.identity.lock().unwrap_or_else(|e| e.into_inner()) = Identity::Format4 {
            id: id.clone(),
            key,
        };
        Ok(Some(id))
    }

    /// The database id these keys know (a format 4 head read or made), for tests that build
    /// heads by hand.
    #[cfg(test)]
    pub(crate) fn database_id(&self) -> Option<String> {
        match &*self.identity.lock().unwrap_or_else(|e| e.into_inner()) {
            Identity::Format4 { id, .. } => Some(id.clone()),
            _ => None,
        }
    }

    /// Take `head`, a head that verified, as this database's format and id, or refuse it when
    /// the keys already know another: a head of another database (another `database_id`), a
    /// format 3 head over a format 4 database (`format downgraded`) or a format 4 head over a
    /// format 3 one is `Corrupt("integrity: <key>")` and the record.
    fn adopt_head(&self, head: &Head) -> Result<()> {
        let Some(base) = &self.integrity else {
            return Ok(());
        };
        let mut known = self.identity.lock().unwrap_or_else(|e| e.into_inner());
        let key = || self.head();
        match (&*known, head.format) {
            (Identity::Unknown, FORMAT_V3) => *known = Identity::Format3,
            (Identity::Unknown, FORMAT_V4 | FORMAT_V5) => {
                let id = head.database_id.clone().unwrap_or_default();
                let bytes = hex::decode(&id).unwrap_or_default();
                *known = Identity::Format4 {
                    key: Arc::new(base.database_key(&bytes)),
                    id,
                };
            }
            (Identity::Format3, FORMAT_V3) => {}
            (Identity::Format4 { id, .. }, FORMAT_V4 | FORMAT_V5) => {
                if head.database_id.as_deref() != Some(id.as_str()) {
                    return Err(integrity_failed(&key(), Kind::Head, head.generation, 0));
                }
            }
            (Identity::Format4 { .. }, _) => {
                return Err(integrity_failed_as(
                    &key(),
                    " format downgraded",
                    Kind::Head,
                    head.generation,
                    0,
                ));
            }
            (Identity::Format3, _) => {
                return Err(integrity_failed(&key(), Kind::Head, head.generation, 0));
            }
            (Identity::Unknown, _) => {}
        }
        Ok(())
    }

    /// These keys for a process's first read of the bucket (an open, a standalone GC): its head
    /// read compares `key_check` as an open does (`Head::decode_checked`).
    pub(crate) fn for_open(&self) -> Self {
        Keys {
            opening: true,
            ..self.clone()
        }
    }

    /// Whether this database is encrypted (its objects carry integrity codes).
    pub(crate) fn coded(&self) -> bool {
        self.integrity.is_some()
    }

    /// The head `format` a new bucket of `mode` is created with.
    pub(crate) fn new_format(&self, mode: JournalMode) -> u32 {
        match (self.coded(), mode) {
            #[cfg(test)]
            (true, _) if LEGACY_FORMAT3.with(|f| f.get()) => FORMAT_V3,
            (true, _) => FORMAT_V4,
            (false, JournalMode::Wal) => FORMAT,
            (false, JournalMode::Mvcc) => FORMAT_V2,
        }
    }

    /// The head `format` a new packed bucket is created with. It is 5, coded or not. The writer
    /// uses `new_format` until it writes packs (a format 5 head has no manifest, which the seal
    /// path of formats 1 to 4 needs).
    #[allow(dead_code)] // the writer makes format 5 heads from the generation switch on
    pub(crate) fn new_format_packed(&self) -> u32 {
        FORMAT_V5
    }

    /// The head's bytes: with an integrity key (a format 3 head) its fields and their code.
    pub(crate) fn encode_head(&self, head: &Head) -> Vec<u8> {
        debug_assert!(!self.coded() || head.is_coded());
        match &self.integrity {
            Some(ik) => head.encode_coded(ik),
            None => head.encode(),
        }
    }

    /// The head object `body` as a read after a good open sees it: a failing `key_check` or
    /// integrity code is tampering (`Head::decode_checked`). Under `for_open` keys, as
    /// `decode_head_open`.
    pub(crate) fn decode_head(&self, body: &[u8]) -> Result<Head> {
        let head =
            Head::decode_checked(body, &self.head(), self.integrity.as_deref(), self.opening)?;
        self.adopt_head(&head)?;
        Ok(head)
    }

    /// The head object `body` as an open reads it: `key_check` is compared first, and a
    /// different one is the wrong-key `Encryption` error and no record.
    pub(crate) fn decode_head_open(&self, body: &[u8]) -> Result<Head> {
        let head = Head::decode_checked(body, &self.head(), self.integrity.as_deref(), true)?;
        self.adopt_head(&head)?;
        Ok(head)
    }

    /// The bytes of the log record at `log/<generation>/<seq>`.
    pub(crate) fn encode_record(&self, r: &LogRecord, generation: u64, seq: u64) -> Vec<u8> {
        let key = String::new();
        let ik = self.object_key();
        super::record::encode_record_with(r, check(ik.as_deref(), &key, generation, seq))
    }

    /// The record at `log/<generation>/<seq>`, checked.
    pub(crate) fn decode_record(
        &self,
        body: &[u8],
        generation: u64,
        seq: u64,
    ) -> Result<LogRecord> {
        let key = self.log(generation, seq);
        let ik = self.object_key();
        super::record::decode_record_with(body, check(ik.as_deref(), &key, generation, seq))
    }

    pub(crate) fn encode_manifest(&self, m: &Manifest, generation: u64) -> Vec<u8> {
        let key = String::new();
        let ik = self.object_key();
        m.encode_with(check(ik.as_deref(), &key, generation, 0))
    }

    /// The manifest at `manifest/<generation>`, checked.
    pub(crate) fn decode_manifest(&self, body: &[u8], generation: u64) -> Result<Manifest> {
        let key = self.manifest(generation);
        let ik = self.object_key();
        Manifest::decode_with(body, check(ik.as_deref(), &key, generation, 0))
    }

    /// The object to PUT at `pages/<generation>/<page>` for a page's bytes.
    pub(crate) fn seal_page<'a>(
        &self,
        page: u32,
        generation: u64,
        bytes: &'a [u8],
    ) -> Cow<'a, [u8]> {
        match self.object_key() {
            Some(ik) => Cow::Owned(integrity::seal_page(&ik, page, generation, bytes)),
            None => Cow::Borrowed(bytes),
        }
    }

    /// A page's bytes from the object at `pages/<generation>/<page>`, checked.
    pub(crate) fn open_page<'a>(
        &self,
        page: u32,
        generation: u64,
        obj: &'a [u8],
    ) -> Result<&'a [u8]> {
        match self.object_key() {
            Some(ik) => {
                let key = self.page(generation, page);
                integrity::open_page(&ik, page, generation, obj, &key)
            }
            None => Ok(obj),
        }
    }

    fn join(&self, rest: &str) -> String {
        if self.prefix.is_empty() {
            rest.to_string()
        } else {
            format!("{}/{rest}", self.prefix)
        }
    }

    /// The part of `key` after the prefix, or None when it is outside it.
    fn strip<'a>(&self, key: &'a str) -> Option<&'a str> {
        if self.prefix.is_empty() {
            return Some(key);
        }
        key.strip_prefix(self.prefix.as_str())?.strip_prefix('/')
    }

    pub(crate) fn head(&self) -> String {
        self.join("head")
    }

    pub(crate) fn log(&self, gen: u64, seq: u64) -> String {
        self.join(&format!("log/{gen:010}/{seq:020}"))
    }

    pub(crate) fn log_prefix(&self, gen: u64) -> String {
        self.join(&format!("log/{gen:010}/"))
    }

    /// Every log object's key starts with this.
    pub(crate) fn log_root(&self) -> String {
        self.join("log/")
    }

    /// Every page object's key starts with this.
    pub(crate) fn page_root(&self) -> String {
        self.join("pages/")
    }

    /// Every manifest's key starts with this.
    pub(crate) fn manifest_root(&self) -> String {
        self.join("manifest/")
    }

    pub(crate) fn page(&self, gen: u64, page: u32) -> String {
        self.join(&format!("pages/{gen:010}/{page:010}"))
    }

    pub(crate) fn manifest(&self, gen: u64) -> String {
        self.join(&format!("manifest/{gen:010}"))
    }

    /// The pack `pack/<generation>/<run id>`.
    pub(crate) fn pack(&self, gen: u64, run_id: &str) -> String {
        debug_assert!(valid_run_id(run_id), "bad run id");
        self.join(&format!("pack/{gen:010}/{run_id}"))
    }

    /// The codes object of the pack `pack/<generation>/<run id>`.
    pub(crate) fn pack_codes(&self, gen: u64, run_id: &str) -> String {
        debug_assert!(valid_run_id(run_id), "bad run id");
        self.join(&format!("pack/{gen:010}/{run_id}.codes"))
    }

    /// The pack pointer.
    pub(crate) fn pack_pointer(&self) -> String {
        self.join("pack-latest")
    }

    /// Every pack's key, and every pack codes object's, starts with this.
    #[cfg_attr(not(test), allow(dead_code))] // GC lists packs with it
    pub(crate) fn pack_root(&self) -> String {
        self.join("pack/")
    }

    /// The generation and run id of a pack key (not of a codes key).
    pub(crate) fn parse_pack(&self, key: &str) -> Option<(u64, String)> {
        let rest = self.strip(key)?.strip_prefix("pack/")?;
        let (g, run) = rest.split_once('/')?;
        let ok = valid_run_id(run);
        let generation = digits(g, 10)?;
        ok.then(|| (generation, run.to_string()))
    }

    /// The pack pointer's bytes: with an integrity key its fields and their code, made under the
    /// database integrity key (the code covers every field of the pointer).
    pub(crate) fn encode_pointer(&self, p: &PackPointer) -> Vec<u8> {
        let mut out = p.encode_fields();
        if let Some(ik) = self.object_key() {
            let code = integrity::pointer_code(&ik, p);
            out.push_str(&format!(",\"integrity\":\"{}\"", hex::encode(code)));
        }
        out.push('}');
        out.into_bytes()
    }

    /// The pack pointer in `body`. In an encrypted bucket its code must verify and its
    /// `database_id` must be this database's, else `Corrupt("integrity: <pointer key>")`; a
    /// pointer without a code or an id is that too. A coded pointer read without a key is
    /// `Encryption`. In every bucket the pointer must be consistent: `codes_key` is `pack_key`
    /// plus `.codes`, `pack_key` parses under the prefix and `pack_generation` is its
    /// generation, else `Corrupt("pack pointer: ...")`.
    pub(crate) fn decode_pointer(&self, body: &[u8]) -> Result<PackPointer> {
        let (p, code) = PackPointer::decode_fields(body)?;
        let key = self.pack_pointer();
        let Some(ik) = self.object_key() else {
            if code.is_some() {
                return Err(crate::error::Error::Encryption(format!(
                    "{key} is an encrypted database's: set BUCKWAL_ENCRYPTION_KEY_FILE"
                )));
            }
            self.check_pointer_fields(&p)?;
            return Ok(p);
        };
        let fail = || integrity_failed(&key, Kind::Pointer, p.pack_generation, 0);
        let known = self.known_database_id();
        if p.database_id.is_none() || p.database_id != known {
            return Err(fail());
        }
        let ok = code
            .and_then(|c| hex::decode(c).ok())
            .is_some_and(|c| integrity::pointer_verifies(&ik, &p, &c));
        if !ok {
            return Err(fail());
        }
        self.check_pointer_fields(&p)?;
        Ok(p)
    }

    /// The database id these keys know (a format 4 or 5 head read or made).
    pub(crate) fn known_database_id(&self) -> Option<String> {
        match &*self.identity.lock().unwrap_or_else(|e| e.into_inner()) {
            Identity::Format4 { id, .. } => Some(id.clone()),
            _ => None,
        }
    }

    /// The checks of a pointer that hold in every bucket.
    fn check_pointer_fields(&self, p: &PackPointer) -> Result<()> {
        let bad = |m: &str| Err(crate::error::Error::Corrupt(format!("pack pointer: {m}")));
        let Some((generation, _)) = self.parse_pack(&p.pack_key) else {
            return bad("pack_key is not a pack of this prefix");
        };
        if p.codes_key != format!("{}.codes", p.pack_key) {
            return bad("codes_key is not pack_key plus .codes");
        }
        if p.pack_generation != generation {
            return bad("pack_generation is not the generation of pack_key");
        }
        Ok(())
    }

    pub(crate) fn parse_log(&self, key: &str) -> Option<(u64, u64)> {
        let rest = self.strip(key)?.strip_prefix("log/")?;
        let (g, s) = rest.split_once('/')?;
        Some((digits(g, 10)?, digits(s, 20)?))
    }

    pub(crate) fn parse_page(&self, key: &str) -> Option<(u64, u32)> {
        let rest = self.strip(key)?.strip_prefix("pages/")?;
        let (g, p) = rest.split_once('/')?;
        Some((digits(g, 10)?, u32::try_from(digits(p, 10)?).ok()?))
    }

    pub(crate) fn parse_manifest(&self, key: &str) -> Option<u64> {
        digits(self.strip(key)?.strip_prefix("manifest/")?, 10)
    }
}
