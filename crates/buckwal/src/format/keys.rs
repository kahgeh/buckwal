//! Object key layout. Generation is `{:010}`, seq `{:020}`, page `{:010}`.
//!
//! `Keys` also carries the integrity key of an encrypted database, and is where every object
//! of the bucket is encoded and decoded: with a key, pages, log records, manifests and the head
//! carry integrity codes (`integrity.rs`) and are checked on every read; without one, the bytes
//! are exactly those of a plaintext bucket.
use super::integrity::{self, Check};
use super::integrity::{integrity_failed, integrity_failed_as, Kind};
use super::{Head, JournalMode, LogRecord, Manifest, FORMAT, FORMAT_V2, FORMAT_V3, FORMAT_V4};
use crate::encryption::IntegrityKey;
use crate::error::Result;
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
    fn object_key(&self) -> Option<Arc<IntegrityKey>> {
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
            (Identity::Unknown, FORMAT_V4) => {
                let id = head.database_id.clone().unwrap_or_default();
                let bytes = hex::decode(&id).unwrap_or_default();
                *known = Identity::Format4 {
                    key: Arc::new(base.database_key(&bytes)),
                    id,
                };
            }
            (Identity::Format3, FORMAT_V3) => {}
            (Identity::Format4 { id, .. }, FORMAT_V4) => {
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

    /// The head's bytes: with an integrity key (a format 3 head) its fields and their code.
    pub(crate) fn encode_head(&self, head: &Head) -> Vec<u8> {
        debug_assert!(!self.coded() || head.format == FORMAT_V3 || head.format == FORMAT_V4);
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
