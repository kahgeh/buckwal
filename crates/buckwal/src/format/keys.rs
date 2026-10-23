//! Object key layout. Generation is `{:010}`, seq `{:020}`, page `{:010}`.
//!
//! `Keys` also carries the integrity key of an encrypted database, and is where every object
//! of the bucket is encoded and decoded: with a key, pages, log records, manifests and the head
//! carry integrity codes (`integrity.rs`) and are checked on every read; without one, the bytes
//! are exactly those of a plaintext bucket.
use super::integrity::{self, Check};
use super::{Head, JournalMode, LogRecord, Manifest, FORMAT, FORMAT_V2, FORMAT_V3};
use crate::encryption::IntegrityKey;
use crate::error::Result;
use std::borrow::Cow;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct Keys {
    prefix: String,
    integrity: Option<Arc<IntegrityKey>>,
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
        }
    }

    /// These keys for an encrypted database (`Some`): its objects carry integrity codes.
    pub(crate) fn with_integrity(mut self, ik: Option<IntegrityKey>) -> Self {
        self.integrity = ik.map(Arc::new);
        self
    }

    /// Whether this database is encrypted (its objects carry integrity codes).
    pub(crate) fn coded(&self) -> bool {
        self.integrity.is_some()
    }

    /// The head `format` a new bucket of `mode` is created with.
    pub(crate) fn new_format(&self, mode: JournalMode) -> u32 {
        match (self.coded(), mode) {
            (true, _) => FORMAT_V3,
            (false, JournalMode::Wal) => FORMAT,
            (false, JournalMode::Mvcc) => FORMAT_V2,
        }
    }

    /// The head's bytes: with an integrity key (a format 3 head) its fields and their code.
    pub(crate) fn encode_head(&self, head: &Head) -> Vec<u8> {
        match &self.integrity {
            Some(ik) => head.encode_coded(ik),
            None => head.encode(),
        }
    }

    /// The head object `body`, checked (`Head::decode_checked`).
    pub(crate) fn decode_head(&self, body: &[u8]) -> Result<Head> {
        Head::decode_checked(body, &self.head(), self.integrity.as_deref())
    }

    /// The bytes of the log record at `log/<generation>/<seq>`.
    pub(crate) fn encode_record(&self, r: &LogRecord, generation: u64, seq: u64) -> Vec<u8> {
        let key = String::new();
        super::record::encode_record_with(r, self.check(&key, generation, seq))
    }

    /// The record at `log/<generation>/<seq>`, checked.
    pub(crate) fn decode_record(
        &self,
        body: &[u8],
        generation: u64,
        seq: u64,
    ) -> Result<LogRecord> {
        let key = self.log(generation, seq);
        super::record::decode_record_with(body, self.check(&key, generation, seq))
    }

    pub(crate) fn encode_manifest(&self, m: &Manifest, generation: u64) -> Vec<u8> {
        let key = String::new();
        m.encode_with(self.check(&key, generation, 0))
    }

    /// The manifest at `manifest/<generation>`, checked.
    pub(crate) fn decode_manifest(&self, body: &[u8], generation: u64) -> Result<Manifest> {
        let key = self.manifest(generation);
        Manifest::decode_with(body, self.check(&key, generation, 0))
    }

    /// The object to PUT at `pages/<generation>/<page>` for a page's bytes.
    pub(crate) fn seal_page<'a>(
        &self,
        page: u32,
        generation: u64,
        bytes: &'a [u8],
    ) -> Cow<'a, [u8]> {
        match &self.integrity {
            Some(ik) => Cow::Owned(integrity::seal_page(ik, page, generation, bytes)),
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
        match &self.integrity {
            Some(ik) => {
                let key = self.page(generation, page);
                integrity::open_page(ik, page, generation, obj, &key)
            }
            None => Ok(obj),
        }
    }

    fn check<'a>(&'a self, key: &'a str, generation: u64, seq: u64) -> Option<Check<'a>> {
        self.integrity.as_deref().map(|ik| Check {
            ik,
            key,
            generation,
            seq,
        })
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
