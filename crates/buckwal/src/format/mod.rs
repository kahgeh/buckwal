//! Bucket format v1: key layout and byte/JSON codecs.

mod head;
pub(crate) mod integrity;
pub(crate) mod json;
mod keys;
mod manifest;
mod record;

use crate::config::JournalMode;

/// The format number of bucket format v1: the head and the local state carry it.
pub(crate) const FORMAT: u32 = 1;
/// Bucket format v2: MVCC buckets only (head `"format":2` plus `"journal_mode":"mvcc"`).
pub(crate) const FORMAT_V2: u32 = 2;
/// Bucket format v3: encrypted buckets (WAL or MVCC; the head's `journal_mode` says which).
pub(crate) const FORMAT_V3: u32 = 3;
/// Bucket format v4: every encrypted bucket this build makes (WAL or MVCC): the format 3 head
/// plus `database_id`, with every integrity code made under the database integrity key.
pub(crate) const FORMAT_V4: u32 = 4;
/// Bucket format v5: the packer's format. The head has `journal_mode` and no `manifest`; the
/// pages live in packs (`pack-latest` names the newest). An encrypted bucket's head also has
/// `database_id`, `key_check` and an integrity code, as format 4.
pub(crate) const FORMAT_V5: u32 = 5;

pub(crate) use head::{Head, LocalState};
pub(crate) use keys::Keys;
#[cfg(test)]
pub(crate) use keys::LegacyFormat3;
pub(crate) use manifest::Manifest;
pub(crate) use record::{apply_ops, LogFile, LogRecord, Op};
#[cfg(test)]
pub(crate) use record::{decode_record, encode_record};

#[cfg(test)]
mod tests;
