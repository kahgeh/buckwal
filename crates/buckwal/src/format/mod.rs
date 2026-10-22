//! Bucket format v1: key layout and byte/JSON codecs.

mod head;
mod json;
mod keys;
mod manifest;
mod record;

/// The format number of bucket format v1: the head and the local state carry it.
pub(crate) const FORMAT: u32 = 1;
/// Bucket format v2: MVCC buckets only (head `"format":2` plus `"journal_mode":"mvcc"`).
pub(crate) const FORMAT_V2: u32 = 2;

pub(crate) use head::{Head, LocalState};
pub(crate) use keys::Keys;
pub(crate) use manifest::Manifest;
pub(crate) use record::{apply_ops, decode_record, encode_record, LogFile, LogRecord, Op};

#[cfg(test)]
mod tests;
