//! Read replicas. Pure code so far: the WAL image rebuilt from log records.
// Nothing calls this yet; the allow goes when the replica loop uses it.
#![allow(dead_code)]

pub(crate) mod image;
