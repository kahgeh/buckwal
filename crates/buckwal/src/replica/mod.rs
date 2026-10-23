//! Read replicas. Pure code so far: the WAL image rebuilt from log records.
// Nothing calls this yet; the allow goes when the replica loop uses it.
#![allow(dead_code)]

pub(crate) mod apply;
pub(crate) mod fence;
pub(crate) mod image;
pub(crate) mod position;
pub(crate) mod sync;
