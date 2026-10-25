//! The packer: a pack is one object that holds a whole database's pages as of one generation.
//! `pointer` is the `pack-latest` object that names the newest pack; `codes` checks a pack's
//! blocks; `run` is one pack run; `first` publishes a new MVCC database's first pack;
//! `thread` is the packer loop and the writer's pack gauges.

pub(crate) mod check;
pub(crate) mod codes;
pub(crate) mod first;
pub(crate) mod kept;
pub(crate) mod pointer;
pub(crate) mod run;
#[cfg(test)]
mod tests;
pub(crate) mod thread;

pub use check::{check_bucket, check_bucket_with_store, BucketCheck};
pub use kept::{DownloadReason, PackSource};
pub(crate) use pointer::PackPointer;
pub use run::{pack, pack_with_store, PackOutcome, PackReport};
pub use thread::{run_packer, run_packer_with_store};
