//! The packer: a pack is one object that holds a whole database's pages as of one generation.
//! `pointer` is the `pack-latest` object that names the newest pack; `codes` checks a pack's
//! blocks; `run` is one pack run.

pub(crate) mod codes;
pub(crate) mod pointer;
pub(crate) mod run;
#[cfg(test)]
mod tests;

pub(crate) use pointer::PackPointer;
pub use run::{pack, pack_with_store, PackOutcome, PackReport};
