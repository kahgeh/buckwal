//! The packer: a pack is one object that holds a whole database's pages as of one generation.
//! `pointer` is the `pack-latest` object that names the newest pack.
// The pack code is used from task 5 on; until then only its tests call it.
#![allow(dead_code)]

pub(crate) mod codes;
pub(crate) mod pointer;
#[cfg(test)]
mod tests;

pub(crate) use pointer::PackPointer;
