// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! The one interface everything uploads through, with request counters.
pub mod fault;
pub mod mem;
pub mod retry;
pub mod s3;

pub use fault::FaultStore;
pub use mem::MemStore;
pub use retry::{with_retry, RetryPolicy};

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    pub body: Vec<u8>,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CondPut {
    Created { etag: String },
    PreconditionFailed,
}

#[derive(Debug, Clone)]
pub enum StoreError {
    /// Worth retrying (timeouts, 5xx, throttling).
    Transient(String),
    /// Not worth retrying (403, bad request).
    Permanent(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Transient(m) => write!(f, "transient store error: {m}"),
            StoreError::Permanent(m) => write!(f, "permanent store error: {m}"),
        }
    }
}

impl std::error::Error for StoreError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsSnap {
    pub puts: u64,
    pub gets: u64,
    pub lists: u64,
    pub deletes: u64,
    pub bytes_up: u64,
    pub bytes_down: u64,
}

impl StatsSnap {
    /// The counters accumulated since the earlier snapshot `e`.
    pub fn since(&self, e: &StatsSnap) -> StatsSnap {
        StatsSnap {
            puts: self.puts - e.puts,
            gets: self.gets - e.gets,
            lists: self.lists - e.lists,
            deletes: self.deletes - e.deletes,
            bytes_up: self.bytes_up - e.bytes_up,
            bytes_down: self.bytes_down - e.bytes_down,
        }
    }
    /// Every request is one sequential round trip.
    pub fn requests(&self) -> u64 {
        self.puts + self.gets + self.lists + self.deletes
    }
}

pub trait ObjectStore: Send + Sync {
    /// Unconditional put; returns the new ETag.
    fn put(&self, key: &str, body: &[u8]) -> std::result::Result<String, StoreError>;
    fn put_if_absent(&self, key: &str, body: &[u8]) -> std::result::Result<CondPut, StoreError>;
    fn put_if_match(
        &self,
        key: &str,
        body: &[u8],
        etag: &str,
    ) -> std::result::Result<CondPut, StoreError>;
    fn get(&self, key: &str) -> std::result::Result<Option<Object>, StoreError>;
    /// Every key under `prefix`, sorted ascending.
    fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StoreError>;
    fn delete(&self, key: &str) -> std::result::Result<(), StoreError>;
    fn stats(&self) -> StatsSnap;
}
