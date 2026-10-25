// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! The one interface everything uploads through, with request counters.
pub mod fault;
pub mod mem;
pub mod retry;
pub mod s3;

pub use fault::FaultStore;
pub use mem::MemStore;
pub use retry::{retry_store, with_retry, RetryPolicy};

use sha2::{Digest, Sha256};
use std::fmt;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    pub body: Vec<u8>,
    pub etag: String,
}

/// An object's ETag, size and age by the store's own clock (not the caller's).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectStat {
    pub etag: String,
    pub age: std::time::Duration,
    pub size: u64,
}

/// The bytes of a multipart upload. A store reads each part from it by offset, so parts can be
/// read in any order and from several threads.
pub trait PartSource: Sync {
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Fill `buf` with the bytes from `offset`. `offset + buf.len()` never passes `len()`.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<()>;
}

/// A [`PartSource`] over a byte slice.
pub struct SliceSource<'a>(pub &'a [u8]);

impl PartSource for SliceSource<'_> {
    fn len(&self) -> u64 {
        self.0.len() as u64
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
        let from = offset as usize;
        buf.copy_from_slice(&self.0[from..from + buf.len()]);
        Ok(())
    }
}

/// A finished multipart upload: the object's ETag and the SHA-256 of each part, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartDone {
    pub etag: String,
    pub part_sha256: Vec<[u8; 32]>,
}

/// S3 refuses an upload of more parts than this.
pub const MAX_PARTS: u64 = 10_000;

const MIB: u64 = 1024 * 1024;

/// The part size a store uses for `len` bytes: `part_size`, grown (to a whole number of MiB)
/// only when `len` would need more than [`MAX_PARTS`] parts.
pub fn part_size_for(len: u64, part_size: usize) -> usize {
    let part_size = part_size.max(1) as u64;
    if len.div_ceil(part_size) <= MAX_PARTS {
        return part_size as usize;
    }
    (len.div_ceil(MAX_PARTS).div_ceil(MIB) * MIB) as usize
}

/// The number of parts of `len` bytes at `part_size`. An empty source has one empty part,
/// because S3 refuses an upload with no part.
pub fn part_count(len: u64, part_size: usize) -> u64 {
    len.div_ceil(part_size.max(1) as u64).max(1)
}

/// Read part `index` (from 0) of `source` into a new buffer. Every part is `part_size` bytes
/// but the last, which holds the rest.
pub(crate) fn read_part(
    source: &dyn PartSource,
    part_size: usize,
    index: u64,
) -> Result<Vec<u8>, StoreError> {
    let offset = index * part_size as u64;
    let n = source.len().saturating_sub(offset).min(part_size as u64) as usize;
    let mut buf = vec![0u8; n];
    source.read_at(offset, &mut buf).map_err(|e| {
        StoreError::Permanent(format!(
            "read part {} of the source: {:?}",
            index + 1,
            e.kind()
        ))
    })?;
    Ok(buf)
}

pub(crate) fn sha256_of(b: &[u8]) -> [u8; 32] {
    Sha256::digest(b).into()
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
    /// The counters accumulated since the earlier snapshot `e` (each floors at 0).
    pub fn since(&self, e: &StatsSnap) -> StatsSnap {
        StatsSnap {
            puts: self.puts.saturating_sub(e.puts),
            gets: self.gets.saturating_sub(e.gets),
            lists: self.lists.saturating_sub(e.lists),
            deletes: self.deletes.saturating_sub(e.deletes),
            bytes_up: self.bytes_up.saturating_sub(e.bytes_up),
            bytes_down: self.bytes_down.saturating_sub(e.bytes_down),
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
    /// The object's ETag and age as the store sees it, counted as a GET. `None` means "no
    /// such key, or (on a HEAD) a bucket the store cannot tell apart": a caller must treat
    /// it as unknown, never as old. The age comes from two headers of 1 s resolution, so it
    /// can exceed the truth by just under 1 s; a caller acting on age adds 1 s for that.
    fn stat(&self, key: &str) -> std::result::Result<Option<ObjectStat>, StoreError>;
    /// Every key under `prefix`, sorted ascending.
    fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StoreError>;
    /// The keys under `prefix` strictly greater than `start_after`, sorted ascending. The
    /// default lists the whole prefix and filters; a store that can ask its server (S3's
    /// `start-after`) overrides it so a long listing is not read again.
    fn list_after(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> std::result::Result<Vec<String>, StoreError> {
        let mut keys = self.list(prefix)?;
        keys.retain(|k| k.as_str() > start_after);
        Ok(keys)
    }
    /// The keys under `prefix` with their sizes in bytes, sorted ascending. The default lists,
    /// then stats each key; S3 and the in-memory store read the sizes from the listing.
    fn list_sized(&self, prefix: &str) -> std::result::Result<Vec<(String, u64)>, StoreError> {
        let mut out = Vec::new();
        for key in self.list(prefix)? {
            // A key deleted since the listing is left out.
            if let Some(st) = self.stat(&key)? {
                out.push((key, st.size));
            }
        }
        Ok(out)
    }
    /// Upload `source` as one object `key` in parts of `part_size` bytes (the last is smaller;
    /// see [`part_size_for`]), up to `concurrency` parts at once. The object is invisible until
    /// the upload completes. A failed upload can leave its parts in the store, hidden, until
    /// [`ObjectStore::abort_incomplete`] removes them. The default reads the whole source and
    /// calls `put`, for stores with no multipart upload.
    fn put_multipart(
        &self,
        key: &str,
        source: &mut dyn PartSource,
        part_size: usize,
        _concurrency: usize,
    ) -> std::result::Result<MultipartDone, StoreError> {
        let len = source.len();
        let size = part_size_for(len, part_size);
        let body = read_part(source, len.max(1) as usize, 0)?;
        let etag = self.put(key, &body)?;
        let part_sha256 = if body.is_empty() {
            vec![sha256_of(&body)]
        } else {
            body.chunks(size).map(sha256_of).collect()
        };
        Ok(MultipartDone { etag, part_sha256 })
    }
    /// Up to `len` bytes of the object from `offset`; fewer at the end of the object, none
    /// from an `offset` at or past it. `None` means no such key.
    fn get_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> std::result::Result<Option<Vec<u8>>, StoreError> {
        Ok(self.get(key)?.map(|o| {
            let from = (offset as usize).min(o.body.len());
            let to = from.saturating_add(len as usize).min(o.body.len());
            o.body[from..to].to_vec()
        }))
    }
    /// Abort every multipart upload under `prefix` that started `older_than` or longer ago,
    /// by the store's clock, and return how many. `Duration::ZERO` aborts them all. The
    /// default has none to abort.
    fn abort_incomplete(
        &self,
        _prefix: &str,
        _older_than: Duration,
    ) -> std::result::Result<u64, StoreError> {
        Ok(0)
    }
    fn delete(&self, key: &str) -> std::result::Result<(), StoreError>;
    /// Delete `key` only while its ETag is `etag` (`If-Match`). `Ok(true)`: deleted, or the
    /// key is gone (S3: 404 `NoSuchKey`); `Ok(false)`: a 412, the key holds another ETag, or
    /// (RustFS) the key is gone; a caller reads the key again. The default refuses: a store
    /// that has no conditional delete cannot remove a pointer with care (spec rev 11, rule 1).
    fn delete_if_match(&self, key: &str, etag: &str) -> std::result::Result<bool, StoreError> {
        let _ = etag;
        Err(StoreError::Permanent(format!(
            "DELETE {key} (if-match): this store has no conditional delete"
        )))
    }
    fn stats(&self) -> StatsSnap;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const MIB: usize = 1024 * 1024;

    #[test]
    fn part_plan_makes_the_last_part_smaller() {
        assert_eq!(part_size_for(40_000_000, 16 * MIB), 16 * MIB);
        assert_eq!(part_count(40_000_000, 16 * MIB), 3);
        assert_eq!(part_count(32 * MIB as u64, 16 * MIB), 2);
        // An empty source still makes one (empty) part: S3 refuses an upload with none.
        assert_eq!(part_count(0, 16 * MIB), 1);
    }

    #[test]
    fn part_size_grows_only_above_10000_parts() {
        let at_limit = 10_000 * 16 * MIB as u64;
        assert_eq!(part_size_for(at_limit, 16 * MIB), 16 * MIB);
        let over = at_limit + 1;
        let grown = part_size_for(over, 16 * MIB);
        assert_eq!(grown, 17 * MIB, "rounded up to a whole MiB");
        assert!(part_count(over, grown) <= MAX_PARTS);
    }

    /// A store that implements only the required methods, over a `MemStore`: the default
    /// multipart, range and size methods work on top of them.
    struct Plain(MemStore);

    impl ObjectStore for Plain {
        fn put(&self, k: &str, b: &[u8]) -> Result<String, StoreError> {
            self.0.put(k, b)
        }
        fn put_if_absent(&self, k: &str, b: &[u8]) -> Result<CondPut, StoreError> {
            self.0.put_if_absent(k, b)
        }
        fn put_if_match(&self, k: &str, b: &[u8], e: &str) -> Result<CondPut, StoreError> {
            self.0.put_if_match(k, b, e)
        }
        fn get(&self, k: &str) -> Result<Option<Object>, StoreError> {
            self.0.get(k)
        }
        fn stat(&self, k: &str) -> Result<Option<ObjectStat>, StoreError> {
            self.0.stat(k)
        }
        fn list(&self, p: &str) -> Result<Vec<String>, StoreError> {
            self.0.list(p)
        }
        fn delete(&self, k: &str) -> Result<(), StoreError> {
            self.0.delete(k)
        }
        fn stats(&self) -> StatsSnap {
            self.0.stats()
        }
    }

    #[test]
    fn default_multipart_range_and_sizes() {
        let s = Plain(MemStore::new());
        let data: Vec<u8> = (0..2500u32).map(|i| (i % 251) as u8).collect();
        let done = s
            .put_multipart("k", &mut SliceSource(&data), 1000, 4)
            .unwrap();
        assert_eq!(done.part_sha256.len(), 3);
        assert_eq!(s.get("k").unwrap().unwrap().body, data);
        assert_eq!(
            s.get_range("k", 1000, 1000).unwrap().unwrap(),
            data[1000..2000]
        );
        assert_eq!(s.get_range("k", 2000, 1000).unwrap().unwrap(), data[2000..]);
        assert_eq!(s.get_range("none", 0, 10).unwrap(), None);
        s.put("a", b"xy").unwrap();
        assert_eq!(
            s.list_sized("").unwrap(),
            vec![("a".to_string(), 2), ("k".to_string(), 2500)]
        );
        assert_eq!(s.abort_incomplete("", Duration::ZERO).unwrap(), 0);
    }

    /// A snapshot taken from a different store (or one whose counters were reset) never
    /// underflows: each counter floors at 0.
    #[test]
    fn stats_since_saturates() {
        let small = StatsSnap {
            puts: 1,
            gets: 5,
            ..StatsSnap::default()
        };
        let big = StatsSnap {
            puts: 3,
            gets: 2,
            lists: 1,
            deletes: 1,
            bytes_up: 10,
            bytes_down: 10,
        };
        assert_eq!(
            small.since(&big),
            StatsSnap {
                gets: 3,
                ..StatsSnap::default()
            }
        );
        assert_eq!(big.since(&StatsSnap::default()), big);
    }
}
