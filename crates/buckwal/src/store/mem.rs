// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! In-process bucket for tests, with real ETags and conditional semantics.
use super::{CondPut, Object, ObjectStat, ObjectStore, StatsSnap, StoreError};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Record `status` on the current span's `s3.status`, as `S3Store` records each response's.
fn status(code: u16) {
    tracing::Span::current().record("s3.status", code);
}

/// The status S3 answers a conditional put with.
fn cond_status(r: &CondPut) -> u16 {
    match r {
        CondPut::Created { .. } => 200,
        CondPut::PreconditionFailed => 412,
    }
}

#[derive(Default)]
struct Inner {
    objects: BTreeMap<String, Object>,
    /// When each key was last written, by the store's clock (real now + `clock_offset`).
    written: BTreeMap<String, Instant>,
    /// Added to the real clock: the test clock `advance_clock` moves.
    clock_offset: Duration,
    version: u64,
    /// ETags from the body alone, as S3's MD5 ETags are (`with_content_etags`).
    content_etags: bool,
}

#[derive(Default)]
pub struct MemStore {
    inner: Mutex<Inner>,
    puts: AtomicU64,
    gets: AtomicU64,
    lists: AtomicU64,
    deletes: AtomicU64,
    bytes_up: AtomicU64,
    bytes_down: AtomicU64,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// A store whose ETag depends only on the body, as a single-part PUT's MD5 ETag does on
    /// S3 and RustFS: equal bytes get equal ETags, however often they are written.
    #[cfg(test)]
    pub(crate) fn with_content_etags() -> Self {
        let s = Self::default();
        s.inner.lock().unwrap().content_etags = true;
        s
    }

    /// Move this store's clock forward: every object's `stat` age grows by `by`.
    pub fn advance_clock(&self, by: Duration) {
        self.inner.lock().unwrap().clock_offset += by;
    }

    fn count_put(&self, n: usize) {
        self.puts.fetch_add(1, Relaxed);
        self.bytes_up.fetch_add(n as u64, Relaxed);
    }

    /// Store `body` under `key` with a fresh ETag. Caller holds the lock.
    fn store(inner: &mut Inner, key: &str, body: &[u8]) -> String {
        inner.version += 1;
        let hash = hex::encode(Sha256::digest(body));
        let etag = if inner.content_etags {
            hash
        } else {
            format!("{hash}-{}", inner.version)
        };
        let now = Instant::now() + inner.clock_offset;
        inner.written.insert(key.to_string(), now);
        inner.objects.insert(
            key.to_string(),
            Object {
                body: body.to_vec(),
                etag: etag.clone(),
            },
        );
        etag
    }
}

impl ObjectStore for MemStore {
    fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.count_put(body.len());
        let mut inner = self.inner.lock().unwrap();
        status(200);
        Ok(Self::store(&mut inner, key, body))
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> Result<CondPut, StoreError> {
        self.count_put(body.len());
        let mut inner = self.inner.lock().unwrap();
        let r = if inner.objects.contains_key(key) {
            CondPut::PreconditionFailed
        } else {
            CondPut::Created {
                etag: Self::store(&mut inner, key, body),
            }
        };
        status(cond_status(&r));
        Ok(r)
    }

    fn put_if_match(&self, key: &str, body: &[u8], etag: &str) -> Result<CondPut, StoreError> {
        self.count_put(body.len());
        let mut inner = self.inner.lock().unwrap();
        let r = match inner.objects.get(key) {
            Some(o) if o.etag == etag => CondPut::Created {
                etag: Self::store(&mut inner, key, body),
            },
            _ => CondPut::PreconditionFailed,
        };
        status(cond_status(&r));
        Ok(r)
    }

    fn get(&self, key: &str) -> Result<Option<Object>, StoreError> {
        let v = self.inner.lock().unwrap().objects.get(key).cloned();
        self.gets.fetch_add(1, Relaxed);
        self.bytes_down
            .fetch_add(v.as_ref().map_or(0, |o| o.body.len()) as u64, Relaxed);
        status(if v.is_some() { 200 } else { 404 });
        Ok(v)
    }

    fn stat(&self, key: &str) -> Result<Option<ObjectStat>, StoreError> {
        let st = {
            let inner = self.inner.lock().unwrap();
            inner.objects.get(key).map(|o| ObjectStat {
                etag: o.etag.clone(),
                age: inner.written.get(key).map_or(Duration::ZERO, |w| {
                    (Instant::now() + inner.clock_offset).saturating_duration_since(*w)
                }),
            })
        };
        self.gets.fetch_add(1, Relaxed);
        status(if st.is_some() { 200 } else { 404 });
        Ok(st)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let keys: Vec<String> = self
            .inner
            .lock()
            .unwrap()
            .objects
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        self.lists
            .fetch_add(keys.len().div_ceil(1000).max(1) as u64, Relaxed);
        status(200);
        Ok(keys)
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.deletes.fetch_add(1, Relaxed);
        let mut inner = self.inner.lock().unwrap();
        inner.objects.remove(key);
        inner.written.remove(key);
        status(204);
        Ok(())
    }

    fn stats(&self) -> StatsSnap {
        StatsSnap {
            puts: self.puts.load(Relaxed),
            gets: self.gets.load(Relaxed),
            lists: self.lists.load(Relaxed),
            deletes: self.deletes.load(Relaxed),
            bytes_up: self.bytes_up.load(Relaxed),
            bytes_down: self.bytes_down.load(Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_list_after_is_strictly_greater_sorted_and_prefix_bound() {
        let s = MemStore::new();
        for k in ["a/3", "a/1", "a/2", "a/4", "b/9", "a"] {
            s.put(k, b"x").unwrap();
        }
        assert_eq!(s.list_after("a/", "a/2").unwrap(), ["a/3", "a/4"]);
        assert_eq!(
            s.list_after("a/", "a/").unwrap(),
            ["a/1", "a/2", "a/3", "a/4"]
        );
        assert!(s.list_after("a/", "a/4").unwrap().is_empty());
        assert!(s.list_after("a/", "b/9").unwrap().is_empty());
    }

    #[test]
    fn mem_conditional_semantics() {
        let s = MemStore::new();
        let CondPut::Created { etag: e1 } = s.put_if_absent("k", b"one").unwrap() else {
            panic!("first put_if_absent must create");
        };
        assert_eq!(
            s.put_if_absent("k", b"x").unwrap(),
            CondPut::PreconditionFailed
        );
        let CondPut::Created { etag: e2 } = s.put_if_match("k", b"two", &e1).unwrap() else {
            panic!("current etag must match");
        };
        assert_ne!(e1, e2);
        assert_eq!(
            s.put_if_match("k", b"three", &e1).unwrap(),
            CondPut::PreconditionFailed
        );
        assert_eq!(
            s.put_if_match("missing", b"x", &e1).unwrap(),
            CondPut::PreconditionFailed
        );
        let o = s.get("k").unwrap().unwrap();
        assert_eq!((o.body.as_slice(), &o.etag), (&b"two"[..], &e2));
        // Same body, new version: ETag still changes.
        let e3 = s.put("k", b"two").unwrap();
        assert_ne!(e3, o.etag);
    }

    #[test]
    fn mem_list_sorted_and_counted() {
        let s = MemStore::new();
        s.put("a/2", b"two").unwrap();
        s.put("a/1", b"one").unwrap();
        s.put("b/1", b"x").unwrap();
        assert_eq!(s.list("a/").unwrap(), vec!["a/1", "a/2"]);
        let st = s.stats();
        assert_eq!((st.puts, st.lists, st.bytes_up), (3, 1, 7));
        s.list("none/").unwrap();
        assert_eq!(s.stats().lists, 2);
        for i in 0..2500 {
            s.put(&format!("m/{i:05}"), b"").unwrap();
        }
        let before = s.stats();
        assert_eq!(s.list("m/").unwrap().len(), 2500);
        assert_eq!(s.stats().since(&before).lists, 3);
    }

    #[test]
    fn mem_get_delete_stats() {
        let s = MemStore::new();
        s.put("k", b"abc").unwrap();
        assert!(s.get("k").unwrap().is_some());
        assert!(s.get("nope").unwrap().is_none());
        s.delete("k").unwrap();
        assert!(s.get("k").unwrap().is_none());
        let st = s.stats();
        assert_eq!((st.gets, st.deletes, st.bytes_down), (3, 1, 3));
    }

    #[test]
    fn stat_age_follows_the_clock() {
        use std::time::Duration;
        let s = MemStore::new();
        assert!(s.stat("k").unwrap().is_none());
        s.put("k", b"abc").unwrap();
        s.advance_clock(Duration::from_secs(5));
        let st = s.stat("k").unwrap().unwrap();
        assert!(st.age >= Duration::from_secs(5), "{:?}", st.age);
        assert_eq!(st.etag, s.get("k").unwrap().unwrap().etag);
        // A rewrite starts the age again, and so does a first put after the clock moved.
        s.put("k", b"abd").unwrap();
        let age = s.stat("k").unwrap().unwrap().age;
        assert!(age < Duration::from_secs(1), "{age:?}");
        s.put("new", b"x").unwrap();
        let age = s.stat("new").unwrap().unwrap().age;
        assert!(age < Duration::from_secs(1), "{age:?}");
    }
}
