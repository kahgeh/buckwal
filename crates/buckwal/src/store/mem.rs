// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! In-process bucket for tests, with real ETags and conditional semantics.
use super::{
    part_count, part_size_for, read_part, sha256_of, CondPut, MultipartDone, Object, ObjectStat,
    ObjectStore, PartSource, StatsSnap, StoreError,
};
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

/// A multipart upload that has not completed: it holds no object, only a start time.
struct Upload {
    id: u64,
    key: String,
    /// When it started, by the store's clock (real now + `clock_offset`).
    started: Instant,
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
    uploads: Vec<Upload>,
    next_upload: u64,
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
        Self::store_with_etag(inner, key, body, etag)
    }

    fn store_with_etag(inner: &mut Inner, key: &str, body: &[u8], etag: String) -> String {
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
                size: o.body.len() as u64,
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

    fn list_sized(&self, prefix: &str) -> Result<Vec<(String, u64)>, StoreError> {
        let out: Vec<(String, u64)> = self
            .inner
            .lock()
            .unwrap()
            .objects
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, o)| (k.clone(), o.body.len() as u64))
            .collect();
        self.lists
            .fetch_add(out.len().div_ceil(1000).max(1) as u64, Relaxed);
        status(200);
        Ok(out)
    }

    /// Parts are read one after the other (`concurrency` is not used). A failed read leaves
    /// the upload behind, as a crashed run leaves its parts in S3.
    fn put_multipart(
        &self,
        key: &str,
        source: &mut dyn PartSource,
        part_size: usize,
        _concurrency: usize,
    ) -> Result<MultipartDone, StoreError> {
        let size = part_size_for(source.len(), part_size);
        let n = part_count(source.len(), size);
        let id = {
            let mut inner = self.inner.lock().unwrap();
            inner.next_upload += 1;
            let (id, started) = (inner.next_upload, Instant::now() + inner.clock_offset);
            inner.uploads.push(Upload {
                id,
                key: key.to_string(),
                started,
            });
            id
        };
        self.count_put(0); // initiate
        let mut body = Vec::with_capacity(source.len() as usize);
        let mut part_sha256 = Vec::new();
        for i in 0..n {
            let part = read_part(source, size, i)?;
            self.count_put(part.len());
            part_sha256.push(sha256_of(&part));
            body.extend_from_slice(&part);
        }
        self.count_put(0); // complete
        let mut inner = self.inner.lock().unwrap();
        // The upload ends here: an abort that ran meanwhile took it away, and then it fails.
        let at = inner
            .uploads
            .iter()
            .position(|u| u.id == id)
            .ok_or_else(|| {
                StoreError::Permanent(format!("multipart upload of {key} was aborted"))
            })?;
        inner.uploads.remove(at);
        let mut all = Sha256::new();
        part_sha256.iter().for_each(|h| all.update(h));
        let etag = format!("{}-{n}", &hex::encode(all.finalize())[..32]);
        Self::store_with_etag(&mut inner, key, &body, etag.clone());
        status(200);
        Ok(MultipartDone { etag, part_sha256 })
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, StoreError> {
        let v = self.inner.lock().unwrap().objects.get(key).map(|o| {
            let from = (offset as usize).min(o.body.len());
            let to = from.saturating_add(len as usize).min(o.body.len());
            o.body[from..to].to_vec()
        });
        self.gets.fetch_add(1, Relaxed);
        self.bytes_down
            .fetch_add(v.as_ref().map_or(0, Vec::len) as u64, Relaxed);
        status(if v.is_some() { 206 } else { 404 });
        Ok(v)
    }

    fn abort_incomplete(&self, prefix: &str, older_than: Duration) -> Result<u64, StoreError> {
        let mut inner = self.inner.lock().unwrap();
        let now = Instant::now() + inner.clock_offset;
        let before = inner.uploads.len();
        inner.uploads.retain(|u| {
            !(u.key.starts_with(prefix) && now.saturating_duration_since(u.started) >= older_than)
        });
        let n = (before - inner.uploads.len()) as u64;
        self.lists.fetch_add(1, Relaxed);
        self.deletes.fetch_add(n, Relaxed);
        status(200);
        Ok(n)
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.deletes.fetch_add(1, Relaxed);
        let mut inner = self.inner.lock().unwrap();
        inner.objects.remove(key);
        inner.written.remove(key);
        status(204);
        Ok(())
    }

    fn delete_if_match(&self, key: &str, etag: &str) -> Result<bool, StoreError> {
        self.deletes.fetch_add(1, Relaxed);
        let mut inner = self.inner.lock().unwrap();
        match inner.objects.get(key) {
            Some(o) if o.etag != etag => {
                status(412);
                Ok(false)
            }
            _ => {
                inner.objects.remove(key);
                inner.written.remove(key);
                status(204);
                Ok(true)
            }
        }
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
    use crate::store::PartSource;
    use std::sync::Arc;

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

    const MIB: usize = 1024 * 1024;

    /// A source over a byte vector that records each read and can run a hook or fail one part.
    struct Probe<'a> {
        data: Vec<u8>,
        reads: Mutex<Vec<(u64, usize)>>,
        fail_at: Option<u64>,
        hook: Option<Box<dyn Fn(u64) + Send + Sync + 'a>>,
    }

    impl<'a> Probe<'a> {
        fn new(data: Vec<u8>) -> Self {
            Probe {
                data,
                reads: Mutex::new(Vec::new()),
                fail_at: None,
                hook: None,
            }
        }
    }

    impl PartSource for Probe<'_> {
        fn len(&self) -> u64 {
            self.data.len() as u64
        }
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
            self.reads.lock().unwrap().push((offset, buf.len()));
            if let Some(h) = &self.hook {
                h(offset);
            }
            if self.fail_at == Some(offset) {
                return Err(std::io::Error::other("injected read failure"));
            }
            buf.copy_from_slice(&self.data[offset as usize..offset as usize + buf.len()]);
            Ok(())
        }
    }

    fn pattern(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i % 251) as u8).collect()
    }

    /// A 40 MB object in 16 MiB parts: every part but the last is exactly 16 MiB, the object
    /// is invisible until the upload completes, and three ranges read it back.
    #[test]
    fn multipart_40mb_in_three_parts_reads_back_in_three_ranges() {
        let s = Arc::new(MemStore::new());
        let data = pattern(40_000_000);
        let mut src = Probe::new(data.clone());
        let seen = s.clone();
        src.hook = Some(Box::new(move |_| {
            assert!(
                seen.get("pack/1").unwrap().is_none(),
                "visible before complete"
            );
        }));
        let done = s.put_multipart("pack/1", &mut src, 16 * MIB, 8).unwrap();
        let mut reads = src.reads.lock().unwrap().clone();
        reads.sort();
        assert_eq!(
            reads,
            vec![
                (0, 16 * MIB),
                (16 * MIB as u64, 16 * MIB),
                (32 * MIB as u64, 40_000_000 - 32 * MIB)
            ]
        );
        assert_eq!(done.part_sha256.len(), 3);
        for (i, sha) in done.part_sha256.iter().enumerate() {
            let (o, l) = reads[i];
            let part = &data[o as usize..o as usize + l];
            assert_eq!(sha.as_slice(), Sha256::digest(part).as_slice(), "part {i}");
        }
        assert!(done.etag.ends_with("-3"), "{}", done.etag);
        assert_eq!(s.get("pack/1").unwrap().unwrap().etag, done.etag);
        let mut back = Vec::new();
        for i in 0..3u64 {
            let r = s
                .get_range("pack/1", i * 16 * MIB as u64, 16 * MIB as u64)
                .unwrap()
                .unwrap();
            back.extend(r);
        }
        assert_eq!(back, data);
        assert_eq!(s.stat("pack/1").unwrap().unwrap().size, 40_000_000);
        // Initiate, three parts and complete are five puts.
        assert_eq!(s.stats().puts, 5);
    }

    #[test]
    fn get_range_missing_key_is_none_and_past_the_end_is_cut() {
        let s = MemStore::new();
        assert_eq!(s.get_range("nope", 0, 10).unwrap(), None);
        s.put("k", b"0123456789").unwrap();
        assert_eq!(s.get_range("k", 2, 3).unwrap().unwrap(), b"234");
        assert_eq!(s.get_range("k", 8, 100).unwrap().unwrap(), b"89");
        assert_eq!(s.get_range("k", 10, 5).unwrap().unwrap(), b"");
        assert_eq!(s.stats().bytes_down, 5);
    }

    /// A part that fails leaves nothing visible; `abort_incomplete` with age 0 removes the
    /// upload, a second call finds none, and a completed upload is never a candidate.
    #[test]
    fn failed_part_leaves_an_upload_that_abort_removes() {
        let s = MemStore::new();
        s.put_multipart("pack/ok", &mut Probe::new(pattern(2500)), 1000, 2)
            .unwrap();
        let mut src = Probe::new(pattern(2500));
        src.fail_at = Some(1000);
        assert!(s.put_multipart("pack/bad", &mut src, 1000, 1).is_err());
        assert!(s.get("pack/bad").unwrap().is_none());
        assert!(s.list("pack/").unwrap() == ["pack/ok"]);
        assert_eq!(s.abort_incomplete("other/", Duration::ZERO).unwrap(), 0);
        assert_eq!(s.abort_incomplete("pack/", Duration::ZERO).unwrap(), 1);
        assert_eq!(s.abort_incomplete("pack/", Duration::ZERO).unwrap(), 0);
        assert!(s.get("pack/ok").unwrap().is_some());
    }

    #[test]
    fn abort_incomplete_ages_by_the_store_clock() {
        let s = MemStore::new();
        let mut src = Probe::new(pattern(100));
        src.fail_at = Some(0);
        assert!(s.put_multipart("p/k", &mut src, 50, 1).is_err());
        let hour = Duration::from_secs(3600);
        assert_eq!(
            s.abort_incomplete("p/", hour).unwrap(),
            0,
            "fresh upload stays"
        );
        s.advance_clock(2 * hour);
        assert_eq!(s.abort_incomplete("p/", hour).unwrap(), 1);
    }

    #[test]
    fn list_sized_returns_sizes_and_stat_has_size() {
        let s = MemStore::new();
        s.put("a/2", b"two").unwrap();
        s.put("a/1", b"1").unwrap();
        s.put("b/1", b"xxxx").unwrap();
        assert_eq!(
            s.list_sized("a/").unwrap(),
            vec![("a/1".to_string(), 1), ("a/2".to_string(), 3)]
        );
        assert_eq!(s.stat("b/1").unwrap().unwrap().size, 4);
        assert_eq!(s.stats().lists, 1);
    }

    #[test]
    fn empty_source_makes_one_empty_part() {
        let s = MemStore::new();
        let done = s
            .put_multipart("e", &mut Probe::new(Vec::new()), 1000, 4)
            .unwrap();
        assert_eq!(
            done.part_sha256,
            vec![<[u8; 32]>::from(Sha256::digest(b""))]
        );
        assert_eq!(s.get("e").unwrap().unwrap().body, b"");
    }
}
