//! Spec rev 11, "A new MVCC database is the one case that publishes a pack at once", rules 1 to
//! 3: two creators at one prefix, interleaved by a hook store at exact request boundaries, as
//! ADR 0009's racing-creators test does for pages. The slow creator's rule 1 is made by hand,
//! one request at a time, so its GET can come before the winner's head and its delete after.
use super::*;
use crate::store::{
    MultipartDone, Object, ObjectStat, ObjectStore, PartSource, StatsSnap, StoreError,
};
use std::sync::Mutex;

type Hook = Box<dyn FnOnce(&MemStore) + Send>;

/// An armed hook: the operation, the key suffix, after the call (else before), the hook.
type Armed = (&'static str, &'static str, bool, Option<Hook>);

/// Forwards every call to `inner`. A hook armed for (`op`, key suffix, before or after) runs
/// once, at the first such call, on `inner` (so the hook's own calls are not hooked).
struct Interleave {
    inner: Arc<MemStore>,
    hooks: Mutex<Vec<Armed>>,
}

impl Interleave {
    fn new(inner: Arc<MemStore>) -> Arc<Interleave> {
        Arc::new(Interleave {
            inner,
            hooks: Mutex::default(),
        })
    }
    fn before(
        &self,
        op: &'static str,
        suffix: &'static str,
        f: impl FnOnce(&MemStore) + Send + 'static,
    ) {
        self.hooks
            .lock()
            .unwrap()
            .push((op, suffix, false, Some(Box::new(f))));
    }
    fn after(
        &self,
        op: &'static str,
        suffix: &'static str,
        f: impl FnOnce(&MemStore) + Send + 'static,
    ) {
        self.hooks
            .lock()
            .unwrap()
            .push((op, suffix, true, Some(Box::new(f))));
    }
    fn fire(&self, op: &str, key: &str, after: bool) {
        let hook = {
            let mut hooks = self.hooks.lock().unwrap();
            hooks
                .iter_mut()
                .find(|(o, s, a, f)| *o == op && key.ends_with(s) && *a == after && f.is_some())
                .and_then(|h| h.3.take())
        };
        if let Some(f) = hook {
            f(&self.inner);
        }
    }
    fn around<T>(&self, op: &str, key: &str, call: impl FnOnce() -> T) -> T {
        self.fire(op, key, false);
        let r = call();
        self.fire(op, key, true);
        r
    }
}

impl ObjectStore for Interleave {
    fn put(&self, key: &str, body: &[u8]) -> std::result::Result<String, StoreError> {
        self.around("put", key, || self.inner.put(key, body))
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> std::result::Result<CondPut, StoreError> {
        self.around("put_if_absent", key, || self.inner.put_if_absent(key, body))
    }
    fn put_if_match(
        &self,
        key: &str,
        body: &[u8],
        etag: &str,
    ) -> std::result::Result<CondPut, StoreError> {
        self.around("put_if_match", key, || {
            self.inner.put_if_match(key, body, etag)
        })
    }
    fn get(&self, key: &str) -> std::result::Result<Option<Object>, StoreError> {
        self.around("get", key, || self.inner.get(key))
    }
    fn stat(&self, key: &str) -> std::result::Result<Option<ObjectStat>, StoreError> {
        self.inner.stat(key)
    }
    fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StoreError> {
        self.inner.list(prefix)
    }
    fn list_after(
        &self,
        prefix: &str,
        after: &str,
    ) -> std::result::Result<Vec<String>, StoreError> {
        self.inner.list_after(prefix, after)
    }
    fn list_sized(
        &self,
        prefix: &str,
    ) -> std::result::Result<Vec<crate::store::Listed>, StoreError> {
        self.inner.list_sized(prefix)
    }
    fn put_multipart(
        &self,
        key: &str,
        source: &mut dyn PartSource,
        part_size: usize,
        concurrency: usize,
    ) -> std::result::Result<MultipartDone, StoreError> {
        self.inner
            .put_multipart(key, source, part_size, concurrency)
    }
    fn get_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> std::result::Result<Option<Vec<u8>>, StoreError> {
        self.inner.get_range(key, offset, len)
    }
    fn abort_incomplete(
        &self,
        prefix: &str,
        older_than: Duration,
    ) -> std::result::Result<u64, StoreError> {
        self.inner.abort_incomplete(prefix, older_than)
    }
    fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
        self.inner.delete(key)
    }
    fn delete_if_match(&self, key: &str, etag: &str) -> std::result::Result<bool, StoreError> {
        self.around("delete_if_match", key, || {
            self.inner.delete_if_match(key, etag)
        })
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

/// What a slow creator's rule 1 read: the pointer's ETag, and whether a head existed.
type Seen = Arc<Mutex<Option<(String, bool)>>>;

/// The slow creator's first two requests of rule 1, on `inner`: GET the pointer (keep its
/// ETag), GET the head.
fn slow_reads(inner: &MemStore, seen: &Seen) {
    let pointer = inner
        .get(&keys().pack_pointer())
        .unwrap()
        .expect("the first pointer");
    let head = inner.get(&keys().head()).unwrap().is_some();
    *seen.lock().unwrap() = Some((pointer.etag, head));
}

/// The slow creator's last request of rule 1: it saw no head, so it deletes the pointer with
/// `If-Match` on the ETag it read. Returns whether the delete went through.
fn slow_delete(inner: &MemStore, seen: &Seen) -> bool {
    let (etag, head) = seen.lock().unwrap().clone().expect("the reads ran");
    assert!(
        !head,
        "the slow creator read the head before the winner's landed"
    );
    inner
        .delete_if_match(&keys().pack_pointer(), &etag)
        .unwrap()
}

/// The winner's MVCC database is whole: its pointer names its own pack, and a restore from
/// the bucket reads that pack and opens.
fn winner_restores_from_its_pack(inner: &Arc<MemStore>) {
    let (p, _) = first_pack(&**inner);
    let t = tempdir();
    let rep = restore_with_store(&mvcc_config(&fresh(&t)), inner.clone(), &fresh(&t))
        .unwrap_or_else(|e| panic!("restore: {e}"));
    assert_eq!(rep.pack_key, Some(p.pack_key));
    let db = open_read_only_with_store(mvcc_config(&fresh(&t)), inner.clone()).unwrap();
    db.close().unwrap();
}

/// The slow creator GETs the winner's first pointer before the winner's head lands, and sends
/// its delete after the winner's rewrite (rule 2): the rewrite has a new `write_id`, so other
/// bytes and another ETag, and the delete fails with 412. The store's ETags are the MD5 of the
/// body, as on S3, so a rewrite with the same bytes would keep the ETag and the delete would
/// remove it.
#[test]
fn slow_create_delete_after_the_winners_rewrite_fails_with_412() {
    let inner = Arc::new(MemStore::with_content_etags());
    let store = Interleave::new(inner.clone());
    let seen: Seen = Arc::default();
    let s = seen.clone();
    store.before("put_if_absent", "/head", move |m| slow_reads(m, &s));
    let t = tempdir();
    create_mvcc(store.clone(), &mvcc_config(&fresh(&t))).unwrap();
    assert!(!slow_delete(&inner, &seen), "the late delete got a 412");
    winner_restores_from_its_pack(&inner);
}

/// The same reads, and the delete sent right after the winner's head landed, before its
/// rewrite: the delete goes through, and the rewrite (`If-None-Match`, as no pointer exists)
/// puts the pointer back.
#[test]
fn slow_create_delete_before_the_winners_rewrite_is_undone() {
    let inner = Arc::new(MemStore::with_content_etags());
    let store = Interleave::new(inner.clone());
    let seen: Seen = Arc::default();
    let s = seen.clone();
    store.before("put_if_absent", "/head", move |m| slow_reads(m, &s));
    let s = seen.clone();
    store.after("put_if_absent", "/head", move |m| {
        assert!(
            slow_delete(m, &s),
            "the delete before the rewrite goes through"
        );
    });
    let t = tempdir();
    create_mvcc(store.clone(), &mvcc_config(&fresh(&t))).unwrap();
    winner_restores_from_its_pack(&inner);
}

/// A WAL create and an MVCC create race. The MVCC creator wrote its pointer; the WAL create
/// runs whole before the MVCC head: its rule 1 finds no head and deletes that pointer, its head
/// lands, and the MVCC head gets a 412 (`Config`). Then a late pointer of the loser appears
/// after the WAL create's rule 2 (as a creator slower still would write it): its `database_id`
/// is not the head's, so a restore reads the log from the first generation and the packer
/// replaces it. With the pointer written between the WAL head and its rule 2 instead, rule 2
/// deletes it. The winner has its own pointer or none.
#[test]
fn wal_and_mvcc_creates_race_and_the_winner_has_its_own_pointer_or_none() {
    let inner = mem();
    let store = Interleave::new(inner.clone());
    let wal_dir = tempdir();
    let wal_cfg = config(&fresh(&wal_dir));
    let (store2, cfg2) = (inner.clone(), wal_cfg.clone());
    let wal_db: Arc<Mutex<Option<Database>>> = Arc::default();
    let slot = wal_db.clone();
    store.before("put_if_absent", "/head", move |_| {
        let db = open_with_store(cfg2, store2).unwrap();
        *slot.lock().unwrap() = Some(db);
    });
    let t = tempdir();
    let r = create_mvcc(store.clone(), &mvcc_config(&fresh(&t))).map(|(_, s)| s);
    assert_eq!(
        r,
        Err(Error::Config(
            "a database already exists at this prefix".into()
        ))
    );
    assert!(
        inner.get(&keys().pack_pointer()).unwrap().is_none(),
        "the WAL create removed the MVCC pointer"
    );
    let db = wal_db.lock().unwrap().take().unwrap();
    assert_eq!(head(&*inner).journal_mode, JournalMode::Wal);
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=3 {
        insert(&c, id).unwrap();
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    insert(&c, 4).unwrap();
    let want = ids(&c);
    // The loser's late pointer.
    let other = mem();
    let to = tempdir();
    create_mvcc(other.clone(), &mvcc_config(&fresh(&to))).unwrap();
    let late = other.get(&keys().pack_pointer()).unwrap().unwrap().body;
    inner.put(&keys().pack_pointer(), &late).unwrap();
    let t2 = tempdir();
    let rep = restore_with_store(&config(&fresh(&t2)), inner.clone(), &fresh(&t2)).unwrap();
    assert_eq!(
        rep.pack_key, None,
        "a pointer of another database_id is no pointer"
    );
    let ro = open_read_only_with_store(config(&fresh(&t2)), inner.clone()).unwrap();
    let rc = ro.connect().unwrap();
    assert_eq!(ids(&rc), want);
    rc.close().unwrap();
    ro.close().unwrap();
    let t3 = tempdir();
    let r =
        crate::pack::run::pack_with_store(&config(&t3.path().join("p")), inner.clone()).unwrap();
    assert_eq!(r.outcome, crate::pack::run::PackOutcome::Packed);
    c.close().unwrap();
    db.close().unwrap();

    // The loser's pointer between the WAL head and the WAL create's rule 2: deleted.
    let inner = mem();
    let store = Interleave::new(inner.clone());
    let late2 = late.clone();
    store.after("put_if_absent", "/head", move |m| {
        m.put(&keys().pack_pointer(), &late2).unwrap();
    });
    let t4 = tempdir();
    let db = open_with_store(config(&fresh(&t4)), store.clone()).unwrap();
    assert!(
        inner.get(&keys().pack_pointer()).unwrap().is_none(),
        "rule 2 deleted the pointer that appeared"
    );
    db.close().unwrap();
}

/// Rule 1 of a WAL create: a pointer left by an MVCC create that crashed after its pointer
/// is deleted before the WAL head is written, so no moment has a WAL head next to it.
#[test]
fn wal_create_removes_a_leftover_pointer_before_its_head() {
    let inner = mem();
    let fault = Arc::new(FaultStore::new(inner.clone()));
    fault.fail_puts_after(3);
    let t = tempdir();
    let mut c = mvcc_config(&fresh(&t));
    c.retry_deadline = Duration::from_millis(100);
    assert!(create_mvcc(fault.clone(), &c).is_err());
    assert!(inner.get(&keys().pack_pointer()).unwrap().is_some());
    let store = Interleave::new(inner.clone());
    let at_head: Arc<Mutex<Option<bool>>> = Arc::default();
    let seen = at_head.clone();
    store.after("put_if_absent", "/head", move |m| {
        *seen.lock().unwrap() = Some(m.get(&keys().pack_pointer()).unwrap().is_some());
    });
    let t2 = tempdir();
    let db = open_with_store(config(&fresh(&t2)), store.clone()).unwrap();
    assert_eq!(
        *at_head.lock().unwrap(),
        Some(false),
        "no pointer when the head landed"
    );
    db.close().unwrap();
}
