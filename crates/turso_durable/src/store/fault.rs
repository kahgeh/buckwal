// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! Wraps any store and injects put failures on demand, to test that a failed upload is never
//! acknowledged and that ambiguous (stored, then errored) puts are handled.
use super::{CondPut, Object, ObjectStat, ObjectStore, StatsSnap, StoreError};
use std::sync::atomic::{
    AtomicU64,
    Ordering::{Relaxed, SeqCst},
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

enum Verdict {
    Pass,
    Fail(StoreError),
    StoreThenFail(StoreError),
}

pub struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    fail_next: AtomicU64,
    fail_next_permanent: AtomicU64,
    ambiguous_next: AtomicU64,
    ok_left: AtomicU64,
    only_keys: Mutex<String>,
    delay: Mutex<Duration>,
    get_delay: Mutex<Duration>,
    events: Mutex<Vec<String>>,
}

fn take_one(c: &AtomicU64) -> bool {
    c.fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1)).is_ok()
}

impl FaultStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            fail_next: AtomicU64::new(0),
            fail_next_permanent: AtomicU64::new(0),
            ambiguous_next: AtomicU64::new(0),
            ok_left: AtomicU64::new(u64::MAX),
            only_keys: Mutex::new(String::new()),
            delay: Mutex::new(Duration::ZERO),
            get_delay: Mutex::new(Duration::ZERO),
            events: Mutex::new(Vec::new()),
        }
    }
    /// The next `n` puts fail transiently; nothing is stored.
    pub fn fail_next_puts(&self, n: u64) {
        self.fail_next.store(n, Relaxed);
    }
    /// The next `n` puts fail permanently (like HTTP 403); nothing is stored.
    pub fn fail_next_puts_permanently(&self, n: u64) {
        self.fail_next_permanent.store(n, Relaxed);
    }
    /// `ok` more puts succeed, then every put fails. `u64::MAX` means never.
    pub fn fail_puts_after(&self, ok: u64) {
        self.ok_left.store(ok, Relaxed);
    }
    /// The next `n` puts are stored, then report a transient error (ambiguous outcome).
    pub fn store_then_fail_next_puts(&self, n: u64) {
        self.ambiguous_next.store(n, Relaxed);
    }
    /// Faults and delays apply only to keys containing `s` ("" = all keys).
    pub fn only_keys_containing(&self, s: &str) {
        *self.only_keys.lock().unwrap() = s.to_string();
    }
    /// Sleep `d` before each matching put.
    pub fn delay_puts(&self, d: Duration) {
        *self.delay.lock().unwrap() = d;
    }
    /// Sleep `d` before each get of a matching key.
    pub fn delay_gets(&self, d: Duration) {
        *self.get_delay.lock().unwrap() = d;
    }
    /// Every call in order, e.g. "put k", "get k", "list p/"; attempts failed by injection included.
    pub fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }

    fn record(&self, what: &str, key: &str) {
        self.events.lock().unwrap().push(format!("{what} {key}"));
    }

    fn verdict(&self, key: &str) -> Verdict {
        if !key.contains(self.only_keys.lock().unwrap().as_str()) {
            return Verdict::Pass;
        }
        let d = *self.delay.lock().unwrap();
        if !d.is_zero() {
            std::thread::sleep(d);
        }
        let injected = || StoreError::Transient(format!("injected put failure for {key}"));
        if take_one(&self.fail_next_permanent) {
            return Verdict::Fail(StoreError::Permanent(format!(
                "injected permanent put failure for {key}"
            )));
        }
        if take_one(&self.fail_next) {
            return Verdict::Fail(injected());
        }
        if take_one(&self.ambiguous_next) {
            return Verdict::StoreThenFail(injected());
        }
        // u64::MAX means "never fail"; otherwise claim one of the remaining successes.
        let claimed = self.ok_left.fetch_update(SeqCst, SeqCst, |n| {
            if n == u64::MAX {
                Some(n)
            } else {
                n.checked_sub(1)
            }
        });
        if claimed.is_err() {
            return Verdict::Fail(injected());
        }
        Verdict::Pass
    }

    /// Shared path for the three put flavours.
    fn faulty<T>(
        &self,
        what: &str,
        key: &str,
        op: impl FnOnce() -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        self.record(what, key);
        match self.verdict(key) {
            Verdict::Pass => op(),
            Verdict::Fail(e) => Err(e),
            Verdict::StoreThenFail(e) => {
                let _ = op();
                Err(e)
            }
        }
    }
}

impl ObjectStore for FaultStore {
    fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.faulty("put", key, || self.inner.put(key, body))
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> Result<CondPut, StoreError> {
        self.faulty("put_if_absent", key, || self.inner.put_if_absent(key, body))
    }
    fn put_if_match(&self, key: &str, body: &[u8], etag: &str) -> Result<CondPut, StoreError> {
        self.faulty("put_if_match", key, || {
            self.inner.put_if_match(key, body, etag)
        })
    }
    fn get(&self, key: &str) -> Result<Option<Object>, StoreError> {
        self.record("get", key);
        let d = *self.get_delay.lock().unwrap();
        if !d.is_zero() && key.contains(self.only_keys.lock().unwrap().as_str()) {
            std::thread::sleep(d);
        }
        self.inner.get(key)
    }
    fn stat(&self, key: &str) -> Result<Option<ObjectStat>, StoreError> {
        self.record("stat", key);
        let d = *self.get_delay.lock().unwrap();
        if !d.is_zero() && key.contains(self.only_keys.lock().unwrap().as_str()) {
            std::thread::sleep(d);
        }
        self.inner.stat(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.record("list", prefix);
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.record("delete", key);
        self.inner.delete(key)
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

/// Test-only: a store that holds one PUT (any flavour) of a key containing the armed text
/// until the test releases it, as if its sender were paused (SIGSTOP) after its lease check,
/// then passes it on and records how it ended.
/// An armed hold: the key text, where to say a PUT is held, and what lets it go on.
#[cfg(test)]
type Hold = (
    String,
    std::sync::mpsc::Sender<String>,
    std::sync::mpsc::Receiver<()>,
);

#[cfg(test)]
pub(crate) struct HoldStore {
    inner: Arc<dyn ObjectStore>,
    armed: Mutex<Option<Hold>>,
    outcomes: Mutex<Vec<String>>,
}

#[cfg(test)]
impl HoldStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            armed: Mutex::new(None),
            outcomes: Mutex::new(Vec::new()),
        }
    }

    /// Hold the next PUT of a key containing `s`. The first receiver gets its key when it is
    /// held; a send on the returned sender lets it go on.
    pub(crate) fn hold_next_put(
        &self,
        s: &str,
    ) -> (
        std::sync::mpsc::Receiver<String>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        *self.armed.lock().unwrap() = Some((s.to_string(), held_tx, go_rx));
        (held_rx, go_tx)
    }

    /// How each held PUT ended, e.g. "put_if_absent k: 412" or "put k: stored".
    pub(crate) fn outcomes(&self) -> Vec<String> {
        self.outcomes.lock().unwrap().clone()
    }

    fn held<T>(
        &self,
        what: &str,
        key: &str,
        op: impl FnOnce() -> Result<T, StoreError>,
        ended: impl Fn(&T) -> String,
    ) -> Result<T, StoreError> {
        let hold = {
            let mut armed = self.armed.lock().unwrap();
            match armed.as_ref() {
                Some((s, _, _)) if key.contains(s.as_str()) => armed.take(),
                _ => None,
            }
        };
        let Some((_, held_tx, go_rx)) = hold else {
            return op();
        };
        let _ = held_tx.send(key.to_string());
        let _ = go_rx.recv();
        let r = op();
        let how = match &r {
            Ok(v) => ended(v),
            Err(e) => format!("{e}"),
        };
        self.outcomes
            .lock()
            .unwrap()
            .push(format!("{what} {key}: {how}"));
        r
    }
}

#[cfg(test)]
fn cond_outcome(r: &CondPut) -> String {
    match r {
        CondPut::Created { .. } => "stored".into(),
        CondPut::PreconditionFailed => "412".into(),
    }
}

#[cfg(test)]
impl ObjectStore for HoldStore {
    fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.held(
            "put",
            key,
            || self.inner.put(key, body),
            |_| "stored".into(),
        )
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> Result<CondPut, StoreError> {
        self.held(
            "put_if_absent",
            key,
            || self.inner.put_if_absent(key, body),
            cond_outcome,
        )
    }
    fn put_if_match(&self, key: &str, body: &[u8], etag: &str) -> Result<CondPut, StoreError> {
        self.held(
            "put_if_match",
            key,
            || self.inner.put_if_match(key, body, etag),
            cond_outcome,
        )
    }
    fn get(&self, key: &str) -> Result<Option<Object>, StoreError> {
        self.inner.get(key)
    }
    fn stat(&self, key: &str) -> Result<Option<ObjectStat>, StoreError> {
        self.inner.stat(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemStore;

    fn setup() -> (Arc<MemStore>, FaultStore) {
        let m = Arc::new(MemStore::new());
        (m.clone(), FaultStore::new(m))
    }

    #[test]
    fn fault_store_ambiguous_put_is_stored() {
        let (_, s) = setup();
        s.store_then_fail_next_puts(1);
        assert!(matches!(
            s.put_if_absent("k", b"body"),
            Err(StoreError::Transient(_))
        ));
        assert_eq!(s.get("k").unwrap().unwrap().body, b"body");
        // Only one was ambiguous.
        assert!(s.put("k2", b"v").is_ok());
    }

    #[test]
    fn fault_store_counts_exactly_under_concurrency() {
        let (_, s) = setup();
        let s = Arc::new(s);
        s.fail_puts_after(100);
        let ok: u64 = (0..8)
            .map(|t| {
                let s = s.clone();
                std::thread::spawn(move || {
                    (0..50)
                        .filter(|i| s.put(&format!("k{t}-{i}"), b"v").is_ok())
                        .count() as u64
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .sum();
        assert_eq!(ok, 100);
        assert!(s.put("after", b"v").is_err());
    }

    #[test]
    fn fault_store_fails_then_recovers_nothing_stored() {
        let (m, s) = setup();
        s.fail_next_puts(1);
        assert!(matches!(s.put("k", b"v"), Err(StoreError::Transient(_))));
        assert!(m.get("k").unwrap().is_none());
        s.put("k", b"v").unwrap();
    }

    #[test]
    fn fault_store_permanent_failure() {
        let (m, s) = setup();
        s.fail_next_puts_permanently(1);
        assert!(matches!(
            s.put_if_match("k", b"v", "e"),
            Err(StoreError::Permanent(_))
        ));
        assert!(m.get("k").unwrap().is_none());
        assert!(s.put("k", b"v").is_ok());
    }

    #[test]
    fn fault_store_key_filter_and_events() {
        let (m, s) = setup();
        s.only_keys_containing("wal");
        s.fail_next_puts(1);
        // Non-matching key neither fails nor consumes the fault.
        s.put("manifest", b"v").unwrap();
        assert!(s.put_if_absent("wal/1", b"v").is_err());
        s.put_if_absent("wal/1", b"v").unwrap();
        s.get("manifest").unwrap();
        s.list("wal/").unwrap();
        s.delete("manifest").unwrap();
        assert_eq!(
            s.events(),
            vec![
                "put manifest",
                "put_if_absent wal/1",
                "put_if_absent wal/1",
                "get manifest",
                "list wal/",
                "delete manifest",
            ]
        );
        assert!(m.get("wal/1").unwrap().is_some());
    }

    #[test]
    fn fault_store_delay_only_matching() {
        let (_, s) = setup();
        s.only_keys_containing("slow");
        s.delay_puts(Duration::from_millis(60));
        let t = std::time::Instant::now();
        s.put("fast", b"v").unwrap();
        assert!(t.elapsed() < Duration::from_millis(40));
        s.put("slow", b"v").unwrap();
        assert!(t.elapsed() >= Duration::from_millis(60));
    }

    #[test]
    fn fault_store_stat_records_and_delays_like_get() {
        let (m, s) = setup();
        m.put("slow", b"v").unwrap();
        s.only_keys_containing("slow");
        s.delay_gets(Duration::from_millis(60));
        let t = std::time::Instant::now();
        assert!(s.stat("fast").unwrap().is_none());
        assert!(t.elapsed() < Duration::from_millis(40));
        assert!(s.stat("slow").unwrap().is_some());
        assert!(t.elapsed() >= Duration::from_millis(60));
        assert_eq!(s.events(), vec!["stat fast", "stat slow"]);
    }
}
