//! Registry of Hrana streams. Each stream owns a `turso_durable::Connection` and is
//! addressed by a baton `"{id}.{secret}"` that rotates on every use.
//!
//! An anchor connection is held from `Streams::new` until `close_all`: the last connection
//! to close checkpoints the WAL, which seals and uploads pages, so without the anchor every
//! stream that happened to be the only one open would trigger that on close.
//!
//! Batons are never logged. Connections are closed outside the map's lock.
use crate::exec::{open_stream, StreamState};
use rustls::crypto::ring::default_provider;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use subtle::ConstantTimeEq;
use turso_durable::{Connection, Database};

#[derive(Clone, Debug)]
pub struct StreamsCfg {
    /// Idle time after which a stream is closed.
    pub timeout: Duration,
    /// `PRAGMA busy_timeout` of each stream.
    pub busy: Duration,
    /// Most streams, idle and busy together.
    pub max: usize,
}

/// A stream taken out of the registry. It holds no secret.
pub struct Leased {
    pub id: u64,
    pub state: StreamState,
}

impl std::fmt::Debug for Leased {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Leased")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

// `Leased` is moved once per request; boxing it would only add an allocation.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Checkout {
    Stream(Leased),
    /// The baton is unknown, stale, forged, malformed, or its stream is busy or timed out.
    Expired,
    /// The stream cap is reached.
    Full,
    /// A new stream could not be opened, or the registry is shutting down.
    Failed(turso_durable::Error),
}

// Busy is a short-lived marker in a map of mostly idle streams.
#[allow(clippy::large_enum_variant)]
enum Slot {
    Idle {
        state: StreamState,
        secret: String,
        last_used: Instant,
    },
    Busy,
}

pub struct Streams {
    db: Database,
    cfg: StreamsCfg,
    slots: Mutex<HashMap<u64, Slot>>,
    next_id: AtomicU64,
    anchor: Mutex<Option<Connection>>,
    /// Set under the slot lock by `close_all`; read under it.
    closing: AtomicBool,
}

fn new_secret() -> String {
    let mut buf = [0u8; 16];
    // The ring provider's RNG failing means the platform RNG is gone; no safe baton exists.
    default_provider()
        .secure_random
        .fill(&mut buf)
        .expect("system random number generator failed");
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

fn parse_baton(baton: &str) -> Option<(u64, &str)> {
    let (id, secret) = baton.split_once('.')?;
    let canonical = !id.is_empty()
        && id.bytes().all(|b| b.is_ascii_digit())
        && (id == "0" || !id.starts_with('0'));
    if !canonical {
        return None;
    }
    Some((id.parse().ok()?, secret))
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn warn_close(e: &turso_durable::Error) {
    tracing::warn!(target: "turso_durable::server", error = %e, "stream close failed");
}

fn close_state(st: StreamState) {
    if let Err(e) = st.conn.close() {
        warn_close(&e);
    }
}

impl Streams {
    pub fn new(db: Database, cfg: StreamsCfg) -> turso_durable::Result<Streams> {
        let anchor = db.connect()?;
        Ok(Streams {
            db,
            cfg,
            slots: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            anchor: Mutex::new(Some(anchor)),
            closing: AtomicBool::new(false),
        })
    }

    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Streams currently checked out.
    pub fn busy(&self) -> usize {
        lock(&self.slots)
            .values()
            .filter(|s| matches!(s, Slot::Busy))
            .count()
    }

    /// Blocking when `baton` is `None`: a new stream connects.
    pub fn checkout(&self, baton: Option<&str>) -> Checkout {
        match baton {
            None => self.open_new(),
            Some(b) => self.take(b),
        }
    }

    fn open_new(&self) -> Checkout {
        let id = {
            let mut slots = lock(&self.slots);
            if self.closing.load(Ordering::SeqCst) {
                return Checkout::Failed(turso_durable::Error::Config(
                    "the server is shutting down".into(),
                ));
            }
            if slots.len() >= self.cfg.max {
                return Checkout::Full;
            }
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            slots.insert(id, Slot::Busy);
            id
        };
        match open_stream(&self.db, self.cfg.busy) {
            Ok(state) => Checkout::Stream(Leased { id, state }),
            Err(e) => {
                lock(&self.slots).remove(&id);
                Checkout::Failed(e)
            }
        }
    }

    fn take(&self, baton: &str) -> Checkout {
        let Some((id, secret)) = parse_baton(baton) else {
            return Checkout::Expired;
        };
        let mut stale = None;
        let out = {
            let mut slots = lock(&self.slots);
            if self.closing.load(Ordering::SeqCst) {
                return Checkout::Expired;
            }
            match slots.get(&id) {
                Some(Slot::Idle {
                    secret: want,
                    last_used,
                    ..
                }) => {
                    let ok: bool = want.as_bytes().ct_eq(secret.as_bytes()).into();
                    if !ok {
                        Checkout::Expired
                    } else if last_used.elapsed() >= self.cfg.timeout {
                        stale = slots.remove(&id);
                        Checkout::Expired
                    } else {
                        match slots.insert(id, Slot::Busy) {
                            Some(Slot::Idle { state, .. }) => {
                                Checkout::Stream(Leased { id, state })
                            }
                            _ => Checkout::Expired,
                        }
                    }
                }
                _ => Checkout::Expired,
            }
        };
        if let Some(Slot::Idle { state, .. }) = stale {
            close_state(state);
        }
        out
    }

    /// The new baton, or `None` when the stream is over (its connection is then closed).
    pub fn give_back(&self, leased: Leased) -> Option<String> {
        let Leased { id, state } = leased;
        // One lock acquisition decides and acts: a `close_all` between a separate check and
        // insert would leave a live connection in the map after shutdown.
        let mut slots = lock(&self.slots);
        if state.closed || self.closing.load(Ordering::SeqCst) {
            slots.remove(&id);
            drop(slots);
            close_state(state);
            return None;
        }
        let secret = new_secret();
        let baton = format!("{id}.{secret}");
        slots.insert(
            id,
            Slot::Idle {
                state,
                secret,
                last_used: Instant::now(),
            },
        );
        Some(baton)
    }

    /// Close idle streams past the timeout; returns how many.
    pub fn reap(&self) -> usize {
        let dead = {
            let mut slots = lock(&self.slots);
            let ids: Vec<u64> = slots
                .iter()
                .filter_map(|(id, s)| match s {
                    Slot::Idle { last_used, .. } if last_used.elapsed() >= self.cfg.timeout => {
                        Some(*id)
                    }
                    _ => None,
                })
                .collect();
            ids.into_iter()
                .filter_map(|id| slots.remove(&id))
                .collect::<Vec<_>>()
        };
        let n = dead.len();
        for s in dead {
            if let Slot::Idle { state, .. } = s {
                close_state(state);
            }
        }
        n
    }

    /// Refuse new work, then close idle streams and the anchor; returns how many streams.
    /// Busy streams are closed by their holders' `give_back`.
    pub fn close_all(&self) -> usize {
        {
            // Under the slot lock, so `give_back` sees the flag and the map consistently.
            let _slots = lock(&self.slots);
            self.closing.store(true, Ordering::SeqCst);
        }
        self.close_connections()
    }

    /// Close idle streams and the anchor without touching `closing`.
    fn close_connections(&self) -> usize {
        let idle = {
            let mut slots = lock(&self.slots);
            let ids: Vec<u64> = slots
                .iter()
                .filter(|(_, s)| matches!(s, Slot::Idle { .. }))
                .map(|(id, _)| *id)
                .collect();
            ids.into_iter()
                .filter_map(|id| slots.remove(&id))
                .collect::<Vec<_>>()
        };
        let n = idle.len();
        for s in idle {
            if let Slot::Idle { state, .. } = s {
                close_state(state);
            }
        }
        let anchor = lock(&self.anchor).take();
        if let Some(a) = anchor {
            if let Err(e) = a.close() {
                warn_close(&e);
            }
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{execute_request, Fence, Version};
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;
    use turso_durable::store::{MemStore, ObjectStore};
    use turso_durable::{open_with_store, Config};

    fn config(dir: &std::path::Path) -> Config {
        let mut c = Config::new(dir, "http://127.0.0.1:1", "b", "k", "s");
        c.retry_deadline = Duration::from_secs(2);
        c.request_timeout = Duration::from_millis(500);
        c.lease = Duration::from_secs(5);
        c
    }

    struct Fx {
        _dir: TempDir,
        store: Arc<MemStore>,
        streams: Streams,
    }

    fn fx_with(timeout_ms: u64, max: usize) -> Fx {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(MemStore::new());
        let db = open_with_store(config(&dir.path().join("db")), store.clone()).unwrap();
        let streams = Streams::new(
            db,
            StreamsCfg {
                timeout: Duration::from_millis(timeout_ms),
                busy: Duration::from_millis(200),
                max,
            },
        )
        .unwrap();
        Fx {
            _dir: dir,
            store,
            streams,
        }
    }

    fn fx() -> Fx {
        fx_with(60_000, 10)
    }

    fn lease(c: Checkout) -> Leased {
        match c {
            Checkout::Stream(l) => l,
            other => panic!("expected a stream, got {other:?}"),
        }
    }

    fn run(l: &mut Leased, sql: &str) -> serde_json::Value {
        let r = execute_request(
            &mut l.state,
            Version::V3,
            json!({"type":"execute","stmt":{"sql":sql}}),
            &Fence::new(),
        );
        serde_json::to_value(r).unwrap()
    }

    fn is_expired(c: Checkout) -> bool {
        matches!(c, Checkout::Expired)
    }

    #[test]
    fn new_stream_then_rotating_batons() {
        let f = fx();
        let l = lease(f.streams.checkout(None));
        let b1 = f.streams.give_back(l).unwrap();
        let l = lease(f.streams.checkout(Some(&b1)));
        let b2 = f.streams.give_back(l).unwrap();
        assert_ne!(b1, b2);
        assert!(is_expired(f.streams.checkout(Some(&b1))));
        assert!(matches!(f.streams.checkout(Some(&b2)), Checkout::Stream(_)));
    }

    #[test]
    fn forged_and_malformed_batons_expire() {
        let f = fx();
        let l = lease(f.streams.checkout(None));
        let b = f.streams.give_back(l).unwrap();
        let (id, secret) = b.split_once('.').unwrap();
        assert_eq!(secret.len(), 32);
        let zeros = "0".repeat(32);
        for bad in [
            format!("{id}.{zeros}"),
            format!("{id}.{}", &secret[..31]),
            format!("{id}.{secret}00"),
            format!("{id}."),
            id.to_string(),
            "x".to_string(),
            String::new(),
            format!("999.{secret}"),
            format!("-1.{secret}"),
            format!("+1.{secret}"),
            format!("01.{secret}"),
            format!("x.{secret}"),
            format!(".{secret}"),
        ] {
            assert!(is_expired(f.streams.checkout(Some(&bad))), "{bad}");
        }
        // The real baton still works after the forgeries.
        assert!(matches!(f.streams.checkout(Some(&b)), Checkout::Stream(_)));
    }

    #[test]
    fn busy_stream_is_not_reaped_and_concurrent_checkout_expires() {
        let f = fx_with(20, 10);
        let l = lease(f.streams.checkout(None));
        let b = f.streams.give_back(l).unwrap();
        let l = lease(f.streams.checkout(Some(&b)));
        assert_eq!(f.streams.busy(), 1);
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(f.streams.reap(), 0);
        assert!(is_expired(f.streams.checkout(Some(&b))));
        let id = l.id;
        let b2 = f.streams.give_back(l).unwrap();
        assert!(b2.starts_with(&format!("{id}.")));
        assert_eq!(f.streams.busy(), 0);
    }

    #[test]
    fn cap_counts_busy_and_idle() {
        let f = fx_with(60_000, 2);
        let a = lease(f.streams.checkout(None));
        let b = lease(f.streams.checkout(None));
        assert!(matches!(f.streams.checkout(None), Checkout::Full));
        f.streams.give_back(a).unwrap();
        // One idle, one busy: still full.
        assert!(matches!(f.streams.checkout(None), Checkout::Full));
        drop(b);
    }

    #[test]
    fn closed_state_returns_no_baton_and_frees_the_slot() {
        let f = fx_with(60_000, 1);
        let mut l = lease(f.streams.checkout(None));
        assert!(matches!(f.streams.checkout(None), Checkout::Full));
        l.state.closed = true;
        assert!(f.streams.give_back(l).is_none());
        assert!(matches!(f.streams.checkout(None), Checkout::Stream(_)));
    }

    #[test]
    fn abandoned_transaction_is_rolled_back_after_timeout() {
        let f = fx_with(100, 10);
        let mut s = lease(f.streams.checkout(None));
        run(&mut s, "CREATE TABLE t(v TEXT)");
        let b0 = f.streams.give_back(s).unwrap();
        let mut a = lease(f.streams.checkout(Some(&b0)));
        assert_eq!(run(&mut a, "BEGIN")["type"], "ok");
        assert_eq!(run(&mut a, "INSERT INTO t VALUES ('x')")["type"], "ok");
        let ba = f.streams.give_back(a).unwrap();
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(f.streams.reap(), 1);
        let mut b = lease(f.streams.checkout(None));
        assert_eq!(run(&mut b, "INSERT INTO t VALUES ('y')")["type"], "ok");
        let r = run(&mut b, "SELECT v FROM t");
        let rows = r["response"]["result"]["rows"].as_array().unwrap().clone();
        assert_eq!(rows.len(), 1, "{r}");
        assert_eq!(rows[0][0]["value"], "y");
        assert!(is_expired(f.streams.checkout(Some(&ba))));
        f.streams.give_back(b).unwrap();
    }

    fn page_keys(f: &Fx) -> Vec<String> {
        f.store.list("pages/").unwrap()
    }

    #[test]
    fn anchor_keeps_stream_close_from_checkpointing() {
        let f = fx();
        let mut l = lease(f.streams.checkout(None));
        assert_eq!(run(&mut l, "CREATE TABLE t(v TEXT)")["type"], "ok");
        assert_eq!(run(&mut l, "INSERT INTO t VALUES ('x')")["type"], "ok");
        l.state.closed = true;
        f.streams.give_back(l);
        let pages = page_keys(&f);
        for _ in 0..3 {
            let mut l = lease(f.streams.checkout(None));
            assert_eq!(run(&mut l, "INSERT INTO t VALUES ('y')")["type"], "ok");
            l.state.closed = true;
            assert!(f.streams.give_back(l).is_none());
        }
        assert_eq!(page_keys(&f), pages, "a stream close sealed pages");
    }

    #[test]
    fn close_all_closes_idle_and_anchor() {
        let f = fx();
        for _ in 0..2 {
            let l = lease(f.streams.checkout(None));
            f.streams.give_back(l).unwrap();
        }
        assert_eq!(f.streams.close_all(), 2);
        let Fx { streams, .. } = f;
        let Streams { db, .. } = streams;
        db.close().unwrap();
    }

    #[test]
    fn streams_is_send_and_sync() {
        fn check<T: Send + Sync>() {}
        check::<Streams>();
    }

    #[test]
    fn stale_idle_stream_expires_on_checkout_then_reap_finds_nothing() {
        let f = fx_with(30, 10);
        let l = lease(f.streams.checkout(None));
        let b = f.streams.give_back(l).unwrap();
        std::thread::sleep(Duration::from_millis(60));
        assert!(is_expired(f.streams.checkout(Some(&b))));
        assert_eq!(f.streams.reap(), 0);
    }

    #[test]
    fn failed_connect_is_failed_and_frees_the_reservation() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(MemStore::new());
        let db = open_with_store(config(&dir.path().join("db")), store).unwrap();
        let other = db.clone();
        let streams = Streams::new(
            db,
            StreamsCfg {
                timeout: Duration::from_secs(60),
                busy: Duration::from_millis(200),
                max: 5,
            },
        )
        .unwrap();
        // Close the anchor, then the shared database, so connect fails.
        streams.close_connections();
        other.close().unwrap();
        match streams.checkout(None) {
            Checkout::Failed(turso_durable::Error::Config(m)) => {
                assert_eq!(m, "the database is closed")
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(streams.busy(), 0);
        assert!(lock(&streams.slots).is_empty());
    }

    #[test]
    fn give_back_after_close_all_closes_the_stream() {
        let f = fx();
        let l = lease(f.streams.checkout(None));
        assert_eq!(f.streams.close_all(), 0);
        assert!(f.streams.give_back(l).is_none());
        assert_eq!(f.streams.busy(), 0);
        match f.streams.checkout(None) {
            Checkout::Failed(turso_durable::Error::Config(m)) => {
                assert_eq!(m, "the server is shutting down")
            }
            other => panic!("{other:?}"),
        }
        assert!(is_expired(f.streams.checkout(Some("1.00"))));
        let Fx { streams, .. } = f;
        let Streams { db, .. } = streams;
        db.close().unwrap();
    }
}
