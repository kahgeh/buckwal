//! Black-box takeover tests, through `takeover_with_store` and `open_with_store` on a
//! `FaultStore` over a shared `MemStore`. Every scenario runs under `Batching::Off` and under a
//! Window setting (`window 5ms 64 1MiB`): a fence must fail every waiting batched commit too.
use buckwal::store::{CondPut, FaultStore, MemStore, Object, ObjectStore, StatsSnap, StoreError};
use buckwal::{
    open_read_only_with_store, open_with_store, takeover_with_store, Batching, CheckpointMode,
    Config, Connection, Database, Error, TakeoverReport, Value,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

const PREFIX: &str = "db";

/// A commit's id and what its insert returned.
type Outcome = (i64, buckwal::Result<u64>);

/// Run `f` under `Off`, then under `window 5ms 64 1MiB`.
fn for_each_batching(f: impl Fn(Batching)) {
    for b in [
        Batching::Off,
        Batching::Window {
            max_wait: Duration::from_millis(5),
            max_commits: 64,
            max_bytes: 1 << 20,
        },
    ] {
        eprintln!("batching: {b:?}");
        f(b);
    }
}

/// Short timeouts, valid: trim grace (10 min) > lease 5 s + retry deadline 2 s + request
/// timeout 500 ms.
fn config(dir: &Path, b: Batching) -> Config {
    let mut c = Config::new(dir, "http://127.0.0.1:1", "bucket", "key", "secret");
    c.prefix = PREFIX.into();
    c.retry_deadline = Duration::from_secs(2);
    c.request_timeout = Duration::from_millis(500);
    c.lease = Duration::from_secs(5);
    c.batching = b;
    c
}

fn tempdir() -> TempDir {
    tempfile::tempdir().unwrap()
}

/// A path inside a new temp dir that does not exist yet.
fn fresh(t: &TempDir) -> std::path::PathBuf {
    t.path().join("db")
}

fn create(c: &Connection) {
    c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)", &[])
        .unwrap();
}

fn insert(c: &Connection, id: i64) -> buckwal::Result<u64> {
    c.execute("INSERT INTO t(id) VALUES (?)", &[Value::from_i64(id)])
}

fn ids(c: &Connection) -> BTreeSet<i64> {
    c.query("SELECT id FROM t", &[])
        .unwrap()
        .iter()
        .map(|r| r[0].as_int().unwrap())
        .collect()
}

/// Every id a restore of the bucket (a read-only open of a new directory) holds.
fn restored_ids(store: Arc<dyn ObjectStore>, b: Batching) -> BTreeSet<i64> {
    let v = tempdir();
    let db = open_read_only_with_store(config(v.path(), b), store).unwrap();
    let c = db.connect().unwrap();
    let ids = ids(&c);
    c.close().unwrap();
    db.close().unwrap();
    ids
}

/// One field of the stored head, from its JSON body.
fn head_field(mem: &MemStore, name: &str) -> String {
    let body = mem.get(&format!("{PREFIX}/head")).unwrap().unwrap().body;
    let body = String::from_utf8(body).unwrap();
    let key = format!("\"{name}\":");
    let rest = &body[body.find(&key).unwrap_or_else(|| panic!("{body}")) + key.len()..];
    let end = rest.find([',', '}']).unwrap();
    rest[..end].trim_matches('"').to_string()
}

fn head_epoch(mem: &MemStore) -> u64 {
    head_field(mem, "epoch").parse().unwrap()
}

/// Insert `ids` on `n` threads, each on its own connection; every insert must be acked.
fn insert_on_threads(db: &Database, n: usize, ids: Vec<i64>) {
    let threads: Vec<_> = (0..n)
        .map(|t| {
            let db = db.clone();
            let mine: Vec<i64> = ids.iter().copied().skip(t).step_by(n).collect();
            thread::spawn(move || {
                let c = db.connect().unwrap();
                for id in mine {
                    insert(&c, id).unwrap();
                }
                c.close().unwrap();
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
}

/// A writer on a `FaultStore` over `mem`, its table created and `1..=n` acked on 4
/// connections.
fn writer_a(mem: &Arc<MemStore>, b: Batching, n: i64) -> (Arc<FaultStore>, TempDir, Database) {
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let a = tempdir();
    let db = open_with_store(config(a.path(), b), fault.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    c.close().unwrap();
    insert_on_threads(&db, 4, (1..=n).collect());
    (fault, a, db)
}

/// Review Focus 3: a takeover while the old writer has 8 commits waiting on 4 connections
/// (its log PUTs delayed 300 ms). No waiting commit is acked after the fence unless it is in
/// the log before it; every acked one restores.
#[test]
fn takeover_fences_the_old_writer() {
    for_each_batching(|b| {
        let mem = Arc::new(MemStore::new());
        let (fault, _a, db_a) = writer_a(&mem, b, 200);
        let a_epoch = head_epoch(&mem);
        fault.only_keys_containing("/log/");
        fault.delay_puts(Duration::from_millis(300));

        let results: Arc<Mutex<Vec<Outcome>>> = Arc::default();
        let threads: Vec<_> = (0..4)
            .map(|t| {
                let (db, results) = (db_a.clone(), results.clone());
                thread::spawn(move || {
                    let c = db.connect().unwrap();
                    for k in 0..2 {
                        let id = 201 + t * 2 + k;
                        let r = insert(&c, id);
                        results.lock().unwrap().push((id, r));
                    }
                    drop(c);
                })
            })
            .collect();
        // While the commits wait on their delayed PUTs.
        thread::sleep(Duration::from_millis(50));
        let bt = tempdir();
        let rep: TakeoverReport = takeover_with_store(config(&fresh(&bt), b), mem.clone()).unwrap();
        assert_eq!(rep.epoch, a_epoch + 1, "{rep:?}");
        assert_eq!(rep.quiet, Duration::ZERO);
        assert_eq!(head_field(&mem, "writer_id"), rep.writer_id);
        for t in threads {
            t.join().unwrap();
        }
        fault.delay_puts(Duration::ZERO);

        let mut acked: BTreeSet<i64> = (1..=200).collect();
        let results = std::mem::take(&mut *results.lock().unwrap());
        assert_eq!(results.len(), 8);
        let (mut ok, mut fenced) = (Vec::new(), Vec::new());
        for (id, r) in results {
            match r {
                Ok(_) => {
                    ok.push(id);
                    acked.insert(id);
                }
                Err(Error::Fenced(_)) => fenced.push(id),
                Err(e) => panic!("{b:?}: commit {id}: {e:?}"),
            }
        }
        eprintln!("{b:?}: in flight at the takeover: ok {ok:?}, fenced {fenced:?}");
        // Off sends one 300 ms PUT at a time, and a Window sends each batch as one 300 ms
        // PUT: under both, commits are still waiting at the fence, and the fence fails them.
        assert!(
            !fenced.is_empty(),
            "{b:?}: no waiting commit was fenced: ok {ok:?}"
        );
        let c = db_a.connect().unwrap();
        let next = insert(&c, 300);
        assert!(matches!(next, Err(Error::Fenced(_))), "{b:?}: {next:?}");
        drop(c);

        let db_b = open_with_store(config(&fresh(&bt), b), mem.clone()).unwrap();
        let cb = db_b.connect().unwrap();
        for id in 1001..=1010 {
            insert(&cb, id).unwrap();
            acked.insert(id);
        }
        cb.close().unwrap();
        db_b.close().unwrap();
        drop(db_a);

        let restored = restored_ids(mem.clone(), b);
        let missing: Vec<&i64> = acked.difference(&restored).collect();
        assert!(missing.is_empty(), "{b:?}: acked ids missing: {missing:?}");
        let fenced_present: Vec<&i64> = fenced.iter().filter(|i| restored.contains(i)).collect();
        assert!(
            fenced_present.is_empty(),
            "{b:?}: commits failed Fenced but restored: {fenced_present:?}"
        );
    });
}

/// An idle old writer's seal after a takeover is refused before it uploads a page.
#[test]
fn old_writer_cannot_seal_after_takeover() {
    for_each_batching(|b| {
        let mem = Arc::new(MemStore::new());
        let (fault, _a, db_a) = writer_a(&mem, b, 20);
        let bt = tempdir();
        takeover_with_store(config(&fresh(&bt), b), mem.clone()).unwrap();
        let before = fault.events().len();
        let c = db_a.connect().unwrap();
        let r = c.checkpoint(CheckpointMode::Truncate);
        assert!(matches!(r, Err(Error::Fenced(_))), "{b:?}: {r:?}");
        let pages: Vec<String> = fault.events()[before..]
            .iter()
            .filter(|e| e.starts_with("put") && e.contains("/pages/"))
            .cloned()
            .collect();
        assert!(pages.is_empty(), "{b:?}: A uploaded pages: {pages:?}");
        drop(c);
        drop(db_a);
        assert_eq!(restored_ids(mem.clone(), b), (1..=20).collect());
    });
}

/// `takeover` restores into a missing or empty directory only; anything else is refused
/// before any request reaches the bucket.
#[test]
fn takeover_into_a_non_empty_dir_writes_nothing() {
    for_each_batching(|b| {
        let mem = Arc::new(MemStore::new());
        let (_fault, _a, db_a) = writer_a(&mem, b, 5);
        let bt = tempdir();
        std::fs::write(bt.path().join("x.txt"), b"x").unwrap();
        let before = mem.stats();
        let r = takeover_with_store(config(bt.path(), b), mem.clone());
        assert!(matches!(r, Err(Error::Config(_))), "{b:?}: {r:?}");
        assert_eq!(mem.stats(), before, "{b:?}: a request reached the bucket");
        assert_eq!(
            std::fs::read_dir(bt.path()).unwrap().count(),
            1,
            "{b:?}: the directory changed"
        );
        let c = db_a.connect().unwrap();
        insert(&c, 6).unwrap();
        drop(c);
        drop(db_a);
    });
}

/// Review Focus 1: two takeovers at once on one prefix. Each either fails or wins its own
/// epoch; the head names the higher one; the other directory can no longer open as writer;
/// a restore holds every ack.
#[test]
fn concurrent_takeovers_take_distinct_epochs() {
    for_each_batching(|b| {
        let mem = Arc::new(MemStore::new());
        let (_fault, _a, db_a) = writer_a(&mem, b, 20);
        let slow = Arc::new(FaultStore::new(mem.clone()));
        slow.only_keys_containing("/head");
        slow.delay_puts(Duration::from_millis(50));
        let (bt, ct) = (tempdir(), tempdir());
        let dirs = [fresh(&bt), fresh(&ct)];
        let runs: Vec<_> = dirs
            .iter()
            .map(|d| {
                let (d, slow) = (d.clone(), slow.clone());
                thread::spawn(move || {
                    let store: Arc<dyn ObjectStore> = slow;
                    takeover_with_store(config(&d, b), store)
                })
            })
            .collect();
        let reports: Vec<buckwal::Result<TakeoverReport>> =
            runs.into_iter().map(|t| t.join().unwrap()).collect();
        slow.delay_puts(Duration::ZERO);
        eprintln!(
            "{b:?}: {:?}",
            reports
                .iter()
                .map(|r| r.as_ref().map(|r| r.epoch))
                .collect::<Vec<_>>()
        );
        let ok: Vec<(usize, &TakeoverReport)> = reports
            .iter()
            .enumerate()
            .filter_map(|(i, r)| r.as_ref().ok().map(|r| (i, r)))
            .collect();
        assert!(!ok.is_empty(), "{b:?}: no takeover won: {reports:?}");
        let epochs: BTreeSet<u64> = ok.iter().map(|(_, r)| r.epoch).collect();
        assert_eq!(epochs.len(), ok.len(), "{b:?}: epochs not distinct");
        let (winner, top) = *ok.iter().max_by_key(|(_, r)| r.epoch).unwrap();
        assert_eq!(head_field(&mem, "writer_id"), top.writer_id, "{b:?}");
        assert_eq!(head_epoch(&mem), top.epoch, "{b:?}");

        let loser = &dirs[1 - winner];
        let r = open_with_store(config(loser, b), mem.clone());
        assert!(
            matches!(r, Err(Error::Fenced(_))),
            "{b:?}: the other directory opened: {:?}",
            r.err()
        );
        let db = open_with_store(config(&dirs[winner], b), mem.clone()).unwrap();
        let c = db.connect().unwrap();
        insert(&c, 2001).unwrap();
        c.close().unwrap();
        db.close().unwrap();
        drop(db_a);
        let mut want: BTreeSet<i64> = (1..=20).collect();
        want.insert(2001);
        assert_eq!(restored_ids(mem.clone(), b), want, "{b:?}");
    });
}

/// A `MemStore` that makes the old writer seal right before a takeover's head check, `left`
/// times: a list (the restore reading the log) arms it, and the next GET of the head (the
/// claim's) first asks the old writer to seal and waits for it.
struct SealsBeforeClaim {
    inner: Arc<MemStore>,
    listed: AtomicBool,
    left: AtomicU64,
    /// Ask the old writer to seal; its reply.
    seal: Mutex<(mpsc::Sender<()>, mpsc::Receiver<()>)>,
}

impl ObjectStore for SealsBeforeClaim {
    fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, body)
    }
    fn put_if_absent(&self, key: &str, body: &[u8]) -> Result<CondPut, StoreError> {
        self.inner.put_if_absent(key, body)
    }
    fn put_if_match(&self, key: &str, body: &[u8], etag: &str) -> Result<CondPut, StoreError> {
        self.inner.put_if_match(key, body, etag)
    }
    fn get(&self, key: &str) -> Result<Option<Object>, StoreError> {
        let claim = key == format!("{PREFIX}/head") && self.listed.swap(false, SeqCst);
        if claim
            && self
                .left
                .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            let ch = self.seal.lock().unwrap();
            ch.0.send(()).unwrap();
            ch.1.recv().unwrap();
        }
        self.inner.get(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.listed.store(true, SeqCst);
        self.inner.list(prefix)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
    fn stat(
        &self,
        key: &str,
    ) -> std::result::Result<Option<buckwal::store::ObjectStat>, buckwal::store::StoreError> {
        self.inner.stat(key)
    }
    fn stats(&self) -> StatsSnap {
        self.inner.stats()
    }
}

/// A takeover that failed "restore again" three times (the old writer sealed between each
/// restore and the claim) leaves its directory holding only `durable.lock`. A retry into the
/// same directory is accepted and, the head now stable, takes the database over.
#[test]
fn takeover_retried_into_its_own_leftover_dir_succeeds() {
    for_each_batching(|b| {
        let mem = Arc::new(MemStore::new());
        let (_fault, _a, db_a) = writer_a(&mem, b, 5);
        let (req_tx, req_rx) = mpsc::channel::<()>();
        let (done_tx, done_rx) = mpsc::channel::<()>();
        let old = thread::spawn(move || {
            let c = db_a.connect().unwrap();
            for id in 101.. {
                if req_rx.recv().is_err() {
                    break;
                }
                insert(&c, id).unwrap();
                c.checkpoint(CheckpointMode::Truncate).unwrap();
                done_tx.send(()).unwrap();
            }
            drop(c);
            drop(db_a);
        });
        let store = Arc::new(SealsBeforeClaim {
            inner: mem.clone(),
            listed: AtomicBool::new(false),
            left: AtomicU64::new(3),
            seal: Mutex::new((req_tx, done_rx)),
        });
        let bt = tempdir();
        let dir = fresh(&bt);
        let r = takeover_with_store(config(&dir, b), store.clone());
        match r {
            Err(Error::Fenced(m)) => assert!(m.contains("restore again"), "{b:?}: {m}"),
            r => panic!("{b:?}: expected Fenced(restore again), got {r:?}"),
        }
        let left: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(left, vec!["durable.lock".to_string()], "{b:?}");

        let rep = takeover_with_store(config(&dir, b), store.clone())
            .unwrap_or_else(|e| panic!("{b:?}: the retry into the same directory: {e:?}"));
        assert_eq!(head_field(&mem, "writer_id"), rep.writer_id, "{b:?}");
        // The old writer is told to stop; its seal requests channel closes.
        drop(store);
        old.join().unwrap();
        let db = open_with_store(config(&dir, b), mem.clone()).unwrap();
        let c = db.connect().unwrap();
        insert(&c, 1001).unwrap();
        c.close().unwrap();
        db.close().unwrap();
        let mut want: BTreeSet<i64> = (1..=5).chain(101..=103).collect();
        want.insert(1001);
        assert_eq!(restored_ids(mem.clone(), b), want, "{b:?}");
    });
}
