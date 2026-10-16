//! Black-box batching tests, through `open_with_store` on a `MemStore` (or a `FaultStore`
//! over one): the setting a commit uses reaches its upload, every ack waits for a confirmed
//! log PUT holding that commit, and failures, fences and seals behave as in `Off`.
use std::path::Path;
use std::sync::{Arc, Barrier, Mutex};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use turso_durable::store::{FaultStore, MemStore, ObjectStore};
use turso_durable::{
    open_read_only_with_store, open_with_store, restore_with_store, Batching, CheckpointMode,
    Config, Connection, Database, Error, Event, EventHook, UploadStats, Value,
};

/// Every event, with the thread that emitted it (`CommitAcked` comes from the committing
/// connection's thread).
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<(ThreadId, Event)>>>);

impl Recorder {
    fn hook(&self) -> EventHook {
        let seen = self.0.clone();
        Arc::new(move |e: &Event| {
            seen.lock()
                .unwrap()
                .push((thread::current().id(), e.clone()))
        })
    }

    /// Every event so far, in order; the recorder is emptied.
    fn take(&self) -> Vec<Event> {
        std::mem::take(&mut *self.0.lock().unwrap())
            .into_iter()
            .map(|(_, e)| e)
            .collect()
    }

    fn events(&self) -> Vec<Event> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(_, e)| e.clone())
            .collect()
    }

    /// Poll until an event matches `pred`, for at most 10 s.
    fn wait_for(&self, pred: impl Fn(&Event) -> bool) {
        let until = Instant::now() + Duration::from_secs(10);
        while Instant::now() < until {
            if self.0.lock().unwrap().iter().any(|(_, e)| pred(e)) {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("no matching event in 10 s: {:?}", self.events());
    }

    /// The `CommitAcked` events `thread` emitted, in order.
    fn acks_on(&self, thread: ThreadId) -> Vec<(u64, Batching)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(t, e)| match e {
                Event::CommitAcked { commit, batching } if *t == thread => {
                    Some((*commit, *batching))
                }
                _ => None,
            })
            .collect()
    }
}

fn mem() -> Arc<dyn ObjectStore> {
    Arc::new(MemStore::new())
}

fn config(dir: &Path) -> Config {
    Config::new(dir, "http://127.0.0.1:1", "b", "k", "s")
}

fn open(store: Arc<dyn ObjectStore>, events: Recorder) -> (TempDir, Database) {
    open_with(store, events, |_| {})
}

fn open_with(
    store: Arc<dyn ObjectStore>,
    events: Recorder,
    f: impl FnOnce(&mut Config),
) -> (TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let mut c = config(dir.path());
    c.on_event = Some(events.hook());
    f(&mut c);
    let db = open_with_store(c, store).unwrap();
    (dir, db)
}

/// A FaultStore over a MemStore whose failures are short: a 200 ms retry deadline.
fn faulty(events: Recorder) -> (Arc<MemStore>, Arc<FaultStore>, TempDir, Database) {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let (d, db) = open_with(fault.clone(), events, |c| {
        c.retry_deadline = Duration::from_millis(200);
        c.request_timeout = Duration::from_millis(50);
    });
    fault.only_keys_containing("log/");
    (mem, fault, d, db)
}

fn w(ms: u64, n: u32, bytes: u64) -> Batching {
    Batching::Window {
        max_wait: Duration::from_millis(ms),
        max_commits: n,
        max_bytes: bytes,
    }
}

/// Restore the bucket into a fresh directory and read `t` there.
fn restore_rows(store: Arc<dyn ObjectStore>) -> Vec<i64> {
    let dir = tempfile::tempdir().unwrap();
    restore_with_store(&config(dir.path()), store.clone(), dir.path()).unwrap();
    let db = open_read_only_with_store(config(dir.path()), store).unwrap();
    let c = db.connect().unwrap();
    let rows = c.query("SELECT x FROM t ORDER BY x", &[]).unwrap();
    rows.iter().map(|r| r[0].as_int().unwrap()).collect()
}

fn create(c: &Connection) {
    c.execute("CREATE TABLE t(x INTEGER PRIMARY KEY)", &[])
        .unwrap();
}

fn insert(c: &Connection, x: i64) -> turso_durable::Result<u64> {
    c.execute("INSERT INTO t VALUES (?)", &[Value::from_i64(x)])
}

fn count(c: &Connection) -> i64 {
    c.query("SELECT count(*) FROM t", &[]).unwrap()[0][0]
        .as_int()
        .unwrap()
}

fn delta(a: &UploadStats, b: &UploadStats) -> (u64, u64) {
    (b.commits_acked - a.commits_acked, b.log_puts - a.log_puts)
}

/// One phase of `run_phases`: the database's setting, and each connection's.
#[derive(Clone, Copy)]
struct Phase {
    db: Batching,
    conn: Option<Batching>,
}

const THREADS: usize = 8;
const PER_PHASE: usize = 10;

/// Run `phases` on THREADS connections, PER_PHASE inserts each per phase, all starting each
/// phase together. Returns each phase's (commits acked, log puts) and the stats after it.
fn run_phases(db: &Database, phases: &[Phase]) -> Vec<((u64, u64), UploadStats)> {
    let c = db.connect().unwrap();
    create(&c);
    // start, settings applied (workers released), end
    let barrier = Arc::new(Barrier::new(THREADS + 1));
    let threads: Vec<_> = (0..THREADS)
        .map(|t| {
            let (db, barrier, phases) = (db.clone(), barrier.clone(), phases.to_vec());
            thread::spawn(move || {
                let c = db.connect().unwrap();
                for (p, phase) in phases.iter().enumerate() {
                    barrier.wait();
                    c.set_batching(phase.conn).unwrap();
                    barrier.wait();
                    for i in 0..PER_PHASE {
                        let id = (p * THREADS * PER_PHASE + t * PER_PHASE + i) as i64;
                        insert(&c, id).unwrap();
                    }
                    barrier.wait();
                }
                c.close().unwrap();
            })
        })
        .collect();
    let mut out = Vec::new();
    for phase in phases {
        db.set_batching(phase.db).unwrap();
        barrier.wait();
        // Between the two waits: no worker inserts before the second one releases them.
        let s0 = db.upload_stats();
        barrier.wait();
        barrier.wait();
        let s1 = db.upload_stats();
        out.push((delta(&s0, &s1), s1));
    }
    for t in threads {
        t.join().unwrap();
    }
    c.close().unwrap();
    out
}

#[test]
fn off_makes_each_commit_its_own_put() {
    let (_d, db) = open(mem(), Recorder::default());
    let c = db.connect().unwrap();
    create(&c);
    let s0 = db.upload_stats();
    for i in 0..20 {
        insert(&c, i).unwrap();
    }
    let s1 = db.upload_stats();
    assert_eq!(s1.commits_acked - s0.commits_acked, 20);
    assert_eq!(s1.log_puts - s0.log_puts, 20);
}

#[test]
fn switching_live_connections_changes_put_sharing_without_reopening() {
    let store = mem();
    let (_d, db) = open(store.clone(), Recorder::default());
    let win = w(50, 64, 1 << 20);
    let off = Batching::Off;
    let phases = [
        // Through each connection's own setting.
        Phase {
            db: off,
            conn: None,
        },
        Phase {
            db: off,
            conn: Some(win),
        },
        Phase {
            db: off,
            conn: Some(off),
        },
        // Through the database's, every connection falling back to it.
        Phase {
            db: off,
            conn: None,
        },
        Phase {
            db: win,
            conn: None,
        },
        Phase {
            db: off,
            conn: None,
        },
    ];
    let r = run_phases(&db, &phases);
    let n = (THREADS * PER_PHASE) as u64;
    for (p, ((acked, puts), _)) in r.iter().enumerate() {
        eprintln!("phase {p}: {acked} acked in {puts} log PUTs");
        assert_eq!(*acked, n, "phase {p}");
        if p == 1 || p == 4 {
            assert!(
                *puts < n / 2,
                "phase {p}: {puts} log PUTs for {n} window commits"
            );
        } else {
            assert_eq!(*puts, n, "phase {p}: Off");
        }
    }
    db.close().unwrap();
    assert_eq!(restore_rows(store), (0..6 * n as i64).collect::<Vec<_>>());
}

#[test]
fn upload_stats_ratios_and_ack_quantiles() {
    let (_d, db) = open(mem(), Recorder::default());
    let phases = [
        Phase {
            db: Batching::Off,
            conn: None,
        },
        Phase {
            db: Batching::Off,
            conn: Some(w(50, 64, 1 << 20)),
        },
    ];
    let (_, s) = run_phases(&db, &phases)[1];
    assert!(s.commits_per_log_put > 1.0, "{s:?}");
    assert!(s.bytes_per_log_put > 0.0, "{s:?}");
    assert!(s.ack_wait_p50 <= s.ack_wait_p95, "{s:?}");
    assert!(s.ack_wait_p95 <= s.ack_wait_p99, "{s:?}");
    assert!(s.ack_wait_p99 > Duration::ZERO, "{s:?}");
}

/// Every `CommitAcked` comes after a `LogUploaded` that holds its commit, and the log holds
/// the commits in commit order.
fn check_acks_follow_puts(ev: &[Event]) {
    let mut uploaded = std::collections::HashSet::new();
    let mut acks = 0;
    for e in ev {
        match e {
            Event::LogUploaded { commits, .. } => uploaded.extend(commits.iter().copied()),
            Event::CommitAcked { commit, .. } => {
                acks += 1;
                assert!(uploaded.contains(commit), "ack {commit} before its PUT");
            }
            _ => {}
        }
    }
    assert!(acks > 0);
    let order: Vec<u64> = ev
        .iter()
        .filter_map(|e| match e {
            Event::LogUploaded { commits, .. } => Some(commits.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    assert!(
        order.windows(2).all(|p| p[0] < p[1]),
        "log order is commit order: {order:?}"
    );
}

/// 4 Off and 4 Window connections, 15 inserts each; `alternate` flips each connection's
/// setting at every commit and checks that its `CommitAcked` carries the one it used.
fn mixed_connections(alternate: bool) {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    fault.only_keys_containing("log/");
    fault.delay_puts(Duration::from_millis(3));
    let rec = Recorder::default();
    let (_d, db) = open(fault, rec.clone());
    let c = db.connect().unwrap();
    create(&c);
    let win = w(20, 64, 1 << 20);
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let (db, rec) = (db.clone(), rec.clone());
            thread::spawn(move || {
                let c = db.connect().unwrap();
                let me = thread::current().id();
                for i in 0..15 {
                    let b = if (t + if alternate { i } else { 0 }) % 2 == 0 {
                        Batching::Off
                    } else {
                        win
                    };
                    c.set_batching(Some(b)).unwrap();
                    insert(&c, (t * 15 + i) as i64).unwrap();
                    let acks = rec.acks_on(me);
                    assert_eq!(acks.len(), i + 1, "one ack per commit");
                    assert_eq!(acks[i].1, b, "the ack carries the setting its commit used");
                }
                c.close().unwrap();
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    c.close().unwrap();
    db.close().unwrap();
    check_acks_follow_puts(&rec.take());
    assert_eq!(restore_rows(mem), (0..8 * 15).collect::<Vec<_>>());
}

#[test]
fn every_ack_follows_a_confirmed_put_holding_its_commit() {
    mixed_connections(false);
}

#[test]
fn mixed_off_and_window_connections_ack_only_after_confirmed_put() {
    mixed_connections(true);
}

fn commits_uploaded(ev: &[Event]) -> Vec<Vec<u64>> {
    ev.iter()
        .filter_map(|e| match e {
            Event::LogUploaded { commits, .. } if !commits.is_empty() => Some(commits.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn off_commit_carries_waiting_batched_commits() {
    let rec = Recorder::default();
    let (_d, db) = open(mem(), rec.clone());
    let b = db.connect().unwrap();
    create(&b);
    rec.take();
    let a = {
        let db = db.clone();
        thread::spawn(move || {
            let c = db.connect().unwrap();
            c.set_batching(Some(w(10_000, 64, 1 << 20))).unwrap();
            let t = Instant::now();
            insert(&c, 1).unwrap();
            (t.elapsed(), thread::current().id())
        })
    };
    rec.wait_for(|e| matches!(e, Event::BatchWaiting { commits: 1 }));
    insert(&b, 2).unwrap();
    let (took, a_thread) = a.join().unwrap();
    assert!(took < Duration::from_secs(1), "{took:?}");
    let a_commit = rec.acks_on(a_thread)[0].0;
    let b_commit = rec.acks_on(thread::current().id())[0].0;
    assert_eq!(
        commits_uploaded(&rec.events()),
        vec![vec![a_commit, b_commit]]
    );
}

#[test]
fn single_connection_window_ack_waits_at_most_max_wait_plus_put() {
    let (_d, db) = open(mem(), Recorder::default());
    let c = db.connect().unwrap();
    create(&c);
    c.set_batching(Some(w(30, 64, 1 << 20))).unwrap();
    let s0 = db.upload_stats();
    for i in 0..5 {
        let t = Instant::now();
        insert(&c, i).unwrap();
        let took = t.elapsed();
        assert!(took >= Duration::from_millis(30), "{took:?}");
        assert!(took < Duration::from_millis(530), "{took:?}");
    }
    assert_eq!(delta(&s0, &db.upload_stats()), (5, 5));
}

/// Three Window commits (max_commits 3) whose shared upload fails; each returns the error.
/// Returns an open connection: closing the last one would checkpoint, and that seal would
/// fail too, leaving a generation boundary for the next commit to trip over first.
fn three_failed_window_commits(db: &Database, fault: &FaultStore) -> Connection {
    let c = db.connect().unwrap();
    create(&c);
    fault.fail_puts_after(0);
    let threads: Vec<_> = (1..=3)
        .map(|x| {
            let db = db.clone();
            thread::spawn(move || {
                let c = db.connect().unwrap();
                c.set_batching(Some(w(10_000, 3, 1 << 20))).unwrap();
                insert(&c, x)
            })
        })
        .collect();
    for t in threads {
        let r = t.join().unwrap();
        assert!(
            matches!(r, Err(Error::Timeout(_) | Error::UploadFailed(_))),
            "{r:?}"
        );
    }
    c
}

#[test]
fn window_upload_failure_leaves_row_visible_and_later_durable() {
    let (mem, fault, _d, db) = faulty(Recorder::default());
    let c = three_failed_window_commits(&db, &fault);
    let fourth = db.connect().unwrap();
    assert_eq!(count(&fourth), 3, "committed locally");
    fourth.close().unwrap();
    fault.fail_puts_after(u64::MAX);
    insert(&c, 4).unwrap();
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(restore_rows(mem), vec![1, 2, 3, 4]);
}

#[test]
fn window_commit_while_degraded_takes_the_off_path() {
    let (_mem, fault, _d, db) = faulty(Recorder::default());
    let _open = three_failed_window_commits(&db, &fault);
    let c = db.connect().unwrap();
    c.set_batching(Some(w(10_000, 64, 1 << 20))).unwrap();
    let t = Instant::now();
    let r = insert(&c, 5);
    assert!(r.is_err(), "{r:?}");
    assert!(
        t.elapsed() < Duration::from_secs(5),
        "it failed with the retries, not after max_wait"
    );
    assert_eq!(count(&c), 3, "rolled back, not left waiting");
}

#[test]
fn fence_fails_every_waiting_batched_commit() {
    let mem = Arc::new(MemStore::new());
    let rec = Recorder::default();
    let (_d, db) = open(mem.clone(), rec.clone());
    let c = db.connect().unwrap();
    create(&c);
    let spawn = |x: i64| {
        let db = db.clone();
        thread::spawn(move || {
            let c = db.connect().unwrap();
            c.set_batching(Some(w(10_000, 3, 1 << 20))).unwrap();
            insert(&c, x)
        })
    };
    let first = [spawn(1), spawn(2)];
    rec.wait_for(|e| matches!(e, Event::BatchWaiting { commits: 2 }));
    let seqs: Vec<u64> = mem
        .list("log/0000000001/")
        .unwrap()
        .iter()
        .map(|k| k.rsplit('/').next().unwrap().parse().unwrap())
        .collect();
    let next = seqs.iter().max().unwrap() + 1;
    mem.put(
        &format!("log/0000000001/{next:020}"),
        b"another writer's record",
    )
    .unwrap();
    let third = spawn(3);
    for t in first.into_iter().chain([third]) {
        let r = t.join().unwrap();
        assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
    }
    let r = insert(&c, 4);
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
}

/// A Window commit (max_wait 10 s) on its own thread; returns once the batch holds it.
fn waiting_commit(
    db: &Database,
    rec: &Recorder,
) -> thread::JoinHandle<(turso_durable::Result<u64>, ThreadId)> {
    let db = db.clone();
    let h = thread::spawn(move || {
        let c = db.connect().unwrap();
        c.set_batching(Some(w(10_000, 64, 1 << 20))).unwrap();
        (insert(&c, 1), thread::current().id())
    });
    rec.wait_for(|e| matches!(e, Event::BatchWaiting { commits: 1 }));
    h
}

/// `checkpoint(Truncate)`, retried while the engine is busy: `BatchWaiting` is emitted while
/// the waiting commit's thread may still hold the write lock, finishing its local commit.
fn truncate(c: &Connection) -> turso_durable::Result<()> {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        match c.checkpoint(CheckpointMode::Truncate) {
            Err(Error::Engine(m)) if m.contains("busy") && Instant::now() < until => {
                thread::sleep(Duration::from_millis(5))
            }
            r => return r,
        }
    }
}

#[test]
fn seal_waits_for_batched_commits() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let rec = Recorder::default();
    let (_d, db) = open(fault.clone(), rec.clone());
    let c = db.connect().unwrap();
    create(&c);
    let t = Instant::now();
    let a = waiting_commit(&db, &rec);
    truncate(&c).unwrap();
    let (r, a_thread) = a.join().unwrap();
    r.unwrap();
    assert!(t.elapsed() < Duration::from_secs(5), "the seal flushed it");
    let commit = rec.acks_on(a_thread)[0].0;
    let key = rec
        .events()
        .iter()
        .find_map(|e| match e {
            Event::LogUploaded {
                generation,
                seq,
                commits,
                ..
            } if commits.contains(&commit) => Some(format!("log/{generation:010}/{seq:020}")),
            _ => None,
        })
        .expect("the record holding the commit");
    let ev = fault.events();
    let log = ev
        .iter()
        .position(|e| *e == format!("put_if_absent {key}"))
        .expect("its PUT");
    let page = ev
        .iter()
        .position(|e| e.starts_with("put ") && e.contains("pages/"))
        .expect("a page PUT");
    assert!(log < page, "{ev:?}");
}

#[test]
fn seal_fails_when_batched_commits_cannot_upload() {
    let rec = Recorder::default();
    let (_mem, fault, _d, db) = faulty(rec.clone());
    let c = db.connect().unwrap();
    create(&c);
    fault.fail_puts_after(0);
    let a = waiting_commit(&db, &rec);
    let r = truncate(&c);
    assert!(
        matches!(r, Err(Error::Timeout(_) | Error::UploadFailed(_))),
        "{r:?}"
    );
    let (r, _) = a.join().unwrap();
    assert!(r.is_err(), "{r:?}");
    let ev = fault.events();
    assert!(
        !ev.iter()
            .any(|e| e.starts_with("put ") && e.contains("pages/")),
        "{ev:?}"
    );
}

#[test]
fn a_change_never_alters_a_waiting_commit() {
    let rec = Recorder::default();
    let (_d, db) = open(mem(), rec.clone());
    let c = db.connect().unwrap();
    create(&c);
    let win = w(300, 64, 1 << 20);
    let a = {
        let db = db.clone();
        thread::spawn(move || {
            let c = db.connect().unwrap();
            c.set_batching(Some(win)).unwrap();
            let t = Instant::now();
            insert(&c, 1).unwrap();
            (t.elapsed(), thread::current().id())
        })
    };
    rec.wait_for(|e| matches!(e, Event::BatchWaiting { commits: 1 }));
    db.set_batching(Batching::Off).unwrap();
    c.set_batching(Some(Batching::Off)).unwrap();
    let (took, a_thread) = a.join().unwrap();
    assert!(took >= Duration::from_millis(300), "{took:?}");
    assert_eq!(rec.acks_on(a_thread)[0].1, win);
}

#[test]
fn set_batching_validates_and_falls_back() {
    let (_d, db) = open(mem(), Recorder::default());
    assert_eq!(db.batching(), Batching::Off);
    let c = db.connect().unwrap();
    assert_eq!(c.batching(), None);
    assert!(matches!(db.set_batching(w(1, 0, 1)), Err(Error::Config(_))));
    assert!(matches!(
        c.set_batching(Some(w(1, 1, 0))),
        Err(Error::Config(_))
    ));
    assert_eq!((db.batching(), c.batching()), (Batching::Off, None));
    db.set_batching(w(5, 2, 3)).unwrap();
    c.set_batching(Some(Batching::Off)).unwrap();
    assert_eq!(db.batching(), w(5, 2, 3));
    assert_eq!(c.batching(), Some(Batching::Off));
    c.set_batching(None).unwrap();
    assert_eq!(c.batching(), None);
}

#[test]
fn initial_database_setting_is_the_config_batching() {
    let (_d, db) = open_with(mem(), Recorder::default(), |c| {
        c.batching = w(40, 8, 1 << 10)
    });
    assert_eq!(db.batching(), w(40, 8, 1 << 10));
}

/// A Window commit whose call failed afterwards is not acked, but its upload goes on: it
/// waits in the batch (max_wait 10 s) until closing drains it. (A call that returns `Ok`
/// has already waited for its commits, so this is the one way a commit can still be waiting
/// when its connection closes.)
#[test]
fn close_drains_waiting_commits() {
    let store = mem();
    let rec = Recorder::default();
    let (_d, db) = open(store.clone(), rec.clone());
    let c = db.connect().unwrap();
    create(&c);
    c.set_batching(Some(w(10_000, 64, 1 << 20))).unwrap();
    rec.take();
    // The first INSERT commits; the second fails, so the call returns Err unacked.
    let r = c.execute_batch("INSERT INTO t VALUES (1); INSERT INTO t VALUES (1)");
    assert!(r.is_err(), "{r:?}");
    rec.wait_for(|e| matches!(e, Event::BatchWaiting { commits: 1 }));
    assert!(commits_uploaded(&rec.events()).is_empty(), "still waiting");
    let t = Instant::now();
    c.close().unwrap();
    db.close().unwrap();
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    // Closing the last connection seals (its TRUNCATE), which flushes the batch; the
    // close's own WAL sync in the next generation is an Off commit, acked by the close.
    let ev = rec.events();
    let waiting = commits_uploaded(&ev)[0][0];
    assert!(
        !ev.iter()
            .any(|e| matches!(e, Event::CommitAcked { commit, .. } if *commit == waiting)),
        "never acked: {ev:?}"
    );
    assert_eq!(restore_rows(store), vec![1]);
}

/// The `CommitAcked` settings `thread` emitted after its first `from` acks.
fn ack_settings(rec: &Recorder, thread: ThreadId, from: usize) -> Vec<Batching> {
    rec.acks_on(thread)[from..].iter().map(|a| a.1).collect()
}

/// What `PRAGMA durable_batching` reads on `c`.
fn setting_text(c: &Connection) -> String {
    let rows = c.query("PRAGMA durable_batching", &[]).unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    rows[0][0].to_text().unwrap().to_string()
}

#[test]
fn transaction_with_batches_only_that_transaction() {
    let rec = Recorder::default();
    let (_d, db) = open(mem(), rec.clone());
    let c = db.connect().unwrap();
    create(&c);
    let win = w(50, 64, 1 << 20);
    // start, transactions done, plain inserts released, plain inserts done
    let barrier = Arc::new(Barrier::new(THREADS + 1));
    let threads: Vec<_> = (0..THREADS)
        .map(|t| {
            let (db, barrier) = (db.clone(), barrier.clone());
            // A failure is returned, not panicked, so no barrier is left waiting.
            thread::spawn(
                move || -> turso_durable::Result<(ThreadId, Option<Batching>)> {
                    let c = db.connect().unwrap();
                    c.set_batching(Some(Batching::Off)).unwrap();
                    barrier.wait();
                    let txns = (0..5).try_for_each(|i| {
                        let id = (t * 10 + 2 * i) as i64;
                        c.transaction_with(win, |tx| {
                            tx.execute(
                                "INSERT INTO t VALUES (?), (?)",
                                &[Value::from_i64(id), Value::from_i64(id + 1)],
                            )
                        })
                        .map(|_| ())
                    });
                    barrier.wait();
                    barrier.wait();
                    let plain =
                        (0..5).try_for_each(|i| insert(&c, (1000 + t * 10 + i) as i64).map(|_| ()));
                    barrier.wait();
                    txns?;
                    plain?;
                    let own = c.batching();
                    c.close()?;
                    Ok((thread::current().id(), own))
                },
            )
        })
        .collect();
    // Each snapshot is taken while every worker waits at the next barrier.
    let s0 = db.upload_stats();
    barrier.wait();
    barrier.wait();
    let s1 = db.upload_stats();
    barrier.wait();
    barrier.wait();
    let s2 = db.upload_stats();
    for t in threads {
        let (thread, own) = t.join().unwrap().unwrap();
        assert_eq!(own, Some(Batching::Off));
        assert_eq!(
            ack_settings(&rec, thread, 0),
            [[win; 5], [Batching::Off; 5]].concat()
        );
    }
    let (acked, puts) = delta(&s0, &s1);
    eprintln!("transaction_with: {acked} acked in {puts} log PUTs");
    assert_eq!(acked, 40);
    assert!(puts < 20, "{puts} log PUTs for 40 window transactions");
    assert_eq!(delta(&s1, &s2), (40, 40), "plain inserts: Off again");
    assert_eq!(count(&c), 120);
}

#[test]
fn durable_batching_pragma_sets_the_transaction() {
    let rec = Recorder::default();
    let (_d, db) = open(mem(), rec.clone());
    let c = db.connect().unwrap();
    create(&c);
    let me = thread::current().id();
    let win = w(40, 64, 1 << 20);
    let text = |s: &str| vec![vec![Value::from_text(s.to_string())]];
    let setting = |c: &Connection| c.query("PRAGMA durable_batching", &[]).unwrap();
    assert_eq!(setting(&c), text("off"));

    c.execute("BEGIN", &[]).unwrap();
    assert_eq!(
        c.execute("PRAGMA durable_batching = 'window 40ms 64 1MiB'", &[]),
        Ok(0)
    );
    assert_eq!(setting(&c), text("window 40ms 64 1MiB"));
    assert_eq!(c.execute("PRAGMA durable_batching", &[]), Ok(0));
    assert_eq!(
        c.batching(),
        None,
        "the connection's own setting is untouched"
    );
    insert(&c, 1).unwrap();
    let n = rec.acks_on(me).len();
    let t = Instant::now();
    c.execute("COMMIT", &[]).unwrap();
    assert!(
        t.elapsed() >= Duration::from_millis(40),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(ack_settings(&rec, me, n), vec![win]);
    assert_eq!(setting(&c), text("off"));
    assert_eq!(c.batching(), None);

    // The same in one execute_batch, which runs the PRAGMA in place.
    let n = rec.acks_on(me).len();
    let t = Instant::now();
    c.execute_batch(
        "BEGIN; PRAGMA durable_batching = 'window 40ms 64 1MiB'; INSERT INTO t VALUES (2); \
         COMMIT",
    )
    .unwrap();
    assert!(
        t.elapsed() >= Duration::from_millis(40),
        "{:?}",
        t.elapsed()
    );
    assert_eq!(ack_settings(&rec, me, n), vec![win]);
    assert_eq!(setting(&c), text("off"));
    // An INSERT after the COMMIT is outside the transaction, so Off (and its upload carries
    // the waiting window commit with it).
    let n = rec.acks_on(me).len();
    c.execute_batch(
        "BEGIN; PRAGMA durable_batching = 'window 10s 64 1MiB'; INSERT INTO t VALUES (3); \
         COMMIT; INSERT INTO t VALUES (4)",
    )
    .unwrap();
    assert_eq!(
        ack_settings(&rec, me, n),
        vec![w(10_000, 64, 1 << 20), Batching::Off]
    );

    // The connection's own setting shows when no transaction has one.
    c.set_batching(Some(w(5, 2, 3))).unwrap();
    assert_eq!(setting(&c), text("window 5ms 2 3"));
    assert_eq!(count(&c), 4);
}

#[test]
fn durable_batching_pragma_outside_transaction_or_malformed_is_refused() {
    let (_d, db) = open(mem(), Recorder::default());
    let conn = db.connect().unwrap();
    create(&conn);
    let r = conn.execute("PRAGMA durable_batching = 'window 40ms 64 1MiB'", &[]);
    assert!(
        matches!(&r, Err(Error::Config(m)) if m.contains("current transaction")),
        "{r:?}"
    );
    conn.execute("BEGIN", &[]).unwrap();
    // Our own parse error (the engine never prepares the PRAGMA; it would not know it).
    let r = conn.execute("PRAGMA durable_batching = 'window x'", &[]);
    assert!(
        matches!(&r, Err(Error::Config(m)) if m.starts_with("batching \"window x\"")),
        "{r:?}"
    );
    let r = conn.execute("PRAGMA durable_batching = 'window 1ms 0 1'", &[]);
    assert!(
        matches!(&r, Err(Error::Config(m)) if m.contains("max_commits")),
        "{r:?}"
    );
    let r = conn.execute("PRAGMA durable_batching = off", &[Value::from_i64(1)]);
    assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
    let r = conn.execute("PRAGMA durable_batching = off; SELECT 1", &[]);
    assert!(
        matches!(&r, Err(Error::Config(m)) if m.contains("its own statement")),
        "{r:?}"
    );
    conn.execute("COMMIT", &[]).unwrap();
    assert_eq!(conn.batching(), None);
    assert!(matches!(
        conn.execute_batch("SELECT 1; PRAGMA durable_batching = off"),
        Err(Error::Config(_))
    ));
    // A refused statement anywhere in a batch means none of it runs.
    let r = conn.execute_batch(
        "INSERT INTO t VALUES (1); PRAGMA durable_batching = off; PRAGMA synchronous = off",
    );
    assert_eq!(r, Err(Error::Unsupported("synchronous below FULL")));
    assert_eq!(count(&conn), 0);
}

#[test]
fn a_failed_transaction_clears_its_override() {
    let rec = Recorder::default();
    let (_d, db) = open(mem(), rec.clone());
    let c = db.connect().unwrap();
    create(&c);
    let me = thread::current().id();
    let slow = w(10_000, 64, 1 << 20);
    assert!(matches!(
        c.transaction_with(w(1, 0, 1), |_| Ok(())),
        Err(Error::Config(_))
    ));
    let r: turso_durable::Result<()> = c.transaction_with(slow, |tx| {
        tx.execute("INSERT INTO t VALUES (1)", &[])?;
        Err(Error::Config("x".into()))
    });
    assert_eq!(r, Err(Error::Config("x".into())));
    let n = rec.acks_on(me).len();
    let t = Instant::now();
    insert(&c, 2).unwrap();
    assert!(t.elapsed() < Duration::from_secs(1), "{:?}", t.elapsed());
    assert_eq!(ack_settings(&rec, me, n), vec![Batching::Off]);
    // ROLLBACK clears a PRAGMA's override too.
    c.execute("BEGIN", &[]).unwrap();
    c.execute("PRAGMA durable_batching = 'window 10s 64 1MiB'", &[])
        .unwrap();
    c.execute("ROLLBACK", &[]).unwrap();
    insert(&c, 3).unwrap();
    assert_eq!(ack_settings(&rec, me, n), vec![Batching::Off; 2]);
    assert_eq!(count(&c), 2);
}

/// A COMMIT whose upload fails, and one the engine rolls back: neither leaves the
/// transaction's setting behind.
#[test]
fn a_failed_commit_clears_its_override() {
    let rec = Recorder::default();
    let (mem, fault, _d, db) = faulty(rec.clone());
    let c = db.connect().unwrap();
    create(&c);
    let me = thread::current().id();
    let win = w(20, 64, 1 << 20);
    fault.fail_puts_after(0);
    // Committed locally, then the window's upload fails.
    let r = c.transaction_with(win, |tx| tx.execute("INSERT INTO t VALUES (1)", &[]));
    assert!(
        matches!(r, Err(Error::Timeout(_) | Error::UploadFailed(_))),
        "{r:?}"
    );
    assert_eq!(setting_text(&c), "off");
    // While degraded the commit takes the Off path, its WAL sync fails and the engine
    // rolls the transaction back.
    c.execute("BEGIN", &[]).unwrap();
    c.execute("PRAGMA durable_batching = 'window 20ms 64 1MiB'", &[])
        .unwrap();
    insert(&c, 2).unwrap();
    let r = c.execute("COMMIT", &[]);
    assert!(r.is_err(), "{r:?}");
    assert_eq!(setting_text(&c), "off");
    fault.fail_puts_after(u64::MAX);
    let n = rec.acks_on(me).len();
    insert(&c, 3).unwrap();
    assert_eq!(ack_settings(&rec, me, n), vec![Batching::Off]);
    c.close().unwrap();
    db.close().unwrap();
    assert_eq!(restore_rows(mem), vec![1, 3]);
}
