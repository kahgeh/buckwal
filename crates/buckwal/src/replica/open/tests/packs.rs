//! A replica of a format 5 bucket (packs, spec "Restore"): it follows the log, applies each
//! generation switch with its own local checkpoint, and reads no page from the bucket outside a
//! restore. A replica that falls behind the log that GC kept re-restores from the newest pack;
//! one whose re-restore gives up keeps its current copy and tries again at its next slow poll.
use super::*;
use crate::pack::PackPointer;

/// `wconfig` without the packer thread: the tests run each pack run themselves.
fn packed_wconfig(dir: &Path) -> Config {
    let mut c = wconfig(dir);
    c.pack = false;
    c
}

/// One pack run against `store`, from a working directory of its own; it must pack.
fn pack_now(store: &Arc<MemStore>, cfg: &Config) -> PackPointer {
    let t = tempfile::tempdir().unwrap();
    let mut cfg = cfg.clone();
    cfg.dir = t.path().join("packer");
    let store: Arc<dyn ObjectStore> = store.clone();
    let r = crate::pack::pack_with_store(&cfg, store.clone()).unwrap();
    assert_eq!(r.outcome, crate::PackOutcome::Packed, "{r:?}");
    pointer(&*store)
}

fn pointer(store: &dyn ObjectStore) -> PackPointer {
    let k = Keys::new(PREFIX).adopting(store);
    k.decode_pointer(&store.get(&k.pack_pointer()).unwrap().unwrap().body)
        .unwrap()
}

/// A format 5 writer (rows 1 to 5 over one generation switch, then packed) and a replica of it
/// behind a [`Spy`] that has caught up.
struct Packed {
    store: Arc<MemStore>,
    spy: Arc<Spy>,
    _w: tempfile::TempDir,
    wcfg: Config,
    wdb: crate::Database,
    wc: crate::Connection,
    _r: tempfile::TempDir,
    dir: PathBuf,
    replica: Replica,
}

fn packed(poll: Duration) -> Packed {
    let store = Arc::new(MemStore::new());
    let w = tempfile::tempdir().unwrap();
    let wcfg = packed_wconfig(&w.path().join("w"));
    let wdb = crate::open_with_store(wcfg.clone(), store.clone()).unwrap();
    let wc = wdb.connect().unwrap();
    wc.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    for i in 1..=3 {
        add(&wc, i);
    }
    wc.checkpoint(CheckpointMode::Truncate).unwrap();
    for i in 4..=5 {
        add(&wc, i);
    }
    let p = pack_now(&store, &wcfg);
    assert_eq!(p.pack_generation, wdb.generation());
    let spy = Spy::new(store.clone());
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let mut c = rconfig(&dir);
    c.replica_poll_interval = poll;
    let replica = open_replica_with_store(c, spy.clone()).unwrap();
    let rc = replica.connect().unwrap();
    wait_rows(&rc, 5);
    rc.close().unwrap();
    Packed {
        store,
        spy,
        _w: w,
        wcfg,
        wdb,
        wc,
        _r: r,
        dir,
        replica,
    }
}

impl Packed {
    /// Hold the replica's pass, write rows 6 to 11 over two generation switches, make a new
    /// pack, and do GC's work: delete the old pack, its codes and the log of every generation
    /// below the new pack's. Returns the new pointer; the pass is still held.
    fn fall_behind_gc(&self) -> PackPointer {
        self.spy.block();
        self.replica.waker().wake();
        self.spy.wait_blocked();
        let old = pointer(&*self.store);
        for i in 6..=8 {
            add(&self.wc, i);
        }
        self.wc.checkpoint(CheckpointMode::Truncate).unwrap();
        for i in 9..=10 {
            add(&self.wc, i);
        }
        self.wc.checkpoint(CheckpointMode::Truncate).unwrap();
        add(&self.wc, 11);
        let new = pack_now(&self.store, &self.wcfg);
        assert!(new.pack_generation > old.pack_generation, "{new:?}");
        let keys = Keys::new(PREFIX);
        ObjectStore::delete(&*self.store, &old.pack_key).unwrap();
        ObjectStore::delete(&*self.store, &old.codes_key).unwrap();
        for g in 1..new.pack_generation {
            for k in ObjectStore::list(&*self.store, &keys.log_prefix(g)).unwrap() {
                ObjectStore::delete(&*self.store, &k).unwrap();
            }
        }
        new
    }
}

/// The keys the replica read from the pack: the pointer, the codes and the pack itself.
fn pack_reads(keys: &[String]) -> Vec<&String> {
    keys.iter()
        .filter(|k| k.contains("/pack/") || k.ends_with("/pack-latest"))
        .collect()
}

/// A writer switches generations twice while the replica follows. The replica applies each
/// switch with its own local checkpoint (its `-wal` is empty after the switch, with no reader
/// in a transaction), moves to the writer's generation and reads no page, pack or pointer from
/// the bucket after its restore at open.
#[test]
fn replica_follows_generation_switch_with_local_checkpoint() {
    let p = packed(POLL);
    let opened = p.spy.got().len();
    assert!(
        !pack_reads(&p.spy.got()).is_empty(),
        "the restore at open read the pack"
    );
    let rc = p.replica.connect().unwrap();
    for i in 6..=8 {
        add(&p.wc, i);
    }
    p.wc.checkpoint(CheckpointMode::Truncate).unwrap();
    add(&p.wc, 9);
    p.wc.checkpoint(CheckpointMode::Truncate).unwrap();
    let want = p.wdb.generation();
    until("the writer's generation", || {
        p.replica.stats().generation == want
    });
    wait_rows(&rc, 9);
    let wal = std::fs::metadata(p.dir.join(crate::files::WAL_FILE)).map_or(0, |m| m.len());
    assert_eq!(
        wal, 0,
        "the switch was applied with a local TRUNCATE checkpoint"
    );
    add(&p.wc, 10);
    wait_rows(&rc, 10);
    let s = p.replica.stats();
    assert_eq!((s.re_restores, s.stopped.clone()), (0, None), "{s:?}");
    let after = p.spy.got()[opened..].to_vec();
    assert_eq!(pack_reads(&after), Vec::<&String>::new());
    assert_eq!(p.spy.page_gets(), 0, "a format 5 bucket has no page object");
    assert_eq!(p.spy.puts.load(SeqCst) + p.spy.deletes.load(SeqCst), 0);
    rc.close().unwrap();
    p.replica.close().unwrap();
}

/// The replica falls behind: GC deleted its generation's log and the pack it restored from
/// after a newer pack replaced it. It re-restores once, from the newest pack, and follows on.
#[test]
fn replica_behind_gc_re_restores_from_newest_pack() {
    let cap = Capture::new();
    let (spans, new) = tracing::subscriber::with_default(cap.subscriber(), || {
        let p = packed(POLL);
        let opened = p.spy.got().len();
        let new = p.fall_behind_gc();
        p.spy.unblock();
        until("a re-restore", || p.replica.stats().re_restores >= 1);
        let rc = p.replica.connect().unwrap();
        wait_rows(&rc, 11);
        add(&p.wc, 12);
        wait_rows(&rc, 12);
        let s = p.replica.stats();
        assert_eq!(s.re_restores, 1, "{s:?}");
        assert_eq!(s.generation, p.wdb.generation());
        assert_eq!(s.stopped, None);
        let after = p.spy.got()[opened..].to_vec();
        assert!(after.contains(&new.codes_key), "{after:?}");
        assert!(after.contains(&new.pack_key), "{after:?}");
        assert_eq!(p.spy.puts.load(SeqCst) + p.spy.deletes.load(SeqCst), 0);
        rc.close().unwrap();
        p.replica.close().unwrap();
        (cap.spans(), new)
    });
    let warns = fell_behind_events(&spans);
    assert_eq!(warns.len(), 1, "{warns:?}");
    assert_eq!(warns[0].field("reason"), Some("trimmed"));
    let rr = spans.named("buckwal.replica.re_restore");
    assert_eq!(rr.len(), 1, "{rr:?}");
    let restore = spans.with_parent(rr[0].id, "buckwal.restore");
    assert_eq!(restore.field("buckwal.restore.outcome"), Some("restored"));
    assert!(new.pack_generation > 1);
}

/// The re-restore of a replica that fell behind gives up: the pack the pointer names stays
/// gone through the restart limit. The replica keeps its current copy readable, logs the
/// restore's ERROR `restore gave up`, does not try again on notifications, and tries again at
/// its next slow poll, which then succeeds.
#[test]
fn replica_whose_re_restore_gives_up_keeps_its_copy_and_retries_at_next_slow_poll() {
    let interval = Duration::from_secs(3);
    let cap = Capture::new();
    let gave_up = |cap: &Capture| {
        cap.spans()
            .events
            .iter()
            .filter(|e| e.field("message") == Some("restore gave up"))
            .count()
    };
    tracing::subscriber::with_default(cap.subscriber(), || {
        let p = packed(interval);
        let new = p.fall_behind_gc();
        let saved = ObjectStore::get(&*p.store, &new.pack_key)
            .unwrap()
            .unwrap()
            .body;
        ObjectStore::delete(&*p.store, &new.pack_key).unwrap();
        p.spy.unblock();
        until("the re-restore gave up", || gave_up(&cap) >= 1);
        let t0 = Instant::now();
        let rc = p.replica.connect().unwrap();
        assert_eq!(count(&rc), 5, "the current copy stays readable");
        let s = p.replica.stats();
        assert_eq!((s.re_restores, s.stopped.clone()), (0, None), "{s:?}");
        // Notifications before the next slow poll do not start the re-restore again: they
        // run for 1.5 s, three times the first wait of another failure.
        let waker = p.replica.waker();
        for _ in 0..50 {
            waker.wake();
            std::thread::sleep(Duration::from_millis(30));
        }
        assert_eq!(gave_up(&cap), 1, "no attempt before the next slow poll");
        assert_eq!(count(&rc), 5);
        ObjectStore::put(&*p.store, &new.pack_key, &saved).unwrap();
        until("a re-restore", || p.replica.stats().re_restores >= 1);
        let waited = t0.elapsed();
        assert!(
            waited + Duration::from_millis(300) >= interval,
            "tried again after {waited:?}, before the slow poll"
        );
        wait_rows(&rc, 11);
        rc.close().unwrap();
        p.replica.close().unwrap();
    });
    let spans = cap.spans();
    let errors: Vec<_> = spans
        .events
        .iter()
        .filter(|e| e.field("message") == Some("restore gave up"))
        .collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].level, tracing::Level::ERROR);
    let outcomes: Vec<_> = spans
        .named("buckwal.restore")
        .iter()
        .filter_map(|s| s.field("buckwal.restore.outcome"))
        .collect();
    assert!(outcomes.contains(&"gave_up"), "{outcomes:?}");
    assert_eq!(outcomes.last(), Some(&"restored"), "{outcomes:?}");
}

/// A replica copy of a format 1 to 4 bucket (no `app.db-pack`, not complete: some pages are
/// still in `pages/`) opens under a format 5 head, as after `buckwal upgrade`. The copy has no
/// page source then, so the open restores again from the format 5 bucket (its pack and log)
/// with a WARN, and the replica reads the format 5 database.
#[test]
fn a_copy_without_a_pack_under_a_format_5_head_restores_again_at_open() {
    let legacy = Arc::new(MemStore::new());
    let (_lw, _ldb, _lc) = writer(&legacy, 3);
    let r = tempfile::tempdir().unwrap();
    let dir = r.path().join("r");
    let old = open_replica_with_store(rconfig(&dir), legacy.clone()).unwrap();
    let rc = old.connect().unwrap();
    wait_rows(&rc, 3);
    rc.close().unwrap();
    old.close().unwrap();
    // Some pages are still in the bucket, as in a large copy whose fetch did not end.
    let mut state = LocalState::read(&dir).unwrap().unwrap();
    state.complete = false;
    state.write_atomic(&dir).unwrap();
    assert!(!dir.join(crate::files::PACK_FILE).exists());

    // The bucket is now format 5, with a pack: the same prefix, another store.
    let store = Arc::new(MemStore::new());
    let w = tempfile::tempdir().unwrap();
    let wcfg = packed_wconfig(&w.path().join("w"));
    let wdb = crate::open_with_store(wcfg.clone(), store.clone()).unwrap();
    let wc = wdb.connect().unwrap();
    wc.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    for i in 1..=7 {
        add(&wc, i);
    }
    wc.checkpoint(CheckpointMode::Truncate).unwrap();
    add(&wc, 8);
    pack_now(&store, &wcfg);

    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let replica = open_replica_with_store(rconfig(&dir), store.clone()).unwrap();
        let rc = replica.connect().unwrap();
        wait_rows(&rc, 8);
        add(&wc, 9);
        wait_rows(&rc, 9);
        assert_eq!(replica.stats().stopped, None);
        rc.close().unwrap();
        replica.close().unwrap();
        cap.spans()
    });
    let warns: Vec<_> = spans
        .events
        .iter()
        .filter(|e| {
            e.field("message")
                == Some("replica copy has no pack under a format 5 head, restoring again")
        })
        .collect();
    assert_eq!(warns.len(), 1, "one WARN for the restore at open");
    assert_eq!(warns[0].level, tracing::Level::WARN);
    assert_eq!(spans.named("buckwal.restore").len(), 1);
}

/// The writer vacuums (`buckwal vacuum`, with the writer closed for it) while a replica
/// follows. The replica applies the vacuum's commit like any other: after the writer's next
/// commits, its rows equal the writer's and its copy passes `PRAGMA integrity_check`.
#[test]
fn replica_follows_a_vacuum() {
    let Packed {
        store,
        _w: _wdir,
        wcfg,
        wdb,
        wc,
        _r: _rdir,
        replica,
        ..
    } = packed(POLL);
    let big = |id: i64| format!("{id:06}{}", "y".repeat(3500));
    for id in 6..=200 {
        wc.execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            &[int(id), Value::build_text(big(id))],
        )
        .unwrap();
    }
    wc.execute("DELETE FROM t WHERE id > 100", &[]).unwrap();
    wc.checkpoint(CheckpointMode::Truncate).unwrap();
    wc.close().unwrap();
    wdb.close().unwrap();
    let v = crate::vacuum_with_store(&wcfg, store.clone()).unwrap();
    assert!(v.pages_after * 10 < v.pages_before * 7, "{v:?}");
    let wdb = crate::open_with_store(wcfg.clone(), store.clone()).unwrap();
    let wc = wdb.connect().unwrap();
    for id in 201..=210 {
        wc.execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            &[int(id), Value::build_text(big(id))],
        )
        .unwrap();
    }
    let all = "SELECT id, length(v), substr(v, 1, 6) FROM t ORDER BY id";
    let want = wc.query(all, &[]).unwrap();
    assert_eq!(want.len(), 110);
    let rc = replica.connect().unwrap();
    until("the replica has the writer's rows", || {
        rc.query(all, &[]).unwrap() == want
    });
    let ok = rc.query("PRAGMA integrity_check", &[]).unwrap();
    assert_eq!(ok[0][0].to_text(), Some("ok"));
    rc.close().unwrap();
    replica.close().unwrap();
    wc.close().unwrap();
    wdb.close().unwrap();
}
