//! System tests of read replicas against a real RustFS (first half; see the spec's Tests
//! section). The writer is `buckwal soak` or an in-process `Writer`; the replica is either
//! `buckwal follow` as a child (status lines, kill -9, SIGTERM) or `open_replica` in the test
//! process over a counting store (`stats()`, row counts, the S3 store's PUT and DELETE counts).
//! The notification listener is the same code `follow` runs; the test posts the S3-format
//! `ObjectCreated` events itself (RustFS cannot deliver to the listener, docs/read-replicas-recon.md,
//! question 8).
//!
//! In the second pass of `scripts/test.sh` (`BUCKWAL_TLS_DIR` set) every listener serves HTTPS
//! with a token and the database is encrypted (`BUCKWAL_ENCRYPTION_KEY_FILE` reaches the
//! writer, the replica and the children through `Ctx`); the first pass uses plain loopback HTTP.
//! Run through `./scripts/test.sh` (`--test-threads=1`).
mod common;

use buckwal::store::ObjectStore;
use common::*;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::time::{Duration, Instant};

/// How long a commit may take to show on a replica polling every 500 ms.
const BOUND: Duration = Duration::from_secs(20);

const WINDOW: &str = "window 20ms 64 1MiB";

/// The newest `log/` key of the test's prefix (what a bucket notification would name).
fn newest_log_key(ctx: &Ctx) -> String {
    ctx.list("log/").pop().expect("a log object")
}

/// `put`s and `delete`s the replica's store sent: both must be 0.
fn assert_no_writes(rep: &Rep, what: &str) {
    let s = rep.store.stats();
    assert_eq!(
        (s.puts, s.deletes),
        (0, 0),
        "{what}: the replica sent PUTs or DELETEs: {s:?}"
    );
}

// ---------------------------------------------------------------------------------------
// replica_follows_writer_off_and_window
// ---------------------------------------------------------------------------------------

/// A `soak` writer commits 600 rows while a replica (slow poll 500 ms, no notifications)
/// follows. Whenever the test reads the ack log (n acks), the replica must show at least n
/// rows within `BOUND`, its count never decreases, and when the writer has exited the replica's
/// rows are the ack log's (count and id sum). `stats()` reports a lag; zero PUT and DELETE.
fn follows_running_writer(name: &str, writer_args: &[&str]) {
    let ctx = Ctx::new(name);
    let a = ctx.dir("a");
    let mut soak = SoakProc::spawn(&ctx, &a, writer_args);
    soak.wait_acks(1, Duration::from_secs(120));
    let rep = Rep::open(&ctx, &ctx.dir("r"), Duration::from_millis(500));
    let (mut last, mut worst, mut samples) = (0, Duration::ZERO, 0);
    let status = loop {
        let acked = soak.acked().len() as i64;
        let t = Instant::now();
        let n = rep.wait_at_least(acked, BOUND);
        worst = worst.max(t.elapsed());
        assert!(
            n >= last,
            "{name}: the replica's rows stepped back: {last} then {n}"
        );
        last = n;
        samples += 1;
        if let Some(s) = soak.exited() {
            break s;
        }
        std::thread::sleep(Duration::from_millis(250));
    };
    assert!(
        status.success(),
        "{name}: soak exited {status}: {:?}",
        soak.lines()
    );
    let acked = soak.acked();
    let sum: i64 = acked.iter().sum();
    rep.wait_count(acked.len() as i64, BOUND);
    assert_eq!(
        rep.sum(),
        sum,
        "{name}: the replica's ids are the ack log's"
    );
    let st = rep.stats();
    assert!(st.lag_ms.is_some(), "{name}: stats() reports a lag: {st:?}");
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, name);
    eprintln!(
        "{name}: {} acks, {samples} samples, worst wait for an ack {worst:?}, lag_ms={:?}, \
         slow-poll passes {}",
        acked.len(),
        st.lag_ms,
        st.passes_slow_poll
    );
    rep.close();
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_follows_writer_off_and_window() {
    follows_running_writer("replica_follows_writer_off", &["--count", "600"]);
    follows_running_writer(
        "replica_follows_writer_window",
        &["--count", "600", "--batching", WINDOW, "--connections", "4"],
    );
}

// ---------------------------------------------------------------------------------------
// replica_wakes_on_posted_notification_under_5s
// ---------------------------------------------------------------------------------------

/// Slow poll 60 s. In process: a commit is not visible for 2 s (no event, no poll), then the
/// test posts the event (bearer token in pass 2) and the commit shows in under 5 s; `stats()`
/// counts it as a notification pass and no slow-poll pass ran. Then the same through
/// `buckwal follow` (its `notify listening` port, basic authentication in pass 2): after the
/// event, `notified received=1` and `applied generation=G seq=N` (N the writer's log object
/// count) appear in under 5 s.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_wakes_on_posted_notification_under_5s() {
    let ctx = Ctx::new("replica_wakes_on_posted_notification_under_5s");
    let access = NotifyAccess::of_pass(&ctx);
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=20);
    let rep = Rep::open(&ctx, &ctx.dir("r"), Duration::from_secs(60));
    let listener = Listener::start(&access, rep.waker());
    assert_eq!(rep.count(), 20);

    w.insert(21);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        rep.count(),
        20,
        "no event and no poll yet: the commit is not visible"
    );
    let t0 = Instant::now();
    listener.poster.post_ok(&newest_log_key(&ctx), Auth::Bearer);
    rep.wait_count(21, Duration::from_secs(5));
    let seen = t0.elapsed();
    assert!(seen < Duration::from_secs(5), "visible after {seen:?}");
    let st = rep.stats();
    assert!(st.passes_notification >= 1, "{st:?}");
    assert_eq!(st.passes_slow_poll, 0, "{st:?}");
    assert!(st.notifications_received >= 1, "{st:?}");
    assert_no_writes(&rep, "in process");

    // The same through the `follow` binary.
    let mut f = Follower::spawn(&ctx, &ctx.dir("c"), 60_000, Some(&access));
    let poster = f.poster.clone().expect("follow has a listener");
    let from = f.lines.len();
    w.insert(22);
    let g = ctx.head().generation;
    let n = ctx.assert_log_contiguous(g);
    let t0 = Instant::now();
    poster.post_ok(&newest_log_key(&ctx), Auth::Basic);
    f.wait_line(from, "notified", Duration::from_secs(5), |l| {
        l == "notified received=1"
    });
    f.wait_applied(from, g, n, Duration::from_secs(5));
    eprintln!(
        "replica_wakes_on_posted_notification_under_5s: in process {seen:?}; follow {:?}",
        t0.elapsed()
    );
    assert!(f.sigterm().success(), "{}", f.tail());
    w.close().expect("close writer");
    rep.close();
}

// ---------------------------------------------------------------------------------------
// replica_dropped_notification_slow_poll_applies
// ---------------------------------------------------------------------------------------

/// The event is never posted (a dropped notification). Slow poll 3 s: the commit shows
/// within 12 s, `stats()` counts a slow-poll pass, no notification pass, no notification
/// received.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_dropped_notification_slow_poll_applies() {
    let ctx = Ctx::new("replica_dropped_notification_slow_poll_applies");
    let access = NotifyAccess::of_pass(&ctx);
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=10);
    let rep = Rep::open(&ctx, &ctx.dir("r"), Duration::from_secs(3));
    let _listener = Listener::start(&access, rep.waker());
    assert_eq!(rep.count(), 10);
    w.insert(11);
    let _dropped = s3_event(&newest_log_key(&ctx)); // what the bucket sent, and the test discards
    rep.wait_count(11, Duration::from_secs(12));
    let st = rep.stats();
    assert!(st.passes_slow_poll >= 1, "{st:?}");
    assert_eq!(st.passes_notification, 0, "{st:?}");
    assert_eq!(st.notifications_received, 0, "{st:?}");
    assert_no_writes(&rep, "dropped");
    w.close().expect("close writer");
    rep.close();
}

// ---------------------------------------------------------------------------------------
// replica_duplicate_and_out_of_order_notifications
// ---------------------------------------------------------------------------------------

/// Slow poll 60 s. Phase 1: 5 commits, the events for their log objects posted newest first
/// and each twice. Phase 2: 3 more commits, the events for them posted newest first, twice.
/// The replica's rows are the writer's after each phase (count and id sum). Then events for
/// old objects are replayed and the extra passes find nothing new: `objects_fetched` does not
/// move and `notification_passes_nothing_new` grows.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_duplicate_and_out_of_order_notifications() {
    let ctx = Ctx::new("replica_duplicate_and_out_of_order_notifications");
    let access = NotifyAccess::of_pass(&ctx);
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert(1);
    let rep = Rep::open(&ctx, &ctx.dir("r"), Duration::from_secs(60));
    let listener = Listener::start(&access, rep.waker());
    rep.wait_equal(&w, BOUND);
    let known = ctx.list("log/").len();

    let post_reversed_twice = |ctx: &Ctx, keys: &[String]| {
        let _ = ctx;
        for _ in 0..2 {
            for k in keys.iter().rev() {
                listener.poster.post_ok(k, Auth::Bearer);
            }
        }
    };
    w.insert_range(2..=5);
    let keys = ctx.list("log/");
    post_reversed_twice(&ctx, &keys[known..]);
    rep.wait_equal(&w, Duration::from_secs(5));

    let known = ctx.list("log/").len();
    w.insert_range(6..=8);
    let keys = ctx.list("log/");
    post_reversed_twice(&ctx, &keys[known..]);
    rep.wait_equal(&w, Duration::from_secs(5));

    // Replays: wait for each notification pass to finish, then compare the counters.
    std::thread::sleep(Duration::from_secs(1));
    let before = rep.stats();
    for k in keys.iter().take(3) {
        let passes = rep.stats().passes_notification;
        listener.poster.post_ok(k, Auth::Bearer);
        wait_until("the replayed event's pass", Duration::from_secs(5), || {
            (rep.stats().passes_notification > passes).then_some(())
        });
    }
    let after = rep.stats();
    assert_eq!(
        after.objects_fetched, before.objects_fetched,
        "{before:?} then {after:?}"
    );
    assert!(
        after.notification_passes_nothing_new >= before.notification_passes_nothing_new + 3,
        "{before:?} then {after:?}"
    );
    assert_eq!(after.passes_slow_poll, 0, "{after:?}");
    assert_eq!((rep.count(), rep.sum()), (8, w.sum()));
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, "duplicates");
    w.close().expect("close writer");
    rep.close();
}

// ---------------------------------------------------------------------------------------
// replica_without_notification_setup_runs_on_slow_poll
// ---------------------------------------------------------------------------------------

/// `follow` with no BUCKWAL_REPLICA_NOTIFY_* setting: no `notify listening` line and no
/// `notified` line ever; it applies each commit on the slow poll (2 s) with the writer's log
/// object count as `seq`; after SIGTERM (exit 0) a reopen of its directory holds the writer's
/// rows.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_without_notification_setup_runs_on_slow_poll() {
    let ctx = Ctx::new("replica_without_notification_setup_runs_on_slow_poll");
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=5);
    let c = ctx.dir("c");
    let mut f = Follower::spawn(&ctx, &c, 2000, None);
    assert!(
        !f.lines.iter().any(|l| l.starts_with("notify listening")),
        "{:?}",
        f.lines
    );
    for id in 6..=8 {
        let from = f.lines.len();
        w.insert(id);
        let g = ctx.head().generation;
        let n = ctx.assert_log_contiguous(g);
        f.wait_applied(from, g, n, Duration::from_secs(15));
    }
    assert!(f.sigterm().success(), "{}", f.tail());
    assert!(
        !f.lines
            .iter()
            .any(|l| l.starts_with("notified") || l.starts_with("stopped")),
        "{:?}",
        f.lines
    );
    let rep = Rep::open(&ctx, &c, Duration::from_secs(2));
    rep.wait_equal(&w, BOUND);
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, "reopen");
    w.close().expect("close writer");
    rep.close();
}

// ---------------------------------------------------------------------------------------
// replica_follows_seal_new_generation_and_third_machine_takeover
// ---------------------------------------------------------------------------------------

/// Machine A writes 30 rows, a replica follows; A seals (TRUNCATE checkpoint: the head moves
/// to the next generation), writes 10 more: the replica follows into the new generation. Then
/// machine B (an empty directory) takes over (`open` on an empty directory with a database in
/// the bucket) and writes 11 rows with a seal in between: A's next commit fails (fenced), and
/// the replica keeps following the new writer: its generation is the head's and its rows
/// (count and id sum) are A's 40 plus B's 11. Zero PUT and DELETE. Short lease so B's seal
/// does not wait for the default 30 s.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_follows_seal_new_generation_and_third_machine_takeover() {
    let ctx = Ctx::new("replica_follows_seal_new_generation_and_third_machine_takeover");
    let mut a = Writer::open(short_lease(ctx.config(&ctx.dir("a"))));
    a.insert_range(1..=30);
    let mut cfg = short_lease(ctx.config(&ctx.dir("r")));
    cfg.replica_poll_interval = Duration::from_millis(500);
    let rep = Rep::open_cfg(&ctx, cfg);
    rep.wait_equal(&a, BOUND);
    let g0 = ctx.head().generation;
    assert_eq!(rep.stats().generation, g0);

    a.seal();
    let g1 = ctx.head().generation;
    assert!(g1 > g0, "the seal moved the head: {g0} then {g1}");
    a.insert_range(31..=40);
    rep.wait_equal(&a, Duration::from_secs(30));
    assert_eq!(rep.stats().generation, ctx.head().generation);

    // The third machine takes over.
    let mut b = Writer::open(short_lease(ctx.config(&ctx.dir("b"))));
    b.insert_range(101..=105);
    b.seal();
    b.insert_range(106..=111);
    let late = a.try_insert(41);
    assert!(
        matches!(late, Err(buckwal::Error::Fenced(_))),
        "A's commit after the takeover: {late:?}"
    );
    let want_count = (a.acked.len() + b.acked.len()) as i64;
    let want_sum = a.sum() + b.sum();
    wait_until(
        "the replica to equal A plus B",
        Duration::from_secs(60),
        || (rep.count() == want_count && rep.sum() == want_sum).then_some(()),
    );
    let head = ctx.head();
    assert!(head.generation > g1 && head.epoch >= 1, "{head:?}");
    assert_eq!(rep.stats().generation, head.generation);
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, "takeover");
    eprintln!(
        "takeover: generations {g0} -> {g1} -> {}, epoch {}, {:?}",
        head.generation,
        head.epoch,
        rep.stats()
    );
    let _ = a.close();
    b.close().expect("close B");
    rep.close();
}

// ---------------------------------------------------------------------------------------
// replica_reader_held_across_seal
// ---------------------------------------------------------------------------------------

/// Slow poll 500 ms (checkpoint budget 1 s). A read transaction on the replica sees 10 rows;
/// the writer adds 5, seals, adds 5 more. The replica numbers on into the new generation
/// (`stats().generation` = the head's, `deferred_checkpoints` >= 1), the held transaction still
/// sees 10 rows, a new connection sees all 20, and the position file still has a positive
/// offset (the checkpoint has not run). After the reader commits and the writer commits again,
/// the deferred checkpoint runs: the position file reads `applying` with a negative offset.
/// Rows equal the writer's, `integrity_check` ok.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_reader_held_across_seal() {
    let ctx = Ctx::new("replica_reader_held_across_seal");
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=10);
    let r = ctx.dir("r");
    let rep = Rep::open(&ctx, &r, Duration::from_millis(500));
    rep.wait_equal(&w, BOUND);
    let reader = rep.r().connect().expect("reader");
    reader
        .transaction(|tx| {
            let held = tx.query("SELECT count(*) FROM soak", &[])?[0][0].as_int();
            assert_eq!(held, Some(10));
            w.insert_range(11..=15);
            w.seal();
            w.insert_range(16..=20);
            let head = ctx.head().generation;
            wait_until("the replica to number on", Duration::from_secs(60), || {
                let s = rep.stats();
                (s.generation == head && s.deferred_checkpoints >= 1 && rep.count() == 20)
                    .then_some(())
            });
            assert_eq!(
                tx.query("SELECT count(*) FROM soak", &[])?[0][0].as_int(),
                Some(10),
                "the held reader keeps its snapshot"
            );
            let p = read_position(&r).expect("position file");
            assert!(
                p.stage == "applying" && p.offset > 0 && p.generation == head,
                "the deferred checkpoint has not run: {p:?}"
            );
            Ok(())
        })
        .expect("held transaction");
    reader.close().expect("close reader");
    w.insert_range(21..=25);
    wait_until("the deferred checkpoint", Duration::from_secs(60), || {
        read_position(&r).filter(|p| p.stage == "applying" && p.offset < 0)
    });
    rep.wait_equal(&w, BOUND);
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, "reader held");
    w.close().expect("close writer");
    rep.close();
}

// ---------------------------------------------------------------------------------------
// replica_kill_matrix
// ---------------------------------------------------------------------------------------

/// What the poller thread of one kill round watches for.
#[derive(Clone, Copy, Debug)]
enum Kill {
    /// The head names a newer generation (the follower has not yet written `ckpt_pending`).
    HeadMoved,
    /// The position file reads `ckpt_pending` of a newer generation (inside the checkpoint),
    /// then `delay_ms` later.
    CkptPending { delay_ms: u64 },
    /// The position file reads `applying` of a newer generation (after the checkpoint).
    ApplyingNew,
    /// No seal: 3 ms after the writer's commits, while the follower fetches and applies.
    MidApply,
}

/// `kill -9` of the `follow` child at each position-file step, as far as a child process can
/// be killed at a step: a poller thread reads the position file every millisecond (and the
/// head every 10 ms) while the writer commits 10 rows, seals and commits 10 more, and kills
/// the child the moment its trigger shows (the writer's head moving = before `ckpt_pending`;
/// `ckpt_pending` seen = inside the checkpoint, also +5 ms and +15 ms to land between the
/// checkpoint and the `applying` write; `applying` of the new generation = after it). The
/// steps inside a deferred checkpoint, and before/after each write by name, are the unit
/// tests' `Step` hooks (`kill_inside_a_deferred_checkpoint_resumes_to_the_writers_rows` and
/// the others in replica/sync/tests.rs); a deferred checkpoint cannot be set up here since the
/// child owns the directory's lock. After every kill a replica is reopened in process on the
/// same directory: its first reading is not below the last count the matrix saw (rows never
/// step back across a resume), it reaches the writer's rows (count and id sum),
/// `integrity_check` is ok, and the store sent no PUT or DELETE. The test fails when fewer
/// than 4 of the 7 seal triggers were caught (the rest are timed fallbacks, printed).
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_kill_matrix() {
    let ctx = Ctx::new("replica_kill_matrix");
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=20);
    let r = ctx.dir("r");
    let rep = Rep::open(&ctx, &r, Duration::from_millis(300));
    rep.wait_equal(&w, BOUND);
    rep.close();
    let kills = [
        Kill::HeadMoved,
        Kill::CkptPending { delay_ms: 0 },
        Kill::CkptPending { delay_ms: 5 },
        Kill::CkptPending { delay_ms: 15 },
        Kill::ApplyingNew,
        Kill::MidApply,
        Kill::CkptPending { delay_ms: 0 },
        Kill::HeadMoved,
    ];
    let store = ctx.store();
    let head_generation = || {
        let o = store.get(&format!("{}/head", ctx.prefix())).ok()??;
        Some(json_u64(&String::from_utf8(o.body).ok()?, "generation"))
    };
    let (mut seen, mut next, mut caught) = (20i64, 21i64, 0);
    for (round, kill) in kills.iter().enumerate() {
        let mut f = Follower::spawn(&ctx, &r, 300, None);
        let g0 = head_generation().expect("head");
        let pid = f.pid().to_string();
        let done = AtomicBool::new(false);
        let hit = std::thread::scope(|sc| {
            let poller = sc.spawn(|| {
                let deadline = Instant::now() + Duration::from_secs(40);
                let mut last_head = Instant::now() - Duration::from_secs(1);
                let mut moved = false;
                while Instant::now() < deadline && !done.load(SeqCst) {
                    let p = read_position(&r);
                    if last_head.elapsed() >= Duration::from_millis(10) {
                        last_head = Instant::now();
                        moved = head_generation().is_some_and(|g| g > g0);
                    }
                    let fire = match kill {
                        Kill::HeadMoved => moved,
                        Kill::CkptPending { .. } => p
                            .as_ref()
                            .is_some_and(|p| p.stage == "ckpt_pending" && p.generation > g0),
                        Kill::ApplyingNew => p
                            .as_ref()
                            .is_some_and(|p| p.stage == "applying" && p.generation > g0),
                        Kill::MidApply => false,
                    };
                    if fire {
                        if let Kill::CkptPending { delay_ms } = kill {
                            std::thread::sleep(Duration::from_millis(*delay_ms));
                        }
                        let _ = std::process::Command::new("kill")
                            .args(["-9", &pid])
                            .status();
                        return Some(format!("{p:?}"));
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                None
            });
            match kill {
                Kill::MidApply => {
                    w.insert_range(next..=next + 9);
                    next += 10;
                    std::thread::sleep(Duration::from_millis(3));
                    let _ = std::process::Command::new("kill")
                        .args(["-9", &pid])
                        .status();
                    done.store(true, SeqCst);
                    poller.join().unwrap();
                    Some("timed".to_string())
                }
                _ => {
                    w.insert_range(next..=next + 9);
                    w.seal();
                    w.insert_range(next + 10..=next + 19);
                    next += 20;
                    let hit = poller.join().unwrap();
                    done.store(true, SeqCst);
                    hit
                }
            }
        });
        f.kill9();
        let reached = hit.is_some() && !matches!(kill, Kill::MidApply);
        if reached {
            caught += 1;
        }
        eprintln!("kill round {round} {kill:?}: caught at {hit:?}");
        // The resume.
        let rep = Rep::open(&ctx, &r, Duration::from_millis(300));
        let first = rep.count();
        assert!(
            first >= seen,
            "round {round} {kill:?}: rows stepped back across the kill: {seen} then {first}"
        );
        let mut last = first;
        wait_until(
            "the resume to reach the writer's rows",
            Duration::from_secs(60),
            || {
                let n = rep.count();
                assert!(
                    n >= last,
                    "round {round}: rows stepped back while catching up: {last} then {n}"
                );
                last = n;
                (n == w.acked.len() as i64).then_some(())
            },
        );
        assert_eq!(rep.sum(), w.sum(), "round {round} {kill:?}");
        assert_eq!(rep.integrity(), "ok", "round {round} {kill:?}");
        assert_no_writes(&rep, "resume");
        seen = w.acked.len() as i64;
        rep.close();
    }
    assert!(
        caught >= 4,
        "only {caught} of the seal triggers were caught"
    );
    w.close().expect("close writer");
}

// ---------------------------------------------------------------------------------------
// replica_rolled_back_large_transaction
// ---------------------------------------------------------------------------------------

/// The writer runs a transaction of 3,000 rows of 4 KiB (about 12 MB, past the page cache so
/// its pages spill to the WAL and may reach the log) with pauses, then rolls it back, while a
/// replica polls every 300 ms. After every chunk the replica holds exactly the 10 committed
/// rows (never an uncommitted one); after the rollback and one more commit it equals the
/// writer (11 rows, same id sum). The replica made at least 2 passes during the transaction.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_rolled_back_large_transaction() {
    let ctx = Ctx::new("replica_rolled_back_large_transaction");
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=10);
    let rep = Rep::open(&ctx, &ctx.dir("r"), Duration::from_millis(300));
    rep.wait_equal(&w, BOUND);
    let passes0 = {
        let s = rep.stats();
        s.passes_slow_poll
    };
    let logs0 = ctx.list("log/").len();
    w.conn().execute("BEGIN", &[]).expect("begin");
    for chunk in 0..15i64 {
        for i in 0..200i64 {
            let id = 1000 + chunk * 200 + i;
            w.conn()
                .execute(
                    &format!("INSERT INTO soak(id, payload) VALUES ({id}, zeroblob(4096))"),
                    &[],
                )
                .expect("insert in the transaction");
        }
        std::thread::sleep(Duration::from_millis(150));
        assert_eq!(
            rep.count(),
            10,
            "chunk {chunk}: an uncommitted row is visible"
        );
    }
    let passes = rep.stats().passes_slow_poll - passes0;
    assert!(
        passes >= 2,
        "the replica polled {passes} times during the transaction"
    );
    let logs_in_tx = ctx.list("log/").len() - logs0;
    w.conn().execute("ROLLBACK", &[]).expect("rollback");
    w.insert(11);
    rep.wait_equal(&w, Duration::from_secs(30));
    assert_eq!(
        rep.query_int("SELECT count(*) FROM soak WHERE id >= 1000"),
        0
    );
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, "rollback");
    eprintln!("rolled back: {passes} passes and {logs_in_tx} log objects during the transaction");
    w.close().expect("close writer");
    rep.close();
}

// ---------------------------------------------------------------------------------------
// replica_follows_before_prefetch_finishes
// ---------------------------------------------------------------------------------------

/// The writer commits 4,000 rows of 1 KiB (about 4.5 MB) and seals, so the pages are in
/// `pages/`; then 20 more rows reach the log. A replica opens with prefetch on over a store
/// that delays every page GET by 100 ms, so its prefetch is still running while the test
/// reads and the follower applies: the first reading right after open already equals the
/// writer's rows (read through the lazy page storage), a full scan of the payload sizes
/// is correct, a commit made meanwhile is followed, and after the prefetch has had time to
/// finish `integrity_check` is ok. Whether the prefetch was still running at the first
/// reading is shown by the page GETs counted then and later (printed), not asserted.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_follows_before_prefetch_finishes() {
    let ctx = Ctx::new("replica_follows_before_prefetch_finishes");
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    for chunk in 0..4i64 {
        w.insert_bulk(chunk * 1000 + 1..=chunk * 1000 + 1000, 1024);
    }
    w.seal();
    w.insert_range(5001..=5020);
    let fault = std::sync::Arc::new(buckwal::store::FaultStore::new(std::sync::Arc::new(
        ctx.store(),
    )));
    fault.only_keys_containing("pages/");
    fault.delay_gets(Duration::from_millis(100));
    let mut cfg = ctx.config(&ctx.dir("r"));
    cfg.prefetch = true;
    cfg.replica_poll_interval = Duration::from_millis(300);
    let store: std::sync::Arc<dyn buckwal::store::ObjectStore> = fault.clone();
    let t0 = Instant::now();
    let rep = Rep::open_store(cfg, store);
    let opened = t0.elapsed();
    let page_gets = |f: &buckwal::store::FaultStore| {
        f.events()
            .iter()
            .filter(|e| e.starts_with("get ") && e.contains("pages/"))
            .count()
    };
    let first = rep.count();
    let gets_first = page_gets(&fault);
    let want = w.acked.len() as i64;
    assert_eq!(first, want, "the first reading right after open");
    assert_eq!(
        rep.query_int("SELECT coalesce(sum(length(payload)), 0) FROM soak"),
        1024 * want
    );
    w.insert_range(5021..=5030);
    rep.wait_equal(&w, Duration::from_secs(60));
    let gets_scan = page_gets(&fault);
    std::thread::sleep(Duration::from_secs(5));
    assert_eq!(rep.integrity(), "ok");
    assert_eq!(rep.sum(), w.sum());
    assert_no_writes(&rep, "prefetch");
    eprintln!(
        "prefetch: open took {opened:?}; page GETs {gets_first} at the first reading, \
         {gets_scan} after the follow, {} at the end",
        page_gets(&fault)
    );
    w.close().expect("close writer");
    rep.close();
}
