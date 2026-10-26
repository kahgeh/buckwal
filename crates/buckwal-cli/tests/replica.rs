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
    // The listener runs and the bucket's event for the commit is never delivered to it.
    let _listener = Listener::start(&access, rep.waker());
    assert_eq!(rep.count(), 10);
    w.insert(11);
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
    for (i, k) in keys.iter().take(3).enumerate() {
        listener.poster.post_ok(k, Auth::Bearer);
        // The counter rises when the pass ends (passes_notification rises when it starts).
        let want = before.notification_passes_nothing_new + i as u64 + 1;
        wait_until(
            "the replayed event's pass to end",
            Duration::from_secs(5),
            || (rep.stats().notification_passes_nothing_new >= want).then_some(()),
        );
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
    wait_until("the replica's generation", Duration::from_secs(10), || {
        (rep.stats().generation == g0).then_some(())
    });

    a.seal();
    let g1 = ctx.head().generation;
    assert!(g1 > g0, "the seal moved the head: {g0} then {g1}");
    a.insert_range(31..=40);
    rep.wait_equal(&a, Duration::from_secs(30));
    let g_now = ctx.head().generation;
    wait_until("the replica's generation", Duration::from_secs(10), || {
        (rep.stats().generation == g_now).then_some(())
    });

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
    wait_until("the replica's generation", Duration::from_secs(10), || {
        (rep.stats().generation == head.generation).then_some(())
    });
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
                (p.stage == "applying" || p.stage == "ckpt_pending")
                    && p.offset > 0
                    && p.generation == head,
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
    /// No seal: steady commits against a 50 ms poll, killed on the follower's second `applied`
    /// line with commits still arriving (it is applying continuously).
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
/// than 6 of the 7 seal-trigger rounds (all but the mid-apply round) were caught after up to 4
/// attempts each.
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
        // A trigger that shows for a millisecond or two can be missed by the poller: the round
        // is run again (a fresh burst of commits and a seal) up to 4 times until it is caught.
        for attempt in 1..=4 {
            let poll_ms = if matches!(kill, Kill::MidApply) {
                50
            } else {
                300
            };
            let mut f = Follower::spawn(&ctx, &r, poll_ms, None);
            let g0 = head_generation().expect("head");
            let pid = f.pid().to_string();
            let burst_done = AtomicBool::new(false);
            let kill_it = |pid: &str| {
                let _ = std::process::Command::new("kill")
                    .args(["-9", pid])
                    .status();
            };
            let hit = std::thread::scope(|sc| {
                let poller = sc.spawn(|| {
                    if matches!(kill, Kill::MidApply) {
                        return None;
                    }
                    let deadline = Instant::now() + Duration::from_secs(30);
                    let mut burst_end = None;
                    let mut last_head = Instant::now() - Duration::from_secs(1);
                    let mut moved = false;
                    while Instant::now() < deadline {
                        // Give up 4 s after the burst: the trigger was missed.
                        if burst_done.load(SeqCst) {
                            let t = *burst_end.get_or_insert(Instant::now());
                            if t.elapsed() > Duration::from_secs(4) {
                                return None;
                            }
                        }
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
                            kill_it(&pid);
                            return Some(format!("{p:?}"));
                        }
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    None
                });
                match kill {
                    Kill::MidApply => {
                        // Steady commits against a 50 ms poll: the follower is applying
                        // all the time. Kill on the second `applied` line, with commits
                        // still arriving, i.e. while its next pass is about to run or running.
                        let from = f.lines.len();
                        let mut hit = None;
                        for id in next..next + 400 {
                            w.insert(id);
                            let applied = f.count_lines(from, |l| l.starts_with("applied "));
                            if applied >= 2 {
                                kill_it(&pid);
                                hit = Some(format!("after {applied} applied lines, row {id}"));
                                break;
                            }
                        }
                        next = w.acked.last().copied().unwrap() + 1;
                        burst_done.store(true, SeqCst);
                        poller.join().unwrap();
                        hit
                    }
                    _ => {
                        w.insert_range(next..=next + 9);
                        w.seal();
                        w.insert_range(next + 10..=next + 19);
                        next += 20;
                        burst_done.store(true, SeqCst);
                        poller.join().unwrap()
                    }
                }
            });
            f.kill9();
            eprintln!("kill round {round} {kill:?} attempt {attempt}: caught at {hit:?}");
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
            if hit.is_some() {
                if !matches!(kill, Kill::MidApply) {
                    caught += 1;
                }
                break;
            }
        }
    }
    // With the retries every trigger is normally caught; 6 of the 7 seal-trigger rounds (all
    // but the mid-apply round) tolerate one stubborn one.
    assert!(
        caught >= 6,
        "only {caught} of the 7 seal-trigger rounds were caught"
    );
    w.close().expect("close writer");
}

// ---------------------------------------------------------------------------------------
// replica_rolled_back_large_transaction
// ---------------------------------------------------------------------------------------

/// The writer runs a transaction of 6,000 rows of 4 KiB (about 24 MB, past the page cache so
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
    for chunk in 0..30i64 {
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
    // Whether the engine writes (and the writer uploads) the spilled pages of an open
    // transaction as log records before the commit is the engine's choice: 6,000 x 4 KiB
    // (24 MB) is well past its page cache, but a spill reaching `log/` is printed, not
    // asserted, since the library gives no hook to force or observe it.
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

/// One block of a pack (`BLOCK_SIZE` of the library): a page read fetches one block, and the
/// background fetch asks for up to 16 of them in one range GET.
const PACK_BLOCK: u64 = 1 << 20;

/// A store that holds every range GET under `pack/` of more than one block (the background
/// fetch) until `release`, at most 120 s. A one-block GET (a page read or a local checkpoint
/// over a block that is not local yet) goes through at once.
struct HoldFetch {
    inner: std::sync::Arc<dyn ObjectStore>,
    released: std::sync::Mutex<bool>,
    cv: std::sync::Condvar,
}

impl HoldFetch {
    fn new(inner: std::sync::Arc<dyn ObjectStore>) -> HoldFetch {
        HoldFetch {
            inner,
            released: std::sync::Mutex::new(false),
            cv: std::sync::Condvar::new(),
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.cv.notify_all();
    }
}

impl ObjectStore for HoldFetch {
    fn put(&self, key: &str, body: &[u8]) -> Result<String, buckwal::store::StoreError> {
        self.inner.put(key, body)
    }
    fn put_if_absent(
        &self,
        key: &str,
        body: &[u8],
    ) -> Result<buckwal::store::CondPut, buckwal::store::StoreError> {
        self.inner.put_if_absent(key, body)
    }
    fn put_if_match(
        &self,
        key: &str,
        body: &[u8],
        etag: &str,
    ) -> Result<buckwal::store::CondPut, buckwal::store::StoreError> {
        self.inner.put_if_match(key, body, etag)
    }
    fn get(&self, key: &str) -> Result<Option<buckwal::store::Object>, buckwal::store::StoreError> {
        self.inner.get(key)
    }
    fn get_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>, buckwal::store::StoreError> {
        if key.contains("/pack/") && len > PACK_BLOCK {
            let deadline = Instant::now() + Duration::from_secs(120);
            let mut g = self.released.lock().unwrap();
            while !*g && Instant::now() < deadline {
                g = self.cv.wait_timeout(g, Duration::from_secs(1)).unwrap().0;
            }
        }
        self.inner.get_range(key, offset, len)
    }
    fn stat(
        &self,
        key: &str,
    ) -> Result<Option<buckwal::store::ObjectStat>, buckwal::store::StoreError> {
        self.inner.stat(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, buckwal::store::StoreError> {
        self.inner.list(prefix)
    }
    fn list_after(
        &self,
        prefix: &str,
        start_after: &str,
    ) -> Result<Vec<String>, buckwal::store::StoreError> {
        self.inner.list_after(prefix, start_after)
    }
    fn delete(&self, key: &str) -> Result<(), buckwal::store::StoreError> {
        self.inner.delete(key)
    }
    fn stats(&self) -> buckwal::store::StatsSnap {
        self.inner.stats()
    }
}

/// The writer commits 4,000 rows of 1 KiB (about 4.5 MB, five 1 MiB blocks), switches
/// generation, and a pack run packs it; then 20 more rows reach the log. A replica opens over a
/// store that holds the background fetch of the pack ([`HoldFetch`]). While it is held, with
/// no read on the replica, the writer commits, switches generation and commits again. The
/// follower applies all of it, its local checkpoint of the switch included, and moves to the
/// head's generation and its last seq, while the blocks file still names blocks that are not
/// local (asserted before and after). Then the reads equal the writer's rows (each missing
/// block read on demand), the fetch is let go, every block becomes local, and
/// `integrity_check` is ok.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_follows_before_prefetch_finishes() {
    let ctx = Ctx::new("replica_follows_before_prefetch_finishes");
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    for chunk in 0..4i64 {
        w.insert_bulk(chunk * 1000 + 1..=chunk * 1000 + 1000, 1024);
    }
    w.seal();
    let packed = buckwal::pack(&ctx.config(&ctx.dir("p"))).expect("pack run");
    assert_eq!(packed.outcome, buckwal::PackOutcome::Packed, "{packed:?}");
    w.insert_range(5001..=5020);
    let hold = std::sync::Arc::new(HoldFetch::new(std::sync::Arc::new(ctx.store())));
    let rdir = ctx.dir("r");
    let mut cfg = ctx.config(&rdir);
    cfg.prefetch = true;
    cfg.replica_poll_interval = Duration::from_millis(300);
    let store: std::sync::Arc<dyn ObjectStore> = hold.clone();
    let rep = Rep::open_store(cfg, store);
    // The blocks file: the limit (u64 LE), then one byte per block (1 = local).
    let blocks = || std::fs::read(rdir.join("app.db-blocks")).unwrap_or_default();
    let missing = |b: &[u8]| b.get(8..).is_some_and(|f| f.contains(&0));
    let at_open = blocks();
    assert!(missing(&at_open), "every block local at open: {at_open:?}");

    w.insert_range(5021..=5030);
    w.seal();
    w.insert_range(5031..=5040);
    let head = ctx.head();
    let seq = ctx.assert_log_contiguous(head.generation);
    wait_until("the follower at the head", Duration::from_secs(60), || {
        let s = rep.stats();
        (s.generation == head.generation && s.seq == seq).then_some(())
    });
    let applied = blocks();
    assert!(
        missing(&applied),
        "the follower applied only after the background fetch: {applied:?}"
    );

    rep.wait_equal(&w, Duration::from_secs(60));
    let payload = "SELECT coalesce(sum(length(payload)), 0) FROM soak";
    let want = w.conn().query(payload, &[]).expect("writer query")[0][0].as_int();
    assert_eq!(Some(rep.query_int(payload)), want);
    hold.release();
    wait_until("every block local", Duration::from_secs(180), || {
        (!missing(&blocks())).then_some(())
    });
    assert_eq!(rep.integrity(), "ok");
    assert_eq!(rep.sum(), w.sum());
    assert_no_writes(&rep, "prefetch");
    w.close().expect("close writer");
    rep.close();
}

// =======================================================================================
// Second half
// =======================================================================================

/// A 1 s lease, 2 s retry deadline and request timeout and a 6 s grace in every child's
/// environment (`short_lease_ctx` in `system.rs`); `short_lease` is the same for a `Config`.
fn short_lease_ctx(name: &str) -> Ctx {
    Ctx::new(name)
        .set("BUCKWAL_LEASE_MS", "1000")
        .set("BUCKWAL_RETRY_DEADLINE_MS", "2000")
        .set("BUCKWAL_REQUEST_TIMEOUT_MS", "2000")
        .set("BUCKWAL_TRIM_GRACE_SECS", "6")
}

/// The pass's encryption key file (`BUCKWAL_ENCRYPTION_KEY_FILE`), when it has one.
fn pass_key(ctx: &Ctx) -> Option<String> {
    ctx.opt_var(KEY_FILE_VAR).filter(|v| !v.is_empty())
}

/// `open_replica` of `cfg`, which must be refused: the error.
fn open_refused(cfg: buckwal::Config, why: &str) -> buckwal::Error {
    match buckwal::open_replica(cfg) {
        Err(e) => e,
        Ok(_) => panic!("{why}"),
    }
}

/// The keys of `log/<generation>/`, ascending.
fn log_keys(ctx: &Ctx, generation: u64) -> Vec<String> {
    ctx.list(&format!("log/{generation:010}/"))
}

/// The key one seq above `key`'s (same generation).
fn next_log_key(key: &str) -> String {
    let (dir, seq) = key.rsplit_once('/').expect("a log key");
    format!("{dir}/{:020}", seq.parse::<u64>().expect("a seq") + 1)
}

/// Wait for the replica's first pass (the `start` pass after open) to have finished, so a
/// later edit of the bucket cannot be overtaken by it.
fn wait_first_pass(rep: &Rep) {
    wait_until("the first sync pass", Duration::from_secs(60), || {
        rep.stats().last_pass_at.map(|_| ())
    });
}

/// Pause until the sync pass that a `wake` started has ended: wake again and wait for the
/// second notification pass to begin (passes run one after another).
fn wake_and_settle(rep: &Rep) {
    let before = rep.stats().passes_notification;
    rep.waker().wake();
    wait_until("a notification pass", Duration::from_secs(30), || {
        (rep.stats().passes_notification > before).then_some(())
    });
    let mid = rep.stats().passes_notification;
    rep.waker().wake();
    wait_until(
        "the next notification pass",
        Duration::from_secs(30),
        || (rep.stats().passes_notification > mid).then_some(()),
    );
}

// ---------------------------------------------------------------------------------------
// replica_paused_past_trim_grace_re_restores
// ---------------------------------------------------------------------------------------

/// A `follow` child (poll 1 s) is stopped with SIGSTOP at generation G. The writer switches
/// generation twice (G, G+1), a `pack` run packs both, and the `gc` command runs every second
/// until `log/<G>/` is empty (a 6 s grace and a 6 s pack keep time: the new pack's codes are
/// old enough after about 6 s). SIGCONT: the child re-restores from the newest pack, prints
/// `re-restored count=1 reason=trimmed` and catches up (`applied` at the head's generation and
/// the log's object count); it re-restores once, not again. After SIGTERM (exit 0) a reopen of
/// its directory holds the writer's rows (count and id sum). The `replica fell behind` WARN
/// goes to OTLP only (the CLI prints no log lines), so one re-restore line stands for the one
/// WARN.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_paused_past_trim_grace_re_restores() {
    let ctx = short_lease_ctx("replica_paused_past_trim_grace_re_restores")
        .set("BUCKWAL_PACK_KEEP_SECS", "6");
    let mut a = Writer::open(short_lease(ctx.config(&ctx.dir("a"))));
    a.insert_range(1..=20);
    let c = ctx.dir("c");
    let mut f = Follower::spawn(&ctx, &c, 1000, None);
    let g0 = ctx.head().generation;
    f.signal("STOP");

    a.seal();
    a.insert_range(21..=30);
    a.seal();
    a.insert_range(31..=35);
    let head = ctx.head();
    assert!(head.generation >= g0 + 2, "two seals: {g0} then {head:?}");
    let p = ctx.dir("p");
    let pack = ctx.ok(&["pack", "--dir", p.to_str().unwrap()]).stdout;
    eprintln!("paused: {}", pack.trim());
    wait_until(
        "GC to delete the paused replica's log",
        Duration::from_secs(60),
        || {
            let out = ctx.ok(&["gc"]).stdout;
            eprintln!("paused: {}", out.trim());
            if log_keys(&ctx, g0).is_empty() {
                return Some(());
            }
            std::thread::sleep(Duration::from_secs(1));
            None
        },
    );

    let from = f.lines.len();
    f.signal("CONT");
    let line = f.wait_line(from, "re-restored", Duration::from_secs(90), |l| {
        l.starts_with("re-restored ")
    });
    // the reason is the full text, for example "log/1/13 trimmed: replica was 12s behind"
    assert!(
        line.starts_with("re-restored count=1 reason=") && line.contains("trimmed"),
        "{line}"
    );
    let head = ctx.head();
    let n = ctx.assert_log_contiguous(head.generation);
    f.wait_applied(from, head.generation, n, Duration::from_secs(60));
    assert!(f.sigterm().success(), "{}", f.tail());
    assert_eq!(
        f.count_lines(from, |l| l.starts_with("re-restored ")),
        1,
        "one re-restore: {:?}",
        f.lines
    );
    let rep = Rep::open(&ctx, &c, Duration::from_secs(2));
    rep.wait_equal(&a, BOUND);
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, "reopen after the re-restore");
    a.close().expect("close writer");
    rep.close();
}

// ---------------------------------------------------------------------------------------
// replica_refuses_writes_and_sends_zero_puts_and_deletes
// ---------------------------------------------------------------------------------------

/// In process: a replica connection refuses INSERT, DELETE, DDL, a PRAGMA write, ATTACH,
/// `wal_checkpoint` and `PRAGMA buckwal_batching`, before and after the writer sealed and the
/// replica numbered on; each refusal changes nothing (the rows are the writer's). The S3
/// store's request counts show zero PUT and zero DELETE from open to after close.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_refuses_writes_and_sends_zero_puts_and_deletes() {
    let ctx = Ctx::new("replica_refuses_writes_and_sends_zero_puts_and_deletes");
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=10);
    let rep = Rep::open(&ctx, &ctx.dir("r"), Duration::from_millis(500));
    let store = rep.store.clone();
    rep.wait_equal(&w, BOUND);

    let refusals = [
        "INSERT INTO soak(id, payload) VALUES (999, zeroblob(8))",
        "UPDATE soak SET payload = zeroblob(1) WHERE id = 1",
        "DELETE FROM soak",
        "CREATE TABLE refused(x)",
        "DROP TABLE soak",
        "PRAGMA user_version = 7",
        "ATTACH DATABASE ':memory:' AS other",
        "PRAGMA wal_checkpoint(TRUNCATE)",
        "PRAGMA wal_checkpoint(PASSIVE)",
        "PRAGMA buckwal_batching",
    ];
    let check = |what: &str, w: &Writer| {
        let c = rep.r().connect().expect("replica connection");
        for sql in refusals {
            let r = c.query(sql, &[]);
            assert!(r.is_err(), "{what}: the replica accepted {sql:?}: {r:?}");
        }
        c.close().expect("close connection");
        rep.wait_equal(w, BOUND);
    };
    check("first", &w);
    w.insert_range(11..=15);
    w.seal();
    w.insert_range(16..=20);
    rep.wait_equal(&w, Duration::from_secs(60));
    let head = ctx.head().generation;
    wait_until("the replica's generation", Duration::from_secs(30), || {
        (rep.stats().generation == head).then_some(())
    });
    check("after the seal", &w);
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, "before close");
    w.close().expect("close writer");
    rep.close();
    let s = store.stats();
    assert_eq!(
        (s.puts, s.deletes),
        (0, 0),
        "after close: the replica sent PUTs or DELETEs: {s:?}"
    );
    eprintln!("replica life: {s:?}");
}

// ---------------------------------------------------------------------------------------
// two_replicas_each_with_own_listener
// ---------------------------------------------------------------------------------------

/// Two `follow` children on one bucket, each with its own listener (port 0, own directory),
/// slow poll 60 s. Both catch up with the writer first. The writer commits; the event is posted
/// to the first only: it applies the commit (`applied generation=G seq=N`, `notified
/// received=1`) and the second prints no new `applied` line. Then the event goes to the second, which applies it. The same again the other
/// way round. Both exit 0 and their directories hold the writer's rows.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn two_replicas_each_with_own_listener() {
    let ctx = Ctx::new("two_replicas_each_with_own_listener");
    let access = NotifyAccess::of_pass(&ctx);
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=5);
    let (d1, d2) = (ctx.dir("c1"), ctx.dir("c2"));
    let mut f1 = Follower::spawn(&ctx, &d1, 60_000, Some(&access));
    let mut f2 = Follower::spawn(&ctx, &d2, 60_000, Some(&access));
    let (p1, p2) = (f1.poster.clone().unwrap(), f2.poster.clone().unwrap());
    assert_ne!(p1.port, p2.port, "each replica has its own listener");
    let applied = |l: &str| l.starts_with("applied ");

    // Both replicas first catch up with the writer (a replica's own start pass may apply
    // commits made after its restore), so a later `applied` line can only come from an event.
    let (g, n) = {
        let g = ctx.head().generation;
        (g, ctx.assert_log_contiguous(g))
    };
    for f in [&mut f1, &mut f2] {
        let (a, b) = (
            format!("applied generation={g} seq={n}"),
            format!("following generation={g} seq={n}"),
        );
        f.wait_line(
            0,
            "caught up with the writer",
            Duration::from_secs(30),
            |l| l == a || l == b,
        );
    }
    std::thread::sleep(Duration::from_secs(1));

    for (id, first_is_1) in [(6, true), (7, false)] {
        let (from1, from2) = (f1.lines.len(), f2.lines.len());
        w.insert(id);
        let g = ctx.head().generation;
        let n = ctx.assert_log_contiguous(g);
        let key = newest_log_key(&ctx);
        let ((first, ffrom, pf), (second, sfrom, ps)) = if first_is_1 {
            ((&mut f1, from1, &p1), (&mut f2, from2, &p2))
        } else {
            ((&mut f2, from2, &p2), (&mut f1, from1, &p1))
        };
        pf.post_ok(&key, Auth::Bearer);
        first.wait_applied(ffrom, g, n, Duration::from_secs(5));
        first.wait_line(ffrom, "notified", Duration::from_secs(5), |l| {
            l.starts_with("notified ")
        });
        // Settle: the other replica had the same time to react to an event it never got.
        std::thread::sleep(Duration::from_secs(1));
        assert_eq!(
            second.count_lines(sfrom, applied),
            0,
            "the other replica applied without an event: {:?}",
            second.lines
        );
        ps.post_ok(&key, Auth::Basic);
        second.wait_applied(sfrom, g, n, Duration::from_secs(5));
        second.wait_line(sfrom, "notified", Duration::from_secs(5), |l| {
            l.starts_with("notified ")
        });
    }
    assert!(f1.sigterm().success(), "{}", f1.tail());
    assert!(f2.sigterm().success(), "{}", f2.tail());
    for d in [&d1, &d2] {
        let rep = Rep::open(&ctx, d, Duration::from_secs(2));
        rep.wait_equal(&w, BOUND);
        assert_eq!(rep.integrity(), "ok");
        rep.close();
    }
    w.close().expect("close writer");
}

// ---------------------------------------------------------------------------------------
// takeover_from_a_replica_machine
// ---------------------------------------------------------------------------------------

/// Writer A (20 rows); replica 1 is a `follow` child, replica 2 is in process. Replica 1 is
/// stopped (SIGTERM, exit 0) and `buckwal takeover --dir NEW` runs on its machine (epoch
/// +1); NEW is opened as the writer B, which commits 10 rows with a seal between. A's next
/// commit fails `Fenced`. Replica 2, not touched, follows the new writer: its rows are A's
/// 20 plus B's 10 (count and id sum), its generation the head's. Zero PUT and DELETE from
/// replica 2.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn takeover_from_a_replica_machine() {
    let ctx = short_lease_ctx("takeover_from_a_replica_machine");
    let mut a = Writer::open(short_lease(ctx.config(&ctx.dir("a"))));
    a.insert_range(1..=20);
    let mut f1 = Follower::spawn(&ctx, &ctx.dir("c1"), 1000, None);
    let rep2 = Rep::open(&ctx, &ctx.dir("c2"), Duration::from_millis(500));
    rep2.wait_equal(&a, BOUND);
    assert!(f1.sigterm().success(), "{}", f1.tail());

    let before = ctx.head();
    let new = ctx.dir("new");
    let out = ctx.ok(&["takeover", "--dir", new.to_str().unwrap()]);
    let line = out.stdout.trim().to_string();
    assert!(line.starts_with("took over "), "{line}");
    assert_eq!(field(&line, "epoch") as u64, before.epoch + 1, "{line}");

    let mut cfg = short_lease(ctx.config(&new));
    cfg.replica_poll_interval = Duration::from_millis(500);
    let mut b = Writer::open(cfg);
    b.insert_range(101..=105);
    b.seal();
    b.insert_range(106..=110);
    let late = a.try_insert(21);
    assert!(
        matches!(late, Err(buckwal::Error::Fenced(_))),
        "the old writer's commit after the takeover: {late:?}"
    );
    let want_count = (a.acked.len() + b.acked.len()) as i64;
    let want_sum = a.sum() + b.sum();
    wait_until(
        "replica 2 to equal A plus B",
        Duration::from_secs(60),
        || (rep2.count() == want_count && rep2.sum() == want_sum).then_some(()),
    );
    let head = ctx.head();
    assert!(head.epoch > before.epoch, "{head:?}");
    wait_until("replica 2's generation", Duration::from_secs(30), || {
        (rep2.stats().generation == head.generation).then_some(())
    });
    assert_eq!(rep2.integrity(), "ok");
    assert_no_writes(&rep2, "replica 2");
    let _ = a.close();
    b.close().expect("close B");
    rep2.close();
}

// ---------------------------------------------------------------------------------------
// replica_skips_fenced_writers_late_record
// ---------------------------------------------------------------------------------------

/// The old writer A is "paused across a takeover" by a `GateStore`: its commit's append to
/// `log/<G>` is held before it lands. Machine B takes over (its fence takes that seq) and
/// commits; A's held append is released and refused (A's commit fails, nothing acked). The late
/// record, A's own real record body, is then put by the test into `log/<G>` after B's last
/// record (the writer cannot land one: a hook-free stand-in for a late PUT). The replica
/// (slow poll 60 s, passes started by `ReplicaWaker::wake`) skips it: `fenced_records_skipped`
/// is 1, no re-restore, not stopped, and its rows equal A's 20 plus B's (count and id sum).
/// With `seal_first`, B seals before the plant (the head skips a generation) and the late
/// record is the last of the finished `log/<G>`: the replica takes it in while finishing G,
/// numbers on into the head's generation and follows B's later commits.
/// Needs a plaintext bucket for the plant (a record's integrity code binds its seq, so a
/// copy at another seq would stop the replica: the integrity test); with a key (pass 2) the
/// test runs everything but the plant and expects 0 skipped.
fn fenced_late_record(name: &str, seal_first: bool) {
    let ctx = short_lease_ctx(name);
    let plain = pass_key(&ctx).is_none();
    let gate = std::sync::Arc::new(GateStore::new(std::sync::Arc::new(ctx.store())));
    let mut a = Writer::open_with_store(short_lease(ctx.config(&ctx.dir("a"))), gate.clone());
    a.insert_range(1..=20);
    let rep = Rep::open(&ctx, &ctx.dir("r"), Duration::from_secs(60));
    rep.wait_equal(&a, BOUND);
    wait_first_pass(&rep);
    let g = ctx.head().generation;

    gate.arm();
    let paused = std::thread::spawn(move || {
        let r = a.try_insert(21);
        (a, r)
    });
    let (held_key, held_body) = gate.wait_held(Duration::from_secs(30));
    eprintln!("{name}: A's append to {held_key} is held");
    let mut b = Writer::open(short_lease(ctx.config(&ctx.dir("b"))));
    b.insert_range(101..=105);
    if seal_first {
        b.seal();
        b.insert_range(106..=108);
    }
    gate.release();
    let (a, late) = paused.join().expect("A's thread");
    assert!(
        late.is_err(),
        "A's held commit after the takeover: {late:?}"
    );
    assert_eq!(a.acked.len(), 20, "A acked nothing after the fence");

    let head = ctx.head();
    if seal_first {
        // As in `late_put_from_fenced_writer_lands_on_unreferenced_key` (system.rs): the
        // first seal after a takeover skips a generation.
        assert_eq!(
            head.generation,
            g + 2,
            "B's first seal skips a generation: {g} then {head:?}"
        );
    } else {
        assert_eq!(head.generation, g, "no seal yet: {head:?}");
    }
    let planted = plain.then(|| {
        let last = log_keys(&ctx, g).pop().expect("log/<G> has records");
        let key = next_log_key(&last);
        let put = ctx.store().put_if_absent(&key, &held_body).expect("plant");
        assert!(
            matches!(put, buckwal::store::CondPut::Created { .. }),
            "{put:?}"
        );
        key
    });
    rep.waker().wake();
    let want_skipped = u64::from(plain);
    wait_until_waking(
        "the late record to be skipped",
        Duration::from_secs(60),
        || rep.waker().wake(),
        || {
            (rep.stats().fenced_records_skipped >= want_skipped
                && rep.count() == (20 + b.acked.len()) as i64)
                .then_some(())
        },
    );
    let mut expected = (a.acked.len() + b.acked.len()) as i64;
    if seal_first {
        wait_until_waking(
            "the replica's generation",
            Duration::from_secs(30),
            || rep.waker().wake(),
            || (rep.stats().generation == head.generation).then_some(()),
        );
        b.insert_range(109..=110);
        expected += 2;
        rep.waker().wake();
    }
    wait_until_waking(
        "the replica to equal A plus B",
        Duration::from_secs(60),
        || rep.waker().wake(),
        || (rep.count() == expected && rep.sum() == a.sum() + b.sum()).then_some(()),
    );
    let st = rep.stats();
    assert_eq!(st.fenced_records_skipped, want_skipped, "{st:?}");
    assert_eq!(st.re_restores, 0, "{st:?}");
    assert_eq!(st.stopped, None, "{st:?}");
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, name);
    eprintln!("{name}: planted {planted:?}; {st:?}");
    // B's close appends at the planted seq in the first shape (refused there); not asserted.
    if let Err(e) = b.close() {
        eprintln!("{name}: B close: {e}");
    }
    let _ = a.close();
    rep.close();
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_skips_fenced_writers_late_record() {
    fenced_late_record("replica_skips_fenced_late_record_in_g", false);
    fenced_late_record(
        "replica_skips_fenced_late_record_head_skips_a_generation",
        true,
    );
}

// ---------------------------------------------------------------------------------------
// replica_encrypted_bucket_edited_behind_back_stops_with_integrity
// ---------------------------------------------------------------------------------------

/// Pass 2 only (a key; pass 1 returns). The writer commits rows 6 and 7 after the replica
/// is level at 5 rows; the test overwrites the log record of row 7 with a copy of row 6's
/// (a record replayed at another seq: its integrity code does not fit that seq). On a wake
/// the replica stops with `stats().stopped == Some("integrity")`; row 7 is never visible and
/// its rows are a prefix of the writer's (5 or 6 rows). Putting the real record back and
/// reopening the directory gives all 7 rows.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_encrypted_bucket_edited_behind_back_stops_with_integrity() {
    let ctx = Ctx::new("replica_encrypted_bucket_edited_behind_back_stops_with_integrity");
    if pass_key(&ctx).is_none() {
        eprintln!("replica_encrypted_bucket_edited_behind_back_stops_with_integrity: pass 1, no key: skipped");
        return;
    }
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=5);
    let r = ctx.dir("r");
    let rep = Rep::open(&ctx, &r, Duration::from_secs(60));
    rep.wait_equal(&w, BOUND);
    wait_first_pass(&rep);
    w.insert_range(6..=7);
    let g = ctx.head().generation;
    let keys = log_keys(&ctx, g);
    let (k6, k7) = (keys[keys.len() - 2].clone(), keys[keys.len() - 1].clone());
    let store = ctx.store();
    let real7 = store.get(&k7).unwrap().expect("record 7").body;
    let copy6 = store.get(&k6).unwrap().expect("record 6").body;
    assert_ne!(real7, copy6);
    store.put(&k7, &copy6).unwrap();

    rep.waker().wake();
    let stopped = wait_until("the replica to stop", Duration::from_secs(60), || {
        rep.stats().stopped
    });
    assert_eq!(stopped, "integrity");
    let n = rep.count();
    assert!((5..=6).contains(&n), "a prefix of the writer's rows: {n}");
    assert_eq!(rep.sum(), (1..=n).sum::<i64>());
    assert_eq!(
        rep.query_int("SELECT count(*) FROM soak WHERE id = 7"),
        0,
        "nothing of the edited record is applied"
    );
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, "stopped replica");
    rep.close();

    // Restarted with the bucket put right, it catches up.
    store.put(&k7, &real7).unwrap();
    let rep = Rep::open(&ctx, &r, Duration::from_secs(2));
    rep.wait_equal(&w, BOUND);
    assert_eq!(rep.stats().stopped, None);
    w.close().expect("close writer");
    rep.close();
}

// ---------------------------------------------------------------------------------------
// replica_encrypted_stops_on_a_record_copied_from_another_database
// ---------------------------------------------------------------------------------------

/// Pass 2 only (a key). Two encrypted databases share one key file, under two prefixes of
/// the bucket, and make the same commits. The replica of A is level at 5 rows; A commits row
/// 6; the test overwrites A's record for it with B's record at the same seq (same
/// coordinates, another database). Under one integrity key that would verify; under each
/// database's own key (format 4) the replica stops with `stats().stopped == Some("integrity")`
/// and row 6 is never visible. With A's record put back, a restarted replica catches up.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_encrypted_stops_on_a_record_copied_from_another_database() {
    let name = "replica_encrypted_stops_on_a_record_copied_from_another_database";
    let ctx = Ctx::new(name);
    if pass_key(&ctx).is_none() {
        eprintln!("{name}: pass 1, no key: skipped");
        return;
    }
    let ctx_b = Ctx::new(&format!("{name}_b"));
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=5);
    let mut wb = Writer::open(ctx_b.config(&ctx_b.dir("b")));
    wb.insert_range(1..=6);
    let r = ctx.dir("r");
    let rep = Rep::open(&ctx, &r, Duration::from_secs(60));
    rep.wait_equal(&w, BOUND);
    wait_first_pass(&rep);
    w.insert_range(6..=6);
    let keys_a = log_keys(&ctx, ctx.head().generation);
    let keys_b = log_keys(&ctx_b, ctx_b.head().generation);
    let (k6a, k6b) = (
        keys_a.last().unwrap().clone(),
        keys_b.last().unwrap().clone(),
    );
    let suffix = |k: &str| k.rsplit_once("/log/").map(|(_, s)| s.to_string());
    assert_eq!(
        suffix(&k6a),
        suffix(&k6b),
        "the two records sit at the same coordinates"
    );
    let real = ctx.store().get(&k6a).unwrap().expect("record 6").body;
    let foreign = ctx_b.store().get(&k6b).unwrap().expect("B's record 6").body;
    assert_ne!(real, foreign);
    ctx.store().put(&k6a, &foreign).unwrap();

    rep.waker().wake();
    let stopped = wait_until("the replica to stop", Duration::from_secs(60), || {
        rep.stats().stopped
    });
    assert_eq!(stopped, "integrity");
    assert_eq!(rep.count(), 5, "nothing of the foreign record is applied");
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, "stopped replica");
    rep.close();

    // Restarted with the bucket put right, it catches up.
    ctx.store().put(&k6a, &real).unwrap();
    let rep = Rep::open(&ctx, &r, Duration::from_secs(2));
    rep.wait_equal(&w, BOUND);
    assert_eq!(rep.stats().stopped, None);
    w.close().expect("close writer");
    let _ = wb.close();
    rep.close();
}

// ---------------------------------------------------------------------------------------
// replica_missing_seq_waits
// ---------------------------------------------------------------------------------------

/// The writer commits rows 6 and 7 (seqs n+1, n+2) after the replica is level at 5 rows. The
/// test copies `log/<G>/n+1` aside and deletes it while n+2 is listed. After two settled
/// notification passes the replica still has 5 rows (it stops before n+2), is not stopped and
/// has not re-restored. After the test puts n+1 back, both records apply: rows equal the
/// writer's.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_missing_seq_waits() {
    let ctx = Ctx::new("replica_missing_seq_waits");
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=5);
    let rep = Rep::open(&ctx, &ctx.dir("r"), Duration::from_secs(60));
    rep.wait_equal(&w, BOUND);
    wait_first_pass(&rep);
    w.insert_range(6..=7);
    let g = ctx.head().generation;
    let keys = log_keys(&ctx, g);
    let hole = keys[keys.len() - 2].clone();
    let store = ctx.store();
    let aside = store.get(&hole).unwrap().expect("record n+1").body;
    store.delete(&hole).unwrap();

    wake_and_settle(&rep);
    assert_eq!(rep.count(), 5, "the replica stops before n+2");
    let st = rep.stats();
    assert_eq!((st.stopped.clone(), st.re_restores), (None, 0), "{st:?}");

    let put = store.put_if_absent(&hole, &aside).unwrap();
    assert!(
        matches!(put, buckwal::store::CondPut::Created { .. }),
        "{put:?}"
    );
    rep.waker().wake();
    rep.wait_equal(&w, Duration::from_secs(30));
    let st = rep.stats();
    assert_eq!((st.stopped.clone(), st.re_restores), (None, 0), "{st:?}");
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, "missing seq");
    w.close().expect("close writer");
    rep.close();
}

// ---------------------------------------------------------------------------------------
// replica_wrong_key_refused
// ---------------------------------------------------------------------------------------

/// An encrypted bucket (the pass's key; pass 1 makes a key of its own for this test). A
/// replica with a different key, and one with no key, are refused at open with
/// `Error::Encryption` (the message never holds the key), and so is `buckwal follow` with the
/// wrong key (exit 1, no `following` line): none gets as far as following. With the right key
/// the replica follows a later commit, rows equal the writer's, `integrity_check` ok. In
/// pass 2 this is the pass's key, so the scan that must find no plaintext covers this
/// bucket too.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_wrong_key_refused() {
    let ctx = Ctx::new("replica_wrong_key_refused");
    let ctx = match pass_key(&ctx) {
        Some(_) => ctx,
        None => {
            let k = write_key_file(&ctx, "good");
            ctx.set(KEY_FILE_VAR, k.to_str().unwrap())
        }
    };
    let good = ctx.var(KEY_FILE_VAR);
    let good_text = std::fs::read_to_string(&good).unwrap().trim().to_string();
    let wrong = write_key_file(&ctx, "wrong");
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=10);

    let mut cfg = ctx.config(&ctx.dir("wrong"));
    cfg.encryption_key_file = Some(wrong.clone());
    let e = open_refused(cfg, "a wrong key is refused");
    assert!(matches!(e, buckwal::Error::Encryption(_)), "{e:?}");
    assert!(!e.to_string().contains(&good_text));
    let mut cfg = ctx.config(&ctx.dir("none"));
    cfg.encryption_key_file = None;
    let e = open_refused(cfg, "no key is refused");
    assert!(matches!(e, buckwal::Error::Encryption(_)), "{e:?}");

    let d = ctx.dir("cli");
    let out = ctx.run_env(
        &[(KEY_FILE_VAR, wrong.to_str().unwrap())],
        &["follow", "--dir", d.to_str().unwrap()],
    );
    assert_eq!(out.status.code(), Some(1), "{}\n{}", out.stdout, out.stderr);
    assert!(!out.stdout.contains("following"), "{}", out.stdout);
    assert!(!out.stderr.contains(&good_text) && !out.stdout.contains(&good_text));

    let rep = Rep::open(&ctx, &ctx.dir("good"), Duration::from_millis(500));
    rep.wait_equal(&w, BOUND);
    w.insert(11);
    rep.wait_equal(&w, BOUND);
    assert_eq!(rep.integrity(), "ok");
    assert_no_writes(&rep, "encrypted replica");
    w.close().expect("close writer");
    rep.close();
}

// ---------------------------------------------------------------------------------------
// replica_refuses_mvcc
// ---------------------------------------------------------------------------------------

/// A database created with `--journal-mode mvcc` (the head says so): `open_replica` returns
/// `Unsupported("replica: MVCC")` and `buckwal follow` exits 1 with MVCC on stderr and no
/// `following` line.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_refuses_mvcc() {
    let ctx = Ctx::new("replica_refuses_mvcc");
    let d = ctx.dir("db");
    ctx.ok(&[
        "sql",
        "--claim",
        "--dir",
        d.to_str().unwrap(),
        "--journal-mode",
        "mvcc",
        CREATE_SOAK,
        "INSERT INTO soak(id, payload) VALUES (1, zeroblob(16))",
    ]);
    let head = ctx
        .store()
        .get(&format!("{}/head", ctx.prefix()))
        .unwrap()
        .expect("head");
    let json = String::from_utf8(head.body).unwrap();
    assert!(json.contains("\"journal_mode\":\"mvcc\""), "{json}");

    let e = open_refused(ctx.config(&ctx.dir("r1")), "an MVCC database is refused");
    assert!(
        matches!(e, buckwal::Error::Unsupported("replica: MVCC")),
        "{e:?}"
    );
    let r2 = ctx.dir("r2");
    let out = ctx.run(&["follow", "--dir", r2.to_str().unwrap()]);
    assert_eq!(out.status.code(), Some(1), "{}\n{}", out.stdout, out.stderr);
    assert!(out.stderr.contains("MVCC"), "{}", out.stderr);
    assert!(!out.stdout.contains("following"), "{}", out.stdout);
}

// ---------------------------------------------------------------------------------------
// replica_listener_rules
// ---------------------------------------------------------------------------------------

/// `follow` with a non-loopback listen address refuses to start (non-zero exit, no
/// `notify listening` line, its directory never created) with no TLS and no token, with a
/// token only and (pass 2, where the material exists) with TLS only. A loopback address with
/// neither is accepted (`tls=false auth=false`). With a token (pass 2: over TLS as well): a
/// post with a wrong token, a missing token and an empty JSON body without a token answer 401 and wakes nothing
/// (the one `notified received=1` line comes only after the valid post), the valid post wakes
/// the replica and applies the commit, and the token appears nowhere in the child's stdout or
/// stderr.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_listener_rules() {
    let ctx = Ctx::new("replica_listener_rules");
    let token_file = ctx.tmp.path().join("rules-token");
    let token = format!("rules-{:032x}-{}", nanos(), std::process::id());
    std::fs::write(&token_file, format!("{token}\n")).unwrap();
    let access = match std::env::var("BUCKWAL_TLS_DIR") {
        Ok(_) => NotifyAccess::of_pass(&ctx),
        Err(_) => NotifyAccess {
            material: None,
            token_file: Some(token_file.clone()),
            token: Some(token.clone()),
        },
    };
    let secret = access.token.clone().expect("a token");
    let material = access.material.clone();
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=5);

    let refused = |name: &str, extra: &[(&str, &str)]| {
        let d = ctx.dir(name);
        let mut vars = vec![("BUCKWAL_REPLICA_NOTIFY_LISTEN", "0.0.0.0:0")];
        vars.extend_from_slice(extra);
        let out = ctx.run_env(&vars, &["follow", "--dir", d.to_str().unwrap()]);
        assert!(
            !out.status.success(),
            "{name}: started: {}\n{}",
            out.stdout,
            out.stderr
        );
        assert!(
            !out.stdout.contains("notify listening"),
            "{name}: {}",
            out.stdout
        );
        assert!(!d.exists(), "{name}: the directory was touched");
        eprintln!("{name}: refused: {}", out.stderr.trim());
    };
    refused("open", &[]);
    refused(
        "token_only",
        &[(
            "BUCKWAL_REPLICA_NOTIFY_TOKEN_FILE",
            token_file.to_str().unwrap(),
        )],
    );
    if let Some(m) = &material {
        let cert = m.join("server/rustfs_cert.pem");
        let key = m.join("server/rustfs_key.pem");
        refused(
            "tls_only",
            &[
                ("BUCKWAL_REPLICA_NOTIFY_TLS_CERT", cert.to_str().unwrap()),
                ("BUCKWAL_REPLICA_NOTIFY_TLS_KEY", key.to_str().unwrap()),
            ],
        );
    }

    // Loopback with neither is accepted.
    let open = NotifyAccess {
        material: None,
        token_file: None,
        token: None,
    };
    let mut plain = Follower::spawn(&ctx, &ctx.dir("plain"), 60_000, Some(&open));
    assert!(plain.sigterm().success(), "{}", plain.tail());

    let mut f = Follower::spawn(&ctx, &ctx.dir("secured"), 60_000, Some(&access));
    let poster = f.poster.clone().expect("a listener");
    let from = f.lines.len();
    w.insert(6);
    let g = ctx.head().generation;
    let n = ctx.assert_log_contiguous(g);
    let key = newest_log_key(&ctx);
    for bad in [Auth::None, Auth::Wrong] {
        assert_eq!(poster.post(&key, bad), 401, "{bad:?}");
    }
    assert_eq!(
        poster.post_body("{}", Auth::None),
        401,
        "no credentials, any body"
    );
    poster.post_ok(&key, Auth::Bearer);
    f.wait_line(from, "notified received=1", Duration::from_secs(5), |l| {
        l == "notified received=1"
    });
    f.wait_applied(from, g, n, Duration::from_secs(5));
    assert!(f.sigterm().success(), "{}", f.tail());
    assert_eq!(
        f.count_lines(from, |l| l.starts_with("notified ")),
        1,
        "only the valid post woke the replica: {:?}",
        f.lines
    );
    let (out, err) = (f.lines.join("\n"), f.stderr_text());
    assert!(
        !out.contains(&secret) && !err.contains(&secret),
        "the token reached the output"
    );
    w.close().expect("close writer");
}

// ---------------------------------------------------------------------------------------
// replica_sync_spans_reach_clickhouse
// ---------------------------------------------------------------------------------------

/// A `follow` child (it exports under `BUCKWAL_OTEL=1` and the run's `BUCKWAL_TEST_RUN_ID`,
/// as `scripts/test.sh` sets them) follows a short workload with both triggers: the first
/// commits are woken by posted events (`notification` passes), the later ones are left to the
/// 1 s slow poll (`slow_poll` passes). It exits 0 on SIGTERM (which flushes its spans). The
/// ClickHouse side, spans with `buckwal.lag_ms` and `buckwal.trigger`, is `scripts/otel-check.sh`.
/// Without `BUCKWAL_OTEL` the test still runs and exports nothing.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn replica_sync_spans_reach_clickhouse() {
    let ctx = Ctx::new("replica_sync_spans_reach_clickhouse");
    let access = NotifyAccess::of_pass(&ctx);
    let mut w = Writer::open(ctx.config(&ctx.dir("a")));
    w.insert_range(1..=5);
    let mut f = Follower::spawn(&ctx, &ctx.dir("c"), 1000, Some(&access));
    let poster = f.poster.clone().expect("a listener");
    for id in 6..=8 {
        let from = f.lines.len();
        w.insert(id);
        let g = ctx.head().generation;
        let n = ctx.assert_log_contiguous(g);
        poster.post_ok(&newest_log_key(&ctx), Auth::Bearer);
        f.wait_applied(from, g, n, Duration::from_secs(10));
    }
    for id in 9..=11 {
        let from = f.lines.len();
        w.insert(id);
        let g = ctx.head().generation;
        let n = ctx.assert_log_contiguous(g);
        f.wait_applied(from, g, n, Duration::from_secs(15));
    }
    assert!(f.sigterm().success(), "{}", f.tail());
    let applied = f.count_lines(0, |l| l.starts_with("applied "));
    assert!(applied >= 6, "one applied line per commit: {:?}", f.lines);
    eprintln!(
        "replica_sync_spans_reach_clickhouse: BUCKWAL_OTEL={:?}, run id {:?}",
        std::env::var("BUCKWAL_OTEL").ok(),
        std::env::var("BUCKWAL_TEST_RUN_ID").ok()
    );
    w.close().expect("close writer");
}
