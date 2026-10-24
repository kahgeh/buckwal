//! System tests: the `buckwal` binary against a real RustFS. Every acked commit must
//! survive SIGKILL plus the loss of the local disk, under every checkpoint mode and under
//! faults. Run through `./scripts/test.sh` (`--test-threads=1`: some tests stop or pause the
//! shared container).
mod common;

use buckwal::store::ObjectStore;
use buckwal::Value;
use common::*;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

/// How long any wait for a soak line may take: generous, so a hung soak fails the test
/// instead of hanging the suite.
const WAIT: Duration = Duration::from_secs(120);

/// The Window setting the batched system tests run under.
const WINDOW: &str = "window 20ms 64 1MiB";

/// `scripts/rustfs.sh <cmd>`.
fn rustfs(cmd: &str) {
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/rustfs.sh");
    let status = Command::new(&script)
        .arg(cmd)
        .status()
        .unwrap_or_else(|e| panic!("run {} {cmd}: {e}", script.display()));
    assert!(status.success(), "rustfs.sh {cmd}: {status}");
}

/// A RustFS outage: `start` it with `rustfs("stop")`, and `end` it with the undo command.
/// Dropping it while still armed (a failed assertion) runs the undo, so RustFS is never left
/// down for the tests after this one.
struct Outage {
    undo: &'static str,
    armed: bool,
}

impl Outage {
    fn start(cmd: &str, undo: &'static str) -> Outage {
        let o = Outage { undo, armed: true };
        rustfs(cmd);
        o
    }

    fn end(mut self) {
        self.armed = false;
        rustfs(self.undo);
    }
}

impl Drop for Outage {
    /// Never panics (it may run while a failed assertion unwinds): a failed undo is logged.
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/rustfs.sh");
        match Command::new(&script).arg(self.undo).status() {
            Ok(s) if s.success() => {}
            other => eprintln!("rustfs.sh {} after a failed test: {other:?}", self.undo),
        }
    }
}

/// A running `buckwal soak`, its stdout lines read on a thread into a channel.
struct Soak {
    child: Child,
    rx: Receiver<(Instant, String)>,
    lines: Vec<String>,
    /// When the reader thread received each of `lines`.
    stamps: Vec<Instant>,
    stderr: PathBuf,
    eof: bool,
}

impl Soak {
    fn spawn(ctx: &Ctx, dir: &Path, args: &[&str]) -> Soak {
        let mut all = vec!["soak", "--dir", dir.to_str().unwrap()];
        all.extend_from_slice(args);
        let stderr = ctx.tmp.path().join(format!("soak-{}.stderr", nanos()));
        let mut child = ctx
            .command(&all)
            .stdout(Stdio::piped())
            .stderr(fs::File::create(&stderr).unwrap())
            .spawn()
            .expect("spawn soak");
        let out = child.stdout.take().unwrap();
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                if tx.send((Instant::now(), line)).is_err() {
                    break;
                }
            }
        });
        Soak {
            child,
            rx,
            lines: Vec::new(),
            stamps: Vec::new(),
            stderr,
            eof: false,
        }
    }

    fn push(&mut self, (at, line): (Instant, String)) {
        self.stamps.push(at);
        self.lines.push(line);
    }

    /// The longest time between two consecutive acks.
    fn longest_ack_gap(&self) -> Duration {
        let acks: Vec<Instant> = self
            .lines
            .iter()
            .zip(&self.stamps)
            .filter(|(l, _)| l.starts_with("ack "))
            .map(|(_, t)| *t)
            .collect();
        let gaps = acks.windows(2).map(|w| w[1] - w[0]);
        gaps.max().unwrap_or_default()
    }

    fn stderr(&self) -> String {
        fs::read_to_string(&self.stderr).unwrap_or_default()
    }

    fn tail(&self) -> String {
        let from = self.lines.len().saturating_sub(10);
        format!(
            "last stdout lines: {:?}\nstderr:\n{}",
            &self.lines[from..],
            self.stderr()
        )
    }

    /// Read lines until `done` holds (checked after each line), failing on EOF or after
    /// `WAIT` (also while the soak keeps printing lines).
    fn wait_for(&mut self, what: &str, mut done: impl FnMut(&Soak) -> bool) {
        let deadline = Instant::now() + WAIT;
        while !done(self) {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                panic!("soak: no {what} within {WAIT:?}\n{}", self.tail());
            }
            match self.rx.recv_timeout(left) {
                Ok(stamped) => self.push(stamped),
                Err(RecvTimeoutError::Timeout) => {
                    panic!("soak: no {what} within {WAIT:?}\n{}", self.tail())
                }
                Err(RecvTimeoutError::Disconnected) => {
                    panic!("soak exited before {what}\n{}", self.tail())
                }
            }
        }
    }

    /// Wait for one more line starting with `prefix` than the soak has printed so far.
    /// Each line is looked at once, so a long run stays linear.
    fn wait_prefixed(&mut self, prefix: &str) {
        let mut seen = self.lines.len();
        self.wait_for(prefix, |s| {
            let fresh = s.lines[seen..].iter().any(|l| l.starts_with(prefix));
            seen = s.lines.len();
            fresh
        });
    }

    fn wait_acks(&mut self, n: usize) {
        // Counted as lines arrive, so a long run stays linear.
        let (mut seen, mut acks) = (0, 0);
        self.wait_for(&format!("{n} acks"), |s| {
            acks += s.lines[seen..]
                .iter()
                .filter(|l| l.starts_with("ack "))
                .count();
            seen = s.lines.len();
            acks >= n
        });
    }

    /// Ack lines the reader thread received at or after `t` (`before` = false), or before it.
    fn acks_around(&self, t: Instant, before: bool) -> usize {
        let acks = self.lines.iter().zip(&self.stamps);
        acks.filter(|(l, at)| l.starts_with("ack ") && ((**at < t) == before))
            .count()
    }

    /// Wait for `n` ack lines printed after `t`; lines already in the pipe at `t` don't count.
    fn wait_acks_after(&mut self, t: Instant, n: usize) {
        self.wait_for(&format!("{n} acks after a point"), |s| {
            s.acks_around(t, false) >= n
        });
    }

    /// Wait for one more `line` than the soak has printed so far.
    fn wait_line(&mut self, line: &str) {
        let n = self.count(line) + 1;
        self.wait_for(line, |s| s.count(line) >= n);
    }

    fn count(&self, line: &str) -> usize {
        self.lines.iter().filter(|l| *l == line).count()
    }

    fn acks(&self) -> usize {
        self.lines.iter().filter(|l| l.starts_with("ack ")).count()
    }

    fn last_ack(&self) -> i64 {
        let last = self.lines.iter().rev().find_map(|l| l.strip_prefix("ack "));
        last.expect("an ack").parse().unwrap()
    }

    /// The index of the first line at or after `from` that `p` accepts.
    fn find(&self, from: usize, p: impl Fn(&str) -> bool) -> Option<usize> {
        let at = self.lines.iter().skip(from).position(|l| p(l));
        at.map(|i| from + i)
    }

    /// The complete `stats` blocks (`--stats-every`) that start at or after line `from`, each
    /// with the index of its first line. The soak prints a block's nine lines together, but
    /// the reader may have received only some of the last one.
    fn stats_blocks(&self, from: usize) -> Vec<(usize, Stats)> {
        let mut blocks: Vec<(usize, Stats)> = Vec::new();
        for (i, l) in self.lines.iter().enumerate().skip(from) {
            let Some(kv) = l.strip_prefix("stats ") else {
                continue;
            };
            if kv.starts_with("commits_acked=") {
                blocks.push((i, Stats::new()));
            }
            let Some((_, block)) = blocks.last_mut() else {
                continue;
            };
            let (k, v) = kv.split_once('=').unwrap_or_else(|| panic!("{l:?}"));
            block.insert(k.to_string(), v.parse().unwrap_or_else(|_| panic!("{l:?}")));
        }
        blocks.retain(|(_, b)| b.contains_key("ack_wait_p99_ms"));
        blocks
    }

    fn errors(&self) -> Vec<&String> {
        self.lines
            .iter()
            .filter(|l| l.starts_with("error"))
            .collect()
    }

    /// Read everything the process printed until its stdout closed.
    fn drain(&mut self) {
        let deadline = Instant::now() + WAIT;
        while !self.eof {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.rx.recv_timeout(left) {
                Ok(stamped) => self.push(stamped),
                Err(RecvTimeoutError::Disconnected) => self.eof = true,
                Err(RecvTimeoutError::Timeout) => panic!("soak stdout never closed"),
            }
        }
    }

    /// SIGKILL, then keep every line it printed before dying: each `ack` was printed after
    /// its commit returned Ok, so each is a promise the bucket must keep.
    fn kill(&mut self) {
        self.child.kill().expect("SIGKILL soak");
        self.child.wait().expect("reap soak");
        self.drain();
    }

    /// Send `signal` (`STOP`, `CONT`) to the soak with `kill(1)`: no crate for it.
    fn signal(&self, signal: &str) {
        let pid = self.child.id().to_string();
        let status = Command::new("kill")
            .args([&format!("-{signal}"), &pid])
            .status()
            .unwrap_or_else(|e| panic!("kill -{signal} {pid}: {e}"));
        assert!(status.success(), "kill -{signal} {pid}: {status}");
    }

    /// Wait for an `error` line saying the soak was fenced, then for `quiet` with no `ack`
    /// line (other lines, such as the fenced soak's retries, do not count). Fails after
    /// `WAIT`, so a soak that keeps acking fails the test. Returns the index of the first
    /// fenced error.
    fn wait_fenced_then_no_ack(&mut self, quiet: Duration) -> usize {
        let mut seen = 0;
        self.wait_for("a fenced error", |s| {
            let fresh = s.find(seen, is_fenced).is_some();
            seen = s.lines.len();
            fresh
        });
        let fenced = self.find(0, is_fenced).expect("a fenced error");
        let deadline = Instant::now() + WAIT;
        let mut last_ack = Instant::now();
        loop {
            let left = quiet.saturating_sub(last_ack.elapsed());
            if left.is_zero() {
                return fenced;
            }
            if Instant::now() >= deadline {
                panic!(
                    "soak: still acking {WAIT:?} after it was fenced\n{}",
                    self.tail()
                );
            }
            match self.rx.recv_timeout(left) {
                Ok(stamped) => {
                    if stamped.1.starts_with("ack ") {
                        last_ack = stamped.0;
                    }
                    self.push(stamped);
                }
                Err(RecvTimeoutError::Timeout) => return fenced,
                Err(RecvTimeoutError::Disconnected) => {
                    panic!("soak exited after it was fenced\n{}", self.tail())
                }
            }
        }
    }

    /// Wait for a `--count` soak to finish on its own; it must succeed.
    fn finish(&mut self) {
        self.drain();
        let status = self.child.wait().expect("wait soak");
        assert!(status.success(), "soak failed: {status}\n{}", self.tail());
    }
}

impl Drop for Soak {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One `stats` block, or `stats`'s output: key → value.
type Stats = std::collections::BTreeMap<String, f64>;

/// The `key=value` lines of `out` (after `prefix`) as a map; other lines are skipped.
fn key_values(out: &str, prefix: &str) -> Stats {
    out.lines()
        .filter_map(|l| l.strip_prefix(prefix)?.split_once('='))
        .filter_map(|(k, v)| Some((k.to_string(), v.parse().ok()?)))
        .collect()
}

/// `b[key] - a[key]`.
fn delta(a: &Stats, b: &Stats, key: &str) -> f64 {
    b[key] - a[key]
}

/// 150 acks under 4 Window connections write about 39 log objects, so 100 would not seal before
/// the first kill; 20 does, and yields the `kill_after / 100 - 1` seals the later kills need.
const WINDOW_SEAL_LOG_OBJECTS: &str = "20";

/// The crash matrix for one mode: three runs, killed after `150`, `600` and `1500` acks.
/// `auto` is the engine's own checkpoint (no `--checkpoint`). With `batching`, soak runs four
/// connections under it.
fn crash_matrix(mode: &str, batching: Option<&str>) {
    let tag = if batching.is_some() { "window_" } else { "" };
    for kill_after in [150, 600, 1500] {
        let mut ctx = Ctx::new(&format!("crash_{tag}{mode}_{kill_after}"));
        // Under 4 Window connections the engine never restarts the WAL after PASSIVE/FULL, so
        // the maintenance checkpoint ends the generation (README "Maintenance checkpoint").
        if batching.is_some() && matches!(mode, "passive" | "full") {
            ctx = ctx.set("BUCKWAL_MAINTENANCE_LOG_OBJECTS", WINDOW_SEAL_LOG_OBJECTS);
        }
        let dir = ctx.dir("db");
        let mut args = Vec::new();
        if mode != "auto" {
            args.extend(["--checkpoint", mode, "--every", "100"]);
        }
        if let Some(b) = batching {
            args.extend(["--batching", b, "--connections", "4"]);
        }
        let mut soak = Soak::spawn(&ctx, &dir, &args);
        soak.wait_acks(kill_after);
        let sealed_run = mode == "auto" && kill_after >= 1500;
        if sealed_run {
            // The auto-checkpoint (every 1000 WAL frames) seals in the commit after it.
            soak.wait_for("a seal", |s| s.count("event seal-finished") >= 1);
        }
        soak.kill();
        assert!(soak.errors().is_empty(), "{mode}: {:?}", soak.errors());
        if sealed_run {
            assert!(
                ctx.head().manifest.is_some(),
                "{mode}: the bucket holds no sealed manifest after {kill_after} acks"
            );
        }
        let seals = soak.count("event seal-finished");
        if mode != "auto" {
            assert!(
                soak.lines.iter().any(|l| l.starts_with("event checkpoint ")),
                "{mode}: the soak's own checkpoint never ran before the kill after {kill_after} acks\n{}",
                soak.tail()
            );
            // Checkpoints keep sealing through the whole run (the last may be cut by the kill).
            assert!(
                seals >= 1 && seals >= kill_after / 100 - 1,
                "{mode}: {seals} seals before the kill after {kill_after} acks\n{}",
                soak.tail()
            );
        }
        fs::remove_dir_all(&dir).unwrap();
        let v = restore_verify(&ctx, &soak.lines);
        eprintln!(
            "crash_{tag}{mode}: kill after {kill_after} acks: acked={} seals={seals} {} \
             unacked_present={}",
            v.acked, v.restored, v.unacked_present
        );
    }
}

/// The engine's own checkpoint (PASSIVE, every 1000 WAL frames; soak without
/// `--checkpoint`): three runs, each killed after at least `kill_after` acks and at least one
/// finished seal (the auto-checkpoint's WAL restart seals in the commit after it).
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_auto() {
    for kill_after in [700, 1500, 2500] {
        let ctx = Ctx::new(&format!("crash_auto_{kill_after}"));
        let dir = ctx.dir("db");
        let mut soak = Soak::spawn(&ctx, &dir, &[]);
        soak.wait_acks(kill_after);
        soak.wait_for("a seal", |s| s.count("event seal-finished") >= 1);
        soak.kill();
        assert!(soak.errors().is_empty(), "auto: {:?}", soak.errors());
        let seals = soak.count("event seal-finished");
        assert!(seals >= 1, "auto: no seal before the kill\n{}", soak.tail());
        let acks = soak.acks();
        let head = ctx.head();
        assert!(
            head.manifest.is_some(),
            "auto: the bucket holds no sealed manifest"
        );
        fs::remove_dir_all(&dir).unwrap();
        let v = restore_verify(&ctx, &soak.lines);
        eprintln!(
            "crash_auto: kill after {acks} acks (target {kill_after}): acked={} seals={seals} \
             head generation={} {} unacked_present={}",
            v.acked, head.generation, v.restored, v.unacked_present
        );
    }
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_passive() {
    crash_matrix("passive", None);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_full() {
    crash_matrix("full", None);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_restart() {
    crash_matrix("restart", None);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_truncate() {
    crash_matrix("truncate", None);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_reopen() {
    crash_matrix("reopen", None);
}

/// The crash matrix under the engine's own checkpoint, four Window connections. The 150- and
/// 600-ack kills land before the first seal (the auto-checkpoint needs 1000 WAL frames), so
/// they restore from the log alone; the 1500-ack run waits for a seal and checks that the
/// head names a manifest before the wipe.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_window_auto() {
    crash_matrix("auto", Some(WINDOW));
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_window_truncate() {
    crash_matrix("truncate", Some(WINDOW));
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_window_reopen() {
    crash_matrix("reopen", Some(WINDOW));
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_window_passive() {
    crash_matrix("passive", Some(WINDOW));
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_window_full() {
    crash_matrix("full", Some(WINDOW));
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_window_restart() {
    crash_matrix("restart", Some(WINDOW));
}

/// The batch now waiting, when the soak's last event line says two or more commits wait in
/// it and the batch began (its `batch-waiting 1`) at most `young` ago: its count.
fn young_waiting_batch(s: &Soak, young: Duration) -> Option<u64> {
    let mut events = s
        .lines
        .iter()
        .zip(&s.stamps)
        .rev()
        .filter(|(l, _)| l.starts_with("event "));
    let (last, _) = events.next()?;
    let n: u64 = last.strip_prefix("event batch-waiting ")?.parse().ok()?;
    if n < 2 {
        return None;
    }
    // Back through this batch's own lines to its first: none is a log-uploaded.
    for (l, at) in events {
        match l.strip_prefix("event batch-waiting ") {
            Some("1") => return (at.elapsed() <= young).then_some(n),
            Some(_) => continue,
            None => return None,
        }
    }
    None
}

/// SIGKILL while two or more Window commits wait in the uploader's batch (500 ms window, four
/// connections): the waiting commits were never acked, and every acked one restores.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_window_kill_while_batched_commits_wait() {
    let ctx = Ctx::new("crash_window_kill_while_batched_commits_wait");
    let dir = ctx.dir("db");
    let args = [
        "--batching",
        "window 500ms 1000 64MiB",
        "--connections",
        "4",
    ];
    let mut soak = Soak::spawn(&ctx, &dir, &args);
    soak.wait_acks(200);
    // A batch begun at most 200 ms ago still has 300 ms of its window left: the kill lands
    // well before its upload.
    let young = Duration::from_millis(200);
    // Taken inside the predicate: the batch's age is checked once, when it was young.
    let mut waiting = None;
    soak.wait_for("a young batch of 2+ waiting commits", |s| {
        waiting = young_waiting_batch(s, young);
        waiting.is_some()
    });
    let waiting = waiting.expect("the wait returned on a young batch");
    let acks_at_kill = soak.acks();
    soak.kill();
    let last_event = soak.lines.iter().rev().find(|l| l.starts_with("event "));
    let last_event = last_event.expect("an event line");
    assert!(
        last_event.starts_with("event batch-waiting "),
        "the kill landed before the batch's upload: {last_event:?}\n{}",
        soak.tail()
    );
    assert!(soak.errors().is_empty(), "{:?}", soak.errors());
    fs::remove_dir_all(&dir).unwrap();
    let v = restore_verify(&ctx, &soak.lines);
    eprintln!(
        "crash_window_kill_while_batched_commits_wait: killed with {waiting} commits waiting \
         after {acks_at_kill} acks (last event {last_event:?}): acked={} {} unacked_present={}",
        v.acked, v.restored, v.unacked_present
    );
}

/// Killed while a seal's page PUTs are in flight (each delayed 200 ms): on the first seal,
/// and on the third, when earlier generations are already sealed.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_during_seal_upload() {
    for nth in [1, 3] {
        let ctx = Ctx::new(&format!("crash_during_seal_upload_{nth}"));
        let dir = ctx.dir("db");
        let args = [
            "--checkpoint",
            "truncate",
            "--every",
            "100",
            "--page-put-delay-ms",
            "200",
        ];
        let mut soak = Soak::spawn(&ctx, &dir, &args);
        soak.wait_for("seal-started", |s| s.count("event seal-started") >= nth);
        soak.kill();
        assert_eq!(
            soak.count("event seal-finished"),
            nth - 1,
            "{}",
            soak.tail()
        );
        assert!(soak.errors().is_empty(), "{:?}", soak.errors());
        fs::remove_dir_all(&dir).unwrap();
        let v = restore_verify(&ctx, &soak.lines);
        eprintln!(
            "crash_during_seal_upload: killed on seal-started #{nth}: acked={} {}",
            v.acked, v.restored
        );
    }
}

/// The soak arguments of the maintenance tests: sustained Window commits from eight
/// connections, under which the engine never restarts the WAL by itself.
const MAINTENANCE_ARGS: [&str; 4] = ["--connections", "8", "--batching", "window 1ms 64 1MiB"];

/// The maintenance threshold of the maintenance tests (`BUCKWAL_MAINTENANCE_LOG_OBJECTS`).
const MAINTENANCE_LOG_OBJECTS: usize = 200;

fn is_maintenance_end(l: &str) -> bool {
    l.starts_with("event maintenance-finished ") || l.starts_with("event maintenance-skipped ")
}

/// How far past the threshold a run's `log_objects` may be. The run reads its measures
/// right after the poke of the call whose commit crossed the threshold; only the few log
/// objects the uploader appends in between can land first. (bench at threshold 200 saw at
/// most 202-209 objects in a generation, and that also counts those landing while a run
/// drains and seals.)
const MAINTENANCE_START_SLACK: usize = 10;

/// The `log_objects=<n>` of each `event maintenance-started` line, with the number of acks
/// printed before it.
fn maintenance_starts(lines: &[String]) -> Vec<(usize, usize)> {
    let mut acks = 0;
    let mut starts = Vec::new();
    for l in lines {
        if l.starts_with("ack ") {
            acks += 1;
        } else if l.starts_with("event maintenance-started ") {
            starts.push((field(l, "log_objects"), acks));
        }
    }
    starts
}

/// Sustained Window load (8 connections, `window 1ms`, at least 3000 acks). Without
/// maintenance the generation never ends (the "before" run reports its seals and restore
/// GETs, and asserts only that every ack restores). With it (log-object threshold 200, the
/// `-wal` measure off), every run starts at 200 to 200 + `MAINTENANCE_START_SLACK` log
/// objects, at least 2 runs start within the first 3000 acks, and a restore right after a
/// maintenance seal reads at most 200 + 4 objects. Both: every acked id restores,
/// integrity ok, the log contiguous.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn maintenance_bounds_generations_under_window_load() {
    let acks = 3000;
    for on in [false, true] {
        let name = format!(
            "maintenance_bounds_generations_{}",
            if on { "on" } else { "off" }
        );
        let ctx = if on {
            // Only the log-object threshold can trigger a run.
            Ctx::new(&name)
                .set(
                    "BUCKWAL_MAINTENANCE_LOG_OBJECTS",
                    &MAINTENANCE_LOG_OBJECTS.to_string(),
                )
                .set("BUCKWAL_MAINTENANCE_WAL_MIB", "0")
        } else {
            Ctx::new(&name)
                .set("BUCKWAL_MAINTENANCE_LOG_OBJECTS", "0")
                .set("BUCKWAL_MAINTENANCE_WAL_MIB", "0")
        };
        let dir = ctx.dir("db");
        let mut soak = Soak::spawn(&ctx, &dir, &MAINTENANCE_ARGS);
        soak.wait_acks(acks);
        if on {
            // Kill right after a maintenance seal: the restore then reads the new
            // generation, which a run seals again once it holds 200 objects.
            soak.wait_prefixed("event maintenance-finished ");
        }
        soak.kill();
        assert!(soak.errors().is_empty(), "{name}: {:?}", soak.errors());
        let seals = soak.count("event seal-finished");
        let finished = soak
            .lines
            .iter()
            .filter(|l| l.starts_with("event maintenance-finished "))
            .count();
        let runs = soak
            .lines
            .iter()
            .filter(|l| l.starts_with("event maintenance-started "))
            .count();
        let head = ctx.head();
        let log_objects = ctx.assert_log_contiguous(head.generation);
        fs::remove_dir_all(&dir).unwrap();
        let v = restore_verify(&ctx, &soak.lines);
        let gets = field(&v.restored, "gets");
        eprintln!(
            "{name}: acked={} seals={seals} maintenance_runs={runs} \
             maintenance_finished={finished} generation={} log_objects={log_objects} {}",
            v.acked, head.generation, v.restored
        );
        if on {
            let starts = maintenance_starts(&soak.lines);
            eprintln!("{name}: maintenance runs started at (log_objects, acks before): {starts:?}");
            let range = MAINTENANCE_LOG_OBJECTS..=MAINTENANCE_LOG_OBJECTS + MAINTENANCE_START_SLACK;
            assert!(
                starts.iter().all(|(n, _)| range.contains(n)),
                "{name}: a run started outside {range:?} log objects: {starts:?}"
            );
            let early = starts.iter().filter(|(_, a)| *a < acks).count();
            assert!(
                early >= 2,
                "{name}: {early} runs started within {acks} acks: {starts:?}"
            );
            assert!(
                finished >= 1,
                "{name}: no maintenance seal\n{}",
                soak.tail()
            );
            assert!(
                gets <= MAINTENANCE_LOG_OBJECTS + 4,
                "{name}: the restore read {gets} objects: {}",
                v.restored
            );
        } else {
            assert_eq!(runs, 0, "{name}: maintenance is off");
        }
    }
}

/// A crash in the middle of a maintenance seal (its page PUTs delayed 200 ms each): killed
/// after `event maintenance-started` and the seal's `event seal-started`, before the run
/// ends. Every acked id restores, integrity ok, the log contiguous.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_during_maintenance_checkpoint_restores_every_ack() {
    let ctx = Ctx::new("crash_during_maintenance_checkpoint").set(
        "BUCKWAL_MAINTENANCE_LOG_OBJECTS",
        &MAINTENANCE_LOG_OBJECTS.to_string(),
    );
    let dir = ctx.dir("db");
    let mut args = MAINTENANCE_ARGS.to_vec();
    args.extend(["--page-put-delay-ms", "200"]);
    let mut soak = Soak::spawn(&ctx, &dir, &args);
    // A maintenance run whose seal has started and that has not ended yet; each line is
    // looked at once.
    let (mut seen, mut run, mut sealing) = (0, None, false);
    soak.wait_for("a maintenance seal in flight", |s| {
        for (i, l) in s.lines.iter().enumerate().skip(seen) {
            if l.starts_with("event maintenance-started ") {
                (run, sealing) = (Some(i), false);
            } else if is_maintenance_end(l) {
                (run, sealing) = (None, false);
            } else if l == "event seal-started" && run.is_some() {
                sealing = true;
            }
        }
        seen = s.lines.len();
        sealing
    });
    let started = run.unwrap();
    soak.kill();
    assert!(soak.errors().is_empty(), "{:?}", soak.errors());
    assert!(
        soak.find(started, is_maintenance_end).is_none(),
        "the run ended before the kill\n{}",
        soak.tail()
    );
    let head = ctx.head();
    let log_objects = ctx.assert_log_contiguous(head.generation);
    fs::remove_dir_all(&dir).unwrap();
    let v = restore_verify(&ctx, &soak.lines);
    eprintln!(
        "crash_during_maintenance_checkpoint: killed in maintenance run {}: acked={} \
         generation={} log_objects={log_objects} {}",
        soak.lines
            .iter()
            .filter(|l| l.starts_with("event maintenance-started "))
            .count(),
        v.acked,
        head.generation,
        v.restored
    );
}

/// The soak arguments of the batched variant of a test.
const WINDOW_ARGS: [&str; 4] = ["--batching", WINDOW, "--connections", "4"];

/// RustFS stopped mid-run: error lines while it is down, acks again after `start`; every
/// acked id restores and `log/<G>/` has no gap. `extra` goes to soak.
fn rustfs_stopped_mid_commit(name: &str, extra: &[&str]) {
    let ctx = Ctx::new(name).set("BUCKWAL_RETRY_DEADLINE_MS", "2000");
    let dir = ctx.dir("db");
    let mut soak = Soak::spawn(&ctx, &dir, extra);
    soak.wait_acks(200);
    let outage = Outage::start("stop", "start");
    soak.wait_for("an error line", |s| !s.errors().is_empty());
    outage.end();
    let restarted = Instant::now();
    soak.wait_acks_after(restarted, 50);
    soak.kill();
    let before_start = soak.acks_around(restarted, true);
    let errors = soak.errors().len();
    fs::remove_dir_all(&dir).unwrap();
    let head = ctx.head();
    let seqs = ctx.assert_log_contiguous(head.generation);
    let v = restore_verify(&ctx, &soak.lines);
    eprintln!(
        "{name}: {before_start} acks before the restart, {errors} error lines, killed at {} \
         acks: log/{}/ seqs 1..={seqs} {}",
        v.acked, head.generation, v.restored
    );
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn fault_rustfs_stopped_mid_commit() {
    rustfs_stopped_mid_commit("fault_rustfs_stopped_mid_commit", &[]);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn fault_window_rustfs_stopped_mid_commit() {
    rustfs_stopped_mid_commit("fault_window_rustfs_stopped_mid_commit", &WINDOW_ARGS);
}

/// RustFS paused for 1.5 s mid-run, with a 500 ms request timeout: the writer stalls (an ack
/// gap of at least 1 s), then carries on; every acked id restores and `log/<G>/` has no gap.
fn rustfs_paused_ambiguous_put(name: &str, extra: &[&str]) {
    let ctx = Ctx::new(name)
        .set("BUCKWAL_REQUEST_TIMEOUT_MS", "500")
        .set("BUCKWAL_RETRY_DEADLINE_MS", "5000");
    let dir = ctx.dir("db");
    let mut soak = Soak::spawn(&ctx, &dir, extra);
    soak.wait_acks(200);
    let outage = Outage::start("pause", "unpause");
    let paused = Instant::now();
    std::thread::sleep(Duration::from_millis(1500));
    outage.end();
    let unpaused = Instant::now();
    soak.wait_acks_after(unpaused, 100);
    soak.kill();
    let before_pause = soak.acks_around(paused, true);
    let during = soak.acks_around(paused, false) - soak.acks_around(unpaused, false);
    let stall = soak.longest_ack_gap();
    assert!(stall >= Duration::from_millis(1000), "{stall:?}");
    fs::remove_dir_all(&dir).unwrap();
    let head = ctx.head();
    let seqs = ctx.assert_log_contiguous(head.generation);
    let v = restore_verify(&ctx, &soak.lines);
    eprintln!(
        "{name}: {before_pause} acks before the pause, {during} during it, longest ack gap \
         {stall:?}, {} error lines, killed at {}: log/{}/ seqs 1..={seqs} {}",
        soak.errors().len(),
        v.acked,
        head.generation,
        v.restored
    );
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn fault_rustfs_paused_ambiguous_put() {
    rustfs_paused_ambiguous_put("fault_rustfs_paused_ambiguous_put", &[]);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn fault_window_rustfs_paused_ambiguous_put() {
    rustfs_paused_ambiguous_put("fault_window_rustfs_paused_ambiguous_put", &WINDOW_ARGS);
}

/// A sealed page object deleted from the bucket: restore succeeds (pages are fetched
/// lazily), and the first read of that page fails naming its key. `extra` goes to soak.
fn deleted_page_object_fails_read(name: &str, extra: &[&str]) {
    let ctx = Ctx::new(name);
    let dir = ctx.dir("db");
    let mut args = vec![
        "--checkpoint",
        "truncate",
        "--every",
        "100",
        "--count",
        "300",
    ];
    args.extend_from_slice(extra);
    Soak::spawn(&ctx, &dir, &args).finish();
    let head = ctx.head();
    let m = head.manifest.expect("closed soak sealed a manifest");
    let m_gen: u64 = m.strip_prefix("manifest/").unwrap().parse().unwrap();
    let pages = ctx.list(&format!("pages/{m_gen:010}/"));
    // The highest page: a table leaf the scan reads (page 1 is fetched by restore itself).
    let victim = pages.last().expect("sealed pages").clone();
    assert!(!victim.ends_with("/0000000001"), "{pages:?}");
    ctx.store().delete(&victim).unwrap();

    let r = ctx.dir("restored");
    let r = r.to_str().unwrap();
    ctx.ok(&["restore", "--dir", r]);
    let sql = "SELECT count(*), sum(length(payload)) FROM soak";
    // Without --claim, sql refuses the restored directory and leaves the head alone.
    let out = ctx.run(&["sql", "--dir", r, sql]);
    assert_eq!(out.status.code(), Some(1), "{}\n{}", out.stdout, out.stderr);
    assert!(out.stderr.contains("--claim"), "{}", out.stderr);
    assert_eq!(
        ctx.head().writer_id,
        head.writer_id,
        "a refused sql claimed"
    );
    let out = ctx.run(&["sql", "--claim", "--dir", r, sql]);
    assert_eq!(out.status.code(), Some(1), "{}\n{}", out.stdout, out.stderr);
    let key = victim.strip_prefix(&format!("{}/", ctx.prefix())).unwrap();
    assert!(
        out.stderr.contains(key),
        "stderr names {key}: {}",
        out.stderr
    );
    eprintln!("{name}: deleted {key}; stderr: {}", out.stderr.trim());
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn fault_deleted_page_object_fails_read() {
    deleted_page_object_fails_read("fault_deleted_page_object_fails_read", &[]);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn fault_window_deleted_page_object_fails_read() {
    deleted_page_object_fails_read("fault_window_deleted_page_object_fails_read", &WINDOW_ARGS);
}

/// Pass 2 (an encryption key): two sealed page objects swapped in the bucket. Restore
/// succeeds (pages are fetched lazily) and the first read of either page fails with
/// `integrity: <key>`, naming the object and nothing of its bytes. Pass 1 has no integrity
/// codes: the test then only checks that the swap is not caught, so it is not vacuous.
/// `extra` goes to soak.
fn swapped_page_objects_fail_read(name: &str, extra: &[&str]) {
    let ctx = Ctx::new(name);
    let encrypted = ctx
        .opt_var("BUCKWAL_ENCRYPTION_KEY_FILE")
        .is_some_and(|k| !k.is_empty());
    let dir = ctx.dir("db");
    let mut args = vec![
        "--checkpoint",
        "truncate",
        "--every",
        "100",
        "--count",
        "300",
    ];
    args.extend_from_slice(extra);
    Soak::spawn(&ctx, &dir, &args).finish();
    let head = ctx.head();
    let m = head.manifest.expect("closed soak sealed a manifest");
    let m_gen: u64 = m.strip_prefix("manifest/").unwrap().parse().unwrap();
    let pages = ctx.list(&format!("pages/{m_gen:010}/"));
    // The two highest pages: table leaves the scan reads (page 1 is fetched by restore itself).
    assert!(pages.len() >= 3, "{pages:?}");
    let (a, b) = (
        pages[pages.len() - 1].clone(),
        pages[pages.len() - 2].clone(),
    );
    let store = ctx.store();
    let body = |k: &str| store.get(k).unwrap().expect("page object").body;
    let (ba, bb) = (body(&a), body(&b));
    assert_eq!(ba.len(), bb.len());
    store.put(&a, &bb).unwrap();
    store.put(&b, &ba).unwrap();

    let r = ctx.dir("restored");
    let r = r.to_str().unwrap();
    ctx.ok(&["restore", "--dir", r]);
    let out = ctx.run(&[
        "sql",
        "--claim",
        "--dir",
        r,
        "SELECT count(*), sum(length(payload)) FROM soak",
    ]);
    if !encrypted {
        eprintln!(
            "{name}: no integrity codes: the swap read {}",
            out.stdout.trim()
        );
        return;
    }
    assert_eq!(out.status.code(), Some(1), "{}\n{}", out.stdout, out.stderr);
    let prefix = format!("{}/", ctx.prefix());
    let keys = [
        a.strip_prefix(&prefix).unwrap(),
        b.strip_prefix(&prefix).unwrap(),
    ];
    assert!(
        keys.iter().any(|k| out
            .stderr
            .contains(&format!("integrity: {}/{k}", ctx.prefix()))),
        "stderr names an integrity failure of {keys:?}: {}",
        out.stderr
    );
    eprintln!("{name}: swapped {a} and {b}; stderr: {}", out.stderr.trim());
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn encrypted_swapped_page_objects_fail_read() {
    swapped_page_objects_fail_read("encrypted_swapped_page_objects_fail_read", &[]);
}

/// Pass 2 (an encryption key): a plaintext format 1 head put over the encrypted bucket's head
/// while a writer runs. The running writer's head reads compare the head's bytes and fence
/// (`the head was changed by another writer`) at its next seal, with no
/// `integrity check failed` record: the soak prints a fenced `error` line and keeps retrying,
/// so the test waits for that line, then kills it. A fresh restore is an open, and a format 1 head under a key is the
/// `Encryption` "no integrity codes" error there, again with no record. (The post-open
/// `Corrupt("... format downgraded")` is pinned by the unit tests: only GC's reads reach it.)
/// Pass 1 has no key, so the test returns there.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn encrypted_downgraded_head_after_open_is_tampering() {
    let ctx = Ctx::new("encrypted_downgraded_head_after_open_is_tampering");
    if !ctx
        .opt_var("BUCKWAL_ENCRYPTION_KEY_FILE")
        .is_some_and(|k| !k.is_empty())
    {
        eprintln!("encrypted_downgraded_head_after_open_is_tampering: pass 1, no key: skipped");
        return;
    }
    let dir = ctx.dir("db");
    let mut soak = Soak::spawn(&ctx, &dir, &["--checkpoint", "truncate", "--every", "50"]);
    soak.wait_acks(120);
    let store = ctx.store();
    let key = format!("{}/head", ctx.prefix());
    let real = String::from_utf8(store.get(&key).unwrap().expect("head").body).unwrap();
    assert!(real.contains("\"format\":3"), "{real}");
    let old = format_1_head(&real);
    store.put(&key, old.as_bytes()).unwrap();
    // The soak does not exit when fenced: it prints `error ... fenced ...` and retries. Its
    // next seal (a truncate checkpoint every 50 acks) reads the head and fences.
    let fenced = soak.wait_fenced_then_no_ack(Duration::from_secs(2));
    soak.kill();
    let first = soak.lines[fenced].clone();
    assert!(
        first.contains("the head was changed by another writer"),
        "the running writer is fenced by the swapped head: {first}"
    );
    let err = soak.stderr();
    for (where_, text) in [("stdout", soak.lines.join("\n")), ("stderr", err)] {
        assert!(
            !text.contains("integrity check failed"),
            "a fenced writer logs no integrity record on {where_}: {text}"
        );
    }
    fs::remove_dir_all(&dir).ok();
    let r = ctx.dir("restored");
    let out = ctx.run(&["restore", "--dir", r.to_str().unwrap()]);
    assert_eq!(out.status.code(), Some(1), "{}\n{}", out.stdout, out.stderr);
    assert!(
        out.stderr
            .contains("the bucket has no integrity codes: it is plaintext, or was encrypted"),
        "restore (an open) refuses a format 1 head under a key: {}",
        out.stderr
    );
    assert!(
        !out.stderr.contains("integrity check failed"),
        "no record at open: {}",
        out.stderr
    );
}

/// A format 3 head's JSON as a format 1 head: `format` 1, without `journal_mode`, `key_check`
/// and `integrity` (each a `"name":"value"` pair, whatever its place in the object).
fn format_1_head(real: &str) -> String {
    let mut pairs: Vec<String> = Vec::new();
    let inner = real.trim().trim_start_matches('{').trim_end_matches('}');
    // Values are numbers, hex or plain identifiers: no commas or quotes inside a value.
    for pair in inner.split(',') {
        let name = pair.split(':').next().unwrap().trim().trim_matches('"');
        match name {
            "journal_mode" | "key_check" | "integrity" => {}
            "format" => pairs.push("\"format\":1".to_string()),
            _ => pairs.push(pair.to_string()),
        }
    }
    format!("{{{}}}", pairs.join(","))
}

/// The coordinator's ruling: a read-only verify of the same bucket while the writer runs
/// claims nothing (head unchanged) and does not disturb the writer.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn verify_while_writer_runs_loses_nothing() {
    let ctx = Ctx::new("verify_while_writer_runs_loses_nothing");
    let dir = ctx.dir("db");
    let mut soak = Soak::spawn(&ctx, &dir, &["--checkpoint", "truncate", "--every", "100"]);
    soak.wait_acks(250);
    let before = ctx.head();
    let seen = ctx.tmp.path().join("seen.acked");
    fs::write(&seen, soak.lines.join("\n")).unwrap();
    let v = ctx.dir("verify");
    let out = ctx.ok(&[
        "verify",
        "--dir",
        v.to_str().unwrap(),
        "--acked",
        seen.to_str().unwrap(),
    ]);
    let verified = Instant::now();
    let after = ctx.head();
    assert_eq!(after.writer_id, before.writer_id, "verify claimed nothing");
    // 100 acks printed after the verify returned, not ones buffered while it ran.
    soak.wait_acks_after(verified, 100);
    soak.kill();
    let after_verify = soak.acks_around(verified, false);
    assert!(soak.errors().is_empty(), "{:?}", soak.errors());
    assert_eq!(ctx.head().writer_id, before.writer_id);
    fs::remove_dir_all(&dir).unwrap();
    let r = restore_verify(&ctx, &soak.lines);
    eprintln!(
        "verify_while_writer_runs_loses_nothing: concurrent verify: {}; acks after it: \
         {after_verify}; final acked={} {}",
        out.stdout.trim(),
        r.acked,
        r.restored
    );
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn sql_on_a_fresh_prefix_prints_page_count_1() {
    let ctx = Ctx::new("sql_on_a_fresh_prefix_prints_page_count_1");
    let dir = ctx.dir("db");
    let out = ctx.ok(&[
        "sql",
        "--claim",
        "--dir",
        dir.to_str().unwrap(),
        "PRAGMA page_count",
    ]);
    assert_eq!(out.stdout.trim(), "1", "{}\n{}", out.stdout, out.stderr);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn second_process_is_refused() {
    let ctx = Ctx::new("second_process_is_refused");
    let dir = ctx.dir("db");
    let mut soak = Soak::spawn(&ctx, &dir, &[]);
    soak.wait_acks(50);
    let out = ctx.run(&["sql", "--dir", dir.to_str().unwrap(), "SELECT 1"]);
    assert_eq!(out.status.code(), Some(1), "{}\n{}", out.stdout, out.stderr);
    assert!(
        out.stderr.contains("is open in another process"),
        "{}",
        out.stderr
    );
    soak.wait_acks_after(Instant::now(), 20);
    soak.kill();
    assert!(soak.errors().is_empty(), "{:?}", soak.errors());
    fs::remove_dir_all(&dir).unwrap();
    restore_verify(&ctx, &soak.lines);
}

/// The coordinator's correction 1: pages a passive checkpoint backfilled into app.db before
/// any seal must reach the bucket at the seal a later run makes.
///
/// A passive checkpoint backfills the whole WAL here (one connection, no readers), so the
/// next commit restarts the WAL and seals: with `--every 100` the seal follows ack 100 with no
/// gap to kill in. So the first run checkpoints first at ack 160 (past 150) with page PUTs
/// delayed 2 s, and is killed on that first `event seal-started`: the checkpoint has
/// backfilled app.db, no page PUT has finished and the head still names no manifest — the
/// state "after a checkpoint, before any seal".
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn passive_checkpoint_then_restart_then_seal() {
    let ctx = Ctx::new("passive_checkpoint_then_restart_then_seal");
    let dir = ctx.dir("db");
    let args = [
        "--checkpoint",
        "passive",
        "--every",
        "160",
        "--page-put-delay-ms",
        "2000",
    ];
    let mut first = Soak::spawn(&ctx, &dir, &args);
    first.wait_line("event seal-started");
    first.kill();
    let killed_at = first.acks();
    assert!(killed_at > 150, "{killed_at}");
    assert_eq!(first.count("event seal-finished"), 0);
    let head = ctx.head();
    assert_eq!(
        (head.generation, head.manifest.as_deref()),
        (1, None),
        "no seal published"
    );
    let backfilled = fs::metadata(dir.join("app.db")).unwrap().len();
    assert!(
        backfilled > 8192,
        "the passive checkpoint backfilled app.db: {backfilled}"
    );
    let dirty = fs::metadata(dir.join("app.db-dirty")).unwrap().len();
    assert!(dirty > 0, "the backfilled pages are recorded as dirty");
    assert!(ctx.list("pages/").is_empty(), "no page PUT finished");

    let next = (first.last_ack() + 1).to_string();
    let args = [
        "--checkpoint",
        "truncate",
        "--every",
        "100",
        "--start",
        &next,
    ];
    let mut second = Soak::spawn(&ctx, &dir, &args);
    second.wait_line("event seal-finished");
    second.kill();
    assert!(first.errors().is_empty() && second.errors().is_empty());
    let sealed = ctx.head();
    assert!(
        sealed.generation > 1 && sealed.manifest.is_some(),
        "{sealed:?}"
    );
    fs::remove_dir_all(&dir).unwrap();
    let mut lines = first.lines.clone();
    lines.extend(second.lines.iter().cloned());
    let v = restore_verify(&ctx, &lines);
    eprintln!(
        "passive_checkpoint_then_restart_then_seal: first run killed on seal-started after \
         {killed_at} acks (app.db {backfilled} bytes, app.db-dirty {dirty} bytes); second run killed after seal-finished at \
         {} acks; acked={} {}",
        second.acks(),
        v.acked,
        v.restored
    );
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn writer_killed_and_restarted_on_own_disk() {
    let ctx = Ctx::new("writer_killed_and_restarted_on_own_disk");
    let dir = ctx.dir("db");
    let mut first = Soak::spawn(&ctx, &dir, &[]);
    first.wait_acks(300);
    first.kill();
    // Start after the last ack seen: re-inserting an acked id would ack it twice.
    let next = (first.last_ack() + 1).to_string();
    let mut second = Soak::spawn(&ctx, &dir, &["--start", &next, "--count", "200"]);
    second.finish();
    assert_eq!(second.acks(), 200);
    assert!(first.errors().is_empty() && second.errors().is_empty());
    let mut lines = first.lines.clone();
    lines.extend(second.lines.iter().cloned());
    let v = restore_verify(&ctx, &lines);
    eprintln!(
        "writer_killed_and_restarted_on_own_disk: killed after {} acks, restarted at {next}; \
         acked={} {}",
        first.acks(),
        v.acked,
        v.restored
    );
}

/// The background setting the two tests below run under: at most 20 commits behind the bucket.
const BACKGROUND: &str = "background 20ms 8 1MiB 20";
/// Its `max_behind`.
const BEHIND: usize = 20;

/// `SELECT count(*), max(id) FROM soak` through `sql`, as (count, max).
fn soak_rows(ctx: &Ctx, dir: &Path, claim: bool) -> (usize, usize) {
    let d = dir.to_str().unwrap();
    let mut args = vec!["sql", "--dir", d];
    if claim {
        args.push("--claim");
    }
    args.push("SELECT count(*), max(id) FROM soak");
    let out = ctx.ok(&args);
    let line = out.stdout.trim().to_string();
    let (n, max) = line.split_once('\t').unwrap_or_else(|| panic!("{line:?}"));
    (n.parse().unwrap(), max.parse().unwrap_or(0))
}

/// Background commits, SIGKILL and the loss of the disk: the bucket holds a prefix (every id
/// from 1 to some k, none after), at most `max_behind` commits short of the last ack. App
/// catch-up (the how-to's steps, with the ids as the app's messages) replays exactly the
/// missing ones, and then every acked id is present.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn background_killed_and_disk_lost_restores_a_prefix_and_catch_up_replays_the_rest() {
    let ctx = Ctx::new("background_killed_and_disk_lost");
    let dir = ctx.dir("db");
    let mut soak = Soak::spawn(&ctx, &dir, &["--batching", BACKGROUND]);
    soak.wait_acks(400);
    soak.kill();
    assert!(soak.errors().is_empty(), "{:?}", soak.errors());
    let acked = soak.last_ack() as usize;
    fs::remove_dir_all(&dir).unwrap();
    // Restore, then read what the bucket holds.
    let restored = ctx.dir("restored");
    let d = restored.to_str().unwrap();
    ctx.ok(&["restore", "--dir", d]);
    let (count, max) = soak_rows(&ctx, &restored, true);
    assert_eq!(
        count, max,
        "the bucket holds a prefix: ids 1..={max}, no gap"
    );
    // A commit queued just before the kill can land before its ack line is printed, so the
    // bucket may hold a few ids beyond the last ack line, never more than `max_behind`.
    assert!(max <= acked + BEHIND, "{max} restored, {acked} acked");
    assert!(
        acked.saturating_sub(max) <= BEHIND,
        "at most max_behind ({BEHIND}) acked commits were lost: acked {acked}, restored {max}"
    );
    // App catch-up: replay every message after the last one in the database.
    let missing: Vec<String> = (max + 1..=acked)
        .map(|id| format!("({id}, zeroblob(1024))"))
        .collect();
    if !missing.is_empty() {
        let insert = format!(
            "INSERT INTO soak(id, payload) VALUES {}",
            missing.join(", ")
        );
        ctx.ok(&["sql", "--dir", d, "--claim", &insert]);
    }
    let (count, max) = soak_rows(&ctx, &restored, false);
    assert_eq!(count, max, "no gap after the replay");
    assert!(max >= acked, "every acked id is present: {max} < {acked}");
    eprintln!(
        "background_killed_and_disk_lost: acked={acked} lost_and_replayed={}",
        missing.len()
    );
}

/// Background commits and SIGKILL, with the disk kept: nothing is lost. The next writer open
/// finds the commits in the local `-wal` and uploads them, so app catch-up replays nothing.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn background_killed_with_the_disk_kept_loses_nothing_and_catch_up_replays_nothing() {
    let ctx = Ctx::new("background_killed_disk_kept");
    let dir = ctx.dir("db");
    let mut soak = Soak::spawn(&ctx, &dir, &["--batching", BACKGROUND]);
    soak.wait_acks(400);
    soak.kill();
    assert!(soak.errors().is_empty(), "{:?}", soak.errors());
    let acked = soak.last_ack() as usize;
    // The reopen on the same disk reconciles (Ahead) before it answers.
    let (count, max) = soak_rows(&ctx, &dir, false);
    assert_eq!(count, max, "no gap");
    assert!(
        max >= acked,
        "catch-up finds nothing to replay: {max} < {acked}"
    );
    // Close uploaded everything: a restore into a fresh directory holds it all.
    let v = restore_verify(&ctx, &soak.lines);
    assert_eq!(v.present, v.acked);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_pragma_refused_then_all_acks_restore() {
    let ctx = Ctx::new("mvcc_pragma_refused_then_all_acks_restore");
    let dir = ctx.dir("db");
    let d = dir.to_str().unwrap();
    let mut soak = Soak::spawn(
        &ctx,
        &dir,
        &[
            "--checkpoint",
            "truncate",
            "--every",
            "100",
            "--count",
            "200",
        ],
    );
    soak.finish();
    assert!(soak.errors().is_empty(), "{:?}", soak.errors());
    let out = ctx.run(&["sql", "--dir", d, "PRAGMA journal_mode='mvcc'"]);
    assert_eq!(out.status.code(), Some(1), "{}\n{}", out.stdout, out.stderr);
    assert!(
        out.stderr
            .contains("MVCC: switching an existing WAL database"),
        "{}",
        out.stderr
    );
    // The refusal left the local database as it was.
    let count = ctx.ok(&["sql", "--dir", d, "SELECT count(*) FROM soak"]);
    assert_eq!(count.stdout.trim(), "200");
    restore_verify(&ctx, &soak.lines);
    // The harness bites: an id that was never committed fails verify.
    let bogus = ctx.tmp.path().join("bogus.acked");
    fs::write(&bogus, "ack 1\nack 201\n").unwrap();
    let v = ctx.dir("bogus");
    let out = ctx.run(&[
        "verify",
        "--dir",
        v.to_str().unwrap(),
        "--acked",
        bogus.to_str().unwrap(),
    ]);
    assert_eq!(out.status.code(), Some(1), "{}", out.stdout);
    assert!(out.stderr.contains("[201]"), "{}", out.stderr);
}

/// Run `gc` every second until it trims against `head`'s own manifest (generation - 1), at
/// most `limit`: its line. Polling for `outcome=trimmed` alone can stop at a trim against an
/// older manifest (the newest old enough), which leaves the logs above it.
fn gc_until_trimmed_against_head(ctx: &Ctx, head: &Head, limit: Duration) -> String {
    let want = format!("trimmed_against={} ", head.generation - 1);
    let start = Instant::now();
    loop {
        let out = ctx.ok(&["gc"]).stdout;
        if out.contains("outcome=trimmed") && out.contains(&want) {
            return out.trim().to_string();
        }
        assert!(
            start.elapsed() < limit,
            "{}: gc never trimmed against the head's manifest ({head:?}) within {limit:?}: {out}",
            ctx.name
        );
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn trimming_bounds_restore_gets() {
    // The smallest valid grace: 2 s > lease 0 + 400 ms + 500 ms.
    let ctx = Ctx::new("trimming_bounds_restore_gets")
        .set("BUCKWAL_TRIM_GRACE_SECS", "2")
        .set("BUCKWAL_LEASE_MS", "0")
        .set("BUCKWAL_RETRY_DEADLINE_MS", "400")
        .set("BUCKWAL_REQUEST_TIMEOUT_MS", "500");
    let dir = ctx.dir("db");
    let args = [
        "--checkpoint",
        "truncate",
        "--every",
        "50",
        "--count",
        "500",
    ];
    let mut soak = Soak::spawn(&ctx, &dir, &args);
    soak.finish();
    assert!(soak.errors().is_empty(), "{:?}", soak.errors());
    let head = ctx.head();
    // 500 commits at --every 50: a seal per checkpoint (the first at least) plus close.
    assert!(head.generation >= 9, "checkpoints sealed: {head:?}");
    // GC trims against a manifest once the bucket says it is 3.9 s old (2 s + 400 ms +
    // 500 ms + 1 s).
    let gc = gc_until_trimmed_against_head(&ctx, &head, Duration::from_secs(15));
    let logs = ctx.list("log/");
    let root = format!("{}/log/{:010}/", ctx.prefix(), head.generation);
    assert!(
        logs.iter().all(|k| k.starts_with(&root)),
        "only generation {} logs remain: {logs:?}",
        head.generation
    );
    let v = restore_verify(&ctx, &soak.lines);
    let gets = field(&v.restored, "gets");
    let log_objects = field(&v.restored, "log_objects");
    // head, manifest, page 1 (the MVCC check), then one GET per log object.
    assert!(gets <= 3 + log_objects, "{}", v.restored);
    assert_eq!(
        log_objects,
        logs.len(),
        "restore read every log object left"
    );
    eprintln!(
        "trimming_bounds_restore_gets: {} seals; {}; log/ holds {} keys of generation {}; {}",
        soak.count("event seal-finished"),
        gc.trim(),
        logs.len(),
        head.generation,
        v.restored
    );
}

/// Task 10's several-connections test, in process against RustFS: four writer threads and a
/// reader share one database; every row restores.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn several_connections_against_rustfs() {
    let ctx = Ctx::new("several_connections_against_rustfs");
    let config = |dir: &Path| ctx.config(dir);
    let dir = ctx.dir("db");
    let db = buckwal::open(config(&dir)).expect("open");
    db.connect()
        .unwrap()
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let start = Instant::now();
    let writers: Vec<_> = (0..4i64)
        .map(|w| {
            let db = db.clone();
            std::thread::spawn(move || {
                let c = db.connect().unwrap();
                for i in 0..100 {
                    let id = w * 1000 + i;
                    c.execute(
                        "INSERT INTO t(id, v) VALUES (?, ?)",
                        &[Value::from_i64(id), Value::build_text(format!("row {id}"))],
                    )
                    .unwrap_or_else(|e| panic!("writer {w} row {i}: {e}"));
                }
                c.close().unwrap();
            })
        })
        .collect();
    let reader = {
        let db = db.clone();
        let done = done.clone();
        std::thread::spawn(move || {
            let c = db.connect().unwrap();
            let mut reads = 0;
            while !done.load(std::sync::atomic::Ordering::SeqCst) {
                let n = c.query("SELECT count(*) FROM t", &[]).unwrap()[0][0]
                    .as_int()
                    .unwrap();
                assert!((0..=400).contains(&n), "{n}");
                reads += 1;
            }
            c.close().unwrap();
            reads
        })
    };
    for w in writers {
        w.join().unwrap();
    }
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    let reads = reader.join().unwrap();
    let elapsed = start.elapsed();
    let c = db.connect().unwrap();
    let count = c.query("SELECT count(*) FROM t", &[]).unwrap()[0][0].as_int();
    assert_eq!(count, Some(400));
    c.close().unwrap();
    db.close().unwrap();

    let r = ctx.dir("restored");
    buckwal::restore(&config(&r), &r).expect("restore");
    let db = buckwal::open_read_only(config(&r)).expect("open restored");
    let c = db.connect().unwrap();
    let count = c.query("SELECT count(*) FROM t", &[]).unwrap()[0][0].as_int();
    assert_eq!(count, Some(400));
    c.close().unwrap();
    db.close().unwrap();
    eprintln!("several_connections_against_rustfs: 400 inserts, {reads} reads in {elapsed:?}");
}

/// B1 (merge review), end to end: writer A (in process, against RustFS) commits and goes
/// idle; B restores and claims with `sql --claim`, commits and seals with a TRUNCATE
/// checkpoint (and its close); then A closes, and its close-time TRUNCATE would seal the
/// same generation again over B's pages. Every row either writer acked must restore, and
/// `pages/G/*`, `manifest/G` and the head must be exactly what B's seal wrote.
///
/// A runs in process because the CLI has no way to hold a writer idle and then close it
/// (soak acks until killed or `--count`, and SIGKILL or SIGTERM skips close).
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn idle_old_writer_closing_after_claim_and_seal_loses_nothing() {
    let ctx = Ctx::new("idle_old_writer_closing_after_claim_and_seal_loses_nothing");
    let config = ctx.config(&ctx.dir("a"));
    let db_a = buckwal::open(config).expect("open A");
    let ca = db_a.connect().unwrap();
    ca.execute(
        "CREATE TABLE IF NOT EXISTS soak(id INTEGER PRIMARY KEY, payload BLOB)",
        &[],
    )
    .unwrap();
    let mut lines = Vec::new();
    for id in 1..=50 {
        ca.execute(
            "INSERT INTO soak(id, payload) VALUES (?, zeroblob(1024))",
            &[Value::from_i64(id)],
        )
        .unwrap_or_else(|e| panic!("A commit {id}: {e}"));
        lines.push(format!("ack {id}"));
    }
    let gen = ctx.head().generation;

    let b = ctx.dir("b");
    let inserts: Vec<String> = (1001..=1010)
        .map(|id| format!("INSERT INTO soak(id, payload) VALUES ({id}, zeroblob(1024))"))
        .collect();
    let mut args = vec!["sql", "--claim", "--dir", b.to_str().unwrap()];
    args.extend(inserts.iter().map(String::as_str));
    args.push("PRAGMA wal_checkpoint(TRUNCATE)");
    ctx.ok(&args);
    lines.extend((1001..=1010).map(|id| format!("ack {id}")));
    let sealed = ctx.head();
    assert_eq!(
        (sealed.generation, sealed.manifest.is_some()),
        (gen + 2, true),
        "B's first seal after the takeover skips a generation: {sealed:?}"
    );
    // B's seal wrote its objects under G+1, not under A's generation G.
    let key_gen = gen + 1;
    let store = ctx.store();
    let sealed_objects = || {
        let mut keys = ctx.list(&format!("pages/{key_gen:010}/"));
        keys.push(format!("{}/manifest/{key_gen:010}", ctx.prefix()));
        keys.push(format!("{}/head", ctx.prefix()));
        keys.into_iter()
            .map(|k| {
                let body = store.get(&k).expect("get").map(|o| o.body);
                (k, body)
            })
            .collect::<Vec<_>>()
    };
    let before = sealed_objects();

    let closed = ca.close().and_then(|()| db_a.close());
    assert!(
        matches!(closed, Ok(()) | Err(buckwal::Error::Fenced(_))),
        "A's close: {closed:?}"
    );
    let after = sealed_objects();
    let changed: Vec<&String> = before
        .iter()
        .zip(&after)
        .filter(|(b, a)| b != a)
        .map(|(b, _)| &b.0)
        .collect();
    assert!(
        before.len() == after.len() && changed.is_empty(),
        "A's close changed B's seal: {changed:?} ({} keys before, {} after)",
        before.len(),
        after.len()
    );
    assert_eq!(ctx.head(), sealed);
    let v = restore_verify(&ctx, &lines);
    eprintln!(
        "idle_old_writer_closing_after_claim_and_seal_loses_nothing: A's close {closed:?}; \
         acked={} {}",
        v.acked, v.restored
    );
}

/// Item 1 end to end: old writer A is stopped (SIGSTOP) in the middle of a seal, its page PUTs
/// slowed; B takes over and seals, which moves the head two generations (B's pages and manifest
/// go under G+1, never under A's G). Resumed (SIGCONT), A's late PUTs can only land on keys under
/// G, which nothing references: every object B's seal wrote is byte-identical afterwards, and
/// the restore holds every row either writer acked.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn late_put_from_fenced_writer_lands_on_unreferenced_key() {
    let ctx = short_lease_ctx("late_put_from_fenced_writer_lands_on_unreferenced_key");
    let a = ctx.dir("a");
    let mut soak = Soak::spawn(
        &ctx,
        &a,
        &[
            "--checkpoint",
            "passive",
            "--every",
            "160",
            "--page-put-delay-ms",
            "2000",
        ],
    );
    soak.wait_line("event seal-started");
    soak.signal("STOP");
    let before = ctx.head();
    let b = ctx.dir("b");
    takeover(&ctx, &b, &before);
    let rows = insert_rows(
        &ctx,
        &b,
        100001..=100010,
        &["PRAGMA wal_checkpoint(TRUNCATE)"],
    );
    let head = ctx.head();
    assert_eq!(
        head.generation,
        before.generation + 2,
        "B's first seal skips a generation: {before:?} then {head:?}"
    );
    let key_gen = before.generation + 1;
    let store = ctx.store();
    let sealed_objects = || {
        let mut keys = ctx.list(&format!("pages/{key_gen:010}/"));
        keys.push(format!("{}/manifest/{key_gen:010}", ctx.prefix()));
        keys.push(format!("{}/head", ctx.prefix()));
        keys.into_iter()
            .map(|k| {
                let body = store.get(&k).expect("get").map(|o| o.body);
                (k, body)
            })
            .collect::<Vec<_>>()
    };
    let sealed = sealed_objects();
    assert!(sealed.len() > 2, "B's seal wrote pages: {sealed:?}");
    soak.signal("CONT");
    std::thread::sleep(Duration::from_secs(6));
    soak.kill();
    assert_eq!(
        sealed_objects(),
        sealed,
        "A's late PUTs changed B's seal (generation {key_gen})"
    );
    let mut lines = soak.lines.clone();
    lines.extend(rows);
    let v = restore_verify(&ctx, &lines);
    eprintln!(
        "late_put_from_fenced_writer_lands_on_unreferenced_key: generation {} then {}; \
         acked={} {}",
        before.generation, head.generation, v.acked, v.restored
    );
}

/// A soak `error` line saying the writer was fenced (`Error::Fenced`: "fenced by a newer
/// writer: …").
fn is_fenced(l: &str) -> bool {
    l.starts_with("error ") && l.to_ascii_lowercase().contains("fenced")
}

/// `takeover --dir <dir>`: its `took over …` line. Its epoch, and the head's after it, must
/// be one above `before`'s.
fn takeover(ctx: &Ctx, dir: &Path, before: &Head) -> String {
    let out = ctx.ok(&["takeover", "--dir", dir.to_str().unwrap()]);
    let line = out.stdout.trim().to_string();
    assert!(line.starts_with("took over "), "{}: {line}", ctx.name);
    let want = before.epoch + 1;
    assert_eq!(field(&line, "epoch") as u64, want, "{}: {line}", ctx.name);
    assert_eq!(
        ctx.head().epoch,
        want,
        "{}: the head after {line}",
        ctx.name
    );
    line
}

/// `sql --dir <dir>` inserting `ids` into the soak table, then running `then`: the `ack <id>`
/// lines of the rows (each INSERT commits on its own and `sql` exits 0 only if all did).
fn insert_rows(
    ctx: &Ctx,
    dir: &Path,
    ids: std::ops::RangeInclusive<i64>,
    then: &[&str],
) -> Vec<String> {
    let inserts: Vec<String> = ids
        .clone()
        .map(|id| format!("INSERT INTO soak(id, payload) VALUES ({id}, zeroblob(1024))"))
        .collect();
    let mut args = vec!["sql", "--dir", dir.to_str().unwrap()];
    args.extend(inserts.iter().map(String::as_str));
    args.extend_from_slice(then);
    ctx.ok(&args);
    ids.map(|id| format!("ack {id}")).collect()
}

/// A soak in A reaches 200 acks; `takeover --dir B` fences it (epoch + 1) and B commits 10
/// rows. The soak must print a fenced error and then ack nothing more; every ack of both
/// writers restores.
fn takeover_fences_a_running_soak_under(name: &str, extra: &[&str]) {
    let ctx = Ctx::new(name);
    let a = ctx.dir("a");
    let mut soak = Soak::spawn(&ctx, &a, extra);
    soak.wait_acks(200);
    let before = ctx.head();
    let b = ctx.dir("b");
    let took = takeover(&ctx, &b, &before);
    let rows = insert_rows(&ctx, &b, 100001..=100010, &[]);
    let fenced = soak.wait_fenced_then_no_ack(Duration::from_secs(2));
    soak.kill();
    let acks_after_fence = soak.lines[fenced..]
        .iter()
        .filter(|l| l.starts_with("ack "))
        .count();
    let mut lines = soak.lines.clone();
    lines.extend(rows);
    let v = restore_verify(&ctx, &lines);
    eprintln!(
        "{name}: {took}; soak acks={} (after the first fenced error: {acks_after_fence}); \
         first fenced error {:?}; acked={} {}",
        soak.acks(),
        soak.lines[fenced],
        v.acked,
        v.restored
    );
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn takeover_fences_a_running_soak() {
    takeover_fences_a_running_soak_under("takeover_fences_a_running_soak", &[]);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn takeover_window_fences_a_running_soak() {
    takeover_fences_a_running_soak_under(
        "takeover_window_fences_a_running_soak",
        &["--batching", WINDOW, "--connections", "4"],
    );
}

/// A 1 s lease and the smallest valid grace above it (6 s > 1 s + 2 s + 2 s): GC trims
/// against a manifest the bucket says is 11 s old (6 s + 2 s + 2 s + 1 s).
fn short_lease_ctx(name: &str) -> Ctx {
    Ctx::new(name)
        .set("BUCKWAL_LEASE_MS", "1000")
        .set("BUCKWAL_RETRY_DEADLINE_MS", "2000")
        .set("BUCKWAL_REQUEST_TIMEOUT_MS", "2000")
        .set("BUCKWAL_TRIM_GRACE_SECS", "6")
}

/// The generations `log/` holds keys of.
fn log_generations(ctx: &Ctx) -> std::collections::BTreeSet<u64> {
    let root = format!("{}/log/", ctx.prefix());
    ctx.list("log/")
        .iter()
        .map(|k| {
            let rest = k.strip_prefix(&root).unwrap();
            rest.split('/').next().unwrap().parse().unwrap()
        })
        .collect()
}

/// The lease self-fences a writer that was paused through a takeover and a GC: an Off soak in
/// A is stopped (SIGSTOP) after 100 acks; B takes over, commits 10 rows and seals, and GC
/// deletes A's generation's log, so A's next log key is free again. Resumed (SIGCONT), A's
/// lease has lapsed: its head check must fence it before any append, so it acks nothing.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn paused_old_writer_is_fenced_after_resume() {
    let ctx = short_lease_ctx("paused_old_writer_is_fenced_after_resume");
    let a = ctx.dir("a");
    let mut soak = Soak::spawn(&ctx, &a, &[]);
    soak.wait_acks(100);
    soak.signal("STOP");
    let before = ctx.head();
    let b = ctx.dir("b");
    let took = takeover(&ctx, &b, &before);
    let rows = insert_rows(
        &ctx,
        &b,
        100001..=100010,
        &["PRAGMA wal_checkpoint(TRUNCATE)"],
    );
    let head = ctx.head();
    assert!(
        head.generation > before.generation,
        "B sealed A's generation: {before:?} then {head:?}"
    );
    let gc = gc_until_trimmed_against_head(&ctx, &head, Duration::from_secs(20));
    let a_log = format!("log/{:010}/", before.generation);
    assert!(
        ctx.list(&a_log).is_empty(),
        "GC deleted A's {a_log}: {:?}",
        ctx.list(&a_log)
    );
    // Every line A printed before the STOP was read long ago; any line stamped after this
    // point was printed after the CONT.
    let cont = Instant::now();
    soak.signal("CONT");
    std::thread::sleep(Duration::from_secs(3));
    soak.kill();
    let resumed = soak.stamps.iter().position(|t| *t >= cont);
    let tail = &soak.lines[resumed.unwrap_or(soak.lines.len())..];
    // The one connection's commit may have returned before the STOP and print its ack only
    // after the CONT (the STOP landed between the two); `restore_verify` below requires it
    // in B's restore. A second ack would be an append after the CONT.
    let late_acks = soak.acks_around(cont, false);
    assert!(
        late_acks <= 1 && tail.iter().skip(1).all(|l| !l.starts_with("ack ")),
        "A acked after the CONT: {tail:?}"
    );
    assert!(
        tail.iter().any(|l| is_fenced(l)),
        "A printed no fenced error after the CONT: {tail:?}\n{}",
        soak.tail()
    );
    let mut lines = soak.lines.clone();
    lines.extend(rows);
    let v = restore_verify(&ctx, &lines);
    let shown = &tail[..tail.len().min(4)];
    eprintln!(
        "paused_old_writer_is_fenced_after_resume: A stopped after {} acks (generation {}, \
         epoch {}); {took}; {gc}; after the CONT A printed {} lines ({late_acks} acks), the \
         first {shown:?}; acked={} {}",
        soak.acks(),
        before.generation,
        before.epoch,
        tail.len(),
        v.acked,
        v.restored
    );
}

/// GC from the CLI after five short-lived writers, each claiming a fresh directory, committing
/// one row and sealing at close: the log of every generation but the head's is deleted once
/// the head's manifest is old enough, and the five rows restore.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn gc_cli_trims_after_short_lived_processes() {
    let ctx = short_lease_ctx("gc_cli_trims_after_short_lived_processes");
    let mut lines = Vec::new();
    for k in 1..=5 {
        let d = ctx.dir(&format!("d{k}"));
        let insert = format!("INSERT INTO soak(id, payload) VALUES ({k}, zeroblob(1024))");
        ctx.ok(&[
            "sql",
            "--claim",
            "--dir",
            d.to_str().unwrap(),
            "CREATE TABLE IF NOT EXISTS soak(id INTEGER PRIMARY KEY, payload BLOB)",
            &insert,
        ]);
        lines.push(format!("ack {k}"));
    }
    let head = ctx.head();
    let before = log_generations(&ctx);
    assert!(
        before.len() > 1,
        "log/ holds more than one generation: {before:?} ({head:?})"
    );
    let gc = gc_until_trimmed_against_head(&ctx, &head, Duration::from_secs(20));
    let logs = ctx.list("log/");
    let root = format!("{}/log/{:010}/", ctx.prefix(), head.generation);
    assert!(
        logs.iter().all(|k| k.starts_with(&root)),
        "only generation {} logs remain: {logs:?}",
        head.generation
    );
    let v = restore_verify(&ctx, &lines);
    eprintln!(
        "gc_cli_trims_after_short_lived_processes: log/ generations before GC {before:?}, \
         head {head:?}; {gc}; log/ holds {} keys after; acked={} {}",
        logs.len(),
        v.acked,
        v.restored
    );
}

/// Replace the soak's batching file in one step (write aside, rename): an in-place write
/// truncates first, and a soak that looks between the truncate and the write sees two
/// changes and prints `batching …` twice.
fn write_batching_file(path: &Path, setting: &str) {
    let aside = path.with_extension("new");
    fs::write(&aside, setting).unwrap();
    fs::rename(&aside, path).unwrap();
}

/// `stats --claim` under Off (one log PUT per commit), then one soak whose batching file
/// switches Off → Window → Off: the `stats` blocks between the switches show one PUT per
/// commit while Off and more than 1.5 commits per PUT under the window.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn batching_stats_against_rustfs() {
    let ctx = Ctx::new("batching_stats_against_rustfs");
    let dir = ctx.dir("db");
    let d = dir.to_str().unwrap();
    let inserts: Vec<String> = (1..=30)
        .map(|i| format!("INSERT INTO t VALUES ({i})"))
        .collect();
    let mut args = vec![
        "stats",
        "--claim",
        "--dir",
        d,
        "--batching",
        "off",
        "CREATE TABLE t(id INTEGER PRIMARY KEY)",
    ];
    args.extend(inserts.iter().map(String::as_str));
    let out = ctx.ok(&args);
    let off = key_values(&out.stdout, "");
    assert!(off["commits_acked"] >= 31.0, "{}", out.stdout);
    assert_eq!(off["log_puts"], off["commits_acked"], "{}", out.stdout);
    assert_eq!(off["commits_per_log_put"], 1.0, "{}", out.stdout);

    let file = ctx.tmp.path().join("batching");
    write_batching_file(&file, "off");
    let f = file.to_str().unwrap();
    let args = [
        "--connections",
        "8",
        "--batching-file",
        f,
        "--stats-every",
        "100",
    ];
    let mut soak = Soak::spawn(&ctx, &dir, &args);
    // Each phase: the switch's `batching` line, then two stats blocks after it, printed
    // before the next write (a block is a snapshot with no commit running, so a block after
    // a switch line counts no commit made with the setting before it).
    let mut phases: Vec<(String, Stats, Stats)> = Vec::new();
    for (i, setting) in ["off", WINDOW, "off"].into_iter().enumerate() {
        let written = Instant::now();
        if i > 0 {
            write_batching_file(&file, setting);
        }
        let from = soak.lines.len();
        let line = format!("batching {setting}");
        soak.wait_for(&line, |s| s.find(from, |l| l == line).is_some());
        let at = soak.find(from, |l| l == line).unwrap();
        soak.wait_acks_after(written, 200);
        soak.wait_for("two stats blocks", |s| s.stats_blocks(at).len() >= 2);
        let blocks = soak.stats_blocks(at);
        let (first, last) = (blocks[0].1.clone(), blocks.last().unwrap().1.clone());
        phases.push((setting.to_string(), first, last));
    }
    let exited = soak.child.try_wait().expect("poll soak");
    assert!(
        exited.is_none(),
        "one process throughout: soak exited {exited:?}"
    );
    soak.kill();
    assert!(soak.errors().is_empty(), "{:?}", soak.errors());
    let switches = soak.lines.iter().filter(|l| l.starts_with("batching "));
    assert_eq!(switches.count(), 3, "one process saw every switch");
    for (setting, a, b) in &phases {
        let acked = delta(a, b, "commits_acked");
        let puts = delta(a, b, "log_puts");
        let per_put = delta(a, b, "commits_uploaded") / puts;
        eprintln!(
            "batching_stats_against_rustfs: {setting}: Δcommits_acked={acked} Δlog_puts={puts} \
             Δcommits_per_log_put={per_put:.3}"
        );
        // Two blocks 100 soak acks apart; the library's count may differ by the commits
        // that finished while a block waited for no commit to be running.
        assert!(acked >= 50.0, "{setting}: {acked} acks in the window");
        if setting == "off" {
            assert_eq!(puts, acked, "{setting}: one log PUT per commit");
        } else {
            assert!(per_put > 1.5, "{setting}: {per_put} commits per log PUT");
        }
    }
    fs::remove_dir_all(&dir).unwrap();
    let v = restore_verify(&ctx, &soak.lines);
    eprintln!(
        "batching_stats_against_rustfs: stats --batching off: {:?}; soak acked={} {}",
        off, v.acked, v.restored
    );
}

/// Eight connections, the batching file flipped between Off and the window every 50 acks, so
/// Off and Window commits share the log: every acked id restores and the log has no gap.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn batching_mixed_connections_against_rustfs() {
    let ctx = Ctx::new("batching_mixed_connections_against_rustfs");
    let dir = ctx.dir("db");
    let file = ctx.tmp.path().join("batching");
    write_batching_file(&file, "off");
    let args = [
        "--connections",
        "8",
        "--batching-file",
        file.to_str().unwrap(),
    ];
    let mut soak = Soak::spawn(&ctx, &dir, &args);
    soak.wait_line("batching off");
    let flips = 8;
    for i in 0..flips {
        let t = Instant::now();
        soak.wait_acks_after(t, 50);
        let setting = if i % 2 == 0 { WINDOW } else { "off" };
        write_batching_file(&file, setting);
        soak.wait_line(&format!("batching {setting}"));
    }
    soak.wait_acks_after(Instant::now(), 50);
    soak.kill();
    assert!(soak.errors().is_empty(), "{:?}", soak.errors());
    let switches = soak.lines.iter().filter(|l| l.starts_with("batching "));
    assert_eq!(switches.count(), flips + 1);
    let uploads = soak
        .lines
        .iter()
        .filter(|l| l.starts_with("event log-uploaded "));
    let batched = uploads.filter(|l| l.contains(',')).count();
    assert!(batched > 0, "some log PUT held several commits");
    fs::remove_dir_all(&dir).unwrap();
    let head = ctx.head();
    let seqs = ctx.assert_log_contiguous(head.generation);
    let v = restore_verify(&ctx, &soak.lines);
    eprintln!(
        "batching_mixed_connections_against_rustfs: {flips} flips, {batched} multi-commit log \
         PUTs, acked={}: log/{}/ seqs 1..={seqs} {}",
        v.acked, head.generation, v.restored
    );
}

/// `bench --commits 300` with `extra` settings: (commit p50 in ms, wall time).
fn bench_run(name: &str, extra: &[(&str, &str)]) -> (f64, Duration) {
    let mut ctx = Ctx::new(name);
    for (k, v) in extra {
        ctx = ctx.set(k, v);
    }
    let dir = ctx.dir("db");
    let started = Instant::now();
    let out = ctx.ok(&["bench", "--dir", dir.to_str().unwrap(), "--commits", "300"]);
    let wall = started.elapsed();
    let p50 = key_values(&out.stdout, "")["commit_p50_ms"];
    eprintln!(
        "{name}: commit_p50_ms={p50} wall={wall:?} stderr: {}",
        out.stderr.trim()
    );
    (p50, wall)
}

/// Telemetry on, its collector (a) refusing connections, (b) accepting them and never
/// answering: bench still exits 0, commits are not slowed, and exit waits at most the 2 s
/// flush budget.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn telemetry_collector_down_and_blackhole() {
    let (base_p50, base_wall) = bench_run("telemetry_off", &[("BUCKWAL_OTEL", "0")]);

    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = closed.local_addr().unwrap().port();
    drop(closed);
    let down = format!("http://127.0.0.1:{port}");

    let blackhole = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let hole = format!("http://{}", blackhole.local_addr().unwrap());
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let accepted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let holder = {
        let (stop, accepted) = (stop.clone(), accepted.clone());
        std::thread::spawn(move || {
            // Hold every connection open, never reading or writing.
            let mut held = Vec::new();
            for conn in blackhole.incoming() {
                if stop.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                if let Ok(c) = conn {
                    accepted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    held.push(c);
                }
            }
        })
    };

    for (name, endpoint) in [
        ("telemetry_collector_down", &down),
        ("telemetry_collector_blackhole", &hole),
    ] {
        let (p50, wall) = bench_run(
            name,
            &[("BUCKWAL_OTEL", "1"), ("BUCKWAL_OTEL_ENDPOINT", endpoint)],
        );
        assert!(
            p50 <= 2.0 * base_p50 + 1.0,
            "{name}: commit_p50_ms {p50} vs {base_p50} with telemetry off"
        );
        assert!(
            wall <= base_wall + Duration::from_secs(3),
            "{name}: wall {wall:?} vs {base_wall:?} with telemetry off"
        );
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = std::net::TcpStream::connect(hole.trim_start_matches("http://"));
    holder.join().unwrap();
    let accepted = accepted.load(std::sync::atomic::Ordering::SeqCst);
    assert!(accepted > 0, "the exporter connected to the blackhole");
    eprintln!(
        "telemetry_collector_down_and_blackhole: off p50={base_p50} wall={base_wall:?}; \
         blackhole accepted {accepted} connections"
    );
}

/// Every key under `<prefix>/`.
fn keys_under(ctx: &Ctx, prefix: &str) -> Vec<String> {
    ctx.store().list(&format!("{prefix}/")).expect("list")
}

/// Put 20 keys under the context's prefix.
fn put_keys(ctx: &Ctx) {
    let s = ctx.store();
    for i in 0..20 {
        s.put(&format!("{}/k/{i:03}", ctx.prefix()), b"x").unwrap();
    }
}

/// The prefix of a context made, filled and dropped by a test thread that panics, and
/// whether that thread panicked.
fn failed_test_prefix(name: &'static str, keep_failed: bool) -> (String, bool) {
    let (tx, rx) = channel();
    let joined = std::thread::spawn(move || {
        let mut ctx = Ctx::new(name);
        ctx.keep_failed = keep_failed;
        ctx.keep_all = false;
        put_keys(&ctx);
        tx.send(ctx.prefix()).unwrap();
        panic!("{name}: a deliberate failure (the cleanup test)");
    })
    .join();
    (rx.recv().unwrap(), joined.is_err())
}

/// A test's `Ctx` deletes every key under its prefix when it is dropped: after a pass, and
/// after a failure unless `BUCKWAL_KEEP_FAILED=1` (`keep_failed`), which keeps the prefix.
/// `keep_all` (`BUCKWAL_KEEP_PREFIXES=1`) is off here, whatever the caller set.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn ctx_deletes_its_prefix_unless_a_failure_is_kept() {
    let probe = Ctx::new("cleanup_probe");
    let passed = {
        let mut ctx = Ctx::new("cleanup_passed");
        ctx.keep_all = false;
        put_keys(&ctx);
        assert_eq!(keys_under(&probe, &ctx.prefix()).len(), 20);
        ctx.prefix()
    };
    assert_eq!(keys_under(&probe, &passed), Vec::<String>::new(), "passed");

    let (failed, panicked) = failed_test_prefix("cleanup_failed", false);
    assert!(panicked);
    assert_eq!(keys_under(&probe, &failed), Vec::<String>::new(), "failed");

    let (kept, panicked) = failed_test_prefix("cleanup_failed_kept", true);
    assert!(panicked);
    let left = keys_under(&probe, &kept);
    assert_eq!(
        left.len(),
        20,
        "failed with BUCKWAL_KEEP_FAILED=1: {left:?}"
    );
    delete_prefix(&probe.store(), &kept).unwrap();
    eprintln!("ctx cleanup: deleted {passed}/ and {failed}/, kept {kept}/ (then deleted)");
}

// ---- MVCC (`--journal-mode mvcc`, soak `--concurrent`) ----
//
// Every test here runs in both passes of scripts/test.sh: pass 2 serves RustFS over HTTPS and
// encrypts pages, the log and the head's contents with a key, so these tests also cover the
// encrypted `.db-log`. Where a kill must land on one exact op boundary (between a seal and the
// log truncate, between the log truncate and the WAL truncate, mid log PUT) the CLI cannot
// reach it: those boundaries are the library's fault-schedule tests (crates/buckwal/
// tests/mvcc_fencing.rs); the kills here land on the process-level boundaries only.

/// Soak arguments for an MVCC soak writing inside `BEGIN CONCURRENT`, then `extra`.
fn mvcc_soak<'a>(extra: &[&'a str]) -> Vec<&'a str> {
    let mut args = vec!["--journal-mode", "mvcc", "--concurrent"];
    args.extend_from_slice(extra);
    args
}

/// The bucket's head says MVCC (bucket format 2).
fn assert_mvcc_head(ctx: &Ctx) {
    let head = ctx
        .store()
        .get(&format!("{}/head", ctx.prefix()))
        .expect("get head")
        .expect("head exists");
    let json = String::from_utf8(head.body).unwrap();
    assert!(
        json.contains("\"journal_mode\":\"mvcc\""),
        "{}: head is not MVCC: {json}",
        ctx.name
    );
}

/// `args` run to the end and must fail with exit code 1 and `message` on stderr.
fn assert_refused(ctx: &Ctx, args: &[&str], message: &str) {
    let out = ctx.run(args);
    assert_eq!(
        out.status.code(),
        Some(1),
        "{}: {args:?}\nstdout:\n{}\nstderr:\n{}",
        ctx.name,
        out.stdout,
        out.stderr
    );
    assert!(
        out.stderr.contains(message),
        "{}: {args:?}: want {message:?} in\n{}",
        ctx.name,
        out.stderr
    );
}

/// Killed after `kill_after` acks of an MVCC soak that does no checkpoint of its own (the
/// engine's auto-checkpoint, the log and the maintenance checkpoint are all there is): the
/// local disk is deleted, restored, and every ack is there.
fn mvcc_crash(name: &str, connections: &str, kills: &[usize]) {
    for &kill_after in kills {
        let ctx = Ctx::new(&format!("{name}_{kill_after}"));
        let dir = ctx.dir("db");
        let mut soak = Soak::spawn(&ctx, &dir, &mvcc_soak(&["--connections", connections]));
        soak.wait_acks(kill_after);
        soak.kill();
        assert!(soak.errors().is_empty(), "{name}: {:?}", soak.errors());
        assert_mvcc_head(&ctx);
        fs::remove_dir_all(&dir).unwrap();
        let v = restore_verify(&ctx, &soak.lines);
        eprintln!(
            "{name}: kill after {kill_after} acks: acked={} {} unacked_present={}",
            v.acked, v.restored, v.unacked_present
        );
    }
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_crash_commit() {
    mvcc_crash("mvcc_crash_commit", "1", &[150, 600]);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_crash_group_commit() {
    mvcc_crash("mvcc_crash_group_commit", "8", &[200, 800]);
}

/// The soak's own checkpoint (`truncate`, then `reopen`) every 100 commits, killed after 350
/// acks: at least two seals finished, the restore keeps every ack.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_crash_checkpoint() {
    for mode in ["truncate", "reopen"] {
        let ctx = Ctx::new(&format!("mvcc_crash_checkpoint_{mode}"));
        let dir = ctx.dir("db");
        let args = mvcc_soak(&["--checkpoint", mode, "--every", "100"]);
        let mut soak = Soak::spawn(&ctx, &dir, &args);
        soak.wait_acks(350);
        soak.kill();
        assert!(soak.errors().is_empty(), "{mode}: {:?}", soak.errors());
        let seals = soak.count("event seal-finished");
        let checkpoints = soak.count(&format!("event checkpoint {mode}"));
        assert!(
            checkpoints >= 3,
            "{mode}: {checkpoints} checkpoints\n{}",
            soak.tail()
        );
        // A reopen closes and opens the database; in MVCC that keeps the log and seals nothing.
        if mode == "truncate" {
            assert!(seals >= 2, "{mode}: {seals} seals\n{}", soak.tail());
        }
        assert_mvcc_head(&ctx);
        fs::remove_dir_all(&dir).unwrap();
        let v = restore_verify(&ctx, &soak.lines);
        eprintln!(
            "mvcc_crash_checkpoint: {mode}: acked={} seals={seals} {} unacked_present={}",
            v.acked, v.restored, v.unacked_present
        );
    }
}

/// Killed the moment a seal finishes (the 1st and the 3rd), when the log truncate and the WAL
/// truncate that follow it are next: wherever the kill lands, every ack restores.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_crash_right_after_seal() {
    for nth in [1, 3] {
        let ctx = Ctx::new(&format!("mvcc_crash_right_after_seal_{nth}"));
        let dir = ctx.dir("db");
        let args = mvcc_soak(&["--checkpoint", "truncate", "--every", "100"]);
        let mut soak = Soak::spawn(&ctx, &dir, &args);
        soak.wait_for("a finished seal", |s| s.count("event seal-finished") >= nth);
        soak.kill();
        assert!(soak.errors().is_empty(), "{:?}", soak.errors());
        fs::remove_dir_all(&dir).unwrap();
        let v = restore_verify(&ctx, &soak.lines);
        eprintln!(
            "mvcc_crash_right_after_seal: seal #{nth}: acked={} {}",
            v.acked, v.restored
        );
    }
}

/// Killed while a seal's page PUTs are in flight (each delayed 200 ms), on the first seal and
/// the third: the generation is not sealed, the log still holds the commits, every ack restores.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_crash_during_seal_upload() {
    for nth in [1, 3] {
        let ctx = Ctx::new(&format!("mvcc_crash_during_seal_upload_{nth}"));
        let dir = ctx.dir("db");
        let args = mvcc_soak(&[
            "--checkpoint",
            "truncate",
            "--every",
            "100",
            "--page-put-delay-ms",
            "200",
        ]);
        let mut soak = Soak::spawn(&ctx, &dir, &args);
        soak.wait_for("seal-started", |s| s.count("event seal-started") >= nth);
        soak.kill();
        assert_eq!(
            soak.count("event seal-finished"),
            nth - 1,
            "{}",
            soak.tail()
        );
        assert!(soak.errors().is_empty(), "{:?}", soak.errors());
        fs::remove_dir_all(&dir).unwrap();
        let v = restore_verify(&ctx, &soak.lines);
        eprintln!(
            "mvcc_crash_during_seal_upload: killed on seal-started #{nth}: acked={} {}",
            v.acked, v.restored
        );
    }
}

/// `connections` writer threads, `count` rows in all, in the given `--concurrent` form.
fn mvcc_threads(name: &str, concurrent: &[&str], count: &str) {
    let ctx = Ctx::new(name);
    let dir = ctx.dir("db");
    let mut args = vec!["--journal-mode", "mvcc"];
    args.extend_from_slice(concurrent);
    args.extend(["--connections", "8", "--count", count]);
    let mut soak = Soak::spawn(&ctx, &dir, &args);
    soak.finish();
    // A conflict between concurrent writers is retried (`error …` line, then the same row
    // again), never lost: what counts is that every row was acked exactly once.
    let want: usize = count.parse().unwrap();
    assert_eq!(soak.acks(), want, "{name}\n{}", soak.tail());
    assert_mvcc_head(&ctx);
    fs::remove_dir_all(&dir).unwrap();
    let v = restore_verify(&ctx, &soak.lines);
    eprintln!(
        "{name}: acked={} soak error lines={} {}",
        v.acked,
        soak.errors().len(),
        v.restored
    );
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_8_threads_begin_concurrent() {
    mvcc_threads("mvcc_8_threads_begin_concurrent", &["--concurrent"], "800");
}

/// Even-numbered threads write in plain `BEGIN`, odd-numbered ones in `BEGIN CONCURRENT`.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_8_threads_mixed_begin_and_concurrent() {
    mvcc_threads(
        "mvcc_8_threads_mixed_begin_and_concurrent",
        &["--concurrent", "mixed"],
        "800",
    );
}

/// A restore with no WAL frames: the soak never checkpoints and is killed long before the
/// engine's auto-checkpoint, so the bucket holds the creation generation and log objects only.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn verify_acked_on_plain_mvcc_restore() {
    let ctx = Ctx::new("verify_acked_on_plain_mvcc_restore");
    let dir = ctx.dir("db");
    let mut soak = Soak::spawn(&ctx, &dir, &mvcc_soak(&[]));
    soak.wait_acks(100);
    soak.kill();
    assert_eq!(soak.count("event seal-finished"), 0, "{}", soak.tail());
    let head = ctx.head();
    fs::remove_dir_all(&dir).unwrap();
    let v = restore_verify(&ctx, &soak.lines);
    // The restore is of the generation the database was created in.
    assert_eq!(field(&v.restored, "generation") as u64, head.generation);
    assert!(field(&v.restored, "log_objects") >= 1, "{}", v.restored);
    // A second restore of the same bucket gives the same answer.
    restore_verify(&ctx, &soak.lines);
    eprintln!(
        "verify_acked_on_plain_mvcc_restore: acked={} {}",
        v.acked, v.restored
    );
}

/// A restore of a bucket whose writer died during a seal (the page PUTs delayed): the head
/// still names the previous generation, and `verify --acked` finds every ack.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn verify_acked_on_mid_checkpoint_restore() {
    let ctx = Ctx::new("verify_acked_on_mid_checkpoint_restore");
    let dir = ctx.dir("db");
    let args = mvcc_soak(&[
        "--checkpoint",
        "truncate",
        "--every",
        "100",
        "--page-put-delay-ms",
        "300",
    ]);
    let mut soak = Soak::spawn(&ctx, &dir, &args);
    soak.wait_for("the 2nd seal-started", |s| {
        s.count("event seal-started") >= 2
    });
    let head_before = ctx.head();
    soak.kill();
    assert_eq!(soak.count("event seal-finished"), 1, "{}", soak.tail());
    // The killed seal published nothing.
    assert_eq!(ctx.head().generation, head_before.generation);
    fs::remove_dir_all(&dir).unwrap();
    let v = restore_verify(&ctx, &soak.lines);
    assert!(v.acked >= 200, "acked={}", v.acked);
    eprintln!(
        "verify_acked_on_mid_checkpoint_restore: head generation {}; acked={} {}",
        head_before.generation, v.acked, v.restored
    );
}

/// The CLI refusals of an MVCC database: a mode switch in either direction, Window batching,
/// and `PRAGMA journal_mode=wal`.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_refusals_cli() {
    // A WAL bucket asked for as MVCC: refused from the same directory and from a fresh one.
    let ctx = Ctx::new("mvcc_refusals_cli_wal");
    let a = ctx.dir("a");
    let (a, b) = (a.to_str().unwrap(), ctx.dir("b"));
    let b = b.to_str().unwrap();
    ctx.ok(&[
        "sql",
        "--claim",
        "--dir",
        a,
        "CREATE TABLE t(x)",
        "INSERT INTO t VALUES (1)",
    ]);
    let want = "MVCC: switching an existing WAL database";
    assert_refused(
        &ctx,
        &["sql", "--dir", a, "--journal-mode", "mvcc", "SELECT 1"],
        want,
    );
    assert_refused(
        &ctx,
        &[
            "sql",
            "--claim",
            "--dir",
            b,
            "--journal-mode",
            "mvcc",
            "SELECT 1",
        ],
        want,
    );
    // The refusals left the WAL database as it was.
    let n = ctx.ok(&["sql", "--dir", a, "SELECT count(*) FROM t"]);
    assert_eq!(n.stdout.trim(), "1");

    // An MVCC bucket asked for as WAL, by the flag, the environment or nothing.
    let ctx = Ctx::new("mvcc_refusals_cli_mvcc");
    let a = ctx.dir("a");
    let (a, b) = (a.to_str().unwrap(), ctx.dir("b"));
    let b = b.to_str().unwrap();
    let mvcc = ["--journal-mode", "mvcc"];
    let sql_mvcc = |stmts: &[&str]| {
        let mut args = vec!["sql", "--claim", "--dir", a];
        args.extend(mvcc);
        args.extend(stmts);
        ctx.ok(&args)
    };
    sql_mvcc(&["CREATE TABLE t(x)", "INSERT INTO t VALUES (1)"]);
    assert_mvcc_head(&ctx);
    let back = "MVCC: switching back to WAL";
    assert_refused(
        &ctx,
        &["sql", "--dir", a, "--journal-mode", "wal", "SELECT 1"],
        back,
    );
    // Neither flag nor environment: the existing head's mode is used.
    let n = ctx.ok(&["sql", "--dir", a, "SELECT count(*) FROM t"]);
    assert_eq!(n.stdout.trim(), "1");
    assert_refused(
        &ctx,
        &[
            "sql",
            "--claim",
            "--dir",
            b,
            "--journal-mode",
            "wal",
            "SELECT 1",
        ],
        back,
    );
    // BUCKWAL_JOURNAL_MODE applies without the flag; the flag wins over it.
    let env_mvcc = ctx.ok_env(
        &[("BUCKWAL_JOURNAL_MODE", "mvcc")],
        &["sql", "--dir", a, "SELECT count(*) FROM t"],
    );
    assert_eq!(env_mvcc.stdout.trim(), "1");
    let flag_wins = ctx.run_env(
        &[("BUCKWAL_JOURNAL_MODE", "mvcc")],
        &["sql", "--dir", a, "--journal-mode", "wal", "SELECT 1"],
    );
    assert_eq!(flag_wins.status.code(), Some(1));
    assert!(flag_wins.stderr.contains(back), "{}", flag_wins.stderr);
    // Window batching, from the flag and from the environment.
    let window = "MVCC: Window batching";
    assert_refused(
        &ctx,
        &[
            "sql",
            "--dir",
            a,
            "--journal-mode",
            "mvcc",
            "--batching",
            WINDOW,
            "SELECT 1",
        ],
        window,
    );
    let out = ctx.run_env(
        &[
            ("BUCKWAL_JOURNAL_MODE", "mvcc"),
            ("BUCKWAL_BATCHING", WINDOW),
        ],
        &["sql", "--dir", a, "SELECT 1"],
    );
    assert_eq!(out.status.code(), Some(1), "{}\n{}", out.stdout, out.stderr);
    assert!(out.stderr.contains(window), "{}", out.stderr);
    // PRAGMA journal_mode=wal through an MVCC session.
    assert_refused(
        &ctx,
        &[
            "sql",
            "--dir",
            a,
            "--journal-mode",
            "mvcc",
            "PRAGMA journal_mode=wal",
        ],
        back,
    );
    // None of the refusals touched the data, and `batching off` is accepted.
    let n = ctx.ok(&[
        "sql",
        "--dir",
        a,
        "--journal-mode",
        "mvcc",
        "--batching",
        "off",
        "SELECT count(*) FROM t",
    ]);
    assert_eq!(n.stdout.trim(), "1");
    // A fresh directory claiming the bucket with no flag follows the head (it fences `a`,
    // so it comes last among the writers).
    let b2 = ctx.dir("b2");
    let n = ctx.ok(&[
        "sql",
        "--claim",
        "--dir",
        b2.to_str().unwrap(),
        "SELECT count(*) FROM t",
    ]);
    assert_eq!(n.stdout.trim(), "1");
    // verify and restore need no flag: the head decides.
    let r = ctx.dir("r");
    ctx.ok(&["restore", "--dir", r.to_str().unwrap()]);
    let v = ctx.ok(&["verify", "--dir", r.to_str().unwrap()]);
    assert!(v.stdout.starts_with("verify ok "), "{}", v.stdout);
}

/// `takeover --journal-mode mvcc` of a bucket whose writer is stopped (SIGSTOP; a writer
/// committing at full speed can outrun the fence record's 64 probes, which the takeover then
/// reports as a timeout to resume): the new directory continues with `sql`, and the old
/// soak, resumed, is fenced and acks nothing. Every row of both writers restores. Without
/// `--journal-mode` the head's mode is used; an explicit `wal` is refused.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_takeover_cli() {
    let ctx = Ctx::new("mvcc_takeover_cli");
    let a = ctx.dir("a");
    let mut soak = Soak::spawn(&ctx, &a, &mvcc_soak(&[]));
    soak.wait_acks(200);
    soak.signal("STOP");
    let before = ctx.head();
    let b = ctx.dir("b");
    let bs = b.to_str().unwrap();
    // An explicit wal is refused; no flag follows the head.
    assert_refused(
        &ctx,
        &["takeover", "--dir", bs, "--journal-mode", "wal"],
        "MVCC: switching back to WAL",
    );
    let took = ctx.ok(&["takeover", "--dir", bs]);
    let took = took.stdout.trim().to_string();
    assert!(took.starts_with("took over "), "{took}");
    assert_eq!(field(&took, "epoch") as u64, before.epoch + 1, "{took}");
    assert_eq!(ctx.head().epoch, before.epoch + 1);
    // sql continues in the new directory with every row the old writer acked.
    let rows = insert_rows(&ctx, &b, 100001..=100010, &[]);
    let n = ctx.ok(&["sql", "--dir", bs, "SELECT count(*) FROM soak"]);
    let count: usize = n.stdout.trim().parse().unwrap();
    assert!(
        count >= soak.acks() + 10,
        "{count} rows, {} acks",
        soak.acks()
    );
    let cont = Instant::now();
    soak.signal("CONT");
    let fenced = soak.wait_fenced_then_no_ack(Duration::from_secs(2));
    soak.kill();
    // One commit may have returned before the STOP and printed its ack after the CONT; a
    // second ack would be an append after the fence.
    let late = soak.acks_around(cont, false);
    assert!(late <= 1, "{late} acks after the CONT\n{}", soak.tail());
    let mut lines = soak.lines.clone();
    lines.extend(rows);
    let v = restore_verify(&ctx, &lines);
    assert_mvcc_head(&ctx);
    eprintln!(
        "mvcc_takeover_cli: {took}; first fenced error {:?}; soak acks={} ({late} after the \
         CONT); acked={} {}",
        soak.lines[fenced],
        soak.acks(),
        v.acked,
        v.restored
    );
}

/// The MVCC twin of `takeover_fences_a_running_soak`: `takeover` of a live MVCC writer
/// committing flat out (4 connections, no SIGSTOP). It either succeeds or times out after its
/// 64 fence probes (the head's epoch is raised by then; `sql --claim` resumes). Either way the
/// old writer is fenced and acks nothing more, the epoch rose, and every ack it printed
/// restores.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_takeover_fences_a_running_soak() {
    let ctx = Ctx::new("mvcc_takeover_fences_a_running_soak");
    let a = ctx.dir("a");
    let mut soak = Soak::spawn(&ctx, &a, &mvcc_soak(&["--connections", "4"]));
    soak.wait_acks(200);
    let before = ctx.head();
    let b = ctx.dir("b");
    let bs = b.to_str().unwrap();
    let out = ctx.run(&["takeover", "--dir", bs]);
    let outcome = if out.status.success() {
        "took over"
    } else {
        assert!(
            out.stderr.contains("kept appending past 64 log positions"),
            "takeover failed otherwise:\n{}\n{}",
            out.stdout,
            out.stderr
        );
        "timed out (fence probes)"
    };
    assert_eq!(ctx.head().epoch, before.epoch + 1, "epoch after {outcome}");
    let fenced = soak.wait_fenced_then_no_ack(Duration::from_secs(2));
    soak.kill();
    // A takeover that timed out resumes by opening the directory: it must then continue.
    let rows = if out.status.success() {
        insert_rows(&ctx, &b, 100001..=100010, &[])
    } else {
        insert_rows(&ctx, &b, 100001..=100010, &["--claim"])
    };
    let mut lines = soak.lines.clone();
    lines.extend(rows);
    let v = restore_verify(&ctx, &lines);
    assert_mvcc_head(&ctx);
    eprintln!(
        "mvcc_takeover_fences_a_running_soak: {outcome}; soak acks={}; first fenced error {:?}; \
         acked={} {}",
        soak.acks(),
        soak.lines[fenced],
        v.acked,
        v.restored
    );
}

/// GC after several MVCC checkpoints (each ends a generation): it trims every log below the
/// head's manifest, and the restore afterwards still has every row.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_gc_across_generations() {
    let ctx = short_lease_ctx("mvcc_gc_across_generations");
    let dir = ctx.dir("db");
    let args = mvcc_soak(&[
        "--checkpoint",
        "truncate",
        "--every",
        "50",
        "--count",
        "400",
    ]);
    let mut soak = Soak::spawn(&ctx, &dir, &args);
    soak.finish();
    assert!(soak.errors().is_empty(), "{:?}", soak.errors());
    let head = ctx.head();
    assert!(head.generation >= 8, "checkpoints sealed: {head:?}");
    let before = log_generations(&ctx);
    assert!(
        before.len() > 1,
        "log/ holds several generations: {before:?}"
    );
    let gc = gc_until_trimmed_against_head(&ctx, &head, Duration::from_secs(40));
    let logs = ctx.list("log/");
    let root = format!("{}/log/{:010}/", ctx.prefix(), head.generation);
    assert!(
        logs.iter().all(|k| k.starts_with(&root)),
        "only generation {} logs remain: {logs:?}",
        head.generation
    );
    let v = restore_verify(&ctx, &soak.lines);
    assert_eq!(v.acked, 400);
    eprintln!(
        "mvcc_gc_across_generations: head {head:?}; log generations before {before:?}; {gc}; \
         log/ holds {} keys after; {}",
        logs.len(),
        v.restored
    );
}

/// The MVCC bucket in the harness's mode: with an encryption key (pass 2 of test.sh) no
/// object of the prefix holds the marker row's text, log objects and sealed pages included;
/// without one (pass 1) the same scan finds it, so the check is not vacuous.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_encrypted() {
    let ctx = Ctx::new("mvcc_encrypted");
    let encrypted = ctx
        .opt_var("BUCKWAL_ENCRYPTION_KEY_FILE")
        .is_some_and(|k| !k.is_empty());
    let dir = ctx.dir("db");
    let d = dir.to_str().unwrap();
    let marker = format!("mvcc-marker-{}", nanos());
    let insert = format!("INSERT INTO m VALUES ('{marker}')");
    // Rows in the log, then a checkpoint (sealed pages and the emptied log), then more rows.
    ctx.ok(&[
        "sql",
        "--claim",
        "--dir",
        d,
        "--journal-mode",
        "mvcc",
        "CREATE TABLE m(t TEXT)",
    ]);
    ctx.ok(&["sql", "--dir", d, "--journal-mode", "mvcc", &insert]);
    ctx.ok(&[
        "sql",
        "--dir",
        d,
        "--journal-mode",
        "mvcc",
        "PRAGMA wal_checkpoint(TRUNCATE)",
        &insert,
    ]);
    let scan = if encrypted { "absent" } else { "present" };
    let out = ctx.run(&["scan", "--expect", scan, "--marker", &marker]);
    assert!(
        out.status.success(),
        "scan --expect {scan}\n{}\n{}",
        out.stdout,
        out.stderr
    );
    // The data restores and reads back in both modes.
    fs::remove_dir_all(&dir).unwrap();
    let r = ctx.dir("r");
    ctx.ok(&["restore", "--dir", r.to_str().unwrap()]);
    let n = ctx.ok(&["verify", "--dir", r.to_str().unwrap()]);
    assert!(n.stdout.starts_with("verify ok "), "{}", n.stdout);
    eprintln!(
        "mvcc_encrypted: encrypted={encrypted}: scan --expect {scan}: {}",
        out.stdout.trim()
    );
}
