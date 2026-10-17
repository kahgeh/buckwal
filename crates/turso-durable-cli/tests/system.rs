//! System tests: the `turso-durable` binary against a real RustFS. Every acked commit must
//! survive SIGKILL plus the loss of the local disk, under every checkpoint mode and under
//! faults. Run through `./scripts/test.sh` (`--test-threads=1`: some tests stop or pause the
//! shared container).
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use turso_durable::store::s3::{S3Config, S3Store};
use turso_durable::store::ObjectStore;
use turso_durable::{Config, Value};

const BIN: &str = env!("CARGO_BIN_EXE_turso-durable");

/// How long any wait for a soak line may take: generous, so a hung soak fails the test
/// instead of hanging the suite.
const WAIT: Duration = Duration::from_secs(120);

/// The Window setting the batched system tests run under.
const WINDOW: &str = "window 20ms 64 1MiB";

/// Settings a test may set; every child gets them removed first, so the caller's
/// environment never leaks into a test. `DURABLE_OTEL*` and `DURABLE_TEST_RUN_ID` are not
/// here: `scripts/test.sh` sets them so the children export under its run id.
const OPTIONAL: [&str; 10] = [
    "DURABLE_PREFIX",
    "DURABLE_RETRY_DEADLINE_MS",
    "DURABLE_REQUEST_TIMEOUT_MS",
    "DURABLE_UPLOAD_CONCURRENCY",
    "DURABLE_TRIM_GRACE_SECS",
    "DURABLE_PREFETCH",
    "DURABLE_BATCHING",
    "DURABLE_MAINTENANCE_LOG_OBJECTS",
    "DURABLE_MAINTENANCE_WAL_MIB",
    "DURABLE_MAINTENANCE_GATE_WAIT_MS",
];

/// The `DURABLE_*` connection settings, with the defaults of `scripts/rustfs.sh env`.
fn env() -> Vec<(String, String)> {
    [
        ("DURABLE_ENDPOINT", "http://127.0.0.1:9290"),
        ("DURABLE_REGION", "us-east-1"),
        ("DURABLE_PATH_STYLE", "true"),
        ("DURABLE_BUCKET", "turso-durable"),
        ("DURABLE_ACCESS_KEY", "turso-durable-admin"),
        ("DURABLE_SECRET_KEY", "turso-durable-local-secret-not-real"),
    ]
    .iter()
    .map(|(k, d)| {
        (
            k.to_string(),
            std::env::var(k).unwrap_or_else(|_| d.to_string()),
        )
    })
    .collect()
}

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// A bucket prefix no other test or run uses: `sys/<name>-<nanos>`.
fn prefix(name: &str) -> String {
    format!("sys/{name}-{}", nanos())
}

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

/// The parts of the bucket's `head` the tests look at.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Head {
    generation: u64,
    manifest: Option<String>,
    writer_id: String,
}

/// The JSON string value after `"field":`, or None for `null`.
fn json_str(json: &str, field: &str) -> Option<String> {
    let at = json.find(&format!("\"{field}\":"))? + field.len() + 3;
    let rest = json[at..].trim_start().strip_prefix('"')?;
    Some(rest[..rest.find('"')?].to_string())
}

fn json_u64(json: &str, field: &str) -> u64 {
    let at = json
        .find(&format!("\"{field}\":"))
        .unwrap_or_else(|| panic!("no {field} in {json}"))
        + field.len()
        + 3;
    let digits: String = json[at..]
        .trim_start()
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits.parse().unwrap()
}

/// One test's bucket prefix, extra settings and temp directory.
struct Ctx {
    name: String,
    vars: Vec<(String, String)>,
    tmp: TempDir,
}

/// A finished command.
struct Run {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

impl Ctx {
    fn new(name: &str) -> Ctx {
        let mut vars = env();
        vars.push(("DURABLE_PREFIX".into(), prefix(name)));
        Ctx {
            name: name.to_string(),
            vars,
            tmp: tempfile::tempdir().unwrap(),
        }
    }

    fn set(mut self, k: &str, v: &str) -> Ctx {
        self.vars.push((k.to_string(), v.to_string()));
        self
    }

    fn var(&self, k: &str) -> String {
        let v = self.vars.iter().rev().find(|(n, _)| n == k);
        v.unwrap_or_else(|| panic!("{k} not set")).1.clone()
    }

    fn prefix(&self) -> String {
        self.var("DURABLE_PREFIX")
    }

    /// A path in this test's temp directory that does not exist yet.
    fn dir(&self, name: &str) -> PathBuf {
        let d = self.tmp.path().join(name);
        assert!(!d.exists(), "{} already exists", d.display());
        d
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut c = Command::new(BIN);
        c.args(args);
        for k in OPTIONAL {
            c.env_remove(k);
        }
        c.envs(self.vars.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        c
    }

    fn run(&self, args: &[&str]) -> Run {
        let o = self
            .command(args)
            .output()
            .unwrap_or_else(|e| panic!("run {args:?}: {e}"));
        Run {
            status: o.status,
            stdout: String::from_utf8_lossy(&o.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
        }
    }

    /// Run `args` and require success.
    fn ok(&self, args: &[&str]) -> Run {
        let r = self.run(args);
        assert!(
            r.status.success(),
            "{}: {args:?} failed ({})\nstdout:\n{}\nstderr:\n{}",
            self.name,
            r.status,
            r.stdout,
            r.stderr
        );
        r
    }

    fn store(&self) -> S3Store {
        S3Store::new(S3Config {
            endpoint: self.var("DURABLE_ENDPOINT"),
            region: self.var("DURABLE_REGION"),
            bucket: self.var("DURABLE_BUCKET"),
            access_key: self.var("DURABLE_ACCESS_KEY"),
            secret_key: self.var("DURABLE_SECRET_KEY"),
            path_style: self.var("DURABLE_PATH_STYLE") == "true",
            request_timeout: Duration::from_secs(30),
        })
        .expect("S3 store")
    }

    /// Every key under `<prefix>/<sub>`.
    fn list(&self, sub: &str) -> Vec<String> {
        self.store()
            .list(&format!("{}/{sub}", self.prefix()))
            .expect("list")
    }

    fn head(&self) -> Head {
        let o = self
            .store()
            .get(&format!("{}/head", self.prefix()))
            .expect("get head")
            .expect("head exists");
        let json = String::from_utf8(o.body).unwrap();
        Head {
            generation: json_u64(&json, "generation"),
            manifest: json_str(&json, "manifest"),
            writer_id: json_str(&json, "writer_id").expect("writer_id"),
        }
    }

    /// The seqs of `log/<generation>/`, which must be exactly `1..=n` (no gap, no fork).
    fn assert_log_contiguous(&self, generation: u64) -> u64 {
        let root = format!("{}/log/{generation:010}/", self.prefix());
        let seqs: Vec<u64> = self
            .list(&format!("log/{generation:010}/"))
            .iter()
            .map(|k| k.strip_prefix(&root).unwrap().parse().unwrap())
            .collect();
        let want: Vec<u64> = (1..=seqs.len() as u64).collect();
        assert_eq!(seqs, want, "{}: log/{generation:010}/ seqs", self.name);
        seqs.len() as u64
    }
}

/// A running `turso-durable soak`, its stdout lines read on a thread into a channel.
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

/// What `restore` and `verify --acked` reported.
#[derive(Debug)]
struct Verified {
    /// The `restored …` line.
    restored: String,
    acked: usize,
    present: usize,
    unacked_present: usize,
}

fn field(line: &str, key: &str) -> usize {
    let v = line
        .split_whitespace()
        .find_map(|kv| kv.strip_prefix(&format!("{key}=")));
    v.unwrap_or_else(|| panic!("no {key}= in {line:?}"))
        .parse()
        .unwrap()
}

/// Restore the test's prefix into a new directory, then `verify --acked` with `lines` (soak
/// stdout: `ack <id>` lines and others, which verify skips). Requires integrity ok and every
/// acked id present.
fn restore_verify(ctx: &Ctx, lines: &[String]) -> Verified {
    let dir = ctx.dir(&format!("restored-{}", nanos()));
    let d = dir.to_str().unwrap();
    let restored = ctx.ok(&["restore", "--dir", d]).stdout.trim().to_string();
    let acked_file = dir.with_extension("acked");
    fs::write(&acked_file, lines.join("\n")).unwrap();
    let r = ctx.ok(&[
        "verify",
        "--dir",
        d,
        "--acked",
        acked_file.to_str().unwrap(),
    ]);
    let line = r.stdout.trim();
    assert!(line.starts_with("verify ok "), "{}: {line}", ctx.name);
    let want = lines.iter().filter(|l| l.starts_with("ack ")).count();
    let v = Verified {
        restored,
        acked: field(line, "acked"),
        present: field(line, "present"),
        unacked_present: field(line, "unacked_present"),
    };
    assert_eq!(v.acked, want, "{}: every ack line counted once", ctx.name);
    assert_eq!(v.present, v.acked, "{}: every acked id present", ctx.name);
    v
}

/// The crash matrix for one mode: three runs, killed after `150`, `600` and `1500` acks.
/// `auto` is the engine's own checkpoint (no `--checkpoint`). With `batching`, soak runs four
/// connections under it.
fn crash_matrix(mode: &str, batching: Option<&str>) {
    let tag = if batching.is_some() { "window_" } else { "" };
    for kill_after in [150, 600, 1500] {
        let ctx = Ctx::new(&format!("crash_{tag}{mode}_{kill_after}"));
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

/// The maintenance threshold of the maintenance tests (`DURABLE_MAINTENANCE_LOG_OBJECTS`).
const MAINTENANCE_LOG_OBJECTS: usize = 200;

fn is_maintenance_end(l: &str) -> bool {
    l.starts_with("event maintenance-finished ") || l.starts_with("event maintenance-skipped ")
}

/// Sustained Window load (8 connections, `window 1ms`, at least 3000 acks). Without
/// maintenance the generation never ends (the "before" run reports its seals and restore
/// GETs, and asserts only that every ack restores). With it (threshold 200), maintenance
/// seals during the load, and a restore right after a maintenance seal reads at most
/// 200 + 4 objects. Both: every acked id restores, integrity ok, the log contiguous.
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
            Ctx::new(&name).set(
                "DURABLE_MAINTENANCE_LOG_OBJECTS",
                &MAINTENANCE_LOG_OBJECTS.to_string(),
            )
        } else {
            Ctx::new(&name)
                .set("DURABLE_MAINTENANCE_LOG_OBJECTS", "0")
                .set("DURABLE_MAINTENANCE_WAL_MIB", "0")
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
        "DURABLE_MAINTENANCE_LOG_OBJECTS",
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
    let ctx = Ctx::new(name).set("DURABLE_RETRY_DEADLINE_MS", "2000");
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
        .set("DURABLE_REQUEST_TIMEOUT_MS", "500")
        .set("DURABLE_RETRY_DEADLINE_MS", "5000");
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
        out.stderr.contains("MVCC is not supported yet"),
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

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn trimming_bounds_restore_gets() {
    let ctx = Ctx::new("trimming_bounds_restore_gets").set("DURABLE_TRIM_GRACE_SECS", "0");
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
        "trimming_bounds_restore_gets: {} seals; log/ holds {} keys of generation {}; {}",
        soak.count("event seal-finished"),
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
    let config = |dir: &Path| {
        let mut c = Config::new(
            dir,
            &ctx.var("DURABLE_ENDPOINT"),
            &ctx.var("DURABLE_BUCKET"),
            &ctx.var("DURABLE_ACCESS_KEY"),
            &ctx.var("DURABLE_SECRET_KEY"),
        );
        c.region = ctx.var("DURABLE_REGION");
        c.path_style = ctx.var("DURABLE_PATH_STYLE") == "true";
        c.prefix = ctx.prefix();
        c
    };
    let dir = ctx.dir("db");
    let db = turso_durable::open(config(&dir)).expect("open");
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
    turso_durable::restore(&config(&r), &r).expect("restore");
    let db = turso_durable::open_read_only(config(&r)).expect("open restored");
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
    let mut config = Config::new(
        ctx.dir("a"),
        &ctx.var("DURABLE_ENDPOINT"),
        &ctx.var("DURABLE_BUCKET"),
        &ctx.var("DURABLE_ACCESS_KEY"),
        &ctx.var("DURABLE_SECRET_KEY"),
    );
    config.region = ctx.var("DURABLE_REGION");
    config.path_style = ctx.var("DURABLE_PATH_STYLE") == "true";
    config.prefix = ctx.prefix();
    let db_a = turso_durable::open(config).expect("open A");
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
        (gen + 1, true),
        "B sealed generation {gen}: {sealed:?}"
    );
    let store = ctx.store();
    let sealed_objects = || {
        let mut keys = ctx.list(&format!("pages/{gen:010}/"));
        keys.push(format!("{}/manifest/{gen:010}", ctx.prefix()));
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
        matches!(closed, Ok(()) | Err(turso_durable::Error::Fenced(_))),
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
    let (base_p50, base_wall) = bench_run("telemetry_off", &[("DURABLE_OTEL", "0")]);

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
            &[("DURABLE_OTEL", "1"), ("DURABLE_OTEL_ENDPOINT", endpoint)],
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
