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

/// Settings a test may set; every child gets them removed first, so the caller's
/// environment never leaks into a test.
const OPTIONAL: [&str; 6] = [
    "DURABLE_PREFIX",
    "DURABLE_RETRY_DEADLINE_MS",
    "DURABLE_REQUEST_TIMEOUT_MS",
    "DURABLE_UPLOAD_CONCURRENCY",
    "DURABLE_TRIM_GRACE_SECS",
    "DURABLE_PREFETCH",
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
    fn drop(&mut self) {
        if self.armed {
            rustfs(self.undo);
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
    /// `WAIT`.
    fn wait_for(&mut self, what: &str, mut done: impl FnMut(&Soak) -> bool) {
        let deadline = Instant::now() + WAIT;
        while !done(self) {
            let left = deadline.saturating_duration_since(Instant::now());
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

    fn wait_acks(&mut self, n: usize) {
        self.wait_for(&format!("{n} acks"), |s| s.acks() >= n);
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
fn crash_matrix(mode: &str) {
    for kill_after in [150, 600, 1500] {
        let ctx = Ctx::new(&format!("crash_{mode}_{kill_after}"));
        let dir = ctx.dir("db");
        let mut soak = Soak::spawn(&ctx, &dir, &["--checkpoint", mode, "--every", "100"]);
        soak.wait_acks(kill_after);
        soak.kill();
        assert!(soak.errors().is_empty(), "{mode}: {:?}", soak.errors());
        let seals = soak.count("event seal-finished");
        assert!(
            seals >= 1,
            "{mode}: a seal before the kill\n{}",
            soak.tail()
        );
        fs::remove_dir_all(&dir).unwrap();
        let v = restore_verify(&ctx, &soak.lines);
        eprintln!(
            "crash_{mode}: kill after {kill_after} acks: acked={} seals={seals} {} \
             unacked_present={}",
            v.acked, v.restored, v.unacked_present
        );
    }
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_passive() {
    crash_matrix("passive");
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_full() {
    crash_matrix("full");
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_restart() {
    crash_matrix("restart");
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_truncate() {
    crash_matrix("truncate");
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn crash_reopen() {
    crash_matrix("reopen");
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

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn fault_rustfs_stopped_mid_commit() {
    let ctx = Ctx::new("fault_rustfs_stopped_mid_commit").set("DURABLE_RETRY_DEADLINE_MS", "2000");
    let dir = ctx.dir("db");
    let mut soak = Soak::spawn(&ctx, &dir, &[]);
    soak.wait_acks(200);
    let outage = Outage::start("stop", "start");
    let stopped_at = soak.acks();
    soak.wait_for("an error line", |s| !s.errors().is_empty());
    outage.end();
    let restarted_at = soak.acks();
    soak.wait_acks(restarted_at + 50);
    soak.kill();
    let errors = soak.errors().len();
    fs::remove_dir_all(&dir).unwrap();
    let head = ctx.head();
    let seqs = ctx.assert_log_contiguous(head.generation);
    let v = restore_verify(&ctx, &soak.lines);
    eprintln!(
        "fault_rustfs_stopped_mid_commit: stopped after {stopped_at} acks read, {} more read \
         before the first error (printed before the stop took hold), {errors} error lines, \
         killed at {}: log/{}/ seqs 1..={seqs} {}",
        restarted_at - stopped_at,
        v.acked,
        head.generation,
        v.restored
    );
}

/// A pause makes PUTs ambiguous: the client times out (500 ms) while RustFS holds the request
/// and may apply it once unpaused, so a retried log PUT can find its own record already there.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn fault_rustfs_paused_ambiguous_put() {
    let ctx = Ctx::new("fault_rustfs_paused_ambiguous_put")
        .set("DURABLE_REQUEST_TIMEOUT_MS", "500")
        .set("DURABLE_RETRY_DEADLINE_MS", "5000");
    let dir = ctx.dir("db");
    let mut soak = Soak::spawn(&ctx, &dir, &[]);
    soak.wait_acks(200);
    let outage = Outage::start("pause", "unpause");
    let paused_at = soak.acks();
    std::thread::sleep(Duration::from_millis(1500));
    outage.end();
    let unpaused_at = soak.acks();
    soak.wait_acks(soak.acks() + 100);
    soak.kill();
    // The pause stalled the writer: its requests in flight hung, timed out and were retried.
    let stall = soak.longest_ack_gap();
    assert!(stall >= Duration::from_millis(1000), "{stall:?}");
    fs::remove_dir_all(&dir).unwrap();
    let head = ctx.head();
    let seqs = ctx.assert_log_contiguous(head.generation);
    let v = restore_verify(&ctx, &soak.lines);
    eprintln!(
        "fault_rustfs_paused_ambiguous_put: paused after {paused_at} acks read, unpaused \
         after {unpaused_at}, longest ack gap {stall:?}, {} error lines, killed at {}: \
         log/{}/ seqs 1..={seqs} {}",
        soak.errors().len(),
        v.acked,
        head.generation,
        v.restored
    );
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn fault_deleted_page_object_fails_read() {
    let ctx = Ctx::new("fault_deleted_page_object_fails_read");
    let dir = ctx.dir("db");
    let args = [
        "--checkpoint",
        "truncate",
        "--every",
        "100",
        "--count",
        "300",
    ];
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
    let out = ctx.run(&["sql", "--dir", r, sql]);
    assert_eq!(out.status.code(), Some(1), "{}\n{}", out.stdout, out.stderr);
    let key = victim.strip_prefix(&format!("{}/", ctx.prefix())).unwrap();
    assert!(
        out.stderr.contains(key),
        "stderr names {key}: {}",
        out.stderr
    );
    eprintln!(
        "fault_deleted_page_object_fails_read: deleted {key}; stderr: {}",
        out.stderr.trim()
    );
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
    let after_verify = soak.acks();
    let after = ctx.head();
    assert_eq!(after.writer_id, before.writer_id, "verify claimed nothing");
    soak.wait_acks(after_verify + 100);
    soak.kill();
    assert!(soak.errors().is_empty(), "{:?}", soak.errors());
    assert_eq!(ctx.head().writer_id, before.writer_id);
    fs::remove_dir_all(&dir).unwrap();
    let r = restore_verify(&ctx, &soak.lines);
    eprintln!(
        "verify_while_writer_runs_loses_nothing: concurrent verify: {}; acks after it: {}; \
         final acked={} {}",
        out.stdout.trim(),
        r.acked - after_verify,
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
    soak.wait_acks(soak.acks() + 20);
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
         {killed_at} acks (app.db {backfilled} bytes); second run killed after seal-finished at \
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
    let head = ctx.head();
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
