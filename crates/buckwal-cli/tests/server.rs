//! System tests of `buckwal serve` against a real RustFS: the test client of tursodb's SQL
//! over HTTP v3 in the test process (`buckwal_hrana_client`, which builds requests in
//! libSQL's client shapes, still accepted, plus `describe`) talks to the server process. Every
//! row a 200 with an `ok` result acknowledged must survive SIGKILL plus the loss of the local
//! disk, a takeover and a SIGTERM under load.
//!
//! In the second pass of `scripts/test.sh` (`BUCKWAL_TLS_DIR` set) every server serves
//! HTTPS with a bearer token from a file in the test's temp directory, and the database is
//! encrypted; the first pass serves plain HTTP on loopback with no token. Run through
//! `./scripts/test.sh` (`--test-threads=1`).
mod common;

use buckwal_hrana_client::{
    describe, execute, result_error, result_rows, stmt, tx_batch_request, Arg, Client, Proto,
    Reply, Stream,
};
use common::*;
use serde_json::{json, Value};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// How long a server may take to print a line (`serve ready` opens the database, which may
/// restore it): generous, so a hung server fails the test instead of hanging the suite.
const WAIT: Duration = Duration::from_secs(120);

/// The per-stream Window setting the batched streams run under.
const WINDOW_PRAGMA: &str = "PRAGMA buckwal_batching = 'window 20ms 64 1MiB'";

/// The table `verify --acked` checks, as `soak` creates it.
const CREATE: &str = "CREATE TABLE IF NOT EXISTS soak(id INTEGER PRIMARY KEY, payload BLOB)";

const INSERT: &str = "INSERT INTO soak(id, payload) VALUES (?, ?)";

/// The shutdown bound every server runs under (`BUCKWAL_SERVE_SHUTDOWN_MS`), below the 45 s
/// default so a test stays short.
const SHUTDOWN_MS: u64 = 20_000;

/// How long a server may take to exit after SIGTERM: its shutdown bound plus 10 s.
const EXIT_LIMIT: Duration = Duration::from_millis(SHUTDOWN_MS + 10_000);

/// The 1 KiB payload of row `id`.
fn payload(id: i64) -> Vec<u8> {
    vec![(id % 251) as u8; 1024]
}

/// How the tests reach a server: plain HTTP on loopback, or HTTPS with the CA of `material`
/// and a bearer token from a file.
#[derive(Clone)]
struct Access {
    /// A `scripts/tls-material.sh` directory: `ca.pem`, `server/rustfs_cert.pem`,
    /// `server/rustfs_key.pem`.
    material: Option<PathBuf>,
    token_file: Option<PathBuf>,
    /// Never printed nor put in an assertion message.
    token: Option<String>,
}

impl Access {
    fn plain() -> Access {
        Access {
            material: None,
            token_file: None,
            token: None,
        }
    }

    /// HTTPS with `material`, and a new token written to a file in `ctx`'s temp directory.
    fn secured(ctx: &Ctx, material: PathBuf) -> Access {
        let token = format!("tok-{:032x}-{}", nanos(), std::process::id());
        let token_file = ctx.tmp.path().join("serve-token");
        fs::write(&token_file, format!("{token}\n")).expect("write the token file");
        Access {
            material: Some(material),
            token_file: Some(token_file),
            token: Some(token),
        }
    }

    /// The pass's access: `secured` with `$BUCKWAL_TLS_DIR` when it is set, else
    /// `plain`.
    fn of_pass(ctx: &Ctx) -> Access {
        match std::env::var("BUCKWAL_TLS_DIR") {
            Ok(d) if !d.is_empty() => Access::secured(ctx, d.into()),
            _ => Access::plain(),
        }
    }

    fn cert(&self) -> Option<PathBuf> {
        Some(self.material.as_ref()?.join("server/rustfs_cert.pem"))
    }

    fn key(&self) -> Option<PathBuf> {
        Some(self.material.as_ref()?.join("server/rustfs_key.pem"))
    }

    fn ca(&self) -> Option<PathBuf> {
        Some(self.material.as_ref()?.join("ca.pem"))
    }

    /// The `BUCKWAL_SERVE_*` settings a server gets.
    fn vars(&self) -> Vec<(&'static str, PathBuf)> {
        let mut v = Vec::new();
        if let (Some(cert), Some(key)) = (self.cert(), self.key()) {
            v.push(("BUCKWAL_SERVE_TLS_CERT", cert));
            v.push(("BUCKWAL_SERVE_TLS_KEY", key));
        }
        if let Some(t) = &self.token_file {
            v.push(("BUCKWAL_SERVE_TOKEN_FILE", t.clone()));
        }
        v
    }

    fn base(&self, port: u16) -> String {
        match self.material {
            Some(_) => format!("https://localhost:{port}"),
            None => format!("http://127.0.0.1:{port}"),
        }
    }

    fn client(&self, port: u16) -> Client {
        let ca = self.ca();
        Client::new(&self.base(port), self.token.as_deref(), ca.as_deref())
            .unwrap_or_else(|e| panic!("client for port {port}: {e}"))
    }
}

/// A test's `Ctx` with the shutdown bound set, and the pass's access.
fn serve_ctx(name: &str) -> (Ctx, Access) {
    let ctx = Ctx::new(name).set("BUCKWAL_SERVE_SHUTDOWN_MS", &SHUTDOWN_MS.to_string());
    let access = Access::of_pass(&ctx);
    (ctx, access)
}

/// Wait up to `limit` for `child` to exit; SIGKILL it and fail when it does not.
fn wait_exit(child: &mut Child, what: &str, limit: Duration) -> ExitStatus {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(s) = child.try_wait().expect("try_wait") {
            return s;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{what}: did not exit within {limit:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A running `buckwal serve`, its stdout lines read on a thread into a channel.
struct ServerProc {
    child: Child,
    rx: Receiver<String>,
    lines: Vec<String>,
    stderr: PathBuf,
    /// The port of its `serve listening addr=…` line.
    port: u16,
    access: Access,
    eof: bool,
}

impl ServerProc {
    /// `serve --dir <dir> --listen 127.0.0.1:0 <extra>` (no `--listen` added when `extra`
    /// has one) with `access`'s settings; returns once it printed `serve ready`.
    fn spawn(ctx: &Ctx, access: &Access, dir: &Path, extra: &[&str]) -> ServerProc {
        let mut args = vec!["serve", "--dir", dir.to_str().unwrap()];
        if !extra.contains(&"--listen") {
            args.extend_from_slice(&["--listen", "127.0.0.1:0"]);
        }
        args.extend_from_slice(extra);
        let stderr = ctx.tmp.path().join(format!("serve-{}.stderr", nanos()));
        let mut cmd = ctx.command(&args);
        cmd.envs(access.vars());
        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(fs::File::create(&stderr).unwrap())
            .spawn()
            .expect("spawn serve");
        let out = child.stdout.take().unwrap();
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let mut s = ServerProc {
            child,
            rx,
            lines: Vec::new(),
            stderr,
            port: 0,
            access: access.clone(),
            eof: false,
        };
        let listening = s.wait_for("serve listening", |l| l.starts_with("serve listening "));
        let addr = listening
            .split_whitespace()
            .find_map(|kv| kv.strip_prefix("addr="))
            .unwrap_or_else(|| panic!("no addr= in {listening:?}"));
        s.port = addr
            .rsplit_once(':')
            .and_then(|(_, p)| p.parse().ok())
            .unwrap_or_else(|| panic!("no port in {listening:?}"));
        let tls = access.material.is_some();
        let auth = access.token.is_some();
        assert!(
            listening.ends_with(&format!(" tls={tls} auth={auth}")),
            "{listening:?}"
        );
        s.wait_for("serve ready", |l| l.starts_with("serve ready "));
        s
    }

    fn client(&self) -> Client {
        self.access.client(self.port)
    }

    fn stderr(&self) -> String {
        fs::read_to_string(&self.stderr).unwrap_or_default()
    }

    /// The last output, for failure messages, with the token replaced by `<token>`.
    fn tail(&self) -> String {
        let from = self.lines.len().saturating_sub(20);
        let text = format!(
            "last stdout lines: {:?}\nstderr:\n{}",
            &self.lines[from..],
            self.stderr()
        );
        redact(&text, self.access.token.as_deref())
    }

    /// The `serve stopped …` line, once the process has exited.
    fn stop_line(&self) -> Option<&String> {
        self.lines.iter().find(|l| l.starts_with("serve stopped "))
    }

    /// Read lines until one `p` accepts, which is returned; fails on EOF or after `WAIT`.
    fn wait_for(&mut self, what: &str, p: impl Fn(&str) -> bool) -> String {
        let deadline = Instant::now() + WAIT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.rx.recv_timeout(left) {
                Ok(line) => {
                    self.lines.push(line);
                    let l = self.lines.last().unwrap();
                    if p(l) {
                        return l.clone();
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    panic!("serve: no {what} within {WAIT:?}\n{}", self.tail())
                }
                Err(RecvTimeoutError::Disconnected) => {
                    self.eof = true;
                    panic!("serve exited before {what}\n{}", self.tail())
                }
            }
        }
    }

    /// Read everything the process printed until its stdout closed.
    fn drain(&mut self) {
        let deadline = Instant::now() + WAIT;
        while !self.eof {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.rx.recv_timeout(left) {
                Ok(line) => self.lines.push(line),
                Err(RecvTimeoutError::Disconnected) => self.eof = true,
                Err(RecvTimeoutError::Timeout) => panic!("serve stdout never closed"),
            }
        }
    }

    /// SIGTERM, then wait for the exit (at most `EXIT_LIMIT`) and its last lines.
    fn sigterm(&mut self) -> ExitStatus {
        let pid = self.child.id().to_string();
        let status = Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap_or_else(|e| panic!("kill -TERM {pid}: {e}"));
        assert!(status.success(), "kill -TERM {pid}: {status}");
        let exited = wait_exit(&mut self.child, "serve after SIGTERM", EXIT_LIMIT);
        self.drain();
        exited
    }

    /// SIGTERM; the server must stop clean (exit 0, `serve stopped clean=true fenced=false`).
    fn stop_clean(&mut self) {
        let status = self.sigterm();
        assert_eq!(status.code(), Some(0), "serve exit\n{}", self.tail());
        let stopped = self.stop_line();
        assert!(
            stopped.is_some_and(|l| l.starts_with("serve stopped clean=true fenced=false ")),
            "{stopped:?}\n{}",
            self.tail()
        );
    }

    fn kill(&mut self) {
        self.child.kill().expect("SIGKILL serve");
        self.child.wait().expect("reap serve");
        self.drain();
    }
}

/// `text` with every occurrence of `token` replaced by `<token>`.
fn redact(text: &str, token: Option<&str>) -> String {
    match token {
        Some(t) if !t.is_empty() => text.replace(t, "<token>"),
        _ => text.to_string(),
    }
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// What an insert came to.
enum Outcome {
    /// A 200 with an `ok` result: the row is promised.
    Ack,
    /// A 200 whose result is an error, with its code.
    Rejected(Option<String>),
    /// Any other status, or no response at all.
    Failed(String),
}

fn outcome(r: Result<Reply, String>) -> Outcome {
    match r {
        Err(e) => Outcome::Failed(format!("no response: {e}")),
        Ok(r) if r.status != 200 => Outcome::Failed(format!("HTTP {}: {}", r.status, r.body)),
        Ok(r) if r.body["results"][0]["type"] == "ok" => Outcome::Ack,
        Ok(r) => Outcome::Rejected(result_error(&r, 0).and_then(|(_, code)| code)),
    }
}

/// Insert row `id` (its own commit) on `st`.
fn insert(st: &mut Stream<'_>, id: i64) -> Outcome {
    outcome(st.pipeline(vec![execute(
        INSERT,
        &[Arg::Int(id), Arg::Blob(payload(id))],
    )]))
}

/// Insert row `id`, which must be acked: its `ack <id>` line.
fn must_ack(st: &mut Stream<'_>, id: i64) -> String {
    match insert(st, id) {
        Outcome::Ack => format!("ack {id}"),
        Outcome::Rejected(code) => panic!("insert {id}: error {code:?}"),
        Outcome::Failed(e) => panic!("insert {id}: {e}"),
    }
}

/// Run `requests` on `st`; a 200 whose every result is `ok`, or fail.
fn must_ok(st: &mut Stream<'_>, requests: Vec<Value>) -> Reply {
    let r = st.pipeline(requests).expect("pipeline");
    assert_eq!(r.status, 200, "{}", r.body);
    for (i, res) in r.body["results"].as_array().unwrap().iter().enumerate() {
        assert_eq!(res["type"], "ok", "result {i}: {}", r.body);
    }
    r
}

/// `CREATE TABLE soak` on a new stream.
fn create_table(c: &Client) {
    must_ok(&mut c.stream(Proto::V2), vec![execute(CREATE, &[])]);
}

/// What one load thread did.
struct Worker {
    acks: Vec<String>,
    /// The failure or rejection that ended the thread, and whether the server was being
    /// stopped or killed by then.
    ended: Option<(String, bool)>,
}

/// Threads inserting rows on their own streams until a request fails or `stop` is set.
struct Load {
    stop: Arc<AtomicBool>,
    /// Set just before the server is killed or stopped: failures after it are expected.
    disrupted: Arc<AtomicBool>,
    /// Each thread's acks so far.
    acks: Arc<Vec<AtomicUsize>>,
    handles: Vec<JoinHandle<Worker>>,
}

impl Load {
    /// `threads` threads, alternating v2 and v3 streams; thread `t` sets the Window
    /// setting on its stream when `window(t)`, and inserts ids `(t + 1) * 1_000_000 + n`.
    fn start(server: &ServerProc, threads: usize, window: impl Fn(usize) -> bool) -> Load {
        let stop = Arc::new(AtomicBool::new(false));
        let disrupted = Arc::new(AtomicBool::new(false));
        let acks: Arc<Vec<AtomicUsize>> =
            Arc::new((0..threads).map(|_| AtomicUsize::new(0)).collect());
        let handles = (0..threads)
            .map(|t| {
                let client = server.client();
                let windowed = window(t);
                let (stop, disrupted, acks) = (stop.clone(), disrupted.clone(), acks.clone());
                std::thread::spawn(move || {
                    let proto = if t % 2 == 0 { Proto::V2 } else { Proto::V3 };
                    let mut st = client.stream(proto);
                    let mut w = Worker {
                        acks: Vec::new(),
                        ended: None,
                    };
                    if windowed {
                        let r = outcome(st.pipeline(vec![execute(WINDOW_PRAGMA, &[])]));
                        if !matches!(r, Outcome::Ack) {
                            w.ended = Some((
                                format!("thread {t}: the Window pragma failed"),
                                disrupted.load(SeqCst),
                            ));
                            return w;
                        }
                    }
                    let first = (t as i64 + 1) * 1_000_000;
                    for id in first.. {
                        if stop.load(SeqCst) {
                            break;
                        }
                        let ended = match insert(&mut st, id) {
                            Outcome::Ack => {
                                w.acks.push(format!("ack {id}"));
                                acks[t].fetch_add(1, SeqCst);
                                continue;
                            }
                            Outcome::Rejected(code) => format!("insert {id}: error {code:?}"),
                            Outcome::Failed(e) => format!("insert {id}: {e}"),
                        };
                        w.ended = Some((format!("thread {t}: {ended}"), disrupted.load(SeqCst)));
                        break;
                    }
                    w
                })
            })
            .collect();
        Load {
            stop,
            disrupted,
            acks,
            handles,
        }
    }

    fn counts(&self) -> Vec<usize> {
        self.acks.iter().map(|a| a.load(SeqCst)).collect()
    }

    /// Wait until the threads have `n` acks between them and each has at least `each`, so
    /// every kind of stream is mid-load; fails after `WAIT`, or once a thread ended.
    fn wait_acks(&self, n: usize, each: usize) {
        let deadline = Instant::now() + WAIT;
        loop {
            let counts = self.counts();
            if counts.iter().sum::<usize>() >= n && counts.iter().all(|&c| c >= each) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "no {n} acks with {each} per thread within {WAIT:?}: {counts:?}"
            );
            assert!(
                !self.handles.iter().any(|h| h.is_finished()),
                "a load thread ended before {n} acks with {each} per thread: {counts:?}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// The server is about to be killed or stopped.
    fn disrupt(&self) {
        self.disrupted.store(true, SeqCst);
    }

    /// Stop the threads and collect them. No thread may have failed before `disrupt`.
    fn join(mut self) -> Vec<Worker> {
        self.stop.store(true, SeqCst);
        let workers: Vec<Worker> = self
            .handles
            .drain(..)
            .map(|h| h.join().expect("load thread"))
            .collect();
        for w in &workers {
            if let Some((why, disrupted)) = &w.ended {
                assert!(*disrupted, "a request failed before the disruption: {why}");
            }
        }
        workers
    }
}

impl Drop for Load {
    /// Threads end on their own once `stop` is set (each request is bounded by the client's
    /// timeout), so a failed test never waits on them.
    fn drop(&mut self) {
        self.stop.store(true, SeqCst);
    }
}

/// Every thread's `ack <id>` lines; prints each thread's count and why it ended.
fn all_acks(name: &str, workers: &[Worker]) -> Vec<String> {
    let counts: Vec<usize> = workers.iter().map(|w| w.acks.len()).collect();
    let ended: Vec<&str> = workers
        .iter()
        .filter_map(|w| w.ended.as_ref().map(|(why, _)| why.as_str()))
        .collect();
    println!("{name}: acks per thread {counts:?}, threads ended by {ended:?}");
    workers.iter().flat_map(|w| w.acks.clone()).collect()
}

/// Delete `dir` (a database directory inside the test's temp directory): the local disk is
/// lost, so only the bucket remains.
fn lose_disk(ctx: &Ctx, dir: &Path) {
    assert!(dir.starts_with(ctx.tmp.path()), "{}", dir.display());
    fs::remove_dir_all(dir).expect("delete the database directory");
}

/// Stream 1 inserts 50 rows with the database Off; streams 2 to 5, on threads, each set the
/// Window setting for themselves and insert 50; stream 6 runs one transaction batch in
/// @libsql/client's shape, whose first step after BEGIN sets the Window setting for that
/// transaction, with 20 inserts; stream 7 counts 270 rows through a v3 cursor. After a clean
/// SIGTERM and the loss of the disk, every one of the 270 acks restores.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn serve_client_writes_off_window_and_txn_batching() {
    let (ctx, access) = serve_ctx("serve_client_writes_off_window_and_txn_batching");
    let dir = ctx.dir("db");
    let mut server = ServerProc::spawn(&ctx, &access, &dir, &["--claim"]);
    let c = server.client();
    create_table(&c);
    let mut acks = Vec::new();
    let mut st1 = c.stream(Proto::V2);
    for id in 1..=50 {
        acks.push(must_ack(&mut st1, id));
    }
    let windowed: Vec<Vec<String>> = std::thread::scope(|sc| {
        let threads: Vec<_> = (2..=5i64)
            .map(|k| {
                let c = &c;
                sc.spawn(move || {
                    let proto = if k % 2 == 0 { Proto::V2 } else { Proto::V3 };
                    let mut st = c.stream(proto);
                    must_ok(&mut st, vec![execute(WINDOW_PRAGMA, &[])]);
                    (1..=50)
                        .map(|i| must_ack(&mut st, k * 1000 + i))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|h| h.join().expect("stream thread"))
            .collect()
    });
    acks.extend(windowed.into_iter().flatten());

    let mut st6 = c.stream(Proto::V3);
    let ids: Vec<i64> = (6001..=6020).collect();
    let mut steps = vec![stmt(WINDOW_PRAGMA, &[])];
    steps.extend(
        ids.iter()
            .map(|&id| stmt(INSERT, &[Arg::Int(id), Arg::Blob(payload(id))])),
    );
    let r = must_ok(&mut st6, vec![tx_batch_request(&steps)]);
    let result = &r.body["results"][0]["response"]["result"];
    // BEGIN, the pragma, 20 inserts and COMMIT ran without an error; ROLLBACK was skipped.
    let errors = result["step_errors"].as_array().expect("step_errors");
    assert!(errors.iter().all(Value::is_null), "{}", r.body);
    let ran: Vec<bool> = result["step_results"]
        .as_array()
        .expect("step_results")
        .iter()
        .map(|x| !x.is_null())
        .collect();
    let mut want = vec![true; steps.len() + 2];
    want.push(false);
    assert_eq!(ran, want, "{}", r.body);
    acks.extend(ids.iter().map(|id| format!("ack {id}")));
    assert_eq!(acks.len(), 270);

    // A tursodb serverless driver's prepare, then its query, on one stream: describe, then
    // execute with as many arguments as it reported. A described INSERT never runs.
    let mut st8 = c.stream(Proto::V3);
    let select = "SELECT payload FROM soak WHERE id = ?";
    let r = must_ok(&mut st8, vec![describe(select), describe(INSERT)]);
    let described = &r.body["results"][0]["response"]["result"];
    assert_eq!(
        described,
        &json!({"params": [{"name": null}], "cols": [{"name": "payload", "decltype": "BLOB"}],
            "is_explain": false, "is_readonly": true}),
        "{}",
        r.body
    );
    let insert = &r.body["results"][1]["response"]["result"];
    assert_eq!(insert["is_readonly"], json!(false), "{}", r.body);
    let n = described["params"].as_array().expect("params").len();
    let r = must_ok(&mut st8, vec![execute(select, &vec![Arg::Int(6001); n])]);
    assert_eq!(
        result_rows(&r, 0).expect("rows"),
        vec![vec![json!(payload(6001))]]
    );

    let mut st7 = c.stream(Proto::V3);
    let count = json!({"steps": [{"stmt": stmt("SELECT count(*) FROM soak", &[])}]});
    let (status, entries) = st7.cursor(count).expect("cursor");
    assert_eq!(status, 200, "{entries:?}");
    let kinds: Vec<&str> = entries.iter().filter_map(|e| e["type"].as_str()).collect();
    assert_eq!(
        kinds,
        ["step_begin", "row", "step_end", "replication_index"],
        "{entries:?}"
    );
    assert_eq!(
        entries[1]["row"][0],
        json!({"type": "integer", "value": "270"}),
        "{entries:?}"
    );

    server.stop_clean();
    lose_disk(&ctx, &dir);
    let v = restore_verify(&ctx, &acks);
    assert_eq!(v.acked, 270, "{v:?}");
    println!(
        "serve_client_writes_off_window_and_txn_batching: acked={} unacked_present={}",
        v.acked, v.unacked_present
    );
}

/// `PRAGMA buckwal_durable_commit` on a stream: (open_id, last_commit, durable_commit).
fn durable_row(st: &mut Stream<'_>, with: Vec<Value>) -> (String, Option<i64>, i64) {
    let mut requests = with;
    requests.push(execute("PRAGMA buckwal_durable_commit", &[]));
    let at = requests.len() - 1;
    let r = must_ok(st, requests);
    let row = &r.body["results"][at]["response"]["result"]["rows"][0];
    let int = |v: &Value| v["value"].as_str().map(|s| s.parse::<i64>().unwrap());
    (
        row[0]["value"].as_str().expect("open_id").to_string(),
        int(&row[1]),
        int(&row[2]).expect("durable_commit"),
    )
}

/// `serve` with a background setting: the position is read in the same request as a write and
/// is that write's; it becomes durable; and `open_id` changes with a server restart.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn serve_background_commit_position_and_open_id() {
    let (ctx, access) = serve_ctx("serve_background_commit_position_and_open_id");
    let dir = ctx.dir("db");
    let setting = "background 20ms 8 1MiB 20";
    let mut server = ServerProc::spawn(&ctx, &access, &dir, &["--claim", "--batching", setting]);
    let c = server.client();
    create_table(&c);
    let mut st = c.stream(Proto::V3);
    let (open_id, fresh_last, _) = durable_row(&mut c.stream(Proto::V3), vec![]);
    assert_eq!(fresh_last, None, "a new stream has made no commit");
    let write = execute(INSERT, &[Arg::Int(1), Arg::Blob(payload(1))]);
    let (id1, last, _) = durable_row(&mut st, vec![write]);
    let last = last.expect("that write's commit number");
    assert_eq!(id1, open_id, "one open, one id");
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let (_, _, durable) = durable_row(&mut st, vec![]);
        if durable >= last {
            break;
        }
        assert!(Instant::now() < until, "commit {last} never became durable");
        std::thread::sleep(Duration::from_millis(20));
    }
    server.stop_clean();
    let server = ServerProc::spawn(&ctx, &access, &dir, &["--batching", setting]);
    let (second, last2, _) = durable_row(&mut server.client().stream(Proto::V3), vec![]);
    assert_ne!(second, open_id, "a restart is a new open");
    assert_eq!(last2, None);
}

/// Four client threads (two streams Off, two Window) insert until 300 acks, at least 5 on
/// each stream; the server is SIGKILLed and its disk lost. Every ack restores.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn serve_sigkill_mid_load_restores_every_ack() {
    let (ctx, access) = serve_ctx("serve_sigkill_mid_load_restores_every_ack");
    let dir = ctx.dir("db");
    let mut server = ServerProc::spawn(&ctx, &access, &dir, &["--claim"]);
    create_table(&server.client());
    let load = Load::start(&server, 4, |t| t >= 2);
    load.wait_acks(300, 5);
    load.disrupt();
    server.kill();
    let workers = load.join();
    let acks = all_acks("serve_sigkill_mid_load_restores_every_ack", &workers);
    assert!(acks.len() >= 300, "{} acks", acks.len());
    lose_disk(&ctx, &dir);
    let v = restore_verify(&ctx, &acks);
    println!(
        "serve_sigkill_mid_load_restores_every_ack: acked={} unacked_present={}",
        v.acked, v.unacked_present
    );
}

/// `serve --journal-mode mvcc`: four client threads, each on its own stream, write rows in
/// `BEGIN CONCURRENT` ... `COMMIT` pipelines until 300 acks (a row counts only when all three
/// results of its pipeline are `ok`); the server is SIGKILLed and its disk lost. Every ack
/// restores, and the restore needs no `--journal-mode`: the head says MVCC.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn mvcc_serve_begin_concurrent() {
    const NAME: &str = "mvcc_serve_begin_concurrent";
    const OR_REPLACE: &str = "INSERT OR REPLACE INTO soak(id, payload) VALUES (?, ?)";
    let (ctx, access) = serve_ctx(NAME);
    let dir = ctx.dir("db");
    let mut server = ServerProc::spawn(&ctx, &access, &dir, &["--claim", "--journal-mode", "mvcc"]);
    create_table(&server.client());
    let stop = Arc::new(AtomicBool::new(false));
    let disrupted = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicUsize::new(0));
    let handles: Vec<JoinHandle<(Vec<String>, usize)>> = (0..4)
        .map(|t| {
            let client = server.client();
            let (stop, disrupted, total) = (stop.clone(), disrupted.clone(), total.clone());
            std::thread::spawn(move || {
                let proto = if t % 2 == 0 { Proto::V2 } else { Proto::V3 };
                let mut st = client.stream(proto);
                let (mut acks, mut conflicts) = (Vec::new(), 0usize);
                let first = (t as i64 + 1) * 1_000_000;
                let mut id = first;
                while !stop.load(SeqCst) {
                    let r = st.pipeline(vec![
                        execute("BEGIN CONCURRENT", &[]),
                        execute(OR_REPLACE, &[Arg::Int(id), Arg::Blob(payload(id))]),
                        execute("COMMIT", &[]),
                    ]);
                    let all_ok = matches!(&r, Ok(r) if r.status == 200
                    && r.body["results"].as_array().is_some_and(|v| {
                        v.len() >= 3 && v[..3].iter().all(|x| x["type"] == "ok")
                    }));
                    if all_ok {
                        acks.push(format!("ack {id}"));
                        total.fetch_add(1, SeqCst);
                        id += 1;
                        continue;
                    }
                    assert!(
                        disrupted.load(SeqCst) || matches!(&r, Ok(r) if r.status == 200),
                        "thread {t}: request failed before the disruption: {r:?}"
                    );
                    if disrupted.load(SeqCst) {
                        break;
                    }
                    // A conflict between concurrent writers: roll back and retry the row.
                    conflicts += 1;
                    let _ = st.pipeline(vec![execute("ROLLBACK", &[])]);
                }
                (acks, conflicts)
            })
        })
        .collect();
    let deadline = Instant::now() + WAIT;
    while total.load(SeqCst) < 300 {
        assert!(
            Instant::now() < deadline,
            "no 300 acks: {}",
            total.load(SeqCst)
        );
        assert!(
            !handles.iter().any(|h| h.is_finished()),
            "a client thread ended"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    disrupted.store(true, SeqCst);
    server.kill();
    stop.store(true, SeqCst);
    let results: Vec<(Vec<String>, usize)> = handles
        .into_iter()
        .map(|h| h.join().expect("client thread"))
        .collect();
    let conflicts: usize = results.iter().map(|r| r.1).sum();
    let acks: Vec<String> = results.into_iter().flat_map(|r| r.0).collect();
    assert!(acks.len() >= 300, "{} acks", acks.len());
    lose_disk(&ctx, &dir);
    let v = restore_verify(&ctx, &acks);
    println!(
        "{NAME}: acked={} unacked_present={} conflicts retried={conflicts} {}",
        v.acked, v.unacked_present, v.restored
    );
}

/// Server A acks 20 rows; `takeover --dir B` fences it and server B serves B. A's next
/// inserts fail with `FENCED` and its health turns 503 `fenced`; B acks 20 more. All 40
/// restore, and none of A's refused rows.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn serve_takeover_fences_the_old_server() {
    let (ctx, access) = serve_ctx("serve_takeover_fences_the_old_server");
    let (dir_a, dir_b) = (ctx.dir("a"), ctx.dir("b"));
    let mut a = ServerProc::spawn(&ctx, &access, &dir_a, &["--claim"]);
    let ca = a.client();
    create_table(&ca);
    let mut st_a = ca.stream(Proto::V2);
    let mut acks: Vec<String> = (1..=20).map(|id| must_ack(&mut st_a, id)).collect();

    let took = ctx.ok(&["takeover", "--dir", dir_b.to_str().unwrap()]);
    let line = took.stdout.trim();
    assert!(line.starts_with("took over "), "{line}");
    // B is claimed by the takeover, so no --claim.
    let mut b = ServerProc::spawn(&ctx, &access, &dir_b, &[]);

    for (attempt, id) in [(1, 1001), (2, 1002)] {
        match insert(&mut st_a, id) {
            Outcome::Rejected(code) => {
                assert_eq!(code.as_deref(), Some("FENCED"), "insert {attempt} on A")
            }
            Outcome::Ack => panic!("insert {attempt} on A was acked after the takeover"),
            Outcome::Failed(e) => panic!("insert {attempt} on A: {e}"),
        }
        if attempt == 1 {
            let h = ca.get("/health").expect("health of A");
            assert_eq!(h.status, 503, "{}", h.body);
            assert_eq!(h.body, json!({"status": "fenced"}));
        }
    }

    let cb = b.client();
    let mut st_b = cb.stream(Proto::V2);
    acks.extend((21..=40).map(|id| must_ack(&mut st_b, id)));

    let status_a = a.sigterm();
    let stopped_a = a.stop_line();
    println!(
        "serve_takeover_fences_the_old_server: A exited {status_a} after SIGTERM, {stopped_a:?}"
    );
    assert!(
        stopped_a.is_some_and(|l| l.contains(" fenced=true ")),
        "A's stop line must say it was fenced: {stopped_a:?}\n{}",
        a.tail()
    );
    assert_eq!(status_a.code(), Some(1), "a fenced A exits 1\n{}", a.tail());
    b.stop_clean();
    let v = restore_verify(&ctx, &acks);
    assert_eq!(v.acked, 40, "{v:?}");
    assert_eq!(v.unacked_present, 0, "a row A refused as FENCED: {v:?}");
    println!(
        "serve_takeover_fences_the_old_server: acked={} unacked_present={}",
        v.acked, v.unacked_present
    );
}

/// Four client threads (two streams Off, two Window) insert; SIGTERM after 100 acks, at
/// least 5 on each stream. The server exits 0 within its
/// shutdown bound plus 10 s, and after the loss of its disk every ack (every 200 with an `ok`
/// result) restores. Failures after the SIGTERM (no connection, 503) are not acks.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn serve_sigterm_during_load_loses_no_ack() {
    let (ctx, access) = serve_ctx("serve_sigterm_during_load_loses_no_ack");
    let dir = ctx.dir("db");
    let mut server = ServerProc::spawn(&ctx, &access, &dir, &["--claim"]);
    create_table(&server.client());
    let load = Load::start(&server, 4, |t| t >= 2);
    load.wait_acks(100, 5);
    load.disrupt();
    let at = Instant::now();
    server.stop_clean();
    let took = at.elapsed();
    assert!(took <= EXIT_LIMIT, "exit took {took:?}");
    let workers = load.join();
    println!(
        "serve_sigterm_during_load_loses_no_ack: exit after {took:?}, {:?}",
        server.stop_line()
    );
    let acks = all_acks("serve_sigterm_during_load_loses_no_ack", &workers);
    lose_disk(&ctx, &dir);
    let v = restore_verify(&ctx, &acks);
    println!(
        "serve_sigterm_during_load_loses_no_ack: acked={} unacked_present={}",
        v.acked, v.unacked_present
    );
}

/// The TLS material: `$BUCKWAL_TLS_DIR` when set, else `scripts/tls-material.sh` into
/// the test's temp directory.
fn tls_material(ctx: &Ctx) -> PathBuf {
    if let Ok(d) = std::env::var("BUCKWAL_TLS_DIR") {
        if !d.is_empty() {
            return d.into();
        }
    }
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/tls-material.sh");
    let out = ctx.tmp.path().join("m");
    let o = Command::new("bash")
        .arg(&script)
        .arg(&out)
        .output()
        .expect("run tls-material.sh");
    assert!(
        o.status.success(),
        "tls-material.sh: {}",
        String::from_utf8_lossy(&o.stderr)
    );
    out
}

/// `grep -c -F -f <token file> <file>`: how many lines of `file` hold the token. The token
/// goes to grep through its file, never on a command line.
fn grep_count(token_file: &Path, file: &Path) -> String {
    let o = Command::new("grep")
        .args(["-c", "-F", "-f"])
        .arg(token_file)
        .arg(file)
        .output()
        .expect("run grep");
    String::from_utf8_lossy(&o.stdout).trim().to_string()
}

/// A non-loopback listen needs TLS and a token: with neither, or only one, `serve` exits 2
/// naming both settings and writes nothing to the bucket. With both, a request without the
/// token or with a wrong one gets 401 and the right one over HTTPS gets 200. The token never
/// reaches the server's output.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn serve_security_bind_rules_and_token() {
    let ctx = Ctx::new("serve_security_bind_rules_and_token")
        .set("BUCKWAL_SERVE_SHUTDOWN_MS", &SHUTDOWN_MS.to_string());
    let material = tls_material(&ctx);
    let secured = Access::secured(&ctx, material);
    let token_file = secured.token_file.clone().unwrap();
    let token = secured.token.clone().unwrap();
    let dir = ctx.dir("db");
    let d = dir.to_str().unwrap();
    let mut captured: Vec<PathBuf> = Vec::new();

    let tls_only = Access {
        token_file: None,
        token: None,
        ..secured.clone()
    };
    let token_only = Access {
        material: None,
        ..secured.clone()
    };
    for (label, access) in [
        ("neither", Access::plain()),
        ("TLS only", tls_only),
        ("token only", token_only),
    ] {
        let mut cmd = ctx.command(&["serve", "--dir", d, "--listen", "0.0.0.0:0", "--claim"]);
        cmd.envs(access.vars());
        let (out, err) = (
            ctx.tmp
                .path()
                .join(format!("refused-{}.stdout", captured.len())),
            ctx.tmp
                .path()
                .join(format!("refused-{}.stderr", captured.len())),
        );
        let mut child = cmd
            .stdout(fs::File::create(&out).unwrap())
            .stderr(fs::File::create(&err).unwrap())
            .spawn()
            .expect("spawn serve");
        let status = wait_exit(&mut child, label, Duration::from_secs(30));
        let stderr = redact(&fs::read_to_string(&err).unwrap(), Some(&token));
        assert_eq!(status.code(), Some(2), "{label}: {stderr}");
        for setting in ["BUCKWAL_SERVE_TLS_CERT", "BUCKWAL_SERVE_TOKEN_FILE"] {
            assert!(stderr.contains(setting), "{label}: {setting} in {stderr}");
        }
        captured.extend([out, err]);
    }
    assert!(!dir.exists(), "a refused serve created {}", dir.display());
    assert_eq!(ctx.list(""), Vec::<String>::new(), "a refused serve wrote");

    let mut server = ServerProc::spawn(&ctx, &secured, &dir, &["--listen", "0.0.0.0:0", "--claim"]);
    let c = server.client();
    let body = || {
        json!({"baton": null, "requests": [execute("SELECT 1", &[])]})
            .to_string()
            .into_bytes()
    };
    let r = c.post_raw("/v2/pipeline", body(), None).expect("no token");
    assert_eq!(r.status, 401, "no token: {}", r.body);
    let wrong = "Bearer wrong-token-0123456789abcdef";
    let r = c
        .post_raw("/v2/pipeline", body(), Some(wrong))
        .expect("wrong");
    assert_eq!(r.status, 401, "wrong token: {}", r.body);
    let right = format!("Bearer {token}");
    let r = c
        .post_raw("/v2/pipeline", body(), Some(&right))
        .expect("the right token");
    assert_eq!(r.status, 200, "the right token: {}", r.body);
    assert_eq!(r.body["results"][0]["type"], "ok", "{}", r.body);

    server.stop_clean();
    let out = ctx.tmp.path().join("serve.stdout");
    fs::write(&out, server.lines.join("\n")).unwrap();
    captured.extend([out, server.stderr.clone()]);
    for f in &captured {
        let text = fs::read_to_string(f).unwrap();
        assert!(!text.contains(&token), "the token is in {}", f.display());
        assert_eq!(
            grep_count(&token_file, f),
            "0",
            "grep -c on {}",
            f.display()
        );
    }
}
