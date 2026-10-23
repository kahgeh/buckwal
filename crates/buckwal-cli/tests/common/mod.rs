//! The harness the RustFS-backed test files share (`system.rs`, `server.rs`): connection
//! settings, a test's bucket prefix and temp directory (`Ctx`), and `restore_verify`.
#![allow(dead_code)]
use buckwal::store::s3::{S3Config, S3Store};
use buckwal::store::ObjectStore;
use buckwal::Config;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

pub const BIN: &str = env!("CARGO_BIN_EXE_buckwal");

/// Settings a test may set; every child gets them removed first, so the caller's
/// environment never leaks into a test (the TLS and encryption ones come back through
/// `env()`). `BUCKWAL_OTEL*` and `BUCKWAL_TEST_RUN_ID` are not here: `scripts/test.sh` sets
/// them so the children export under its run id.
pub const OPTIONAL: [&str; 30] = [
    "BUCKWAL_REPLICA_POLL_MS",
    "BUCKWAL_REPLICA_NOTIFY_LISTEN",
    "BUCKWAL_REPLICA_NOTIFY_TLS_CERT",
    "BUCKWAL_REPLICA_NOTIFY_TLS_KEY",
    "BUCKWAL_REPLICA_NOTIFY_TOKEN_FILE",
    "BUCKWAL_PREFIX",
    "BUCKWAL_RETRY_DEADLINE_MS",
    "BUCKWAL_REQUEST_TIMEOUT_MS",
    "BUCKWAL_UPLOAD_CONCURRENCY",
    "BUCKWAL_TRIM_GRACE_SECS",
    "BUCKWAL_PREFETCH",
    "BUCKWAL_BATCHING",
    "BUCKWAL_MAINTENANCE_LOG_OBJECTS",
    "BUCKWAL_MAINTENANCE_WAL_MIB",
    "BUCKWAL_MAINTENANCE_GATE_WAIT_MS",
    "BUCKWAL_LEASE_MS",
    "BUCKWAL_GC_AFTER_SEAL",
    "BUCKWAL_CA_BUNDLE",
    "BUCKWAL_ENCRYPTION_KEY_FILE",
    "BUCKWAL_ENCRYPTION_CIPHER",
    "BUCKWAL_JOURNAL_MODE",
    "BUCKWAL_SERVE_LISTEN",
    "BUCKWAL_SERVE_TLS_CERT",
    "BUCKWAL_SERVE_TLS_KEY",
    "BUCKWAL_SERVE_TOKEN_FILE",
    "BUCKWAL_SERVE_STREAM_TIMEOUT_MS",
    "BUCKWAL_SERVE_SHUTDOWN_MS",
    "BUCKWAL_SERVE_BUSY_TIMEOUT_MS",
    "BUCKWAL_SERVE_MAX_STREAMS",
    "BUCKWAL_SERVE_WORKERS",
];

/// Passed through from the caller when set: the second pass of `scripts/test.sh` runs every
/// test over HTTPS with encryption.
pub const PASSED_THROUGH: [&str; 3] = [
    "BUCKWAL_CA_BUNDLE",
    "BUCKWAL_ENCRYPTION_KEY_FILE",
    "BUCKWAL_ENCRYPTION_CIPHER",
];

/// The `BUCKWAL_*` connection settings, with the defaults of `scripts/rustfs.sh env`, then
/// the `PASSED_THROUGH` ones the caller set.
pub fn env() -> Vec<(String, String)> {
    let set = PASSED_THROUGH
        .iter()
        .filter_map(|k| Some((k.to_string(), std::env::var(k).ok()?)));
    [
        ("BUCKWAL_ENDPOINT", "http://127.0.0.1:9290"),
        ("BUCKWAL_REGION", "us-east-1"),
        ("BUCKWAL_PATH_STYLE", "true"),
        ("BUCKWAL_BUCKET", "buckwal"),
        ("BUCKWAL_ACCESS_KEY", "buckwal-admin"),
        ("BUCKWAL_SECRET_KEY", "buckwal-local-secret-not-real"),
    ]
    .iter()
    .map(|(k, d)| {
        (
            k.to_string(),
            std::env::var(k).unwrap_or_else(|_| d.to_string()),
        )
    })
    .chain(set)
    .collect()
}

pub fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

/// A bucket prefix no other test or run uses: `sys/<name>-<nanos>`.
pub fn prefix(name: &str) -> String {
    format!("sys/{name}-{}", nanos())
}

/// Delete every key under `<prefix>/`, from 8 threads: the number deleted, or what failed.
pub fn delete_prefix(store: &S3Store, prefix: &str) -> Result<usize, String> {
    let root = format!("{prefix}/");
    let keys = store.list(&root).map_err(|e| format!("list {root}: {e}"))?;
    let failed = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|sc| {
        for chunk in keys.chunks(keys.len().div_ceil(8).max(1)) {
            let failed = &failed;
            sc.spawn(move || {
                for k in chunk {
                    if let Err(e) = store.delete(k) {
                        failed.lock().unwrap().push(format!("{k}: {e}"));
                    }
                }
            });
        }
    });
    let failed = failed.into_inner().unwrap();
    match failed.first() {
        None => Ok(keys.len()),
        Some(first) => Err(format!(
            "{} of {} deletes under {root} failed, the first: {first}",
            failed.len(),
            keys.len()
        )),
    }
}

/// The parts of the bucket's `head` the tests look at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Head {
    pub generation: u64,
    /// 0 when the head has no `epoch` (bucket format v1 before takeovers).
    pub epoch: u64,
    pub manifest: Option<String>,
    pub writer_id: String,
}

/// The JSON string value after `"field":`, or None for `null`.
pub fn json_str(json: &str, field: &str) -> Option<String> {
    let at = json.find(&format!("\"{field}\":"))? + field.len() + 3;
    let rest = json[at..].trim_start().strip_prefix('"')?;
    Some(rest[..rest.find('"')?].to_string())
}

pub fn json_u64(json: &str, field: &str) -> u64 {
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
pub struct Ctx {
    pub name: String,
    pub vars: Vec<(String, String)>,
    pub tmp: TempDir,
    /// Keep the prefix when the test fails (`BUCKWAL_KEEP_FAILED=1`).
    pub keep_failed: bool,
    /// Keep the prefix, pass or fail (`BUCKWAL_KEEP_PREFIXES=1`): `scripts/test.sh` scans
    /// every object each pass leaves.
    pub keep_all: bool,
}

/// A finished command.
pub struct Run {
    pub status: ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

impl Ctx {
    pub fn new(name: &str) -> Ctx {
        let mut vars = env();
        vars.push(("BUCKWAL_PREFIX".into(), prefix(name)));
        Ctx {
            name: name.to_string(),
            vars,
            tmp: tempfile::tempdir().unwrap(),
            keep_failed: std::env::var("BUCKWAL_KEEP_FAILED").as_deref() == Ok("1"),
            keep_all: std::env::var("BUCKWAL_KEEP_PREFIXES").as_deref() == Ok("1"),
        }
    }

    pub fn set(mut self, k: &str, v: &str) -> Ctx {
        self.vars.push((k.to_string(), v.to_string()));
        self
    }

    pub fn var(&self, k: &str) -> String {
        let v = self.vars.iter().rev().find(|(n, _)| n == k);
        v.unwrap_or_else(|| panic!("{k} not set")).1.clone()
    }

    /// Like `var`, but `None` when unset.
    pub fn opt_var(&self, k: &str) -> Option<String> {
        self.vars
            .iter()
            .rev()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.clone())
    }

    pub fn prefix(&self) -> String {
        self.var("BUCKWAL_PREFIX")
    }

    /// A path in this test's temp directory that does not exist yet.
    pub fn dir(&self, name: &str) -> PathBuf {
        let d = self.tmp.path().join(name);
        assert!(!d.exists(), "{} already exists", d.display());
        d
    }

    pub fn command(&self, args: &[&str]) -> Command {
        let mut c = Command::new(BIN);
        c.args(args);
        for k in OPTIONAL {
            c.env_remove(k);
        }
        c.envs(self.vars.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        c
    }

    pub fn run(&self, args: &[&str]) -> Run {
        self.run_env(&[], args)
    }

    /// Like `run`, with `extra` settings in the child's environment.
    pub fn run_env(&self, extra: &[(&str, &str)], args: &[&str]) -> Run {
        let o = self
            .command(args)
            .envs(extra.iter().copied())
            .output()
            .unwrap_or_else(|e| panic!("run {args:?}: {e}"));
        Run {
            status: o.status,
            stdout: String::from_utf8_lossy(&o.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
        }
    }

    /// Run `args` and require success.
    pub fn ok(&self, args: &[&str]) -> Run {
        self.ok_env(&[], args)
    }

    /// Like `ok`, with `extra` settings in the child's environment.
    pub fn ok_env(&self, extra: &[(&str, &str)], args: &[&str]) -> Run {
        let r = self.run_env(extra, args);
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

    pub fn store(&self) -> S3Store {
        self.try_store().expect("S3 store")
    }

    /// The `Config` of an in-process database in `dir` under this test's prefix, with the
    /// connection, TLS and encryption settings its CLI children get.
    pub fn config(&self, dir: &Path) -> Config {
        let mut c = Config::new(
            dir,
            &self.var("BUCKWAL_ENDPOINT"),
            &self.var("BUCKWAL_BUCKET"),
            &self.var("BUCKWAL_ACCESS_KEY"),
            &self.var("BUCKWAL_SECRET_KEY"),
        );
        c.region = self.var("BUCKWAL_REGION");
        c.path_style = self.var("BUCKWAL_PATH_STYLE") == "true";
        c.prefix = self.prefix();
        let set = |k: &str| self.opt_var(k).filter(|v| !v.is_empty());
        c.ca_bundle = set("BUCKWAL_CA_BUNDLE").map(Into::into);
        c.encryption_key_file = set("BUCKWAL_ENCRYPTION_KEY_FILE").map(Into::into);
        if let Some(cipher) = set("BUCKWAL_ENCRYPTION_CIPHER") {
            c.encryption_cipher = cipher;
        }
        c
    }

    pub fn try_store(&self) -> Result<S3Store, buckwal::Error> {
        S3Store::new(S3Config {
            endpoint: self.var("BUCKWAL_ENDPOINT"),
            region: self.var("BUCKWAL_REGION"),
            bucket: self.var("BUCKWAL_BUCKET"),
            access_key: self.var("BUCKWAL_ACCESS_KEY"),
            secret_key: self.var("BUCKWAL_SECRET_KEY"),
            path_style: self.var("BUCKWAL_PATH_STYLE") == "true",
            request_timeout: Duration::from_secs(30),
            ca_bundle: self
                .opt_var("BUCKWAL_CA_BUNDLE")
                .filter(|v| !v.is_empty())
                .map(Into::into),
        })
    }

    /// Every key under `<prefix>/<sub>`.
    pub fn list(&self, sub: &str) -> Vec<String> {
        self.store()
            .list(&format!("{}/{sub}", self.prefix()))
            .expect("list")
    }

    pub fn head(&self) -> Head {
        let o = self
            .store()
            .get(&format!("{}/head", self.prefix()))
            .expect("get head")
            .expect("head exists");
        let json = String::from_utf8(o.body).unwrap();
        Head {
            generation: json_u64(&json, "generation"),
            epoch: if json.contains("\"epoch\":") {
                json_u64(&json, "epoch")
            } else {
                0
            },
            manifest: json_str(&json, "manifest"),
            writer_id: json_str(&json, "writer_id").expect("writer_id"),
        }
    }

    /// The seqs of `log/<generation>/`, which must be exactly `1..=n` (no gap, no fork).
    pub fn assert_log_contiguous(&self, generation: u64) -> u64 {
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

impl Drop for Ctx {
    /// Delete every key under the prefix, also after a failure unless `keep_failed`, which
    /// prints the prefix instead; `keep_all` keeps it. Never panics (it may run while a
    /// failed assertion unwinds): errors are printed. Tests declare `Ctx` first, so their
    /// `Soak` (killed) and `Outage` (RustFS restored) drop before it.
    fn drop(&mut self) {
        if self.keep_all {
            return;
        }
        let prefix = self.prefix();
        if std::thread::panicking() && self.keep_failed {
            eprintln!(
                "{}: failed; bucket prefix {prefix}/ kept (BUCKWAL_KEEP_FAILED=1)",
                self.name
            );
            return;
        }
        // Retried for up to 30 s: right after an `Outage` restarts RustFS, it answers 503
        // ("waiting for iam") for a few seconds.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let deleted = match self.try_store() {
                Ok(store) => delete_prefix(&store, &prefix),
                Err(e) => Err(format!("S3 store: {e}")),
            };
            let Err(e) = deleted else { return };
            if Instant::now() >= deadline {
                eprintln!("{}: cleanup of bucket prefix {prefix}/: {e}", self.name);
                return;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }
}

/// What `restore` and `verify --acked` reported.
#[derive(Debug)]
pub struct Verified {
    /// The `restored …` line.
    pub restored: String,
    pub acked: usize,
    pub present: usize,
    pub unacked_present: usize,
}

pub fn field(line: &str, key: &str) -> usize {
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
pub fn restore_verify(ctx: &Ctx, lines: &[String]) -> Verified {
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

// ---------------------------------------------------------------------------------------
// Replica helpers (`replica.rs`): an in-process writer and replica, a `follow` child, the
// notification listener's access settings and the S3-format event the tests post to it.
// ---------------------------------------------------------------------------------------

/// The table `soak` creates and `verify --acked` reads.
pub const CREATE_SOAK: &str =
    "CREATE TABLE IF NOT EXISTS soak(id INTEGER PRIMARY KEY, payload BLOB)";

/// Poll `f` every 20 ms until it returns `Some`; panic with `what` after `limit`.
pub fn wait_until<T>(what: &str, limit: Duration, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(v) = f() {
            return v;
        }
        if Instant::now() >= deadline {
            panic!("timed out after {limit:?} waiting for {what}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// `cfg` with the short lease and grace the takeover tests use (`short_lease_ctx` in
/// `system.rs`): a 1 s lease, 2 s retry deadline and request timeout, 6 s grace.
pub fn short_lease(mut cfg: Config) -> Config {
    cfg.lease = Duration::from_millis(1000);
    cfg.retry_deadline = Duration::from_millis(2000);
    cfg.request_timeout = Duration::from_millis(2000);
    cfg.trim_grace = Duration::from_secs(6);
    cfg
}

/// The writer of a replica test, in process: one connection, the `soak` table, and the ids
/// whose commits returned (`acked`, the replica's expected rows).
pub struct Writer {
    db: Option<buckwal::Database>,
    conn: Option<buckwal::Connection>,
    pub acked: Vec<i64>,
}

impl Writer {
    /// Open (creating, or taking over a bucket that has a database) and create the table.
    pub fn open(cfg: Config) -> Writer {
        Writer::from_db(buckwal::open(cfg).expect("open writer"))
    }

    /// Like `open`, over `store` (for example a `GateStore`).
    pub fn open_with_store(cfg: Config, store: std::sync::Arc<dyn ObjectStore>) -> Writer {
        Writer::from_db(buckwal::open_with_store(cfg, store).expect("open writer"))
    }

    fn from_db(db: buckwal::Database) -> Writer {
        let conn = db.connect().expect("writer connection");
        conn.execute(CREATE_SOAK, &[]).expect("create soak");
        Writer {
            db: Some(db),
            conn: Some(conn),
            acked: Vec::new(),
        }
    }

    pub fn conn(&self) -> &buckwal::Connection {
        self.conn.as_ref().expect("writer open")
    }

    /// Insert row `id` in its own commit; acked only when the commit returned.
    pub fn try_insert(&mut self, id: i64) -> Result<(), buckwal::Error> {
        self.conn().execute(
            "INSERT INTO soak(id, payload) VALUES (?, zeroblob(1024))",
            &[buckwal::Value::from_i64(id)],
        )?;
        self.acked.push(id);
        Ok(())
    }

    pub fn insert(&mut self, id: i64) {
        self.try_insert(id)
            .unwrap_or_else(|e| panic!("insert {id}: {e}"));
    }

    pub fn insert_range(&mut self, ids: std::ops::RangeInclusive<i64>) {
        for id in ids {
            self.insert(id);
        }
    }

    /// `ids` in one transaction of `bytes`-byte payloads; acked at the COMMIT.
    pub fn insert_bulk(&mut self, ids: std::ops::RangeInclusive<i64>, bytes: usize) {
        let c = self.conn();
        c.execute("BEGIN", &[]).expect("begin");
        for id in ids.clone() {
            c.execute(
                &format!("INSERT INTO soak(id, payload) VALUES ({id}, zeroblob({bytes}))"),
                &[],
            )
            .unwrap_or_else(|e| panic!("bulk insert {id}: {e}"));
        }
        c.execute("COMMIT", &[]).expect("commit");
        self.acked.extend(ids);
    }

    /// TRUNCATE checkpoint: ends the generation (a seal) before it returns.
    pub fn seal(&self) {
        self.conn()
            .checkpoint(buckwal::CheckpointMode::Truncate)
            .expect("seal");
    }

    pub fn sum(&self) -> i64 {
        self.acked.iter().sum()
    }

    /// Close (a writer's close seals); the error a fenced writer gets is returned.
    pub fn close(mut self) -> Result<(), buckwal::Error> {
        let conn = self.conn.take().expect("open");
        let db = self.db.take().expect("open");
        let first = conn.close();
        db.close().and(first)
    }
}

/// The parts of a replica's position file (`app.db-replica`) the tests look at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PosFile {
    pub generation: u64,
    pub stage: String,
    pub offset: i64,
}

/// The position file in `dir`; `None` when absent or not parseable (it is replaced atomically,
/// so a half-written one is not seen).
pub fn read_position(dir: &Path) -> Option<PosFile> {
    let text = fs::read_to_string(dir.join("app.db-replica")).ok()?;
    let get = |k: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(&format!("{k}=")))
            .map(str::to_string)
    };
    Some(PosFile {
        generation: get("generation")?.parse().ok()?,
        stage: get("stage")?,
        offset: get("offset")?.parse().ok()?,
    })
}

/// An in-process replica over a counting store (the S3 store's request counts).
pub struct Rep {
    pub replica: Option<buckwal::Replica>,
    pub store: std::sync::Arc<dyn ObjectStore>,
    pub dir: PathBuf,
}

impl Rep {
    /// A replica in `dir` with the slow poll `poll`.
    pub fn open(ctx: &Ctx, dir: &Path, poll: Duration) -> Rep {
        let mut cfg = ctx.config(dir);
        cfg.replica_poll_interval = poll;
        Rep::open_cfg(ctx, cfg)
    }

    pub fn open_cfg(ctx: &Ctx, cfg: Config) -> Rep {
        let store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(ctx.store());
        Rep::open_store(cfg, store)
    }

    pub fn open_store(cfg: Config, store: std::sync::Arc<dyn ObjectStore>) -> Rep {
        let dir = cfg.dir.clone();
        let replica = buckwal::open_replica_with_store(cfg, store.clone()).expect("open replica");
        Rep {
            replica: Some(replica),
            store,
            dir,
        }
    }

    pub fn r(&self) -> &buckwal::Replica {
        self.replica.as_ref().expect("replica open")
    }

    pub fn stats(&self) -> buckwal::ReplicaStats {
        self.r().stats()
    }

    pub fn query_int(&self, sql: &str) -> i64 {
        let c = self.r().connect().expect("replica connection");
        let v = c.query(sql, &[]).expect("replica query")[0][0].as_int();
        c.close().expect("close connection");
        v.unwrap_or_else(|| panic!("{sql}: no integer"))
    }

    pub fn count(&self) -> i64 {
        self.query_int("SELECT count(*) FROM soak")
    }

    pub fn sum(&self) -> i64 {
        self.query_int("SELECT coalesce(sum(id), 0) FROM soak")
    }

    pub fn integrity(&self) -> String {
        let c = self.r().connect().expect("replica connection");
        let rows = c.query("PRAGMA integrity_check", &[]).expect("integrity");
        c.close().expect("close connection");
        rows.iter()
            .map(|r| r[0].to_text().unwrap_or("?").to_string())
            .collect::<Vec<_>>()
            .join(";")
    }

    /// Wait until the replica holds exactly `n` rows.
    pub fn wait_count(&self, n: i64, limit: Duration) {
        wait_until(&format!("{n} rows on the replica"), limit, || {
            (self.count() == n).then_some(())
        });
    }

    /// Wait until the replica holds at least `n` rows; the count then.
    pub fn wait_at_least(&self, n: i64, limit: Duration) -> i64 {
        wait_until(&format!("at least {n} rows on the replica"), limit, || {
            Some(self.count()).filter(|c| *c >= n)
        })
    }

    /// Wait until the replica's rows are the writer's: count and id sum.
    pub fn wait_equal(&self, w: &Writer, limit: Duration) {
        self.wait_count(w.acked.len() as i64, limit);
        assert_eq!(self.sum(), w.sum(), "the replica's ids are the writer's");
    }

    pub fn waker(&self) -> buckwal::ReplicaWaker {
        self.r().waker()
    }

    pub fn close(mut self) {
        self.replica
            .take()
            .expect("open")
            .close()
            .expect("close replica");
    }
}

/// Which credentials a test sends the listener.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Auth {
    Bearer,
    /// User `sns`, the token as the password.
    Basic,
    None,
    Wrong,
}

/// How a test reaches a replica's notification listener: plain HTTP on loopback with no token,
/// or (in pass 2, `BUCKWAL_TLS_DIR` set) HTTPS with the CA of the material and a token file.
#[derive(Clone)]
pub struct NotifyAccess {
    pub material: Option<PathBuf>,
    pub token_file: Option<PathBuf>,
    /// Never printed.
    pub token: Option<String>,
}

impl NotifyAccess {
    pub fn of_pass(ctx: &Ctx) -> NotifyAccess {
        match std::env::var("BUCKWAL_TLS_DIR") {
            Ok(d) if !d.is_empty() => {
                let token = format!("ntok-{:032x}-{}", nanos(), std::process::id());
                let token_file = ctx.tmp.path().join("notify-token");
                fs::write(&token_file, format!("{token}\n")).expect("write the token file");
                NotifyAccess {
                    material: Some(d.into()),
                    token_file: Some(token_file),
                    token: Some(token),
                }
            }
            _ => NotifyAccess {
                material: None,
                token_file: None,
                token: None,
            },
        }
    }

    /// The `BUCKWAL_REPLICA_NOTIFY_*` settings, listening on loopback port 0.
    pub fn vars(&self) -> Vec<(String, String)> {
        let mut v = vec![(
            "BUCKWAL_REPLICA_NOTIFY_LISTEN".to_string(),
            "127.0.0.1:0".to_string(),
        )];
        if let Some(m) = &self.material {
            v.push((
                "BUCKWAL_REPLICA_NOTIFY_TLS_CERT".into(),
                m.join("server/rustfs_cert.pem").display().to_string(),
            ));
            v.push((
                "BUCKWAL_REPLICA_NOTIFY_TLS_KEY".into(),
                m.join("server/rustfs_key.pem").display().to_string(),
            ));
        }
        if let Some(t) = &self.token_file {
            v.push((
                "BUCKWAL_REPLICA_NOTIFY_TOKEN_FILE".into(),
                t.display().to_string(),
            ));
        }
        v
    }

    pub fn poster(&self, port: u16) -> Poster {
        Poster {
            access: self.clone(),
            port,
        }
    }
}

/// A notification listener's address, with the means to post events to it.
#[derive(Clone)]
pub struct Poster {
    access: NotifyAccess,
    pub port: u16,
}

/// An S3 `ObjectCreated` event for `key`.
pub fn s3_event(key: &str) -> String {
    format!(
        r#"{{"Records":[{{"eventName":"ObjectCreated:Put","s3":{{"object":{{"key":"{key}"}}}}}}]}}"#
    )
}

impl Poster {
    fn base(&self) -> String {
        match self.access.material {
            Some(_) => format!("https://localhost:{}", self.port),
            None => format!("http://127.0.0.1:{}", self.port),
        }
    }

    /// POST `body` to `/notify`; the status code. In pass 1 (no token) `auth` is ignored.
    pub fn post_body(&self, body: &str, auth: Auth) -> u16 {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut b = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy();
        if let Some(m) = &self.access.material {
            let pem = fs::read(m.join("ca.pem")).expect("read ca.pem");
            for c in reqwest::Certificate::from_pem_bundle(&pem).expect("ca.pem") {
                b = b.add_root_certificate(c);
            }
        }
        let client = b.build().expect("http client");
        let mut req = client
            .post(format!("{}/notify", self.base()))
            .header("content-type", "application/json")
            .body(body.to_string());
        if let Some(t) = &self.access.token {
            req = match auth {
                Auth::Bearer => req.bearer_auth(t),
                Auth::Basic => req.basic_auth("sns", Some(t)),
                Auth::Wrong => req.bearer_auth("not-the-token"),
                Auth::None => req,
            };
        }
        req.send()
            .unwrap_or_else(|e| panic!("post to the listener: {}", e.without_url()))
            .status()
            .as_u16()
    }

    /// POST the event for `key`; the status code.
    pub fn post(&self, key: &str, auth: Auth) -> u16 {
        self.post_body(&s3_event(key), auth)
    }

    /// POST the event for `key` and require 200.
    pub fn post_ok(&self, key: &str, auth: Auth) {
        let s = self.post(key, auth);
        assert_eq!(s, 200, "the listener answered {s} to the event for {key}");
    }
}

/// A listener run in the test process (the same `buckwal_server::notify` code `follow` runs),
/// stopped by `stop` or on drop.
pub struct Listener {
    pub poster: Poster,
    bound: Option<buckwal_server::notify::NotifyBound>,
    stop: std::sync::Arc<std::sync::Mutex<(bool, Option<std::task::Waker>)>>,
}

impl Listener {
    pub fn start(access: &NotifyAccess, waker: buckwal::ReplicaWaker) -> Listener {
        let vars = access.vars();
        let cfg = buckwal_server::notify::NotifyConfig::from_lookup(|k| {
            vars.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone())
        })
        .expect("notify settings")
        .expect("BUCKWAL_REPLICA_NOTIFY_LISTEN set");
        let stop = std::sync::Arc::new(std::sync::Mutex::new((false, None::<std::task::Waker>)));
        let s2 = stop.clone();
        let shutdown = std::future::poll_fn(move |cx| {
            let mut g = s2.lock().unwrap();
            if g.0 {
                return std::task::Poll::Ready(());
            }
            g.1 = Some(cx.waker().clone());
            std::task::Poll::Pending
        });
        let bound = buckwal_server::notify::serve(cfg, waker, shutdown).expect("start listener");
        assert_eq!(bound.tls(), access.material.is_some());
        assert_eq!(bound.auth(), access.token.is_some());
        Listener {
            poster: access.poster(bound.local_addr().port()),
            bound: Some(bound),
            stop,
        }
    }

    pub fn stop(&mut self) {
        {
            let mut g = self.stop.lock().unwrap();
            g.0 = true;
            if let Some(w) = g.1.take() {
                w.wake();
            }
        }
        if let Some(b) = self.bound.take() {
            let _ = b.join();
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The value after `key=` in a status line.
pub fn kv<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|kv| kv.strip_prefix(&format!("{key}=")))
}

/// A running `buckwal follow`, its stdout lines read on a thread.
pub struct Follower {
    child: std::process::Child,
    rx: std::sync::mpsc::Receiver<String>,
    pub lines: Vec<String>,
    stderr: PathBuf,
    /// The port of its `notify listening` line, when it has a listener.
    pub poster: Option<Poster>,
}

impl Follower {
    /// `follow --dir <dir> --poll-ms <poll_ms>`, with the listener `notify` describes when
    /// given; returns once it printed `following`.
    pub fn spawn(ctx: &Ctx, dir: &Path, poll_ms: u64, notify: Option<&NotifyAccess>) -> Follower {
        let poll = poll_ms.to_string();
        let mut cmd = ctx.command(&["follow", "--dir", dir.to_str().unwrap(), "--poll-ms", &poll]);
        if let Some(n) = notify {
            cmd.envs(n.vars());
        }
        let stderr = ctx.tmp.path().join(format!("follow-{}.stderr", nanos()));
        let mut child = cmd
            .stdout(std::process::Stdio::piped())
            .stderr(fs::File::create(&stderr).unwrap())
            .spawn()
            .expect("spawn follow");
        let out = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        let mut f = Follower {
            child,
            rx,
            lines: Vec::new(),
            stderr,
            poster: None,
        };
        f.wait_line(0, "following", Duration::from_secs(120), |l| {
            l.starts_with("following ")
        });
        if let Some(n) = notify {
            let found = f
                .lines
                .iter()
                .find(|l| l.starts_with("notify listening "))
                .cloned();
            let l = found.unwrap_or_else(|| panic!("no notify listening line\n{}", f.tail()));
            let port = kv(&l, "addr")
                .and_then(|a| a.rsplit_once(':'))
                .and_then(|(_, p)| p.parse().ok())
                .unwrap_or_else(|| panic!("no port in {l:?}"));
            assert!(
                l.ends_with(&format!(
                    " tls={} auth={}",
                    n.material.is_some(),
                    n.token.is_some()
                )),
                "{l:?}"
            );
            f.poster = Some(n.poster(port));
        }
        f
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    fn pump(&mut self) {
        while let Ok(l) = self.rx.try_recv() {
            self.lines.push(l);
        }
    }

    /// How many lines at or after `from` `p` accepts, counting what has arrived so far.
    pub fn count_lines(&mut self, from: usize, p: impl Fn(&str) -> bool) -> usize {
        self.pump();
        self.lines[from.min(self.lines.len())..]
            .iter()
            .filter(|l| p(l))
            .count()
    }

    pub fn tail(&mut self) -> String {
        self.pump();
        let from = self.lines.len().saturating_sub(20);
        format!(
            "last stdout lines: {:?}\nstderr:\n{}",
            &self.lines[from..],
            fs::read_to_string(&self.stderr).unwrap_or_default()
        )
    }

    /// The first line at or after index `from` that `p` accepts, waiting up to `limit`.
    pub fn wait_line(
        &mut self,
        from: usize,
        what: &str,
        limit: Duration,
        p: impl Fn(&str) -> bool,
    ) -> String {
        let deadline = Instant::now() + limit;
        loop {
            self.pump();
            if let Some(l) = self.lines[from.min(self.lines.len())..]
                .iter()
                .find(|l| p(l))
            {
                return l.clone();
            }
            if let Some(s) = self.child.try_wait().expect("try_wait") {
                self.pump();
                if let Some(l) = self.lines[from.min(self.lines.len())..]
                    .iter()
                    .find(|l| p(l))
                {
                    return l.clone();
                }
                panic!("follow exited ({s}) before {what}\n{}", self.tail());
            }
            if Instant::now() >= deadline {
                panic!("follow: no {what} within {limit:?}\n{}", self.tail());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Wait for `applied generation=<g> seq=<seq>` (from line `from`).
    pub fn wait_applied(&mut self, from: usize, generation: u64, seq: u64, limit: Duration) {
        let want = format!("applied generation={generation} seq={seq}");
        self.wait_line(from, &want, limit, |l| l == want);
    }

    /// SIGKILL by pid (also callable from another thread, with `pid()`), then reap.
    pub fn kill9(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.pump();
    }

    /// SIGTERM; the exit status once it has stopped (at most 30 s).
    pub fn sigterm(&mut self) -> ExitStatus {
        let pid = self.child.id().to_string();
        let st = Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .expect("kill");
        assert!(st.success(), "kill -TERM {pid}");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(s) = self.child.try_wait().expect("try_wait") {
                self.pump();
                return s;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                panic!("follow did not exit within 30s of SIGTERM");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// `kill -<sig> <pid>` (`STOP`, `CONT`).
    pub fn signal(&self, sig: &str) {
        let st = Command::new("kill")
            .args([&format!("-{sig}"), &self.child.id().to_string()])
            .status()
            .expect("kill");
        assert!(st.success(), "kill -{sig} {}", self.child.id());
    }

    /// Everything the child wrote to stderr so far.
    pub fn stderr_text(&self) -> String {
        fs::read_to_string(&self.stderr).unwrap_or_default()
    }

    /// Whether the child is still running.
    pub fn running(&mut self) -> bool {
        self.child.try_wait().expect("try_wait").is_none()
    }
}

impl Drop for Follower {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A running `buckwal soak`, its stdout lines collected on a thread.
pub struct SoakProc {
    child: std::process::Child,
    reader: Option<std::thread::JoinHandle<()>>,
    lines: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl SoakProc {
    pub fn spawn(ctx: &Ctx, dir: &Path, args: &[&str]) -> SoakProc {
        let mut all = vec!["soak", "--dir", dir.to_str().unwrap()];
        all.extend_from_slice(args);
        let stderr = ctx.tmp.path().join(format!("soak-{}.stderr", nanos()));
        let mut child = ctx
            .command(&all)
            .stdout(std::process::Stdio::piped())
            .stderr(fs::File::create(&stderr).unwrap())
            .spawn()
            .expect("spawn soak");
        let out = child.stdout.take().unwrap();
        let lines = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let l2 = lines.clone();
        let reader = std::thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                l2.lock().unwrap().push(line);
            }
        });
        SoakProc {
            child,
            reader: Some(reader),
            lines,
        }
    }

    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }

    /// The ids of the `ack <id>` lines so far.
    pub fn acked(&self) -> Vec<i64> {
        self.lines()
            .iter()
            .filter_map(|l| l.strip_prefix("ack ")?.trim().parse().ok())
            .collect()
    }

    /// The exit status once the process has exited; its stdout reader is joined first, so
    /// every line it printed is in `lines()`.
    pub fn exited(&mut self) -> Option<ExitStatus> {
        let s = self.child.try_wait().expect("try_wait")?;
        if let Some(r) = self.reader.take() {
            let _ = r.join();
        }
        Some(s)
    }

    pub fn wait_acks(&mut self, n: usize, limit: Duration) {
        wait_until(&format!("{n} soak acks"), limit, || {
            (self.acked().len() >= n).then_some(())
        });
    }
}

impl Drop for SoakProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A store that holds back one `put_if_absent` of a `log/` record until released, and keeps
/// a copy of its key and body: a writer whose append is "in flight" while another machine
/// takes over (the late record of a fenced writer, with no hook in the writer's own path).
pub struct GateStore {
    inner: std::sync::Arc<dyn ObjectStore>,
    state: std::sync::Mutex<GateState>,
    cv: std::sync::Condvar,
}

#[derive(Default)]
struct GateState {
    armed: bool,
    released: bool,
    held: Option<(String, Vec<u8>)>,
    /// Every key held so far: a retry of the same append blocks too, so it cannot win the
    /// seq before the other machine's fence.
    held_keys: Vec<String>,
}

impl GateStore {
    pub fn new(inner: std::sync::Arc<dyn ObjectStore>) -> GateStore {
        GateStore {
            inner,
            state: Default::default(),
            cv: Default::default(),
        }
    }

    /// The next `put_if_absent` of a `log/` key blocks until `release`.
    pub fn arm(&self) {
        *self.state.lock().unwrap() = GateState {
            armed: true,
            ..Default::default()
        };
    }

    /// The key and body of the held put, once a writer is blocked on it.
    pub fn wait_held(&self, limit: Duration) -> (String, Vec<u8>) {
        wait_until("a log put to be held", limit, || {
            self.state.lock().unwrap().held.clone()
        })
    }

    /// Let the held put through (it then runs against the real store, and may be refused).
    pub fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.cv.notify_all();
    }
}

impl ObjectStore for GateStore {
    fn put(&self, key: &str, body: &[u8]) -> Result<String, buckwal::store::StoreError> {
        self.inner.put(key, body)
    }

    fn put_if_absent(
        &self,
        key: &str,
        body: &[u8],
    ) -> Result<buckwal::store::CondPut, buckwal::store::StoreError> {
        let mut g = self.state.lock().unwrap();
        let again = g.held_keys.iter().any(|k| k == key);
        if (g.armed && key.contains("/log/")) || again {
            g.armed = false;
            if !again {
                g.held = Some((key.to_string(), body.to_vec()));
                g.held_keys.push(key.to_string());
            }
            // Bounded, so a test that failed before `release` does not leave this thread forever.
            let deadline = Instant::now() + Duration::from_secs(120);
            while !g.released && Instant::now() < deadline {
                g = self.cv.wait_timeout(g, Duration::from_secs(1)).unwrap().0;
            }
        }
        drop(g);
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

/// The setting that makes a bucket encrypted.
pub const KEY_FILE_VAR: &str = "BUCKWAL_ENCRYPTION_KEY_FILE";

/// A key file (64 hex digits, a test key) in the test's temp directory.
pub fn write_key_file(ctx: &Ctx, tag: &str) -> PathBuf {
    let p = ctx.tmp.path().join(format!("key-{tag}"));
    let a = nanos();
    let b = nanos().rotate_left(29) ^ (std::process::id() as u128) ^ ((tag.len() as u128) << 70);
    fs::write(&p, format!("{a:032x}{b:032x}\n")).expect("write a key file");
    p
}
