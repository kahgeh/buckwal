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
/// `env()`). `DURABLE_OTEL*` and `DURABLE_TEST_RUN_ID` are not here: `scripts/test.sh` sets
/// them so the children export under its run id.
pub const OPTIONAL: [&str; 25] = [
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
    "DURABLE_LEASE_MS",
    "DURABLE_GC_AFTER_SEAL",
    "DURABLE_CA_BUNDLE",
    "DURABLE_ENCRYPTION_KEY_FILE",
    "DURABLE_ENCRYPTION_CIPHER",
    "DURABLE_JOURNAL_MODE",
    "DURABLE_SERVE_LISTEN",
    "DURABLE_SERVE_TLS_CERT",
    "DURABLE_SERVE_TLS_KEY",
    "DURABLE_SERVE_TOKEN_FILE",
    "DURABLE_SERVE_STREAM_TIMEOUT_MS",
    "DURABLE_SERVE_SHUTDOWN_MS",
    "DURABLE_SERVE_BUSY_TIMEOUT_MS",
    "DURABLE_SERVE_MAX_STREAMS",
    "DURABLE_SERVE_WORKERS",
];

/// Passed through from the caller when set: the second pass of `scripts/test.sh` runs every
/// test over HTTPS with encryption.
pub const PASSED_THROUGH: [&str; 3] = [
    "DURABLE_CA_BUNDLE",
    "DURABLE_ENCRYPTION_KEY_FILE",
    "DURABLE_ENCRYPTION_CIPHER",
];

/// The `DURABLE_*` connection settings, with the defaults of `scripts/rustfs.sh env`, then
/// the `PASSED_THROUGH` ones the caller set.
pub fn env() -> Vec<(String, String)> {
    let set = PASSED_THROUGH
        .iter()
        .filter_map(|k| Some((k.to_string(), std::env::var(k).ok()?)));
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
    /// Keep the prefix when the test fails (`DURABLE_KEEP_FAILED=1`).
    pub keep_failed: bool,
    /// Keep the prefix, pass or fail (`DURABLE_KEEP_PREFIXES=1`): `scripts/test.sh` scans
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
        vars.push(("DURABLE_PREFIX".into(), prefix(name)));
        Ctx {
            name: name.to_string(),
            vars,
            tmp: tempfile::tempdir().unwrap(),
            keep_failed: std::env::var("DURABLE_KEEP_FAILED").as_deref() == Ok("1"),
            keep_all: std::env::var("DURABLE_KEEP_PREFIXES").as_deref() == Ok("1"),
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
        self.var("DURABLE_PREFIX")
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
            &self.var("DURABLE_ENDPOINT"),
            &self.var("DURABLE_BUCKET"),
            &self.var("DURABLE_ACCESS_KEY"),
            &self.var("DURABLE_SECRET_KEY"),
        );
        c.region = self.var("DURABLE_REGION");
        c.path_style = self.var("DURABLE_PATH_STYLE") == "true";
        c.prefix = self.prefix();
        let set = |k: &str| self.opt_var(k).filter(|v| !v.is_empty());
        c.ca_bundle = set("DURABLE_CA_BUNDLE").map(Into::into);
        c.encryption_key_file = set("DURABLE_ENCRYPTION_KEY_FILE").map(Into::into);
        if let Some(cipher) = set("DURABLE_ENCRYPTION_CIPHER") {
            c.encryption_cipher = cipher;
        }
        c
    }

    pub fn try_store(&self) -> Result<S3Store, buckwal::Error> {
        S3Store::new(S3Config {
            endpoint: self.var("DURABLE_ENDPOINT"),
            region: self.var("DURABLE_REGION"),
            bucket: self.var("DURABLE_BUCKET"),
            access_key: self.var("DURABLE_ACCESS_KEY"),
            secret_key: self.var("DURABLE_SECRET_KEY"),
            path_style: self.var("DURABLE_PATH_STYLE") == "true",
            request_timeout: Duration::from_secs(30),
            ca_bundle: self
                .opt_var("DURABLE_CA_BUNDLE")
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
                "{}: failed; bucket prefix {prefix}/ kept (DURABLE_KEEP_FAILED=1)",
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
