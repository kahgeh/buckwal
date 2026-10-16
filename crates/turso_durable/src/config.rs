//! Public configuration: [`Config`] and its defaults. `Event` and `EventHook` (the
//! `on_event` field) are test and diagnostics hooks, hidden from the docs.
use crate::error::{Error, Result};
use crate::format::Keys;
use crate::store::s3::{S3Config, S3Store};
use crate::store::{ObjectStore, RetryPolicy};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// The value every optional [`Config`] field takes when it is not set.
pub mod defaults {
    use std::time::Duration;

    /// How long one bucket call (with its retries) may take before it fails.
    pub const RETRY_DEADLINE: Duration = Duration::from_secs(30);
    /// The first sleep after a transient bucket error; each later sleep doubles. Fixed: no
    /// `Config` field or variable sets it.
    pub const FIRST_BACKOFF: Duration = Duration::from_millis(50);
    /// The longest sleep between two retries. Fixed: no `Config` field or variable sets it.
    pub const MAX_BACKOFF: Duration = Duration::from_secs(2);
    /// How long one HTTP request may take.
    pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
    /// Parallel uploads in a seal, and parallel log fetches in a restore.
    pub const UPLOAD_CONCURRENCY: usize = 16;
    /// How long an unreferenced object stays in the bucket before the trimmer deletes it.
    pub const TRIM_GRACE: Duration = Duration::from_secs(10 * 60);
    /// Fetch every sealed page in the background after open.
    pub const PREFETCH: bool = false;
    pub const REGION: &str = "us-east-1";
    pub const PATH_STYLE: bool = true;
    /// Keys go at the root of the bucket.
    pub const PREFIX: &str = "";
}

/// Where a database lives: its local directory and its bucket.
#[derive(Clone)]
pub struct Config {
    /// The local directory: `app.db`, its `-wal` and turso_durable's own files.
    pub dir: PathBuf,
    /// `http://host[:port]`; https is not supported yet.
    pub endpoint: String,
    pub region: String,
    /// Path-style addressing (`endpoint/bucket/key`) rather than virtual-hosted.
    pub path_style: bool,
    pub bucket: String,
    /// Every key of this database goes under `<prefix>/` ("" = the bucket root). Only
    /// `[A-Za-z0-9._/-]`, with no leading, trailing or doubled `/` and no `.` or `..` segment
    /// (`Config` otherwise).
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
    pub retry_deadline: Duration,
    pub request_timeout: Duration,
    pub upload_concurrency: usize,
    pub trim_grace: Duration,
    pub prefetch: bool,
    /// A test and diagnostics hook, not part of the supported API (hidden from the docs).
    #[doc(hidden)]
    pub on_event: Option<EventHook>,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("dir", &self.dir)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("path_style", &self.path_style)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .field("retry_deadline", &self.retry_deadline)
            .field("request_timeout", &self.request_timeout)
            .field("upload_concurrency", &self.upload_concurrency)
            .field("trim_grace", &self.trim_grace)
            .field("prefetch", &self.prefetch)
            .field("on_event", &self.on_event.as_ref().map(|_| "<hook>"))
            .finish()
    }
}

/// Every environment variable [`Config::from_env`] reads.
const ENV_VARS: [&str; 12] = [
    "DURABLE_ENDPOINT",
    "DURABLE_REGION",
    "DURABLE_PATH_STYLE",
    "DURABLE_BUCKET",
    "DURABLE_PREFIX",
    "DURABLE_ACCESS_KEY",
    "DURABLE_SECRET_KEY",
    "DURABLE_RETRY_DEADLINE_MS",
    "DURABLE_REQUEST_TIMEOUT_MS",
    "DURABLE_UPLOAD_CONCURRENCY",
    "DURABLE_TRIM_GRACE_SECS",
    "DURABLE_PREFETCH",
];

fn parse_bool(var: &str, v: &str) -> Result<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        _ => Err(Error::Config(format!(
            "{var} must be 0/1 or true/false, not {v:?}"
        ))),
    }
}

fn parse_num<T: std::str::FromStr>(var: &str, v: &str) -> Result<T> {
    v.trim()
        .parse()
        .map_err(|_| Error::Config(format!("{var} must be a non-negative integer, not {v:?}")))
}

/// A prefix is "" or `/`-separated non-empty segments of `[A-Za-z0-9._-]`, none of them `.`
/// or `..`. Keys must come back from a LIST spelled exactly as they were written; these
/// characters need no XML or URL escaping anywhere, and no segment is a path step that an
/// HTTP stack might normalise away.
fn validate_prefix(prefix: &str) -> Result<()> {
    if prefix.is_empty() {
        return Ok(());
    }
    if let Some(c) = prefix
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '/' | '-')))
    {
        return Err(Error::Config(format!(
            "prefix {prefix:?} holds {c:?}: only A-Z, a-z, 0-9, '.', '_', '-' and '/' are allowed"
        )));
    }
    if prefix.split('/').any(str::is_empty) {
        return Err(Error::Config(format!(
            "prefix {prefix:?} must not start or end with '/' or hold '//'"
        )));
    }
    if prefix.split('/').any(|s| s == "." || s == "..") {
        return Err(Error::Config(format!(
            "prefix {prefix:?} must not hold a '.' or '..' segment"
        )));
    }
    Ok(())
}

impl Config {
    /// A configuration with every optional setting at its [`defaults`].
    pub fn new(
        dir: impl Into<PathBuf>,
        endpoint: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Config {
        Config {
            dir: dir.into(),
            endpoint: endpoint.to_string(),
            region: defaults::REGION.to_string(),
            path_style: defaults::PATH_STYLE,
            bucket: bucket.to_string(),
            prefix: defaults::PREFIX.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            retry_deadline: defaults::RETRY_DEADLINE,
            request_timeout: defaults::REQUEST_TIMEOUT,
            upload_concurrency: defaults::UPLOAD_CONCURRENCY,
            trim_grace: defaults::TRIM_GRACE,
            prefetch: defaults::PREFETCH,
            on_event: None,
        }
    }

    /// Read the configuration from `DURABLE_*` environment variables. `DURABLE_ENDPOINT`,
    /// `DURABLE_BUCKET`, `DURABLE_ACCESS_KEY` and `DURABLE_SECRET_KEY` are required; every
    /// other one falls back to its [`defaults`] value.
    pub fn from_env(dir: impl Into<PathBuf>) -> Result<Config> {
        Config::from_lookup(dir, |k| std::env::var(k).ok())
    }

    fn from_lookup(
        dir: impl Into<PathBuf>,
        get: impl Fn(&str) -> Option<String>,
    ) -> Result<Config> {
        debug_assert!(ENV_VARS.iter().all(|v| v.starts_with("DURABLE_")));
        let required = |k: &str| {
            get(k)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| Error::Config(format!("{k} is not set")))
        };
        let mut c = Config::new(
            dir,
            &required("DURABLE_ENDPOINT")?,
            &required("DURABLE_BUCKET")?,
            &required("DURABLE_ACCESS_KEY")?,
            &required("DURABLE_SECRET_KEY")?,
        );
        if let Some(v) = get("DURABLE_REGION") {
            c.region = v;
        }
        if let Some(v) = get("DURABLE_PATH_STYLE") {
            c.path_style = parse_bool("DURABLE_PATH_STYLE", &v)?;
        }
        if let Some(v) = get("DURABLE_PREFIX") {
            c.prefix = v;
        }
        if let Some(v) = get("DURABLE_RETRY_DEADLINE_MS") {
            c.retry_deadline = Duration::from_millis(parse_num("DURABLE_RETRY_DEADLINE_MS", &v)?);
        }
        if let Some(v) = get("DURABLE_REQUEST_TIMEOUT_MS") {
            c.request_timeout = Duration::from_millis(parse_num("DURABLE_REQUEST_TIMEOUT_MS", &v)?);
        }
        if let Some(v) = get("DURABLE_UPLOAD_CONCURRENCY") {
            c.upload_concurrency = parse_num("DURABLE_UPLOAD_CONCURRENCY", &v)?;
        }
        if let Some(v) = get("DURABLE_TRIM_GRACE_SECS") {
            c.trim_grace = Duration::from_secs(parse_num("DURABLE_TRIM_GRACE_SECS", &v)?);
        }
        if let Some(v) = get("DURABLE_PREFETCH") {
            c.prefetch = parse_bool("DURABLE_PREFETCH", &v)?;
        }
        Ok(c)
    }

    /// The checks `open` and `restore` make before touching anything.
    pub(crate) fn validate(&self) -> Result<()> {
        if self
            .endpoint
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("https://")
        {
            return Err(Error::Unsupported("https"));
        }
        if self.bucket.is_empty() {
            return Err(Error::Config("bucket is empty".into()));
        }
        if self.upload_concurrency == 0 {
            return Err(Error::Config(
                "upload_concurrency must be at least 1".into(),
            ));
        }
        validate_prefix(&self.prefix)
    }

    pub(crate) fn keys(&self) -> Keys {
        Keys::new(self.prefix.trim_matches('/'))
    }

    pub(crate) fn engine(&self, read_only: bool) -> EngineConfig {
        EngineConfig {
            retry: RetryPolicy {
                deadline: self.retry_deadline,
                first_backoff: defaults::FIRST_BACKOFF,
                max_backoff: defaults::MAX_BACKOFF,
            },
            upload_concurrency: self.upload_concurrency,
            prefetch: self.prefetch,
            trim_grace: self.trim_grace,
            events: self.on_event.clone(),
            read_only,
        }
    }

    /// The S3 store this configuration names.
    pub(crate) fn s3_store(&self) -> Result<Arc<dyn ObjectStore>> {
        self.validate()?;
        Ok(Arc::new(S3Store::new(S3Config {
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            path_style: self.path_style,
            request_timeout: self.request_timeout,
        })?))
    }
}

/// Called synchronously, on the thread that did the work, for each [`Event`]. A test and
/// diagnostics hook, not part of the supported API (hidden from the docs).
#[doc(hidden)]
pub type EventHook = Arc<dyn Fn(&Event) + Send + Sync>;

/// Progress notifications: a test and diagnostics hook, not part of the supported API
/// (hidden from the docs).
#[doc(hidden)]
#[derive(Debug, Clone)]
pub enum Event {
    /// A log object landed at `log/<generation>/<seq>`; `bytes` is its encoded size.
    LogUploaded {
        generation: u64,
        seq: u64,
        bytes: u64,
    },
    /// Sealing `generation` began with `pages` dirty pages to upload.
    SealStarted { generation: u64, pages: u64 },
    /// `generation` is sealed: the head now points at generation + 1.
    SealFinished { generation: u64, elapsed: Duration },
    /// The trimmer deleted `deleted` objects no head can reach any more.
    Trimmed { deleted: u64 },
}

impl EngineConfig {
    /// A writer idle this long checks the head before its next append
    /// (`Bucket::set_idle_threshold`, `Bucket::check_idle`): half the trim grace.
    pub(crate) fn idle_threshold(&self) -> Duration {
        self.trim_grace / 2
    }
}

/// How the engine talks to the bucket.
#[derive(Clone)]
pub(crate) struct EngineConfig {
    /// Every bucket call, page fetches included.
    pub retry: RetryPolicy,
    /// Parallel log GETs in a restore; the caller builds its `Bucket` with it too (parallel
    /// page uploads in a seal).
    pub upload_concurrency: usize,
    /// Fetch every sealed page in the background after open.
    pub prefetch: bool,
    /// The trimmer's grace; the writer's idle self-check runs after half of it
    /// (`Bucket::set_idle_threshold`).
    pub trim_grace: Duration,
    pub events: Option<EventHook>,
    /// A read-only open: nothing may be written to the bucket, and the engine's writes are
    /// refused (`ShippingFile`, `BucketDbStorage`).
    pub read_only: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The idle check's margin is exactly half the trim grace (`Bucket::check_idle`).
    #[test]
    fn idle_threshold_is_half_the_trim_grace() {
        let mut c = Config::new("/tmp/x", "http://127.0.0.1:1", "b", "k", "s");
        c.trim_grace = Duration::from_millis(200);
        assert_eq!(c.engine(false).idle_threshold(), Duration::from_millis(100));
        c.trim_grace = Duration::ZERO;
        assert_eq!(c.engine(false).idle_threshold(), Duration::ZERO);
        c.trim_grace = Duration::from_secs(600);
        assert_eq!(c.engine(false).idle_threshold(), Duration::from_secs(300));
    }
    use std::collections::HashMap;

    fn required() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("DURABLE_ENDPOINT", "http://127.0.0.1:9000"),
            ("DURABLE_BUCKET", "b"),
            ("DURABLE_ACCESS_KEY", "ak"),
            ("DURABLE_SECRET_KEY", "sk"),
        ])
    }

    #[test]
    fn config_new_uses_the_defaults() {
        let c = Config::new("/d", "http://e", "b", "ak", "sk");
        assert_eq!(c.region, "us-east-1");
        assert!(c.path_style);
        assert_eq!(c.prefix, "");
        assert_eq!(c.retry_deadline, Duration::from_secs(30));
        assert_eq!(c.request_timeout, Duration::from_secs(10));
        assert_eq!(c.upload_concurrency, 16);
        assert_eq!(c.trim_grace, Duration::from_secs(600));
        assert!(!c.prefetch);
        assert!(!format!("{c:?}").contains("sk\""), "the secret is printed");
        let mut c = c;
        c.retry_deadline = Duration::from_millis(1234);
        let r = c.engine(false).retry;
        assert_eq!(
            (r.deadline, r.first_backoff, r.max_backoff),
            (
                Duration::from_millis(1234),
                Duration::from_millis(50),
                Duration::from_secs(2)
            )
        );
    }

    #[test]
    fn config_from_env_reads_every_setting() {
        let all = [
            ("DURABLE_ENDPOINT", "http://127.0.0.1:9000"),
            ("DURABLE_REGION", "eu-west-2"),
            ("DURABLE_PATH_STYLE", "false"),
            ("DURABLE_BUCKET", "bkt"),
            ("DURABLE_PREFIX", "dbs/one"),
            ("DURABLE_ACCESS_KEY", "ak"),
            ("DURABLE_SECRET_KEY", "sk"),
            ("DURABLE_RETRY_DEADLINE_MS", "1500"),
            ("DURABLE_REQUEST_TIMEOUT_MS", "250"),
            ("DURABLE_UPLOAD_CONCURRENCY", "3"),
            ("DURABLE_TRIM_GRACE_SECS", "7"),
            ("DURABLE_PREFETCH", "1"),
        ];
        assert_eq!(all.len(), ENV_VARS.len());
        // No other unit test reads DURABLE_* variables.
        for (k, v) in all {
            std::env::set_var(k, v);
        }
        let c = Config::from_env("/data/db");
        for k in ENV_VARS {
            std::env::remove_var(k);
        }
        let c = c.unwrap();
        assert_eq!(c.dir, PathBuf::from("/data/db"));
        assert_eq!(c.endpoint, "http://127.0.0.1:9000");
        assert_eq!(c.region, "eu-west-2");
        assert!(!c.path_style);
        assert_eq!(c.bucket, "bkt");
        assert_eq!(c.prefix, "dbs/one");
        assert_eq!(c.access_key, "ak");
        assert_eq!(c.secret_key, "sk");
        assert_eq!(c.retry_deadline, Duration::from_millis(1500));
        assert_eq!(c.request_timeout, Duration::from_millis(250));
        assert_eq!(c.upload_concurrency, 3);
        assert_eq!(c.trim_grace, Duration::from_secs(7));
        assert!(c.prefetch);

        // Only the four required ones: the rest are defaults.
        let env = required();
        let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(c.region, defaults::REGION);
        assert_eq!(c.upload_concurrency, defaults::UPLOAD_CONCURRENCY);

        for missing in [
            "DURABLE_ENDPOINT",
            "DURABLE_BUCKET",
            "DURABLE_ACCESS_KEY",
            "DURABLE_SECRET_KEY",
        ] {
            let mut env = required();
            env.remove(missing);
            match Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())) {
                Err(Error::Config(m)) => assert!(m.contains(missing), "{m}"),
                r => panic!("{missing}: {r:?}"),
            }
        }
        let mut env = required();
        env.insert("DURABLE_PREFETCH", "maybe");
        match Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())) {
            Err(Error::Config(m)) => assert!(m.contains("DURABLE_PREFETCH"), "{m}"),
            r => panic!("{r:?}"),
        }
    }

    #[test]
    fn config_validation() {
        let c = Config::new("/d", "https://s3.example.com", "b", "ak", "sk");
        assert_eq!(c.validate(), Err(Error::Unsupported("https")));
        let c = Config::new("/d", "http://e", "", "ak", "sk");
        assert!(matches!(c.validate(), Err(Error::Config(_))));
        assert!(matches!(crate::open(c), Err(Error::Config(_))));
        let c = Config::new("/d", "https://e", "b", "ak", "sk");
        assert!(matches!(crate::open(c), Err(Error::Unsupported("https"))));
    }

    /// A prefix is limited to `[A-Za-z0-9._/-]` with no empty segment: anything else (XML
    /// special characters above all) would come back from a LIST in another spelling.
    #[test]
    fn config_prefix_validation() {
        let with = |prefix: &str| {
            let mut c = Config::new("/d", "http://e", "b", "ak", "sk");
            c.prefix = prefix.to_string();
            c.validate()
        };
        for ok in [
            "",
            "dbs",
            "dbs/one",
            "a.b_c-d/E9",
            "sys/crash_auto-123",
            "...",
            ".a/b.",
        ] {
            assert_eq!(with(ok), Ok(()), "{ok:?}");
        }
        for bad in [
            "a&b",
            "a<b",
            "a>b",
            "a\"b",
            "a'b",
            "a b",
            "a+b",
            "caf\u{e9}",
            "/a",
            "a/",
            "/",
            "a//b",
            "a\\b",
            ".",
            "..",
            "./a",
            "a/..",
            "a/./b",
            "../x",
        ] {
            match with(bad) {
                Err(Error::Config(m)) => assert!(m.contains("prefix"), "{bad:?}: {m}"),
                r => panic!("{bad:?}: {r:?}"),
            }
        }
        let mut c = Config::new("/d", "http://e", "b", "ak", "sk");
        c.prefix = "x&y".into();
        assert!(matches!(crate::open(c), Err(Error::Config(_))));
    }
}
