//! Public configuration: [`Config`] and its defaults. `Event` and `EventHook` (the
//! `on_event` field) are test and diagnostics hooks, hidden from the docs.
use crate::batching::Batching;
use crate::encryption::{parse_cipher, Encryption};
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
    /// How long a writer trusts its last look at the head (`BUCKWAL_LEASE_MS`): past it, the
    /// next append, page PUT or manifest PUT re-reads the head first.
    pub const LEASE: Duration = Duration::from_secs(30);
    /// How many log positions a takeover's fence record may step over (positions an old
    /// writer still running took first) before the takeover gives up with `Timeout`. Fixed.
    pub const FENCE_MAX_STEPS: u64 = 64;
    /// The longest takeover quiet a local state may carry (`seal_quiet_ms`); a larger value
    /// is refused as `Corrupt` at open. Fixed.
    pub const MAX_SEAL_QUIET: Duration = Duration::from_secs(24 * 60 * 60);
    /// How many rounds in a row of a seal's page or manifest requests that the store answered
    /// (412s, stats) without storing anything count as progress, letting the seal go on past
    /// its retry deadline. A round that stores an object resets it. Fixed.
    pub const MAX_ANSWER_ONLY_ROUNDS: u32 = 3;
    /// How long an unreferenced object stays in the bucket before the trimmer deletes it.
    pub const TRIM_GRACE: Duration = Duration::from_secs(10 * 60);
    /// A writer runs GC itself (`BUCKWAL_GC_AFTER_SEAL`): at open, after each seal and when
    /// the next manifest comes of age.
    pub const GC_AFTER_SEAL: bool = true;
    /// Fetch every sealed page in the background after open.
    pub const PREFETCH: bool = false;
    /// Every commit is uploaded on its own.
    pub const BATCHING: super::Batching = super::Batching::Off;
    /// The longest `max_wait` a window may have (`Batching::validate` refuses more). A
    /// Window commit's ack can wait the whole `max_wait` (always, on a single connection),
    /// so a huge one would hang the caller.
    pub const MAX_BATCH_WAIT: Duration = Duration::from_secs(60);
    /// The maintenance checkpoint runs once the current generation's log holds this many
    /// objects (`BUCKWAL_MAINTENANCE_LOG_OBJECTS`; 0 = this measure is off). A restore reads
    /// about this many log objects at most.
    pub const MAINTENANCE_LOG_OBJECTS: u64 = 1000;
    /// ... or once the local `-wal` has grown to this many bytes since the generation began
    /// (`BUCKWAL_MAINTENANCE_WAL_MIB`, in MiB; 0 = this measure is off).
    pub const MAINTENANCE_WAL_BYTES: u64 = 64 << 20;
    /// How long maintenance waits for in-flight calls (and open transactions) to end, with
    /// new ones held back, before it gives up and retries later
    /// (`BUCKWAL_MAINTENANCE_GATE_WAIT_MS`). It bounds the stall a maintenance run adds to
    /// a commit.
    pub const MAINTENANCE_GATE_WAIT: Duration = Duration::from_millis(500);
    /// After a skipped or failed maintenance run, the next one waits at least this long.
    /// Repeated gate timeouts and busy refusals double it, up to `MAINTENANCE_RETRY_MAX`; a
    /// failed run is retried sooner only once the generation grows by another threshold.
    /// Fixed.
    pub const MAINTENANCE_RETRY: Duration = Duration::from_secs(1);
    /// The longest backoff between maintenance runs. Fixed.
    pub const MAINTENANCE_RETRY_MAX: Duration = Duration::from_secs(30);
    pub const REGION: &str = "us-east-1";
    pub const PATH_STYLE: bool = true;
    /// Keys go at the root of the bucket.
    pub const PREFIX: &str = "";
    /// The cipher of an encrypted database (`BUCKWAL_ENCRYPTION_CIPHER`): any name turso's
    /// `CipherMode::try_from` accepts.
    pub const ENCRYPTION_CIPHER: &str = "aegis256";

    /// Export traces and logs over OTLP (`BUCKWAL_OTEL`): off in the library.
    pub const OTEL: bool = false;
    /// Where OTLP/gRPC goes (`BUCKWAL_OTEL_ENDPOINT`); plain `http://` only, no TLS.
    pub const OTEL_ENDPOINT: &str = "http://localhost:4317";
    /// The least severe log record exported (`BUCKWAL_OTEL_LEVEL`); spans are always INFO.
    pub const OTEL_LEVEL: tracing::Level = tracing::Level::INFO;
    /// How long one OTLP export may take. Fixed.
    pub const OTEL_EXPORT_TIMEOUT: Duration = Duration::from_secs(1);
    /// How long `TelemetryGuard` waits, in total, for both providers to flush and stop. Fixed.
    pub const OTEL_SHUTDOWN: Duration = Duration::from_secs(2);
    /// How many spans, and how many log records, may wait for export; more are dropped.
    /// Fixed.
    pub const OTEL_QUEUE: usize = 2048;
}

/// The engine's journal mode for a database. `Wal` is the default and keeps bucket format v1;
/// `Mvcc` buckets use format v2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JournalMode {
    #[default]
    Wal,
    Mvcc,
}

impl JournalMode {
    /// The name in `BUCKWAL_JOURNAL_MODE` and in a v2 head's `journal_mode`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            JournalMode::Wal => "wal",
            JournalMode::Mvcc => "mvcc",
        }
    }

    pub(crate) fn parse(s: &str) -> Option<JournalMode> {
        match s {
            "wal" => Some(JournalMode::Wal),
            "mvcc" => Some(JournalMode::Mvcc),
            _ => None,
        }
    }
}

/// Where a database lives: its local directory and its bucket.
#[derive(Clone)]
pub struct Config {
    /// The local directory: `app.db`, its `-wal` and buckwal's own files.
    pub dir: PathBuf,
    /// `http(s)://host[:port]`, with no path.
    pub endpoint: String,
    /// A PEM file of the CA certificates that sign the endpoint's certificate; they replace
    /// the system trust store. Only with an `https://` endpoint (`Config` otherwise).
    pub ca_bundle: Option<PathBuf>,
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
    /// How long an unreferenced object stays in the bucket before GC deletes it: GC trims
    /// against a manifest only once it is `trim_grace + retry_deadline + request_timeout +
    /// 1 s` old by the bucket's own clock. It must exceed `lease + retry_deadline +
    /// request_timeout` (`Config` otherwise).
    pub trim_grace: Duration,
    /// A writer open runs GC itself, on a thread of its own: at open, after each seal and
    /// when the next manifest comes of age (`BUCKWAL_GC_AFTER_SEAL`). Off, only
    /// [`crate::gc`] deletes anything.
    pub gc_after_seal: bool,
    /// How long the writer trusts its last confirmation that the head is still its own: a
    /// confirmed log record or head write, or a head GET that found its head. Past it (on
    /// either the monotonic or the wall clock), the next append, and each page or manifest
    /// PUT attempt of a seal, GETs the head first; another writer's head fences this one.
    /// 0 checks before every append and page PUT (tests).
    pub lease: Duration,
    pub prefetch: bool,
    /// How commits are gathered into one upload (`BUCKWAL_BATCHING`).
    pub batching: Batching,
    /// The library checkpoints (TRUNCATE) a writer's generation itself once its log holds
    /// this many objects, so a generation the engine never restarts (sustained concurrent
    /// commits) is still sealed and trimmed. 0 turns this measure off; with
    /// `maintenance_wal_bytes` also 0, maintenance is off.
    pub maintenance_log_objects: u64,
    /// ... or once the local `-wal` holds this many bytes of the generation. 0 turns this
    /// measure off.
    pub maintenance_wal_bytes: u64,
    /// How long a maintenance run holds new calls back while it waits for the ones in flight
    /// (and open transactions) to end; then it gives up and retries later.
    pub maintenance_gate_wait: Duration,
    /// A local file holding the database's encryption key as hex (D16); `None` stores the
    /// database unencrypted. Read at open, never uploaded. An existing database must match
    /// it: a key on an unencrypted database, or none on an encrypted one, is refused with
    /// `Encryption`.
    pub encryption_key_file: Option<PathBuf>,
    /// The cipher (any name turso's `CipherMode::try_from` accepts; `Config` otherwise). A
    /// value other than the default needs `encryption_key_file`.
    pub encryption_cipher: String,
    /// The journal mode a new database is created in (`BUCKWAL_JOURNAL_MODE` = `wal` | `mvcc`).
    pub journal_mode: JournalMode,
    /// A test and diagnostics hook, not part of the supported API (hidden from the docs).
    #[doc(hidden)]
    pub on_event: Option<EventHook>,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("dir", &self.dir)
            .field("endpoint", &self.endpoint)
            .field("ca_bundle", &self.ca_bundle)
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
            .field("gc_after_seal", &self.gc_after_seal)
            .field("lease", &self.lease)
            .field("prefetch", &self.prefetch)
            .field("batching", &self.batching)
            .field("maintenance_log_objects", &self.maintenance_log_objects)
            .field("maintenance_wal_bytes", &self.maintenance_wal_bytes)
            .field("maintenance_gate_wait", &self.maintenance_gate_wait)
            .field("encryption_key_file", &self.encryption_key_file)
            .field("encryption_cipher", &self.encryption_cipher)
            .field("journal_mode", &self.journal_mode)
            .field("on_event", &self.on_event.as_ref().map(|_| "<hook>"))
            .finish()
    }
}

/// The names the programs read before the rename to `BUCKWAL_*`. They are no longer read:
/// [`refuse_legacy_env`] turns one that is set into an error that names its replacement, so a
/// deployment that still sets an old name fails at start instead of running on defaults.
pub const LEGACY_ENV: &[&str] = &[
    "DURABLE_ACCESS_KEY",
    "DURABLE_BATCHING",
    "DURABLE_BUCKET",
    "DURABLE_CA_BUNDLE",
    "DURABLE_ENCRYPTION_CIPHER",
    "DURABLE_ENCRYPTION_KEY_FILE",
    "DURABLE_ENDPOINT",
    "DURABLE_GC_AFTER_SEAL",
    "DURABLE_JOURNAL_MODE",
    "DURABLE_LEASE_MS",
    "DURABLE_MAINTENANCE_GATE_WAIT_MS",
    "DURABLE_MAINTENANCE_LOG_OBJECTS",
    "DURABLE_MAINTENANCE_WAL_MIB",
    "DURABLE_OTEL",
    "DURABLE_OTEL_ENDPOINT",
    "DURABLE_OTEL_LEVEL",
    "DURABLE_PATH_STYLE",
    "DURABLE_PREFETCH",
    "DURABLE_PREFIX",
    "DURABLE_REGION",
    "DURABLE_REQUEST_TIMEOUT_MS",
    "DURABLE_RETRY_DEADLINE_MS",
    "DURABLE_SECRET_KEY",
    "DURABLE_SERVE_BUSY_TIMEOUT_MS",
    "DURABLE_SERVE_LISTEN",
    "DURABLE_SERVE_MAX_STREAMS",
    "DURABLE_SERVE_SHUTDOWN_MS",
    "DURABLE_SERVE_STREAM_TIMEOUT_MS",
    "DURABLE_SERVE_TLS_CERT",
    "DURABLE_SERVE_TLS_KEY",
    "DURABLE_SERVE_TOKEN_FILE",
    "DURABLE_SERVE_WORKERS",
    "DURABLE_TRIM_GRACE_SECS",
    "DURABLE_UPLOAD_CONCURRENCY",
];

/// The message for the first name in [`LEGACY_ENV`] that `get` finds set (an empty value
/// counts as set), or `None`. The value is never read into the message.
pub fn legacy_env_refusal(get: impl Fn(&str) -> Option<String>) -> Option<String> {
    let old = LEGACY_ENV.iter().find(|k| get(k).is_some())?;
    let new = old.replacen("DURABLE_", "BUCKWAL_", 1);
    Some(format!("{old} is no longer read, set {new} instead"))
}

/// [`legacy_env_refusal`] as a `Config` error.
pub fn refuse_legacy_env(get: impl Fn(&str) -> Option<String>) -> Result<()> {
    match legacy_env_refusal(get) {
        Some(m) => Err(Error::Config(m)),
        None => Ok(()),
    }
}

/// Every environment variable [`Config::from_env`] reads.
const ENV_VARS: [&str; 22] = [
    "BUCKWAL_ENDPOINT",
    "BUCKWAL_CA_BUNDLE",
    "BUCKWAL_REGION",
    "BUCKWAL_PATH_STYLE",
    "BUCKWAL_BUCKET",
    "BUCKWAL_PREFIX",
    "BUCKWAL_ACCESS_KEY",
    "BUCKWAL_SECRET_KEY",
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
    "BUCKWAL_ENCRYPTION_KEY_FILE",
    "BUCKWAL_ENCRYPTION_CIPHER",
    "BUCKWAL_JOURNAL_MODE",
];

pub(crate) fn parse_bool(var: &str, v: &str) -> Result<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" => Ok(true),
        "0" | "false" | "off" => Ok(false),
        _ => Err(Error::Config(format!(
            "{var} must be 0/1, true/false or on/off, not {v:?}"
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
            ca_bundle: None,
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
            gc_after_seal: defaults::GC_AFTER_SEAL,
            lease: defaults::LEASE,
            prefetch: defaults::PREFETCH,
            batching: defaults::BATCHING,
            maintenance_log_objects: defaults::MAINTENANCE_LOG_OBJECTS,
            maintenance_wal_bytes: defaults::MAINTENANCE_WAL_BYTES,
            maintenance_gate_wait: defaults::MAINTENANCE_GATE_WAIT,
            encryption_key_file: None,
            encryption_cipher: defaults::ENCRYPTION_CIPHER.to_string(),
            journal_mode: JournalMode::default(),
            on_event: None,
        }
    }

    /// Read the configuration from `BUCKWAL_*` environment variables. `BUCKWAL_ENDPOINT`,
    /// `BUCKWAL_BUCKET`, `BUCKWAL_ACCESS_KEY` and `BUCKWAL_SECRET_KEY` are required; every
    /// other one falls back to its [`defaults`] value.
    pub fn from_env(dir: impl Into<PathBuf>) -> Result<Config> {
        Config::from_lookup(dir, |k| std::env::var(k).ok())
    }

    fn from_lookup(
        dir: impl Into<PathBuf>,
        get: impl Fn(&str) -> Option<String>,
    ) -> Result<Config> {
        refuse_legacy_env(&get)?;
        debug_assert!(ENV_VARS.iter().all(|v| v.starts_with("BUCKWAL_")));
        let required = |k: &str| {
            get(k)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| Error::Config(format!("{k} is not set")))
        };
        let mut c = Config::new(
            dir,
            &required("BUCKWAL_ENDPOINT")?,
            &required("BUCKWAL_BUCKET")?,
            &required("BUCKWAL_ACCESS_KEY")?,
            &required("BUCKWAL_SECRET_KEY")?,
        );
        if let Some(v) = get("BUCKWAL_CA_BUNDLE").filter(|v| !v.is_empty()) {
            c.ca_bundle = Some(PathBuf::from(v));
        }
        if let Some(v) = get("BUCKWAL_REGION") {
            c.region = v;
        }
        if let Some(v) = get("BUCKWAL_PATH_STYLE") {
            c.path_style = parse_bool("BUCKWAL_PATH_STYLE", &v)?;
        }
        if let Some(v) = get("BUCKWAL_PREFIX") {
            c.prefix = v;
        }
        if let Some(v) = get("BUCKWAL_RETRY_DEADLINE_MS") {
            c.retry_deadline = Duration::from_millis(parse_num("BUCKWAL_RETRY_DEADLINE_MS", &v)?);
        }
        if let Some(v) = get("BUCKWAL_REQUEST_TIMEOUT_MS") {
            c.request_timeout = Duration::from_millis(parse_num("BUCKWAL_REQUEST_TIMEOUT_MS", &v)?);
        }
        if let Some(v) = get("BUCKWAL_UPLOAD_CONCURRENCY") {
            c.upload_concurrency = parse_num("BUCKWAL_UPLOAD_CONCURRENCY", &v)?;
        }
        if let Some(v) = get("BUCKWAL_TRIM_GRACE_SECS") {
            c.trim_grace = Duration::from_secs(parse_num("BUCKWAL_TRIM_GRACE_SECS", &v)?);
        }
        if let Some(v) = get("BUCKWAL_GC_AFTER_SEAL") {
            c.gc_after_seal = parse_bool("BUCKWAL_GC_AFTER_SEAL", &v)?;
        }
        if let Some(v) = get("BUCKWAL_LEASE_MS") {
            c.lease = Duration::from_millis(parse_num("BUCKWAL_LEASE_MS", &v)?);
        }
        if let Some(v) = get("BUCKWAL_PREFETCH") {
            c.prefetch = parse_bool("BUCKWAL_PREFETCH", &v)?;
        }
        if let Some(v) = get("BUCKWAL_BATCHING") {
            c.batching = v.parse().map_err(|e| match e {
                Error::Config(m) => Error::Config(format!("BUCKWAL_BATCHING: {m}")),
                e => e,
            })?;
        }
        if let Some(v) = get("BUCKWAL_MAINTENANCE_LOG_OBJECTS") {
            c.maintenance_log_objects = parse_num("BUCKWAL_MAINTENANCE_LOG_OBJECTS", &v)?;
        }
        if let Some(v) = get("BUCKWAL_MAINTENANCE_WAL_MIB") {
            let var = "BUCKWAL_MAINTENANCE_WAL_MIB";
            let mib: u64 = parse_num(var, &v)?;
            c.maintenance_wal_bytes = mib
                .checked_mul(1 << 20)
                .ok_or_else(|| Error::Config(format!("{var} is too large: {v:?}")))?;
        }
        if let Some(v) = get("BUCKWAL_MAINTENANCE_GATE_WAIT_MS") {
            c.maintenance_gate_wait =
                Duration::from_millis(parse_num("BUCKWAL_MAINTENANCE_GATE_WAIT_MS", &v)?);
        }
        if let Some(v) = get("BUCKWAL_ENCRYPTION_KEY_FILE").filter(|v| !v.is_empty()) {
            c.encryption_key_file = Some(PathBuf::from(v));
        }
        if let Some(v) = get("BUCKWAL_ENCRYPTION_CIPHER").filter(|v| !v.is_empty()) {
            c.encryption_cipher = v;
        }
        if let Some(v) = get("BUCKWAL_JOURNAL_MODE") {
            c.journal_mode = JournalMode::parse(&v).ok_or_else(|| {
                Error::Config(format!(
                    "BUCKWAL_JOURNAL_MODE {v:?} is not one of wal, mvcc"
                ))
            })?;
        }
        Ok(c)
    }

    /// The checks `open` and `restore` make before touching anything.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.ca_bundle.is_some()
            && self
                .endpoint
                .trim_start()
                .to_ascii_lowercase()
                .starts_with("http://")
        {
            return Err(Error::Config(
                "BUCKWAL_CA_BUNDLE is set but the endpoint is http://".into(),
            ));
        }
        if self.bucket.is_empty() {
            return Err(Error::Config("bucket is empty".into()));
        }
        if self.upload_concurrency == 0 {
            return Err(Error::Config(
                "upload_concurrency must be at least 1".into(),
            ));
        }
        // The takeover quiet is persisted, and a local state carrying one over
        // `MAX_SEAL_QUIET` is refused at open.
        if self.lease.saturating_add(self.request_timeout) > defaults::MAX_SEAL_QUIET {
            return Err(Error::Config(format!(
                "BUCKWAL_LEASE_MS ({:?}) + request_timeout ({:?}) must be at most 24h (the \
                 longest takeover quiet, MAX_SEAL_QUIET)",
                self.lease, self.request_timeout
            )));
        }
        // A writer's last request inside its lease lands within this long of its last head
        // check (`Bucket::check_lease`); the trim grace must outlast it. No exception for 0.
        let budget = self
            .lease
            .saturating_add(self.retry_deadline)
            .saturating_add(self.request_timeout);
        if self.trim_grace <= budget {
            return Err(Error::Config(format!(
                "trim_grace ({:?}) must exceed lease ({:?}) + retry_deadline ({:?}) + request_timeout ({:?})",
                self.trim_grace, self.lease, self.retry_deadline, self.request_timeout
            )));
        }
        self.batching.validate()?;
        self.batching.check_mode(self.journal_mode)?;
        let cipher = parse_cipher(&self.encryption_cipher)?;
        if self.encryption_key_file.is_none()
            && cipher != parse_cipher(defaults::ENCRYPTION_CIPHER)?
        {
            return Err(Error::Config(format!(
                "BUCKWAL_ENCRYPTION_CIPHER is {} but BUCKWAL_ENCRYPTION_KEY_FILE is not set",
                self.encryption_cipher
            )));
        }
        let maintenance = self.maintenance_log_objects > 0 || self.maintenance_wal_bytes > 0;
        if maintenance && self.maintenance_gate_wait.is_zero() {
            return Err(Error::Config(
                "maintenance_gate_wait must be above 0 while maintenance is on (set both \
                 maintenance thresholds to 0 to turn it off)"
                    .into(),
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
            lease: self.lease,
            request_timeout: self.request_timeout,
            trim_grace: self.trim_grace,
            events: self.on_event.clone(),
            read_only,
            encryption: None,
            journal_mode: self.journal_mode,
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
            ca_bundle: self.ca_bundle.clone(),
        })?))
    }
}

/// Called synchronously, on the thread that did the work, for each [`Event`]. A test and
/// diagnostics hook, not part of the supported API (hidden from the docs). It must not call
/// into the database: the uploader thread emits a seal's events while a maintenance run
/// holds every new call at its gate.
#[doc(hidden)]
pub type EventHook = Arc<dyn Fn(&Event) + Send + Sync>;

/// Progress notifications: a test and diagnostics hook, not part of the supported API
/// (hidden from the docs).
#[doc(hidden)]
#[derive(Debug, Clone)]
pub enum Event {
    /// A log object landed at `log/<generation>/<seq>`; `bytes` is its encoded size.
    /// `commits` lists the commit numbers of the syncs it holds, in order; it is empty for
    /// a record that carries no commit (a fence, a seal start, a reconcile catch-up, or an
    /// in-doubt record settled after its commits were already failed).
    LogUploaded {
        generation: u64,
        seq: u64,
        bytes: u64,
        commits: Vec<u64>,
    },
    /// A Window commit joined the uploader's batch without flushing it; `commits` is how
    /// many commits the batch now holds.
    BatchWaiting { commits: u64 },
    /// A `Connection` call returned after commit number `commit`, made with `batching`, was
    /// confirmed in the bucket. `batching` is the setting the commit was made with, even when
    /// a degraded uploader made a Window commit take the blocking `Off` path.
    CommitAcked { commit: u64, batching: Batching },
    /// Sealing `generation` began with `pages` dirty pages to upload.
    SealStarted { generation: u64, pages: u64 },
    /// `generation` is sealed: the head now points at generation + 1.
    SealFinished { generation: u64, elapsed: Duration },
    /// The trimmer deleted `deleted` objects no head can reach any more.
    Trimmed { deleted: u64 },
    /// A maintenance checkpoint of `generation` began: its log held `log_objects` objects
    /// and the local `-wal` `wal_bytes` bytes.
    MaintenanceStarted {
        generation: u64,
        log_objects: u64,
        wal_bytes: u64,
    },
    /// A maintenance checkpoint sealed `generation`. `gate_wait` is how long it held new
    /// calls back before the checkpoint started; `elapsed` is the whole run, the checkpoint
    /// (and its seal) included.
    MaintenanceFinished {
        generation: u64,
        gate_wait: Duration,
        elapsed: Duration,
    },
    /// A maintenance run of `generation` did not seal, and is retried later: `reason` is
    /// `bucket degraded`, `last bucket operation failed` or `takeover quiet` (it did not
    /// start: no `MaintenanceStarted`, the gate stayed open), `gate_timeout` (calls or an open
    /// transaction still in flight at the gate wait), `busy` (the engine refused the
    /// checkpoint), `closed` (the database was closing), `panic` (the run panicked; the
    /// span's outcome is `error`) or `error: <message>`.
    MaintenanceSkipped { generation: u64, reason: String },
    /// This directory took the database over (a takeover, the first writer open of a
    /// restored directory, or an open that resumes a takeover whose fence never landed): the
    /// head names its writer at `epoch`, and its fence record is at
    /// `log/<generation>/<fence_seq>`.
    TookOver {
        generation: u64,
        epoch: u64,
        fence_seq: u64,
    },
    /// A seal of `generation` waits `wait` before it starts: the takeover quiet. When this
    /// writer took `generation` over, another writer's last record before its fence was a
    /// seal-start record (or a record could not be read), so that writer's page and manifest
    /// PUTs may still be in flight.
    SealQuiet { generation: u64, wait: Duration },
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
    /// The writer's lease (`Bucket::set_lease`, `Bucket::check_lease`).
    pub lease: Duration,
    /// How long one HTTP request may take: the S3 store's own setting, here for the waits
    /// that must outlast a request in flight (the takeover quiet).
    pub request_timeout: Duration,
    /// `Config::trim_grace`, for [`EngineConfig::gc_min_age`].
    pub trim_grace: Duration,
    pub events: Option<EventHook>,
    /// A read-only open: nothing may be written to the bucket, and the engine's writes are
    /// refused (`ShippingFile`, `BucketDbStorage`).
    pub read_only: bool,
    /// The cipher and key the engine opens and connects with (D16); `None` is unencrypted.
    /// [`Config::engine`] leaves it `None`: `open` loads the key file (`encryption::load`).
    pub encryption: Option<Arc<Encryption>>,
    /// The mode the caller opens the database in: a head or local state of the other mode is
    /// refused (`db::refuse_mode_mismatch`).
    pub journal_mode: JournalMode,
}

impl EngineConfig {
    /// How old a manifest must be, by the bucket's clock, before GC trims against it:
    /// `trim_grace + retry_deadline + request_timeout + 1 s`. The second covers the 1 s
    /// resolution of the age a bucket reports (`ObjectStore::stat`).
    pub(crate) fn gc_min_age(&self) -> Duration {
        self.trim_grace
            .saturating_add(self.retry.deadline)
            .saturating_add(self.request_timeout)
            .saturating_add(Duration::from_secs(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_every_legacy_name() {
        for old in LEGACY_ENV {
            let e = refuse_legacy_env(|k| (k == *old).then(|| "SECRETVALUE".to_string()))
                .unwrap_err()
                .to_string();
            let new = old.replacen("DURABLE_", "BUCKWAL_", 1);
            assert!(e.contains(old) && e.contains(&new), "{e}");
            assert!(
                e.contains(&format!("{old} is no longer read, set {new} instead")),
                "{e}"
            );
            assert!(!e.contains("SECRETVALUE"), "{e}");
        }
        assert!(refuse_legacy_env(|_| None).is_ok());
    }

    #[test]
    fn refuses_legacy_name_with_empty_value() {
        let e = refuse_legacy_env(|k| (k == "DURABLE_BUCKET").then(String::new));
        assert!(e.is_err());
    }

    #[test]
    fn legacy_env_matches_current_names() {
        // Every old name has a BUCKWAL_ twin that Config, TelemetryConfig or ServeConfig reads
        // (the serve ones are checked in buckwal-server), and the twin alone is no refusal.
        let mut read: Vec<&str> = ENV_VARS.to_vec();
        read.extend([
            "BUCKWAL_OTEL",
            "BUCKWAL_OTEL_ENDPOINT",
            "BUCKWAL_OTEL_LEVEL",
        ]);
        for old in LEGACY_ENV {
            let new = old.replacen("DURABLE_", "BUCKWAL_", 1);
            if new.starts_with("BUCKWAL_SERVE_") {
                continue;
            }
            assert!(read.contains(&new.as_str()), "{new} is not read");
            assert!(refuse_legacy_env(|k| (k == new).then(|| "x".to_string())).is_ok());
        }
        for v in ENV_VARS {
            let old = v.replacen("BUCKWAL_", "DURABLE_", 1);
            assert!(
                LEGACY_ENV.contains(&old.as_str()),
                "{old} missing from LEGACY_ENV"
            );
        }
    }

    #[test]
    fn config_from_lookup_refuses_old_encryption_key_file() {
        let mut env = required();
        env.insert("DURABLE_ENCRYPTION_KEY_FILE", "/k");
        let e = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string()))
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("DURABLE_ENCRYPTION_KEY_FILE is no longer read, set BUCKWAL_ENCRYPTION_KEY_FILE instead"),
            "{e}"
        );
    }

    #[test]
    fn gc_after_seal_from_env() {
        assert!(Config::new("/d", "http://e", "b", "ak", "sk").gc_after_seal);
        let env = required();
        let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
        assert!(c.gc_after_seal, "on by default");
        for (v, want) in [("off", false), ("0", false), ("true", true)] {
            let mut env = required();
            env.insert("BUCKWAL_GC_AFTER_SEAL", v);
            let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
            assert_eq!(c.gc_after_seal, want, "{v}");
        }
        let mut env = required();
        env.insert("BUCKWAL_GC_AFTER_SEAL", "sometimes");
        match Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())) {
            Err(Error::Config(m)) => assert!(m.contains("BUCKWAL_GC_AFTER_SEAL"), "{m}"),
            r => panic!("{r:?}"),
        }
    }

    #[test]
    fn gc_min_age_adds_the_request_budget_and_a_second() {
        let mut c = Config::new("/d", "http://e", "b", "ak", "sk");
        assert_eq!(
            c.engine(false).gc_min_age(),
            Duration::from_secs(600 + 30 + 10 + 1),
            "defaults"
        );
        c.trim_grace = Duration::from_secs(2);
        c.lease = Duration::ZERO;
        c.retry_deadline = Duration::from_millis(400);
        c.request_timeout = Duration::from_millis(500);
        assert_eq!(c.validate(), Ok(()));
        assert_eq!(c.engine(false).gc_min_age(), Duration::from_millis(3900));
        assert_eq!(c.engine(true).gc_min_age(), Duration::from_millis(3900));
        // Huge durations saturate instead of panicking.
        c.trim_grace = Duration::MAX;
        assert_eq!(c.engine(false).gc_min_age(), Duration::MAX);
    }

    /// `trim_grace` must exceed `lease + retry_deadline + request_timeout`, and 0 is refused
    /// like any other value: an append or page PUT sent inside the lease lands within that
    /// sum of the writer's last head check, and only then before another writer's GC.
    #[test]
    fn trim_grace_must_exceed_lease_and_request_budget() {
        let mut c = Config::new("/tmp/x", "http://127.0.0.1:1", "b", "k", "s");
        assert_eq!(c.validate(), Ok(()), "defaults: 600 s > 70 s");
        c.lease = Duration::from_secs(30);
        c.retry_deadline = Duration::from_secs(30);
        c.request_timeout = Duration::from_secs(10);
        c.trim_grace = Duration::from_secs(70); // == 30 + 30 + 10 s: refused
        assert!(matches!(c.validate(), Err(Error::Config(m))
            if m.contains("trim_grace") && m.contains("lease")
                && m.contains("retry_deadline") && m.contains("request_timeout")));
        c.trim_grace = Duration::from_secs(71);
        assert_eq!(c.validate(), Ok(()));
        c.trim_grace = Duration::ZERO; // no exception for 0
        assert!(matches!(c.validate(), Err(Error::Config(m)) if m.contains("trim_grace")));
        // A zero lease still needs the request budget.
        c.lease = Duration::ZERO;
        c.trim_grace = Duration::from_secs(40);
        assert!(matches!(c.validate(), Err(Error::Config(_))));
        c.trim_grace = Duration::from_secs(41);
        assert_eq!(c.validate(), Ok(()));
        // Huge durations saturate instead of panicking.
        c.retry_deadline = Duration::MAX;
        c.lease = Duration::MAX;
        c.trim_grace = Duration::from_secs(1);
        assert!(matches!(c.validate(), Err(Error::Config(_))));
    }

    /// The takeover quiet (`lease + request_timeout`) is persisted, and a local state carrying
    /// one over `MAX_SEAL_QUIET` is refused at open, so `validate` refuses such a lease first.
    #[test]
    fn lease_and_request_timeout_fit_the_longest_seal_quiet() {
        let mut c = Config::new("/tmp/x", "http://127.0.0.1:1", "b", "k", "s");
        c.request_timeout = Duration::from_secs(10);
        c.lease = defaults::MAX_SEAL_QUIET - c.request_timeout;
        c.trim_grace = defaults::MAX_SEAL_QUIET + Duration::from_secs(3600);
        assert_eq!(c.validate(), Ok(()), "exactly at the limit");
        c.lease += Duration::from_millis(1);
        match c.validate() {
            Err(Error::Config(m)) => assert!(
                m.contains("BUCKWAL_LEASE_MS")
                    && m.contains("request_timeout")
                    && m.contains("24h"),
                "{m}"
            ),
            r => panic!("{r:?}"),
        }
    }

    #[test]
    fn lease_from_env() {
        let mut env = required();
        env.insert("BUCKWAL_LEASE_MS", "1500");
        let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(c.lease, Duration::from_millis(1500));
        assert_eq!(c.engine(false).lease, Duration::from_millis(1500));
        let env = required();
        let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(c.lease, Duration::from_secs(30));
        assert_eq!(defaults::LEASE, Duration::from_secs(30));
        let mut env = required();
        env.insert("BUCKWAL_LEASE_MS", "soon");
        match Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())) {
            Err(Error::Config(m)) => assert!(m.contains("BUCKWAL_LEASE_MS"), "{m}"),
            r => panic!("{r:?}"),
        }
    }

    use std::collections::HashMap;

    fn required() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("BUCKWAL_ENDPOINT", "http://127.0.0.1:9000"),
            ("BUCKWAL_BUCKET", "b"),
            ("BUCKWAL_ACCESS_KEY", "ak"),
            ("BUCKWAL_SECRET_KEY", "sk"),
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
        assert_eq!(c.lease, Duration::from_secs(30));
        assert!(!c.prefetch);
        assert!(!format!("{c:?}").contains("sk\""), "the secret is printed");
        let mut c = c;
        c.retry_deadline = Duration::from_millis(1234);
        c.request_timeout = Duration::from_millis(321);
        let e = c.engine(false);
        assert_eq!((e.lease, e.request_timeout), (c.lease, c.request_timeout));
        let r = e.retry;
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
    fn config_debug_redacts_the_secret_key() {
        let mut c = Config::new("/d", "http://e", "b", "AKIAEXAMPLE", "s3cr3t/SECRET+value");
        c.encryption_key_file = Some("/keys/db.key".into());
        let debug = format!("{c:#?}");
        assert!(!debug.contains("s3cr3t"), "{debug}");
        assert!(
            debug.contains("AKIAEXAMPLE") && debug.contains("/keys/db.key"),
            "{debug}"
        );
    }

    #[test]
    fn config_from_env_reads_every_setting() {
        let all = [
            ("BUCKWAL_ENDPOINT", "http://127.0.0.1:9000"),
            ("BUCKWAL_CA_BUNDLE", "/etc/ca.pem"),
            ("BUCKWAL_REGION", "eu-west-2"),
            ("BUCKWAL_PATH_STYLE", "false"),
            ("BUCKWAL_BUCKET", "bkt"),
            ("BUCKWAL_PREFIX", "dbs/one"),
            ("BUCKWAL_ACCESS_KEY", "ak"),
            ("BUCKWAL_SECRET_KEY", "sk"),
            ("BUCKWAL_RETRY_DEADLINE_MS", "1500"),
            ("BUCKWAL_REQUEST_TIMEOUT_MS", "250"),
            ("BUCKWAL_UPLOAD_CONCURRENCY", "3"),
            ("BUCKWAL_TRIM_GRACE_SECS", "7"),
            ("BUCKWAL_PREFETCH", "1"),
            ("BUCKWAL_BATCHING", "window 10ms 8 64KiB"),
            ("BUCKWAL_MAINTENANCE_LOG_OBJECTS", "250"),
            ("BUCKWAL_MAINTENANCE_WAL_MIB", "3"),
            ("BUCKWAL_MAINTENANCE_GATE_WAIT_MS", "75"),
            ("BUCKWAL_LEASE_MS", "2500"),
            ("BUCKWAL_GC_AFTER_SEAL", "off"),
            ("BUCKWAL_ENCRYPTION_KEY_FILE", "/etc/db.key"),
            ("BUCKWAL_ENCRYPTION_CIPHER", "aes256gcm"),
            ("BUCKWAL_JOURNAL_MODE", "mvcc"),
        ];
        assert_eq!(all.len(), ENV_VARS.len());
        // No other unit test reads BUCKWAL_* variables.
        for (k, v) in all {
            std::env::set_var(k, v);
        }
        let c = Config::from_env("/data/db");
        for k in ENV_VARS {
            std::env::remove_var(k);
        }
        let c = c.unwrap();
        assert_eq!(c.dir, PathBuf::from("/data/db"));
        assert_eq!(c.journal_mode, JournalMode::Mvcc);
        assert_eq!(c.endpoint, "http://127.0.0.1:9000");
        assert_eq!(c.ca_bundle, Some(PathBuf::from("/etc/ca.pem")));
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
        assert_eq!(
            c.batching,
            Batching::Window {
                max_wait: Duration::from_millis(10),
                max_commits: 8,
                max_bytes: 64 << 10
            }
        );
        assert_eq!(c.maintenance_log_objects, 250);
        assert_eq!(c.maintenance_wal_bytes, 3 << 20);
        assert_eq!(c.maintenance_gate_wait, Duration::from_millis(75));
        assert_eq!(c.lease, Duration::from_millis(2500));
        assert!(!c.gc_after_seal);
        assert_eq!(c.encryption_key_file, Some(PathBuf::from("/etc/db.key")));
        assert_eq!(c.encryption_cipher, "aes256gcm");

        // Only the four required ones: the rest are defaults.
        let env = required();
        let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(c.region, defaults::REGION);
        assert_eq!(c.upload_concurrency, defaults::UPLOAD_CONCURRENCY);
        assert_eq!(c.batching, Batching::Off);

        for missing in [
            "BUCKWAL_ENDPOINT",
            "BUCKWAL_BUCKET",
            "BUCKWAL_ACCESS_KEY",
            "BUCKWAL_SECRET_KEY",
        ] {
            let mut env = required();
            env.remove(missing);
            match Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())) {
                Err(Error::Config(m)) => assert!(m.contains(missing), "{m}"),
                r => panic!("{missing}: {r:?}"),
            }
        }
        let mut env = required();
        env.insert("BUCKWAL_PREFETCH", "maybe");
        match Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())) {
            Err(Error::Config(m)) => assert!(m.contains("BUCKWAL_PREFETCH"), "{m}"),
            r => panic!("{r:?}"),
        }
    }

    #[test]
    fn batching_from_env_and_validate() {
        let mut env = required();
        env.insert("BUCKWAL_BATCHING", "nope");
        match Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())) {
            Err(Error::Config(m)) => {
                assert!(
                    m.contains("nope") && m.starts_with("BUCKWAL_BATCHING: "),
                    "{m}"
                )
            }
            r => panic!("{r:?}"),
        }
        let mut c = Config::new("/d", "http://e", "b", "ak", "sk");
        assert_eq!(c.batching, Batching::Off);
        c.batching = Batching::Window {
            max_wait: Duration::from_millis(10),
            max_commits: 0,
            max_bytes: 1,
        };
        assert!(matches!(c.validate(), Err(Error::Config(m)) if m.contains("max_commits")));
    }

    #[test]
    fn maintenance_settings_defaults_env_and_validate() {
        let c = Config::new("/d", "http://e", "b", "ak", "sk");
        assert_eq!(c.maintenance_log_objects, 1000);
        assert_eq!(c.maintenance_wal_bytes, 64 << 20);
        assert_eq!(c.maintenance_gate_wait, Duration::from_millis(500));
        assert_eq!(defaults::MAINTENANCE_RETRY, Duration::from_secs(1));
        assert_eq!(c.validate(), Ok(()));

        // A gate wait of 0 would skip every run: refused while maintenance is on.
        let mut c = Config::new("/d", "http://e", "b", "ak", "sk");
        c.maintenance_gate_wait = Duration::ZERO;
        assert!(
            matches!(c.validate(), Err(Error::Config(m)) if m.contains("maintenance_gate_wait"))
        );
        c.maintenance_log_objects = 0;
        assert!(
            matches!(c.validate(), Err(Error::Config(_))),
            "one measure is still on"
        );
        c.maintenance_wal_bytes = 0;
        assert_eq!(c.validate(), Ok(()), "maintenance off: the wait is unused");

        let mut env = required();
        env.insert("BUCKWAL_MAINTENANCE_LOG_OBJECTS", "0");
        env.insert("BUCKWAL_MAINTENANCE_WAL_MIB", "0");
        let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!((c.maintenance_log_objects, c.maintenance_wal_bytes), (0, 0));
        for var in [
            "BUCKWAL_MAINTENANCE_LOG_OBJECTS",
            "BUCKWAL_MAINTENANCE_WAL_MIB",
            "BUCKWAL_MAINTENANCE_GATE_WAIT_MS",
        ] {
            let mut env = required();
            env.insert(var, "-1");
            match Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())) {
                Err(Error::Config(m)) => assert!(m.contains(var), "{m}"),
                r => panic!("{var}: {r:?}"),
            }
        }
        // MiB that overflow u64 bytes are refused, not wrapped.
        let mut env = required();
        env.insert("BUCKWAL_MAINTENANCE_WAL_MIB", "18446744073709551615");
        assert!(matches!(
            Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())),
            Err(Error::Config(m)) if m.contains("BUCKWAL_MAINTENANCE_WAL_MIB")
        ));
    }

    #[test]
    fn config_validation() {
        let c = Config::new("/d", "http://e", "", "ak", "sk");
        assert!(matches!(c.validate(), Err(Error::Config(_))));
        assert!(matches!(crate::open(c), Err(Error::Config(_))));
    }

    #[test]
    fn https_endpoint_is_accepted() {
        let c = Config::new("/d", "https://s3.example.com", "b", "ak", "sk");
        assert_eq!(c.validate(), Ok(()));
    }

    #[test]
    fn ca_bundle_with_http_endpoint_is_refused() {
        let mut c = Config::new("/d", "http://e", "b", "ak", "sk");
        c.ca_bundle = Some("/ca.pem".into());
        assert_eq!(
            c.validate(),
            Err(Error::Config(
                "BUCKWAL_CA_BUNDLE is set but the endpoint is http://".into()
            ))
        );
        c.endpoint = "https://e".into();
        assert_eq!(c.validate(), Ok(()));
    }

    #[test]
    fn from_env_reads_ca_bundle() {
        let mut env = required();
        let read = |env: &HashMap<&str, &str>| {
            Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap()
        };
        assert_eq!(read(&env).ca_bundle, None);
        env.insert("BUCKWAL_CA_BUNDLE", "");
        assert_eq!(read(&env).ca_bundle, None);
        env.insert("BUCKWAL_CA_BUNDLE", "/etc/ca.pem");
        let c = read(&env);
        assert_eq!(c.ca_bundle, Some(PathBuf::from("/etc/ca.pem")));
        assert!(format!("{c:?}").contains("/etc/ca.pem"));
    }

    #[test]
    fn unknown_cipher_is_config_error() {
        let mut c = Config::new("/d", "http://e", "b", "ak", "sk");
        c.encryption_key_file = Some("/k".into());
        c.encryption_cipher = "rot13".into();
        assert_eq!(
            c.validate(),
            Err(Error::Config(
                "BUCKWAL_ENCRYPTION_CIPHER rot13 is not a cipher turso supports".into()
            ))
        );
        // Every spelling turso accepts.
        for name in ["aes256gcm", "AES-256-GCM", "aegis_128l", "aegis256x4"] {
            c.encryption_cipher = name.into();
            assert_eq!(c.validate(), Ok(()), "{name}");
        }
    }

    #[test]
    fn cipher_without_key_file_is_config_error() {
        let mut c = Config::new("/d", "http://e", "b", "ak", "sk");
        assert_eq!(c.encryption_cipher, defaults::ENCRYPTION_CIPHER);
        assert_eq!(defaults::ENCRYPTION_CIPHER, "aegis256");
        assert_eq!(c.validate(), Ok(()), "the default cipher, no key: off");
        c.encryption_cipher = "aes256gcm".into();
        match c.validate() {
            Err(Error::Config(m)) => assert!(
                m.contains("BUCKWAL_ENCRYPTION_CIPHER")
                    && m.contains("BUCKWAL_ENCRYPTION_KEY_FILE"),
                "{m}"
            ),
            r => panic!("{r:?}"),
        }
        c.encryption_key_file = Some("/k".into());
        assert_eq!(c.validate(), Ok(()));
    }

    #[test]
    fn from_env_reads_encryption_settings() {
        let read = |env: &HashMap<&str, &str>| {
            Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap()
        };
        let mut env = required();
        let c = read(&env);
        assert_eq!(c.encryption_key_file, None);
        assert_eq!(c.encryption_cipher, "aegis256");
        env.insert("BUCKWAL_ENCRYPTION_KEY_FILE", "");
        env.insert("BUCKWAL_ENCRYPTION_CIPHER", "");
        let c = read(&env);
        assert_eq!(c.encryption_key_file, None, "empty = off");
        assert_eq!(c.encryption_cipher, "aegis256", "empty = the default");
        env.insert("BUCKWAL_ENCRYPTION_KEY_FILE", "/keys/db.key");
        env.insert("BUCKWAL_ENCRYPTION_CIPHER", "aegis128l");
        let c = read(&env);
        assert_eq!(c.encryption_key_file, Some(PathBuf::from("/keys/db.key")));
        assert_eq!(c.encryption_cipher, "aegis128l");
        let debug = format!("{c:?}");
        assert!(
            debug.contains("/keys/db.key") && debug.contains("aegis128l"),
            "{debug}"
        );
        assert!(c.engine(false).encryption.is_none(), "open loads the key");
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

    #[test]
    fn journal_mode_from_env() {
        let mut env = required();
        let read = |env: &HashMap<&str, &str>| {
            Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string()))
        };
        assert_eq!(JournalMode::default(), JournalMode::Wal);
        assert_eq!(read(&env).unwrap().journal_mode, JournalMode::Wal);
        assert_eq!(
            Config::new("/d", "http://e", "b", "ak", "sk").journal_mode,
            JournalMode::Wal
        );
        env.insert("BUCKWAL_JOURNAL_MODE", "mvcc");
        assert_eq!(read(&env).unwrap().journal_mode, JournalMode::Mvcc);
        env.insert("BUCKWAL_JOURNAL_MODE", "wal");
        assert_eq!(read(&env).unwrap().journal_mode, JournalMode::Wal);
    }

    #[test]
    fn unknown_journal_mode_is_config_error() {
        let mut env = required();
        for bad in ["MVCC", "delete", "", "wal2"] {
            env.insert("BUCKWAL_JOURNAL_MODE", bad);
            match Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())) {
                Err(Error::Config(m)) => assert!(m.contains("BUCKWAL_JOURNAL_MODE"), "{m}"),
                r => panic!("{bad:?}: {r:?}"),
            }
        }
    }

    #[test]
    fn window_refused_in_mvcc() {
        let window = Batching::Window {
            max_wait: Duration::from_millis(10),
            max_commits: 4,
            max_bytes: 1024,
        };
        let mut c = Config::new("/d", "http://e", "b", "ak", "sk");
        c.batching = window;
        assert_eq!(c.validate(), Ok(()), "Window stays valid in WAL mode");
        c.journal_mode = JournalMode::Mvcc;
        assert_eq!(
            c.validate(),
            Err(Error::Unsupported("MVCC: Window batching"))
        );
        c.batching = Batching::Off;
        assert_eq!(c.validate(), Ok(()));
        // From the environment.
        let mut env = required();
        env.insert("BUCKWAL_JOURNAL_MODE", "mvcc");
        env.insert("BUCKWAL_BATCHING", "window 10ms 4 1KiB");
        let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(
            c.validate(),
            Err(Error::Unsupported("MVCC: Window batching"))
        );
    }
}
