//! Public configuration: [`Config`] and its defaults. `Event` and `EventHook` (the
//! `on_event` field) are test and diagnostics hooks, hidden from the docs.
use crate::batching::Batching;
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
    /// How long a writer trusts its last look at the head (`DURABLE_LEASE_MS`): past it, the
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
    /// A writer runs GC itself (`DURABLE_GC_AFTER_SEAL`): at open, after each seal and when
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
    /// objects (`DURABLE_MAINTENANCE_LOG_OBJECTS`; 0 = this measure is off). A restore reads
    /// about this many log objects at most.
    pub const MAINTENANCE_LOG_OBJECTS: u64 = 1000;
    /// ... or once the local `-wal` has grown to this many bytes since the generation began
    /// (`DURABLE_MAINTENANCE_WAL_MIB`, in MiB; 0 = this measure is off).
    pub const MAINTENANCE_WAL_BYTES: u64 = 64 << 20;
    /// How long maintenance waits for in-flight calls (and open transactions) to end, with
    /// new ones held back, before it gives up and retries later
    /// (`DURABLE_MAINTENANCE_GATE_WAIT_MS`). It bounds the stall a maintenance run adds to
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

    /// Export traces and logs over OTLP (`DURABLE_OTEL`): off in the library.
    pub const OTEL: bool = false;
    /// Where OTLP/gRPC goes (`DURABLE_OTEL_ENDPOINT`); plain `http://` only, no TLS.
    pub const OTEL_ENDPOINT: &str = "http://localhost:4317";
    /// The least severe log record exported (`DURABLE_OTEL_LEVEL`); spans are always INFO.
    pub const OTEL_LEVEL: tracing::Level = tracing::Level::INFO;
    /// How long one OTLP export may take. Fixed.
    pub const OTEL_EXPORT_TIMEOUT: Duration = Duration::from_secs(1);
    /// How long `TelemetryGuard` waits, in total, for both providers to flush and stop. Fixed.
    pub const OTEL_SHUTDOWN: Duration = Duration::from_secs(2);
    /// How many spans, and how many log records, may wait for export; more are dropped.
    /// Fixed.
    pub const OTEL_QUEUE: usize = 2048;
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
    /// How long an unreferenced object stays in the bucket before GC deletes it: GC trims
    /// against a manifest only once it is `trim_grace + retry_deadline + request_timeout +
    /// 1 s` old by the bucket's own clock. It must exceed `lease + retry_deadline +
    /// request_timeout` (`Config` otherwise).
    pub trim_grace: Duration,
    /// A writer open runs GC itself, on a thread of its own: at open, after each seal and
    /// when the next manifest comes of age (`DURABLE_GC_AFTER_SEAL`). Off, only
    /// [`crate::gc`] deletes anything.
    pub gc_after_seal: bool,
    /// How long the writer trusts its last confirmation that the head is still its own: a
    /// confirmed log record or head write, or a head GET that found its head. Past it (on
    /// either the monotonic or the wall clock), the next append, and each page or manifest
    /// PUT attempt of a seal, GETs the head first; another writer's head fences this one.
    /// 0 checks before every append and page PUT (tests).
    pub lease: Duration,
    pub prefetch: bool,
    /// How commits are gathered into one upload (`DURABLE_BATCHING`).
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
            .field("gc_after_seal", &self.gc_after_seal)
            .field("lease", &self.lease)
            .field("prefetch", &self.prefetch)
            .field("batching", &self.batching)
            .field("maintenance_log_objects", &self.maintenance_log_objects)
            .field("maintenance_wal_bytes", &self.maintenance_wal_bytes)
            .field("maintenance_gate_wait", &self.maintenance_gate_wait)
            .field("on_event", &self.on_event.as_ref().map(|_| "<hook>"))
            .finish()
    }
}

/// Every environment variable [`Config::from_env`] reads.
const ENV_VARS: [&str; 18] = [
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
    "DURABLE_BATCHING",
    "DURABLE_MAINTENANCE_LOG_OBJECTS",
    "DURABLE_MAINTENANCE_WAL_MIB",
    "DURABLE_MAINTENANCE_GATE_WAIT_MS",
    "DURABLE_LEASE_MS",
    "DURABLE_GC_AFTER_SEAL",
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
        if let Some(v) = get("DURABLE_GC_AFTER_SEAL") {
            c.gc_after_seal = parse_bool("DURABLE_GC_AFTER_SEAL", &v)?;
        }
        if let Some(v) = get("DURABLE_LEASE_MS") {
            c.lease = Duration::from_millis(parse_num("DURABLE_LEASE_MS", &v)?);
        }
        if let Some(v) = get("DURABLE_PREFETCH") {
            c.prefetch = parse_bool("DURABLE_PREFETCH", &v)?;
        }
        if let Some(v) = get("DURABLE_BATCHING") {
            c.batching = v.parse().map_err(|e| match e {
                Error::Config(m) => Error::Config(format!("DURABLE_BATCHING: {m}")),
                e => e,
            })?;
        }
        if let Some(v) = get("DURABLE_MAINTENANCE_LOG_OBJECTS") {
            c.maintenance_log_objects = parse_num("DURABLE_MAINTENANCE_LOG_OBJECTS", &v)?;
        }
        if let Some(v) = get("DURABLE_MAINTENANCE_WAL_MIB") {
            let var = "DURABLE_MAINTENANCE_WAL_MIB";
            let mib: u64 = parse_num(var, &v)?;
            c.maintenance_wal_bytes = mib
                .checked_mul(1 << 20)
                .ok_or_else(|| Error::Config(format!("{var} is too large: {v:?}")))?;
        }
        if let Some(v) = get("DURABLE_MAINTENANCE_GATE_WAIT_MS") {
            c.maintenance_gate_wait =
                Duration::from_millis(parse_num("DURABLE_MAINTENANCE_GATE_WAIT_MS", &v)?);
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
    fn gc_after_seal_from_env() {
        assert!(Config::new("/d", "http://e", "b", "ak", "sk").gc_after_seal);
        let env = required();
        let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
        assert!(c.gc_after_seal, "on by default");
        for (v, want) in [("off", false), ("0", false), ("true", true)] {
            let mut env = required();
            env.insert("DURABLE_GC_AFTER_SEAL", v);
            let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
            assert_eq!(c.gc_after_seal, want, "{v}");
        }
        let mut env = required();
        env.insert("DURABLE_GC_AFTER_SEAL", "sometimes");
        match Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())) {
            Err(Error::Config(m)) => assert!(m.contains("DURABLE_GC_AFTER_SEAL"), "{m}"),
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

    #[test]
    fn lease_from_env() {
        let mut env = required();
        env.insert("DURABLE_LEASE_MS", "1500");
        let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(c.lease, Duration::from_millis(1500));
        assert_eq!(c.engine(false).lease, Duration::from_millis(1500));
        let env = required();
        let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(c.lease, Duration::from_secs(30));
        assert_eq!(defaults::LEASE, Duration::from_secs(30));
        let mut env = required();
        env.insert("DURABLE_LEASE_MS", "soon");
        match Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())) {
            Err(Error::Config(m)) => assert!(m.contains("DURABLE_LEASE_MS"), "{m}"),
            r => panic!("{r:?}"),
        }
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
            ("DURABLE_BATCHING", "window 10ms 8 64KiB"),
            ("DURABLE_MAINTENANCE_LOG_OBJECTS", "250"),
            ("DURABLE_MAINTENANCE_WAL_MIB", "3"),
            ("DURABLE_MAINTENANCE_GATE_WAIT_MS", "75"),
            ("DURABLE_LEASE_MS", "2500"),
            ("DURABLE_GC_AFTER_SEAL", "off"),
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

        // Only the four required ones: the rest are defaults.
        let env = required();
        let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!(c.region, defaults::REGION);
        assert_eq!(c.upload_concurrency, defaults::UPLOAD_CONCURRENCY);
        assert_eq!(c.batching, Batching::Off);

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
    fn batching_from_env_and_validate() {
        let mut env = required();
        env.insert("DURABLE_BATCHING", "nope");
        match Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())) {
            Err(Error::Config(m)) => {
                assert!(
                    m.contains("nope") && m.starts_with("DURABLE_BATCHING: "),
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
        env.insert("DURABLE_MAINTENANCE_LOG_OBJECTS", "0");
        env.insert("DURABLE_MAINTENANCE_WAL_MIB", "0");
        let c = Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())).unwrap();
        assert_eq!((c.maintenance_log_objects, c.maintenance_wal_bytes), (0, 0));
        for var in [
            "DURABLE_MAINTENANCE_LOG_OBJECTS",
            "DURABLE_MAINTENANCE_WAL_MIB",
            "DURABLE_MAINTENANCE_GATE_WAIT_MS",
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
        env.insert("DURABLE_MAINTENANCE_WAL_MIB", "18446744073709551615");
        assert!(matches!(
            Config::from_lookup("/d", |k| env.get(k).map(|v| v.to_string())),
            Err(Error::Config(m)) if m.contains("DURABLE_MAINTENANCE_WAL_MIB")
        ));
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
