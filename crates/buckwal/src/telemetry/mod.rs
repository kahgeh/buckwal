//! Telemetry: the library reports through `tracing` only. Spans (all INFO, target
//! `buckwal`) and log events are a contract with the HyperDX searches:
//!
//! - `buckwal.commit` (`buckwal.batching`, `buckwal.journal_mode`, `buckwal.commit_no`, `buckwal.bytes`,
//!   `buckwal.outcome`), with children `buckwal.local_write`, `buckwal.local_fsync` and
//!   `buckwal.ack_wait` (`io/wal_file.rs`, `conn.rs`); with `buckwal.batching` starting
//!   `background` the span ends with its `buckwal.local_fsync`, has no `buckwal.ack_wait` and no
//!   `buckwal.outcome`;
//! - `buckwal.upload` (`buckwal.commits`, `buckwal.commit_first`, `buckwal.commit_last`,
//!   `buckwal.bytes`, `buckwal.generation`, `buckwal.seq`, `buckwal.retries`,
//!   `buckwal.oldest_behind_ms`, `s3.status`; a root span when its first commit is a background
//!   one), `buckwal.generation_switch` (format 5: `buckwal.generation`, `buckwal.next_generation`,
//!   `buckwal.seq`, `buckwal.retries`, `s3.status`, `buckwal.elapsed_ms`, `buckwal.outcome`:
//!   `switched`, `fenced` or `upload_failed`) and, for formats 1 to 4, `buckwal.checkpoint_upload`
//!   (`buckwal.generation`, `buckwal.seq`, `buckwal.pages`, `buckwal.retries`, `s3.status`,
//!   and `buckwal.skipped_generation`, on the first seal after a takeover only),
//!   on the uploader thread (`uploader.rs`, fields recorded by `bucket.rs` and the stores);
//! - `buckwal.restore` (`buckwal.generation`, `buckwal.log_objects`, `buckwal.gets`,
//!   `buckwal.bytes`, `buckwal.seq`, `buckwal.retries`, `s3.status`, `buckwal.outcome`)
//!   (`restore.rs`);
//! - `buckwal.takeover` (`buckwal.generation`, `buckwal.epoch`, `buckwal.seq` (the fence's),
//!   `buckwal.retries`, `s3.status`, `buckwal.outcome`, `buckwal.quiet_ms`,
//!   `buckwal.quiet_old_ms` (absent for a head without timeouts), `buckwal.quiet_own_ms`,
//!   `buckwal.next_generation_floor`), around a takeover's head write and
//!   fence, or around the fence of a takeover an `open` resumes (`db.rs`), and
//!   `buckwal.fence` (`buckwal.generation`, `buckwal.seq`, `buckwal.epoch`,
//!   `buckwal.retries`, `s3.status`) around each fence record (`bucket.rs`);
//! - `buckwal.pack`, one per pack run (`buckwal.pack.generation`, `buckwal.pack.key`,
//!   `buckwal.pack.bytes_down`, `buckwal.pack.bytes_up`, `buckwal.pack.size`,
//!   `buckwal.free_page_share` (free pages over all pages of the new pack: the data of the warn
//!   alert past 0.2 that tells you to run `buckwal vacuum`, spec Choice A (a)),
//!   `buckwal.pack.<step>_ms` for each step it made,
//!   `buckwal.elapsed_ms`, `buckwal.pack.failures`, `buckwal.outcome`: `packed`,
//!   `nothing_to_do`, `superseded` or `failed`, with `buckwal.pack.failed_step`,
//!   `buckwal.pack.failed_key` and `buckwal.error`) (`pack/run.rs`), and
//!   `buckwal.pack.gauges`, the writer's gauges every 5 minutes (`buckwal.pack.behind_bytes`,
//!   `buckwal.pack.age_seconds`, `buckwal.pack.generation`, `buckwal.pack.size_bytes`,
//!   `buckwal.outcome`) (`pack/thread.rs`);
//! - `buckwal.vacuum`, one per `buckwal vacuum` (`buckwal.vacuum.page_size`,
//!   `buckwal.vacuum.pages_before`, `buckwal.vacuum.free_pages_before`,
//!   `buckwal.vacuum.pages_after`, `buckwal.vacuum.free_pages_after`,
//!   `buckwal.vacuum.bytes_before`, `buckwal.vacuum.bytes_after`,
//!   `buckwal.free_page_share` before the vacuum, `buckwal.elapsed_ms`, `buckwal.outcome`:
//!   `vacuumed` or the error's, and `buckwal.error`) (`vacuum.rs`).
//!
//! `buckwal.outcome = integrity` marks a span whose read failed an integrity check, next to
//! the ERROR record `integrity check failed` (`format/integrity.rs`).
//!
//! Log events have the target `buckwal` exactly.
//!
//! Threads the library spawns (its own and the scoped ones of a seal or a restore) run under
//! the dispatcher of the thread that spawned them, so a subscriber set on the opening thread
//! sees their spans.
//!
//! [`init`] exports all of it over OTLP/gRPC, through a global subscriber. A process that
//! instead only sets scoped subscribers (`tracing::subscriber::with_default`) may lose
//! callsites: tracing caches each callsite's interest the first time it is hit, and while at
//! most one dispatcher exists, a callsite first hit on a thread with no subscriber is cached
//! as disabled for every thread (standard tracing behaviour). A global subscriber avoids it.

mod pipeline;

/// Record `buckwal.outcome = outcome` on the innermost span enclosing the caller that declares
/// that field (a `buckwal.restore`, `buckwal.gc` or `buckwal.takeover` span, or a caller's
/// own). Spans of the engine (`turso_core`) in between do not declare it and are skipped.
/// Does nothing without a `tracing_subscriber` registry (no subscriber, or another kind).
pub(crate) fn record_enclosing_outcome(outcome: &str) {
    use tracing_subscriber::registry::{LookupSpan, Registry};
    tracing::Span::current().with_subscriber(|(id, dispatch)| {
        let Some(registry) = dispatch.downcast_ref::<Registry>() else {
            return;
        };
        let Some(span) = registry.span(id) else {
            return;
        };
        for s in span.scope() {
            let fields = s.metadata().fields();
            let Some(field) = fields.field("buckwal.outcome") else {
                continue;
            };
            let value: &dyn tracing::field::Value = &outcome;
            let pairs = [(&field, Some(value))];
            let values = fields.value_set(&pairs);
            dispatch.record(&s.id(), &tracing::span::Record::new(&values));
            return;
        }
    });
}

use crate::config::{defaults, parse_bool};
use crate::error::{Error, Result};
use pipeline::Providers;
use tracing::Level;

/// Whether a span or log record with `target` may be exported: only the library's own
/// (`buckwal*`), never the engine's (`turso_core`), nor the exporter's own stack
/// (`opentelemetry*`, `tonic`, `h2`, `hyper`, `tower`, `reqwest`), whose records would loop
/// back into the exporter, nor anything else.
pub(crate) fn export_filter() -> fn(&str) -> bool {
    |target| target.starts_with("buckwal")
}

/// What [`init`] installs. [`Default`] is every [`defaults`] value: disabled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryConfig {
    /// Export at all (`BUCKWAL_OTEL`).
    pub enabled: bool,
    /// The OTLP/gRPC collector, `http://host:port` (`BUCKWAL_OTEL_ENDPOINT`). Set in code,
    /// so `OTEL_EXPORTER_OTLP_*ENDPOINT` cannot redirect it; `OTEL_EXPORTER_OTLP_*HEADERS`
    /// are still sent.
    pub endpoint: String,
    /// The least severe log record exported (`BUCKWAL_OTEL_LEVEL`).
    pub level: Level,
    /// Added as `test_run_id` to every span and log record when set (`BUCKWAL_TEST_RUN_ID`).
    pub test_run_id: Option<String>,
}

impl Default for TelemetryConfig {
    fn default() -> TelemetryConfig {
        TelemetryConfig {
            enabled: defaults::OTEL,
            endpoint: defaults::OTEL_ENDPOINT.to_string(),
            level: defaults::OTEL_LEVEL,
            test_run_id: None,
        }
    }
}

fn parse_level(var: &str, v: &str) -> Result<Level> {
    match v.trim().to_ascii_lowercase().as_str() {
        "trace" => Ok(Level::TRACE),
        "debug" => Ok(Level::DEBUG),
        "info" => Ok(Level::INFO),
        "warn" => Ok(Level::WARN),
        "error" => Ok(Level::ERROR),
        _ => Err(Error::Config(format!(
            "{var} must be trace, debug, info, warn or error, not {v:?}"
        ))),
    }
}

impl TelemetryConfig {
    /// Read `BUCKWAL_OTEL` (0/1, true/false or on/off), `BUCKWAL_OTEL_ENDPOINT`, `BUCKWAL_OTEL_LEVEL`
    /// (trace|debug|info|warn|error) and `BUCKWAL_TEST_RUN_ID`; an unset one keeps its
    /// [`defaults`] value. A bad value is a `Config` error.
    pub fn from_env() -> Result<TelemetryConfig> {
        TelemetryConfig::from_lookup(|k| std::env::var(k).ok())
    }

    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<TelemetryConfig> {
        let mut c = TelemetryConfig::default();
        if let Some(v) = get("BUCKWAL_OTEL") {
            c.enabled = parse_bool("BUCKWAL_OTEL", &v)?;
        }
        if let Some(v) = get("BUCKWAL_OTEL_ENDPOINT") {
            c.endpoint = v;
        }
        if let Some(v) = get("BUCKWAL_OTEL_LEVEL") {
            c.level = parse_level("BUCKWAL_OTEL_LEVEL", &v)?;
        }
        c.test_run_id = get("BUCKWAL_TEST_RUN_ID").filter(|v| !v.is_empty());
        Ok(c)
    }
}

/// The endpoint to export to, trimmed once: the value checked is the value used. It must
/// be `http://`.
fn endpoint(cfg: &TelemetryConfig) -> Result<&str> {
    let endpoint = cfg.endpoint.trim();
    if !endpoint.to_ascii_lowercase().starts_with("http://") {
        return Err(Error::Config(format!(
            "BUCKWAL_OTEL_ENDPOINT must be http://host:port (no TLS), not {:?}",
            cfg.endpoint
        )));
    }
    Ok(endpoint)
}

/// Export spans and log records over OTLP/gRPC to `cfg.endpoint`, with `service.name =
/// buckwal`, through a global `tracing` subscriber. Only `buckwal*` targets are
/// exported: spans at INFO and above, log records at `cfg.level` and above. Export runs on
/// background threads through bounded queues ([`defaults::OTEL_QUEUE`]) that drop when full,
/// so a slow or missing collector never slows a commit.
///
/// Disabled, nothing is installed and the guard is empty. A process that already has a
/// global subscriber gets a `Config` error. Keep the guard: dropping it flushes and stops
/// the export, for at most [`defaults::OTEL_SHUTDOWN`].
pub fn init(cfg: TelemetryConfig) -> Result<TelemetryGuard> {
    if !cfg.enabled {
        return Ok(TelemetryGuard { running: None });
    }
    let endpoint = endpoint(&cfg)?;
    let (providers, runtime) = Providers::otlp(endpoint, cfg.test_run_id.clone())?;
    let subscriber = providers.subscriber(cfg.level);
    let guard = TelemetryGuard {
        running: Some(Running {
            providers,
            runtime: Some(runtime),
        }),
    };
    // Not `try_init`: with tracing-subscriber's `tracing-log` feature (which turso_core
    // turns on) it would also install a `log` logger, and fail after the subscriber is set
    // when a `log` logger already exists. On an error the guard stops the export it built.
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|_| Error::Config("a global tracing subscriber is already set".into()))?;
    Ok(guard)
}

/// Keeps the export running. Dropping it, or [`TelemetryGuard::shutdown`], flushes and stops
/// both providers on helper threads, waiting [`defaults::OTEL_SHUTDOWN`] at most in total,
/// then lets the export runtime go without blocking. The global subscriber stays installed
/// and exports nothing more.
pub struct TelemetryGuard {
    running: Option<Running>,
}

struct Running {
    providers: Providers,
    runtime: Option<tokio::runtime::Runtime>,
}

impl TelemetryGuard {
    /// Flush and stop now (as dropping the guard does).
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        let Some(mut r) = self.running.take() else {
            return;
        };
        r.providers.shutdown_within(defaults::OTEL_SHUTDOWN);
        if let Some(rt) = r.runtime.take() {
            rt.shutdown_background();
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

impl std::fmt::Debug for TelemetryGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelemetryGuard")
            .field("running", &self.running.is_some())
            .finish()
    }
}

#[cfg(test)]
pub(crate) mod testing;

#[cfg(test)]
mod tests;
