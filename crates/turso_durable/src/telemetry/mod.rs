//! Telemetry: the library reports through `tracing` only. Spans (all INFO, target
//! `turso_durable`) and log events are a contract with the HyperDX searches:
//!
//! - `durable.commit` (`durable.batching`, `durable.commit_no`, `durable.bytes`,
//!   `durable.outcome`), with children `durable.local_write`, `durable.local_fsync` and
//!   `durable.ack_wait` (`io/wal_file.rs`, `conn.rs`);
//! - `durable.upload` (`durable.commits`, `durable.bytes`, `durable.generation`,
//!   `durable.seq`, `durable.retries`, `s3.status`) and `durable.checkpoint_upload`
//!   (`durable.generation`, `durable.pages`, `durable.retries`, `s3.status`), on the uploader
//!   thread (`uploader.rs`, fields recorded by `bucket.rs` and the stores);
//! - `durable.restore` (`durable.generation`, `durable.log_objects`, `durable.gets`,
//!   `durable.bytes`) (`restore.rs`).
//!
//! Threads the library spawns run under the dispatcher of the thread that spawned them, so a
//! subscriber set on the opening thread sees their spans.

/// Whether a span or log record with `target` may be exported: only the library's own
/// (`turso_durable*`), never the engine's (`turso_core`) or anything else. A stub over the
/// target; the export pipeline builds its filter on it.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn export_filter() -> fn(&str) -> bool {
    |target| target.starts_with("turso_durable")
}

#[cfg(test)]
pub(crate) mod testing;

#[cfg(test)]
mod tests;
