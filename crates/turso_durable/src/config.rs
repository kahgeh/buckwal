//! Public configuration types.
use std::sync::Arc;
use std::time::Duration;

/// Called synchronously, on the thread that did the work, for each [`Event`].
pub type EventHook = Arc<dyn Fn(&Event) + Send + Sync>;

/// Progress notifications, for tests and instrumentation.
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
