//! Public configuration types.
use crate::store::RetryPolicy;
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
    pub events: Option<EventHook>,
}
