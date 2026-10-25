//! The trimmer (D10): deletes the log objects, manifests and page objects that no head can
//! reach any more. Generations only move forward, so an object unreferenced by one head stays
//! unreferenced by every later one. Which head is old enough to trim against is GC's choice
//! (`gc::gc_pass`), on the bucket's clock. A format 5 bucket is trimmed by `gc::pack_pass`
//! (against the pack pointer), never by `trim_once`.

use crate::config::defaults::MAINTENANCE_RETRY_MAX;
use crate::config::{Event, EventHook};
use crate::error::{Error, Result};
use crate::format::{Head, Keys, Manifest, FORMAT_V5};
use crate::gc::{gc_pass, GcAges, GcOutcome};
use crate::store::ObjectStore;
use crate::uploader::TrimSnapshot;
use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError};
use std::sync::Arc;
use std::thread::JoinHandle;

fn list(store: &dyn ObjectStore, prefix: &str) -> Result<Vec<String>> {
    store
        .list(prefix)
        .map_err(|e| Error::Bucket(format!("list {prefix}: {e}")))
}

/// Delete what head `{G, manifest M}` no longer reaches: `log/<g < G>/*`, `manifest/<m < M>`
/// and `pages/<g < M>/<p>` where manifest M maps `p` to a generation other than `g`. Nothing
/// of a generation ≥ M is touched, except the logs below G. Without a manifest only older logs
/// go. Returns how many objects were deleted. Formats 1 to 4 only: a format 5 head is
/// `Unsupported` and nothing is deleted, because its log below the head can be the only copy
/// of the database (GC of format 5 deletes log against the pack pointer, `gc::pack_pass`).
pub(crate) fn trim_once(
    store: &dyn ObjectStore,
    keys: &Keys,
    head: &Head,
    manifest: Option<&Manifest>,
) -> Result<u64> {
    if head.format == FORMAT_V5 {
        return Err(Error::Unsupported(
            "a seal trim of a format 5 head (GC trims it against the pack pointer)",
        ));
    }
    let mut doomed: Vec<String> = list(store, &keys.log_root())?
        .into_iter()
        .filter(|k| matches!(keys.parse_log(k), Some((g, _)) if g < head.generation))
        .collect();
    if let (Some(m_gen), Some(m)) = (head.manifest, manifest) {
        doomed.extend(
            list(store, &keys.manifest_root())?
                .into_iter()
                .filter(|k| matches!(keys.parse_manifest(k), Some(g) if g < m_gen)),
        );
        doomed.extend(list(store, &keys.page_root())?.into_iter().filter(|k| {
            matches!(keys.parse_page(k), Some((g, p)) if g < m_gen && m.pages.get(&p) != Some(&g))
        }));
    }
    for k in &doomed {
        store
            .delete(k)
            .map_err(|e| Error::Bucket(format!("delete {k}: {e}")))?;
    }
    Ok(doomed.len() as u64)
}

#[cfg(test)]
thread_local! {
    static NO_WRITER_GC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Tests only: while alive, a writer opened on this thread starts no GC thread, so that only
/// the test's own passes (or none) touch the bucket, and its request counts and spans are the
/// test's.
#[cfg(test)]
pub(crate) struct NoWriterGc;

#[cfg(test)]
impl NoWriterGc {
    pub(crate) fn on() -> Self {
        NO_WRITER_GC.with(|f| f.set(true));
        NoWriterGc
    }
}

#[cfg(test)]
impl Drop for NoWriterGc {
    fn drop(&mut self) {
        NO_WRITER_GC.with(|f| f.set(false));
    }
}

/// Whether a writer opened on this thread starts no GC thread (tests: `NoWriterGc`). A
/// writer always runs one otherwise.
pub(crate) fn no_writer_gc() -> bool {
    #[cfg(test)]
    {
        NO_WRITER_GC.with(|f| f.get())
    }
    #[cfg(not(test))]
    {
        false
    }
}

/// The key of the pack the writer's background fetch still reads, asked before each pass
/// (`BucketDbStorage::reading_pack`): GC never deletes it.
pub(crate) type Fetching = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// The writer's GC thread. It runs a GC pass (`gc::gc_pass`, with `ages`) at start, on every
/// message (the open's, then one per seal or generation switch; messages queued together make
/// one pass) and when the next manifest, pack or orphan comes of age (the pass's `next_due`).
/// It emits `Trimmed` after each pass that trimmed. A failed pass is retried on the next
/// message, or after `MAINTENANCE_RETRY_MAX`; it never ends the thread. It exits when every
/// sender is gone: a pass in progress finishes first, and the messages of a closing writer
/// (its last seal) start none.
pub(crate) fn spawn_trimmer(
    store: Arc<dyn ObjectStore>,
    keys: Keys,
    ages: GcAges,
    rx: Receiver<TrimSnapshot>,
    events: Option<EventHook>,
    fetching: Fetching,
) -> JoinHandle<()> {
    // The thread reports to the subscriber of the thread that opened the database.
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    std::thread::Builder::new()
        .name("buckwal-trimmer".into())
        .spawn(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                trimmer_loop(&*store, &keys, ages, &rx, events.as_ref(), &fetching)
            })
        })
        .expect("spawn trimmer thread")
}

fn trimmer_loop(
    store: &dyn ObjectStore,
    keys: &Keys,
    ages: GcAges,
    rx: &Receiver<TrimSnapshot>,
    events: Option<&EventHook>,
    fetching: &Fetching,
) {
    // The open's message is the pass at start: it is not run twice.
    while rx.try_recv().is_ok() {}
    let mut last = None;
    loop {
        let reading = fetching();
        let wait = match gc_pass(store, keys, ages, last.as_ref(), reading.as_deref()) {
            Ok(pass) => {
                if pass.outcome == GcOutcome::Trimmed {
                    last = pass.mark;
                }
                // A pass that deleted nothing is not reported: the trimmer wakes on every
                // generation switch.
                if pass.deleted > 0 {
                    if let Some(hook) = events {
                        hook(&Event::Trimmed {
                            deleted: pass.deleted,
                        });
                    }
                }
                pass.next_due
            }
            Err(e) => {
                tracing::warn!(target: "buckwal", error = %e, "gc pass failed");
                Some(MAINTENANCE_RETRY_MAX)
            }
        };
        let got = match wait {
            Some(wait) => rx.recv_timeout(wait),
            None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
        };
        match got {
            Ok(_) => {
                // Gather every message already queued into this one pass; a writer that
                // closed meanwhile gets none.
                loop {
                    match rx.try_recv() {
                        Ok(_) => {}
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => return,
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

#[cfg(test)]
mod tests;
