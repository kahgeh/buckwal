//! The trimmer (D10): deletes the log objects, manifests and page objects that no head can
//! reach any more. Generations only move forward, so an object unreferenced by one head stays
//! unreferenced by every later one.

use crate::config::{Event, EventHook};
use crate::error::{Error, Result};
use crate::format::{Head, Keys, Manifest};
use crate::store::ObjectStore;
use crate::uploader::TrimSnapshot;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

fn list(store: &dyn ObjectStore, prefix: &str) -> Result<Vec<String>> {
    store
        .list(prefix)
        .map_err(|e| Error::Bucket(format!("list {prefix}: {e}")))
}

/// Delete what head `{G, manifest M}` no longer reaches: `log/<g < G>/*`, `manifest/<m < M>`
/// and `pages/<g < M>/<p>` where manifest M maps `p` to a generation other than `g`. Nothing
/// of a generation ≥ M is touched, except the logs below G. Without a manifest only older logs
/// go. Returns how many objects were deleted.
pub(crate) fn trim_once(
    store: &dyn ObjectStore,
    keys: &Keys,
    head: &Head,
    manifest: Option<&Manifest>,
) -> Result<u64> {
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

/// The trimmer thread: it acts on the newest snapshot older than `grace` (older ones are
/// superseded) and emits `Trimmed` after each successful pass. A failed pass is left for the
/// next snapshot. It exits when every sender is gone.
pub(crate) fn spawn_trimmer(
    store: Arc<dyn ObjectStore>,
    keys: Keys,
    grace: Duration,
    rx: Receiver<TrimSnapshot>,
    events: Option<EventHook>,
) -> JoinHandle<()> {
    // The thread reports to the subscriber of the thread that opened the database.
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    std::thread::Builder::new()
        .name("turso-durable-trimmer".into())
        .spawn(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                // Oldest first: snapshots arrive in seal order.
                let mut pending: Vec<TrimSnapshot> = Vec::new();
                loop {
                    let now = Instant::now();
                    if let Some(i) = pending
                        .iter()
                        .rposition(|s| now.saturating_duration_since(s.at) >= grace)
                    {
                        let s = pending
                            .drain(..=i)
                            .next_back()
                            .expect("drained at least one");
                        if let Ok(deleted) = trim_once(&*store, &keys, &s.head, s.manifest.as_ref())
                        {
                            if let Some(hook) = &events {
                                hook(&Event::Trimmed { deleted });
                            }
                        }
                        continue;
                    }
                    let next = pending
                        .first()
                        .and_then(|s| s.at.checked_add(grace))
                        .map(|due| due.saturating_duration_since(now));
                    let got = match next {
                        Some(wait) => rx.recv_timeout(wait),
                        None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
                    };
                    match got {
                        Ok(s) => {
                            pending.push(s);
                            pending.extend(rx.try_iter());
                        }
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => return,
                    }
                }
            })
        })
        .expect("spawn trimmer thread")
}

#[cfg(test)]
mod tests;
