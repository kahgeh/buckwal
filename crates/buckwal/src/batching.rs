//! The batching setting: how long and how much the uploader gathers before it ships a log
//! object. Parsed from and shown as the PRAGMA text, `off` or `window <max_wait>
//! <max_commits> <max_bytes>`, e.g. `window 50ms 64 1MiB`.
use crate::config::defaults;
use crate::error::{Error, Result};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

/// Whether, and how, commits are gathered into one upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Batching {
    /// Every commit is uploaded on its own.
    Off,
    /// Gather commits until `max_wait` has passed since the first, or `max_commits` or
    /// `max_bytes` is reached. Both counts must be at least 1, and `max_wait` at most 60 s
    /// ([`Batching::validate`]).
    Window {
        max_wait: Duration,
        max_commits: u32,
        max_bytes: u64,
    },
}

fn bad(text: &str, why: &str) -> Error {
    Error::Config(format!("batching {text:?}: {why}"))
}

fn parse_wait(text: &str, tok: &str) -> Result<Duration> {
    let (digits, unit) = match tok.find(|c: char| !c.is_ascii_digit()) {
        Some(i) => tok.split_at(i),
        None => {
            return Err(bad(
                text,
                &format!("max_wait {tok:?} needs a unit (us, ms or s)"),
            ))
        }
    };
    let n: u64 = digits
        .parse()
        .map_err(|_| bad(text, &format!("max_wait {tok:?} is not <integer>(us|ms|s)")))?;
    match unit {
        "us" => Ok(Duration::from_micros(n)),
        "ms" => Ok(Duration::from_millis(n)),
        "s" => Ok(Duration::from_secs(n)),
        _ => Err(bad(
            text,
            &format!("max_wait {tok:?} has unknown unit {unit:?}"),
        )),
    }
}

fn parse_bytes(text: &str, tok: &str) -> Result<u64> {
    let split = tok.find(|c: char| !c.is_ascii_digit()).unwrap_or(tok.len());
    let (digits, unit) = tok.split_at(split);
    let n: u64 = digits.parse().map_err(|_| {
        bad(
            text,
            &format!("max_bytes {tok:?} is not <integer>[B|KiB|MiB|GiB]"),
        )
    })?;
    let shift = match unit {
        "" | "B" => 0,
        "KiB" => 10,
        "MiB" => 20,
        "GiB" => 30,
        _ => {
            return Err(bad(
                text,
                &format!("max_bytes {tok:?} has unknown unit {unit:?}"),
            ))
        }
    };
    n.checked_mul(1u64 << shift)
        .ok_or_else(|| bad(text, &format!("max_bytes {tok:?} is too large")))
}

impl FromStr for Batching {
    type Err = Error;

    /// `off` or `window <max_wait> <max_commits> <max_bytes>`; keywords are case-insensitive
    /// and any whitespace separates the words. Zero limits parse; [`Batching::validate`]
    /// refuses them.
    fn from_str(text: &str) -> Result<Batching> {
        let words: Vec<&str> = text.split_whitespace().collect();
        match words.as_slice() {
            [w] if w.eq_ignore_ascii_case("off") => Ok(Batching::Off),
            [w, wait, commits, bytes] if w.eq_ignore_ascii_case("window") => {
                let max_commits = commits.parse::<u32>().map_err(|_| {
                    bad(text, &format!("max_commits {commits:?} is not an integer"))
                })?;
                Ok(Batching::Window {
                    max_wait: parse_wait(text, wait)?,
                    max_commits,
                    max_bytes: parse_bytes(text, bytes)?,
                })
            }
            _ => Err(bad(
                text,
                "expected `off` or `window <max_wait> <max_commits> <max_bytes>`",
            )),
        }
    }
}

impl fmt::Display for Batching {
    /// The canonical text: the largest unit that divides the value evenly.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Batching::Window {
            max_wait,
            max_commits,
            max_bytes,
        } = *self
        else {
            return f.write_str("off");
        };
        let us = max_wait.as_micros();
        let wait = if us == 0 {
            "0ms".to_string()
        } else if us % 1_000_000 == 0 {
            format!("{}s", us / 1_000_000)
        } else if us % 1_000 == 0 {
            format!("{}ms", us / 1_000)
        } else {
            format!("{us}us")
        };
        let bytes = [(30, "GiB"), (20, "MiB"), (10, "KiB")]
            .into_iter()
            .find(|&(shift, _)| max_bytes != 0 && max_bytes % (1u64 << shift) == 0)
            .map(|(shift, unit)| format!("{}{unit}", max_bytes >> shift))
            .unwrap_or_else(|| max_bytes.to_string());
        write!(f, "window {wait} {max_commits} {bytes}")
    }
}

impl Batching {
    /// Window batching acks followers before their upload is confirmed, which an MVCC
    /// database never does (`MVCC: Window batching`).
    pub(crate) fn check_mode(&self, mode: crate::config::JournalMode) -> Result<()> {
        match (self, mode) {
            (Batching::Window { .. }, crate::config::JournalMode::Mvcc) => {
                Err(Error::Unsupported("MVCC: Window batching"))
            }
            _ => Ok(()),
        }
    }

    /// A window needs room for at least one commit and one byte, and may wait at most
    /// [`defaults::MAX_BATCH_WAIT`] (60 s).
    pub fn validate(&self) -> Result<()> {
        if let Batching::Window {
            max_wait,
            max_commits,
            max_bytes,
        } = *self
        {
            if max_wait > defaults::MAX_BATCH_WAIT {
                return Err(Error::Config(format!(
                    "batching max_wait must be at most {}s",
                    defaults::MAX_BATCH_WAIT.as_secs()
                )));
            }
            if max_commits == 0 {
                return Err(Error::Config(
                    "batching max_commits must be at least 1".into(),
                ));
            }
            if max_bytes == 0 {
                return Err(Error::Config(
                    "batching max_bytes must be at least 1".into(),
                ));
            }
        }
        Ok(())
    }
}

/// A Window commit's `buckwal.commit` span, held until its ack. The exporter stamps a span's end
/// at its last exit, not at its close (tracing-opentelemetry), so dropping this enters the
/// span once more: the span then ends when its ack is collected, or when the caller drops
/// the commit unacked, and never earlier.
pub(crate) struct AckSpan(tracing::Span);

impl From<tracing::Span> for AckSpan {
    fn from(span: tracing::Span) -> Self {
        AckSpan(span)
    }
}

impl std::ops::Deref for AckSpan {
    type Target = tracing::Span;
    fn deref(&self) -> &tracing::Span {
        &self.0
    }
}

impl Drop for AckSpan {
    fn drop(&mut self) {
        let _end = self.0.enter();
    }
}

/// One commit of a `Connection` call, for the call to ack before it returns. `batching` is
/// the setting the commit was made with, even when a degraded uploader made a Window commit
/// take the blocking `Off` path (`Done`). `span` is the commit's `buckwal.commit` span. A
/// `Done` commit's span ended with its WAL sync (its record was in the bucket by then); a
/// `Waiting` commit's ends when the call collects its ack, or drops it.
pub(crate) enum PendingAck {
    /// Uploaded inside the WAL sync (`Off`, or a Window commit while the uploader is
    /// degraded): `waited` is how long the upload took.
    Done {
        commit: u64,
        waited: Duration,
        batching: Batching,
        span: tracing::Span,
    },
    /// Submitted to the uploader's batch; the call waits on the ticket.
    Waiting {
        ticket: crate::uploader::Ticket,
        batching: Batching,
        span: AckSpan,
    },
}

/// Record a commit's `buckwal.outcome`: `ok` when its record is in the bucket, else `error`.
pub(crate) fn record_outcome<T>(commit: &tracing::Span, r: &Result<T>) {
    commit.record("buckwal.outcome", if r.is_ok() { "ok" } else { "error" });
}

/// The commit context: the setting a `Connection` call commits with, set on the caller's
/// thread for the engine call. The WAL sync runs inside it, on the same thread
/// (`ShippingFile::sync`), reads the setting and leaves its ack here.
pub(crate) mod ctx {
    use super::{Batching, PendingAck};
    use std::cell::RefCell;
    use std::marker::PhantomData;

    struct CommitCtx {
        batching: Batching,
        acks: Vec<PendingAck>,
    }

    thread_local! {
        static CTX: RefCell<Option<CommitCtx>> = const { RefCell::new(None) };
    }

    /// Restores the context that was current before [`enter`] when dropped (none, outside
    /// a nested call), so neither an early return nor a panic leaks it; acks still in it are
    /// dropped (not acked).
    #[must_use]
    pub(crate) struct CtxGuard {
        prev: Option<CommitCtx>,
        /// The context is this thread's.
        _thread: PhantomData<*const ()>,
    }

    impl Drop for CtxGuard {
        fn drop(&mut self) {
            let prev = self.prev.take();
            CTX.with(|c| *c.borrow_mut() = prev);
        }
    }

    /// Commit with `b`, with no acks yet, until the guard is dropped.
    pub(crate) fn enter(b: Batching) -> CtxGuard {
        let fresh = CommitCtx {
            batching: b,
            acks: Vec::new(),
        };
        CtxGuard {
            prev: CTX.with(|c| c.borrow_mut().replace(fresh)),
            _thread: PhantomData,
        }
    }

    /// The current call's setting; `None` outside every `Connection` call.
    pub(crate) fn current() -> Option<Batching> {
        CTX.with(|c| c.borrow().as_ref().map(|c| c.batching))
    }

    /// The current call commits with `b` from its next commit on (a `PRAGMA
    /// buckwal_batching`, or a transaction's end, in the middle of an `execute_batch`).
    pub(crate) fn set(b: Batching) {
        CTX.with(|c| {
            if let Some(c) = c.borrow_mut().as_mut() {
                c.batching = b;
            }
        });
    }

    /// Leave `ack` for the current call; outside every call there is no one to ack, and it
    /// is dropped.
    pub(crate) fn push(ack: PendingAck) {
        CTX.with(|c| {
            if let Some(c) = c.borrow_mut().as_mut() {
                c.acks.push(ack);
            }
        });
    }

    /// The current call's acks, in commit order; clears them.
    pub(crate) fn take() -> Vec<PendingAck> {
        CTX.with(|c| {
            c.borrow_mut()
                .as_mut()
                .map(|c| std::mem::take(&mut c.acks))
                .unwrap_or_default()
        })
    }
}

#[cfg(test)]
mod tests;
