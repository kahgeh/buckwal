//! Bucket write semantics: the one place that decides what is in the bucket.
//!
//! Two promises are kept here. An acknowledged log record is in the bucket (every log object
//! is a conditional `put_if_absent`, retried until it lands or the deadline passes). And the
//! log never forks: a record that may or may not have landed stays "in doubt" and is settled
//! at the same position before anything else is written, and a position or head held by
//! another writer fences this one for good.

use crate::clock::Stamp;
use crate::config::{Event, EventHook};
use crate::error::{Error, Result};
use crate::format::{decode_record, encode_record, Head, Keys, LogRecord, Manifest, Op, FORMAT};
use crate::store::{with_retry, CondPut, Object, ObjectStore, RetryPolicy, StoreError};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::field::Empty;
use tracing::Span;

/// Where a log object lives: `log/<generation>/<seq>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LogPos {
    pub generation: u64,
    pub seq: u64,
}

/// Running totals, shared with the uploader so they can be read without blocking.
#[derive(Debug, Default)]
pub(crate) struct Counters {
    /// Log objects that landed (including in-doubt records settled as ours).
    pub log_puts: AtomicU64,
    pub page_puts: AtomicU64,
    pub seals: AtomicU64,
    /// Extra attempts made by `with_retry` (attempt numbers above 0).
    pub retries: AtomicU64,
}

/// A log record that `append_log` confirmed in the bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Appended {
    pub pos: LogPos,
    /// The encoded record's length.
    pub bytes: u64,
    /// How much `Counters::retries` grew during the call: its head checks and settle included.
    #[cfg_attr(not(test), allow(dead_code))]
    pub retries: u64,
}

/// Why `append_log` did not confirm a record, and whether its syncs can be sent again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppendFailure {
    pub error: Error,
    /// `Some(syncs)`, the syncs as passed, when no PUT of this record was attempted: the call
    /// failed before the record was built (fenced already, or in `check_head`, `check_lease`
    /// or `settle`). They are in no object and owed nowhere, so they may be sent again.
    /// `None` when the record went to `put_log`: it is in doubt (owed at its seq, settled by
    /// the next call) or the writer is fenced. Its syncs must not be sent in another record.
    /// A `Fenced` error wins over `Some`: the caller drops those syncs too, since a fenced
    /// writer never sends again (`check_fenced` refuses every later append).
    pub unsent: Option<Vec<Vec<Op>>>,
}

/// A log record whose upload failed ambiguously: it may be in the bucket at `pos`.
struct InDoubt {
    pos: LogPos,
    body: Vec<u8>,
}

/// A seal that reached its head write without a definite answer: `head` may already be
/// published, referencing `manifest` and the pages exactly as they were uploaded.
struct PendingSeal {
    head: Head,
    manifest: Manifest,
}

/// How a round of a seal's page PUTs ended (`Bucket::put_pages_until_lapse`).
enum Lapse {
    /// Every page of the round is uploaded.
    Done,
    /// The lease lapsed first: these indices (into the seal's pages) are not uploaded yet.
    Remaining(Vec<usize>),
}

/// How an epoch raise (`Bucket::raise_epoch_at`) ended.
enum Raise {
    /// The raised head is published and adopted.
    Raised,
    /// The head holds the pending seal's head (or, at open, this writer's own sealed head)
    /// instead: its late PUT landed first, with this ETag. Nothing was raised.
    Published(String),
}

/// What a GET of the head says about a pending seal.
enum HeadCheck {
    /// No seal is pending, or the head is unchanged: carry on in this generation.
    Unmoved,
    /// The pending seal's head is published (with this ETag).
    Published(String),
}

pub(crate) struct Bucket {
    store: Arc<dyn ObjectStore>,
    keys: Keys,
    retry: RetryPolicy,
    concurrency: usize,
    head: Head,
    head_etag: String,
    manifest: Option<Manifest>,
    /// The seq the next log object of `head.generation` goes to.
    seq: u64,
    in_doubt: Option<InDoubt>,
    /// Set while a seal's head write has no definite answer: the head may already name
    /// generation + 1, so nothing may go to this generation until that is known.
    pending_seal: Option<PendingSeal>,
    fenced: bool,
    /// The lease (`check_lease`): when the request behind the last confirmation was sent, on
    /// both clocks. It is renewed by a confirmed log record (`put_log`), a confirmed head
    /// write (`put_head`, `raise_epoch`, and `create`'s), open's head read (`attach`), and a
    /// head GET that passes `check_head` (finding its own raised head) or `verify_head`. A
    /// failed or ambiguous request renews nothing, and neither do page and manifest PUTs
    /// (which only makes the check run sooner).
    last_confirmed: Stamp,
    /// See `set_lease`.
    lease: Duration,
    counters: Arc<Counters>,
    events: Option<EventHook>,
}

/// `with_retry` that also counts extra attempts in `counters.retries`.
fn retrying<T>(
    counters: &Counters,
    policy: &RetryPolicy,
    what: &str,
    mut op: impl FnMut() -> std::result::Result<T, StoreError>,
) -> Result<T> {
    with_retry(policy, what, |attempt| {
        if attempt > 0 {
            counters.retries.fetch_add(1, Relaxed);
        }
        op()
    })
}

/// A permanent store error ends an upload: report it as `UploadFailed`.
fn upload_error(e: Error) -> Error {
    match e {
        Error::Bucket(m) => Error::UploadFailed(m),
        e => e,
    }
}

/// `with_retry` under what is left of `retry.deadline` since `start`, adding its extra
/// attempts to `retries`.
fn retry_rest<T>(
    retry: &RetryPolicy,
    start: Instant,
    retries: &AtomicU64,
    what: &str,
    mut op: impl FnMut() -> std::result::Result<T, StoreError>,
) -> Result<T> {
    let policy = RetryPolicy {
        deadline: retry.deadline.saturating_sub(start.elapsed()),
        ..retry.clone()
    };
    with_retry(&policy, what, |attempt| {
        if attempt > 0 {
            retries.fetch_add(1, Relaxed);
        }
        op()
    })
}

/// What `take_over` tells a restored directory whose generation is no longer the head's.
pub(crate) const RESTORE_AGAIN: &str =
    "the bucket changed since this directory was restored; restore again";

/// The first half of a takeover (the claim of a restored copy) as `writer_id`: the head is
/// written again with the new writer id and its epoch raised by one, same generation and
/// manifest. The second half is the fence record (`Bucket::append_fence`), which the caller
/// appends once attached (`db::attach_existing`). Returns the new head and its ETag.
///
/// It GETs the head. One that already names `writer_id` is returned as is: a claim whose head
/// write landed before its process crashed resumes under the same id (the caller persisted it
/// first). One of another generation than `expect_generation` (the restored one; the old
/// writer sealed since, so the restored copy is stale), or none at all, is `Fenced` with
/// "restore again", and nothing is written. Otherwise a `put_if_match` on the ETag just read.
/// A 412 means the head moved: it is read again and the same rules apply, so a concurrent
/// takeover of the same generation is followed by this one at the next epoch (the last
/// takeover wins), and a write of ours that landed though its reply was lost is found as a
/// head naming `writer_id`. Every request shares one deadline (`retry.deadline`); extra
/// attempts are added to `retries`.
///
/// Why the generation and not the restore's ETag: every writer open raises the epoch, so
/// the ETag also changes when the old writer merely restarts. A head of the same generation,
/// whatever its writer and epoch, still has the restored generation's log, and the records
/// the old writer acked since the restore come in through reconcile (`Behind`), the fence
/// record landing after them.
///
/// Why a still-running old writer cannot lose an ack: it does not see the head change and
/// keeps appending to the same generation. The fence record takes the next free seq, so
/// either the old writer's next append collides with it and the old writer is fenced for
/// good, or its record got there first, is stepped over (`append_fence`) and taken in by
/// reconcile. After a takeover, an old writer restarting on its own disk is fenced by the
/// writer-id check at attach. A still-running one is fenced at its next append: through the
/// fence record when it appends within the trim grace, and otherwise (`log/G` may be trimmed
/// by then) through the lease's head check before that append (`Bucket::check_lease`, under
/// the assumption it lists). It is also fenced at the start of its next seal
/// (`Bucket::seal_start`: its seal-start record collides, or the head moved), before it
/// uploads a page or a manifest, so it cannot overwrite a generation the new writer sealed,
/// and its page and manifest PUTs stop at the first lapse of its lease (`Bucket::seal`).
/// The log stays linear: such a generation holds the old writer's records, then the fence,
/// then the new writer's, which `restore::generation_image` accepts.
pub(crate) fn take_over_counted(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    expect_generation: u64,
    writer_id: &str,
    retries: &AtomicU64,
) -> Result<(Head, String)> {
    if !writer_id.is_ascii() || writer_id.len() > u8::MAX as usize {
        return Err(Error::Config(
            "writer id must be ASCII and at most 255 bytes".into(),
        ));
    }
    let start = Instant::now();
    let key = keys.head();
    let restore_again = || Error::Fenced(RESTORE_AGAIN.into());
    loop {
        let Some(current) = retry_rest(retry, start, retries, &key, || store.get(&key))? else {
            return Err(restore_again());
        };
        let head = Head::decode(&current.body)?;
        if head.writer_id == writer_id {
            return Ok((head, current.etag));
        }
        if head.generation != expect_generation {
            return Err(restore_again());
        }
        let new = Head {
            writer_id: writer_id.to_string(),
            epoch: head.epoch + 1,
            ..head
        };
        let body = new.encode();
        let put = retry_rest(retry, start, retries, &key, || {
            store.put_if_match(&key, &body, &current.etag)
        })
        .map_err(upload_error)?;
        if let CondPut::Created { etag } = put {
            return Ok((new, etag));
        }
        // The head moved since the GET: read it again, within the same deadline.
        let elapsed = start.elapsed();
        if elapsed >= retry.deadline {
            return Err(Error::Timeout(format!(
                "takeover: the head kept changing for {elapsed:?}; retry the takeover"
            )));
        }
    }
}

/// `take_over_counted`, its extra attempts not counted anywhere.
#[cfg(test)]
pub(crate) fn take_over(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    expect_generation: u64,
    writer_id: &str,
) -> Result<(Head, String)> {
    take_over_counted(
        store,
        keys,
        retry,
        expect_generation,
        writer_id,
        &AtomicU64::new(0),
    )
}

/// What `Bucket::append_fence` landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FenceOutcome {
    pub pos: LogPos,
    /// The record just before the fence is another writer's Extents record with no syncs:
    /// that writer's last act may have been a seal-start record, so its seal's page PUTs may
    /// still be in flight. Also true when that record could not be read.
    #[cfg_attr(not(test), allow(dead_code))]
    pub after_foreign_empty: bool,
}

/// What a fence PUT found at its seq.
enum FencePut {
    /// The fence is there (created, or an earlier attempt of ours landed).
    Landed,
    /// Another writer's record the fence steps over: an old writer's Extents record, or a
    /// fence of a lower epoch (a losing concurrent takeover).
    Occupied(LogRecord),
}

/// Another writer's Extents record with no syncs: a seal-start record (or a stage-2 claim
/// record).
fn foreign_empty(rec: &LogRecord, writer_id: &str) -> bool {
    matches!(rec, LogRecord::Extents { writer_id: w, syncs } if w != writer_id && syncs.is_empty())
}

impl Bucket {
    fn new(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        concurrency: usize,
        head: Head,
        head_etag: String,
    ) -> Bucket {
        Bucket {
            last_confirmed: Stamp::now(),
            lease: crate::config::defaults::LEASE,
            store,
            keys,
            retry,
            concurrency: concurrency.max(1),
            head,
            head_etag,
            manifest: None,
            seq: 1,
            in_doubt: None,
            pending_seal: None,
            fenced: false,
            counters: Arc::default(),
            events: None,
        }
    }

    /// Start a new database: `put_if_absent(head)` at generation 1 and epoch 1, with no
    /// manifest.
    ///
    /// A create that times out may still have stored its head. A later `create` then finds a
    /// head and returns `Config` (its first attempt gets a 412, which is never compared);
    /// the caller should `attach` with the same writer id instead.
    pub(crate) fn create(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        concurrency: usize,
        writer_id: String,
    ) -> Result<Bucket> {
        if !writer_id.is_ascii() || writer_id.len() > u8::MAX as usize {
            return Err(Error::Config(
                "writer id must be ASCII and at most 255 bytes".into(),
            ));
        }
        let head = Head {
            format: FORMAT,
            generation: 1,
            manifest: None,
            writer_id,
            epoch: 1,
        };
        let body = head.encode();
        let key = keys.head();
        let counters = Counters::default();
        let sent = Stamp::now();
        let exists = || Error::Config("a database already exists at this prefix".into());
        let r = retrying(&counters, &retry, &key, || store.put_if_absent(&key, &body))?;
        let etag = match r {
            CondPut::Created { etag } => etag,
            // On the first attempt a 412 can only be someone else's head.
            CondPut::PreconditionFailed if counters.retries.load(Relaxed) == 0 => {
                return Err(exists())
            }
            // After a failed attempt it may be our own, stored before the error.
            CondPut::PreconditionFailed => {
                match retrying(&counters, &retry, &key, || store.get(&key))? {
                    Some(o) if o.body == body => o.etag,
                    _ => return Err(exists()),
                }
            }
        };
        let mut b = Bucket::new(store, keys, retry, concurrency, head, etag);
        b.last_confirmed = sent;
        b.counters
            .retries
            .store(counters.retries.load(Relaxed), Relaxed);
        Ok(b)
    }

    /// Continue an existing database as `writer_id`, after the last log object of the
    /// current generation.
    ///
    /// `Fenced` when the head names another writer, or an epoch below `min_epoch` (the epoch
    /// the directory's local state holds): such a head is older than one this directory
    /// already saw, so the bucket is not the one the directory continues.
    pub(crate) fn attach(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        concurrency: usize,
        writer_id: &str,
        min_epoch: u64,
    ) -> Result<Bucket> {
        let counters = Counters::default();
        let key = keys.head();
        let sent = Stamp::now();
        let obj = retrying(&counters, &retry, &key, || store.get(&key))?
            .ok_or_else(|| Error::NotFound("head".into()))?;
        let head = Head::decode(&obj.body)?;
        let refused = if head.writer_id != writer_id {
            Some(format!(
                "head names writer {:?}, not {writer_id:?}",
                head.writer_id
            ))
        } else if head.epoch < min_epoch {
            Some(format!(
                "head is at epoch {}, below this directory's epoch {min_epoch}",
                head.epoch
            ))
        } else {
            None
        };
        if let Some(m) = refused {
            let e = Error::Fenced(m);
            tracing::error!(
                target: "turso_durable",
                generation = head.generation,
                writer_id,
                error = %e,
                "writer fenced"
            );
            return Err(e);
        }
        let manifest = match head.manifest {
            None => None,
            Some(g) => {
                let key = keys.manifest(g);
                let m = retrying(&counters, &retry, &key, || store.get(&key))?
                    .ok_or_else(|| Error::NotFound(key.clone()))?;
                Some(Manifest::decode(&m.body)?)
            }
        };
        let prefix = keys.log_prefix(head.generation);
        let last = retrying(&counters, &retry, &prefix, || store.list(&prefix))?
            .iter()
            .filter_map(|k| keys.parse_log(k))
            .filter(|(g, _)| *g == head.generation)
            .map(|(_, s)| s)
            .max();
        let mut b = Bucket::new(store, keys, retry, concurrency, head, obj.etag);
        b.manifest = manifest;
        b.seq = last.map_or(1, |s| s + 1);
        b.last_confirmed = sent;
        b.counters
            .retries
            .store(counters.retries.load(Relaxed), Relaxed);
        Ok(b)
    }

    pub(crate) fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    pub(crate) fn keys(&self) -> &Keys {
        &self.keys
    }

    pub(crate) fn head(&self) -> &Head {
        &self.head
    }

    pub(crate) fn manifest(&self) -> Option<&Manifest> {
        self.manifest.as_ref()
    }

    pub(crate) fn next_seq(&self) -> u64 {
        self.seq
    }

    pub(crate) fn counters(&self) -> Arc<Counters> {
        self.counters.clone()
    }

    /// The lease (`check_lease`): the engine sets `Config::lease`; zero checks the head before
    /// every append and every round of page PUTs.
    pub(crate) fn set_lease(&mut self, lease: Duration) {
        self.lease = lease;
    }

    /// Move the last confirmation back by `instant_by` on the monotonic clock and `wall_by`
    /// on the wall clock, as if it were that much older on each.
    #[cfg(test)]
    pub(crate) fn age_lease(&mut self, instant_by: Duration, wall_by: Duration) {
        self.last_confirmed.back(instant_by, wall_by);
    }

    pub(crate) fn set_event_hook(&mut self, events: Option<EventHook>) {
        self.events = events;
    }

    fn emit(&self, e: Event) {
        if let Some(h) = &self.events {
            h(&e);
        }
    }

    /// This writer is fenced for good: every later append and seal is refused. Returns `e`.
    fn fence(&mut self, e: Error) -> Error {
        self.fenced = true;
        tracing::error!(
            target: "turso_durable",
            generation = self.head.generation,
            writer_id = %self.head.writer_id,
            error = %e,
            "writer fenced"
        );
        e
    }

    fn check_fenced(&self) -> Result<()> {
        if self.fenced {
            return Err(Error::Fenced(
                "another writer took over this database".into(),
            ));
        }
        Ok(())
    }

    /// The retry policy for the rest of a call that started at `start`.
    fn remaining(&self, start: Instant) -> RetryPolicy {
        RetryPolicy {
            deadline: self.retry.deadline.saturating_sub(start.elapsed()),
            ..self.retry.clone()
        }
    }

    /// Write one log record at the next seq of the current generation.
    /// Every retry in the call shares one deadline, counted from its start.
    ///
    /// The record is built only once `check_fenced` and `before_record` have passed; a
    /// failure before that hands the syncs back as `AppendFailure::unsent`.
    ///
    /// It emits no `Event::LogUploaded` for this record: only the caller knows its commit
    /// numbers, so the uploader emits it. (Claim and reconcile records are appended before
    /// the uploader sets the event hook.) A record settled on the way is emitted here.
    ///
    /// It records `durable.generation` and `durable.seq` (the record's position, or the one
    /// it was meant for) and `durable.retries` (`Appended::retries`) on the current span.
    pub(crate) fn append_log(
        &mut self,
        syncs: Vec<Vec<Op>>,
    ) -> std::result::Result<Appended, AppendFailure> {
        let retries = self.counters.retries.load(Relaxed);
        let r = self.append_log_at(syncs, retries);
        let span = Span::current();
        let pos = match &r {
            Ok(a) => a.pos,
            Err(_) => LogPos {
                generation: self.head.generation,
                seq: self.seq,
            },
        };
        span.record("durable.generation", pos.generation);
        span.record("durable.seq", pos.seq);
        span.record(
            "durable.retries",
            self.counters.retries.load(Relaxed).saturating_sub(retries),
        );
        r
    }

    fn append_log_at(
        &mut self,
        syncs: Vec<Vec<Op>>,
        retries: u64,
    ) -> std::result::Result<Appended, AppendFailure> {
        let start = Instant::now();
        if let Err(e) = self.check_fenced().and_then(|()| self.before_record(start)) {
            return Err(AppendFailure {
                error: upload_error(e),
                unsent: Some(syncs),
            });
        }
        let body = encode_record(&LogRecord::Extents {
            writer_id: self.head.writer_id.clone(),
            syncs,
        });
        let bytes = body.len() as u64;
        match self.put_log(start, body) {
            Ok(pos) => Ok(Appended {
                pos,
                bytes,
                retries: self.counters.retries.load(Relaxed).saturating_sub(retries),
            }),
            Err(e) => Err(AppendFailure {
                error: upload_error(e),
                unsent: None,
            }),
        }
    }

    /// The second half of a takeover (`take_over`), and what a writer open appends when its
    /// generation's last record is another writer's: a fence record `Fence { writer_id,
    /// epoch }` (this writer's, at the head's epoch) at the next free seq, with
    /// `put_if_absent`, through the same head, lease and in-doubt checks as any record
    /// (`before_record`). A still-running old writer's next append collides with it and
    /// fences that writer for good; restore accepts only this writer's records after it.
    ///
    /// A seq already taken by another writer's Extents record (the old writer appended
    /// between the takeover's head write and this PUT), or by a fence of a lower epoch (a
    /// concurrent takeover that lost the head), is stepped over: the fence moves to the next
    /// seq, under the same deadline, at most `defaults::FENCE_MAX_STEPS` times. Past that, or
    /// past the deadline, it gives up with `Timeout`: the head already names this writer, so a
    /// retry resumes the takeover, and the old writer's records stay restorable before the
    /// fence. The records stepped over come in through reconcile, which runs after the fence.
    /// A fence of an equal or higher epoch (a takeover after this one) fences this writer.
    /// Our own identical bytes count as landed. An ambiguous PUT leaves the fence in doubt at
    /// its seq, like any record.
    ///
    /// It runs in a `durable.fence` span (`durable.generation`, `durable.seq`,
    /// `durable.epoch`, `durable.retries`, `s3.status`).
    pub(crate) fn append_fence(&mut self) -> Result<FenceOutcome> {
        let start = Instant::now();
        let retries = self.counters.retries.load(Relaxed);
        let span = tracing::info_span!(
            target: "turso_durable",
            "durable.fence",
            durable.generation = self.head.generation,
            durable.seq = Empty,
            durable.epoch = self.head.epoch,
            durable.retries = Empty,
            s3.status = Empty,
        );
        let r = span.in_scope(|| {
            self.check_fenced()
                .and_then(|()| self.append_fence_at(start))
                .map_err(upload_error)
        });
        span.record("durable.seq", r.as_ref().map_or(self.seq, |o| o.pos.seq));
        span.record(
            "durable.retries",
            self.counters.retries.load(Relaxed).saturating_sub(retries),
        );
        r
    }

    fn append_fence_at(&mut self, start: Instant) -> Result<FenceOutcome> {
        self.before_record(start)?;
        let body = encode_record(&LogRecord::Fence {
            writer_id: self.head.writer_id.clone(),
            epoch: self.head.epoch,
        });
        let mut steps = 0u64;
        // The record at `self.seq - 1`, once a step has read it.
        let mut previous: Option<LogRecord> = None;
        loop {
            let pos = LogPos {
                generation: self.head.generation,
                seq: self.seq,
            };
            let key = self.keys.log(pos.generation, pos.seq);
            let sent = Stamp::now();
            match self.put_fence_at(start, &key, &body) {
                Ok(FencePut::Landed) => {
                    self.last_confirmed = sent;
                    self.seq += 1;
                    self.counters.log_puts.fetch_add(1, Relaxed);
                    self.emit_uploaded(pos, body.len() as u64);
                    let after_foreign_empty = match previous {
                        Some(rec) => foreign_empty(&rec, &self.head.writer_id),
                        None => self.foreign_empty_before(start, pos),
                    };
                    return Ok(FenceOutcome {
                        pos,
                        after_foreign_empty,
                    });
                }
                Ok(FencePut::Occupied(rec)) => {
                    if steps == crate::config::defaults::FENCE_MAX_STEPS
                        || start.elapsed() >= self.retry.deadline
                    {
                        return Err(Error::Timeout(format!(
                            "takeover: the old writer kept appending past {steps} log \
                             positions; retry the takeover"
                        )));
                    }
                    steps += 1;
                    self.seq += 1;
                    previous = Some(rec);
                }
                Err(e @ Error::Fenced(_)) => return Err(self.fence(e)),
                // The seq holds a record that is not one: not ambiguous, nothing owed.
                Err(e @ Error::Corrupt(_)) => return Err(e),
                Err(e) => {
                    self.in_doubt = Some(InDoubt { pos, body });
                    return Err(e);
                }
            }
        }
    }

    /// `put_if_absent` of the fence `body` at `key`; on a 412, what holds the key decides.
    fn put_fence_at(&self, start: Instant, key: &str, body: &[u8]) -> Result<FencePut> {
        let c = &self.counters;
        let r = retrying(c, &self.remaining(start), key, || {
            self.store.put_if_absent(key, body)
        })?;
        if let CondPut::Created { .. } = r {
            return Ok(FencePut::Landed);
        }
        let Some(o) = retrying(c, &self.remaining(start), key, || self.store.get(key))? else {
            return Err(Error::Fenced(format!(
                "{key} was taken, then removed, by another writer"
            )));
        };
        // An earlier attempt of ours landed and its reply was lost.
        if o.body == body {
            return Ok(FencePut::Landed);
        }
        let rec = decode_record(&o.body)?;
        let (ours, epoch) = (&self.head.writer_id, self.head.epoch);
        match &rec {
            LogRecord::Extents { writer_id, .. } if writer_id != ours => {
                Ok(FencePut::Occupied(rec))
            }
            LogRecord::Fence { epoch: k, .. } if *k < epoch => Ok(FencePut::Occupied(rec)),
            LogRecord::Extents { .. } => Err(Error::Fenced(format!(
                "{key} holds a record of this writer id, not its fence"
            ))),
            LogRecord::Fence {
                writer_id,
                epoch: k,
            } => Err(Error::Fenced(format!(
                "{key} holds the fence of writer {writer_id:?} at epoch {k}, not below this \
                 writer's {epoch}"
            ))),
        }
    }

    /// Whether the record before `pos` is another writer's empty Extents record. A record
    /// that cannot be read counts as one (the caller then assumes the more cautious case).
    fn foreign_empty_before(&self, start: Instant, pos: LogPos) -> bool {
        if pos.seq <= 1 {
            return false;
        }
        let key = self.keys.log(pos.generation, pos.seq - 1);
        match retrying(&self.counters, &self.remaining(start), &key, || {
            self.store.get(&key)
        }) {
            Ok(Some(o)) => {
                decode_record(&o.body).map_or(true, |rec| foreign_empty(&rec, &self.head.writer_id))
            }
            Ok(None) | Err(_) => true,
        }
    }

    /// Everything an append does before it builds its record (`catch_up_at`); a head that a
    /// pending seal may have published refuses the append.
    fn before_record(&mut self, start: Instant) -> Result<()> {
        if let HeadCheck::Published(_) = self.catch_up_at(start)? {
            // Only a seal may finish this: it adopts the published head without uploading.
            return Err(Error::UploadFailed(format!(
                "generation {} may be sealed; seal again before appending",
                self.head.generation
            )));
        }
        Ok(())
    }

    /// Resolve what an earlier failure left owed, without writing a new record: a pending
    /// seal's head (`check_head`), the lease (`check_lease`), then the in-doubt record
    /// (`settle`). With nothing owed it sends no PUT. One deadline, counted from its start.
    /// A head that a pending seal published is left for the next seal to adopt (`Ok`).
    pub(crate) fn catch_up(&mut self) -> Result<()> {
        let start = Instant::now();
        self.check_fenced()?;
        self.catch_up_at(start).map(|_| ()).map_err(upload_error)
    }

    /// Is the bucket answering? For the uploader after a failed operation that left nothing
    /// to settle (not degraded), where `catch_up` would send no request at all: one GET of
    /// the head, no retries, checked as `verify_head` checks it (a moved head fences).
    /// Writes nothing. Degraded, it is `catch_up`.
    pub(crate) fn probe(&mut self) -> Result<()> {
        if self.degraded() {
            return self.catch_up();
        }
        self.check_fenced()?;
        let once = RetryPolicy {
            deadline: Duration::ZERO,
            ..self.retry.clone()
        };
        self.verify_head_with(&once).map_err(upload_error)
    }

    /// `check_head`, then `check_lease` and `settle` unless the head is `Published`.
    fn catch_up_at(&mut self, start: Instant) -> Result<HeadCheck> {
        if let published @ HeadCheck::Published(_) = self.check_head(start)? {
            return Ok(published);
        }
        self.check_lease(start)?;
        self.settle(start)?;
        Ok(HeadCheck::Unmoved)
    }

    /// The writer is fenced, a record is in doubt or a seal's head write is unresolved: the
    /// next append first has to settle something, or fails.
    pub(crate) fn degraded(&self) -> bool {
        self.fenced || self.in_doubt.is_some() || self.pending_seal.is_some()
    }

    /// The lease check before an append. Another writer's fence record fences this one at its
    /// next append, but only while `log/G` exists: once that writer has sealed G and its
    /// trimmer has deleted `log/G` (its trim grace after the seal), this writer's next record
    /// would land in a log no restore reads, and its commit would be acked. So an append once
    /// `lease` has passed since the last confirmation (`last_confirmed`, on the larger of the
    /// monotonic and the wall clock, so a machine that slept counts its sleep) GETs the head
    /// first, within the call's deadline (`verify_head`): ours, it proceeds and the lease is
    /// renewed; anything else fences. A seal checks the lease before each page and manifest
    /// PUT attempt instead (`seal`).
    ///
    /// An append that skipped the check sends its last attempt within `lease` +
    /// `retry_deadline` of the last confirmation, and that attempt ends within
    /// `request_timeout`; `Config::validate` requires the trim grace to exceed that sum.
    /// Against another process this holds only if that process's trim grace exceeds this
    /// writer's sum too (README, Settings): nothing in the bucket records another process's
    /// settings.
    fn check_lease(&mut self, start: Instant) -> Result<()> {
        if self.last_confirmed.age() < self.lease {
            return Ok(());
        }
        self.verify_head(start)
    }

    /// With a seal pending, GET the head. The pending head: it is published.
    ///
    /// Unchanged ETag: the head write has not landed, but a PUT of the failed attempt may
    /// still be in flight and could land later, on that same ETag, publishing the generation
    /// without whatever this writer appends next. So the epoch is raised on that ETag first
    /// (`raise_epoch_at`): the late PUT then gets a 412. Only then is the pending seal
    /// dropped. If the late PUT lands first, the raise finds the pending head instead, and
    /// it is published after all.
    ///
    /// The raised head itself (a raise of an earlier call landed, and its reply was lost): it
    /// is adopted, and the pending seal dropped. Anything else: fenced.
    fn check_head(&mut self, start: Instant) -> Result<HeadCheck> {
        let Some(pending) = &self.pending_seal else {
            return Ok(HeadCheck::Unmoved);
        };
        let pending_body = pending.head.encode();
        let raised = self.raised_head();
        let key = self.keys.head();
        let sent = Stamp::now();
        let stored = retrying(&self.counters, &self.remaining(start), &key, || {
            self.store.get(&key)
        })?;
        match stored {
            Some(o) if o.etag == self.head_etag => {
                if let Raise::Published(etag) = self.raise_epoch_at(start, Some(&pending_body))? {
                    return Ok(HeadCheck::Published(etag));
                }
                self.pending_seal = None;
                Ok(HeadCheck::Unmoved)
            }
            Some(o) if o.body == pending_body => Ok(HeadCheck::Published(o.etag)),
            Some(o) if o.body == raised.encode() => {
                self.adopt_head(raised, o.etag, sent);
                self.pending_seal = None;
                Ok(HeadCheck::Unmoved)
            }
            _ => Err(self.fence(Error::Fenced(
                "the head was changed by another writer".into(),
            ))),
        }
    }

    /// The current head with its epoch raised by one: same writer, generation and manifest.
    fn raised_head(&self) -> Head {
        Head {
            epoch: self.head.epoch + 1,
            ..self.head.clone()
        }
    }

    /// `head` is published with `etag`, by a request sent at `sent`: adopt it, renewing the
    /// lease.
    fn adopt_head(&mut self, head: Head, etag: String, sent: Stamp) {
        self.head = head;
        self.head_etag = etag;
        self.last_confirmed = sent;
    }

    /// Raise the head's epoch by one, keeping writer, generation and manifest: a
    /// `put_if_match` on the head's ETag, every retry under one deadline. A 412 whose stored
    /// head is exactly the raised one counts as landed; any other head fences this writer.
    /// It renews the lease and returns the new epoch.
    ///
    /// Every writer open that does not take over calls it (`db::attach_existing`): a head
    /// PUT that a crashed process of this writer still had in flight carries the ETag from
    /// before the raise, so it can no longer land after the restarted writer acked more
    /// records. A failure leaves the open failed; the next open reads whatever the head is
    /// then.
    ///
    /// `None` when the 412 found, instead, the head a seal of the current generation by this
    /// writer publishes (`sealed_head`): such a late PUT landed between `attach`'s head read
    /// and the raise. Nothing was raised and this writer is not fenced; the caller attaches
    /// again, at the sealed generation.
    pub(crate) fn raise_epoch(&mut self) -> Result<Option<u64>> {
        let start = Instant::now();
        self.check_fenced()?;
        let sealed = self.sealed_head().encode();
        match self
            .raise_epoch_at(start, Some(&sealed))
            .map_err(upload_error)?
        {
            Raise::Raised => Ok(Some(self.head.epoch)),
            Raise::Published(_) => Ok(None),
        }
    }

    /// `raise_epoch` within the call that started at `start`. A 412 whose stored head is
    /// `pending` (a pending seal's head, or the open's own sealed head, encoded) is
    /// `Raise::Published`.
    fn raise_epoch_at(&mut self, start: Instant, pending: Option<&[u8]>) -> Result<Raise> {
        let head = self.raised_head();
        let sent = Stamp::now();
        match self.put_head_if_match(start, &head)? {
            Ok(etag) => {
                self.adopt_head(head, etag, sent);
                Ok(Raise::Raised)
            }
            Err(Some(o)) if pending == Some(o.body.as_slice()) => Ok(Raise::Published(o.etag)),
            Err(_) => Err(self.fence(Error::Fenced(
                "the head was changed by another writer".into(),
            ))),
        }
    }

    /// The head a seal of the current generation publishes.
    fn sealed_head(&self) -> Head {
        Head {
            format: FORMAT,
            generation: self.head.generation + 1,
            manifest: Some(self.head.generation),
            writer_id: self.head.writer_id.clone(),
            epoch: self.head.epoch,
        }
    }

    /// Re-put the in-doubt record, if any, at the position it was meant for.
    fn settle(&mut self, start: Instant) -> Result<()> {
        let Some(d) = self.in_doubt.take() else {
            return Ok(());
        };
        let here = LogPos {
            generation: self.head.generation,
            seq: self.seq,
        };
        if d.pos != here {
            let e = Error::Corrupt(format!(
                "in-doubt record at {:?} but the log is at {here:?}",
                d.pos
            ));
            self.in_doubt = Some(d);
            return Err(e);
        }
        let bytes = d.body.len() as u64;
        let pos = self.put_log(start, d.body)?;
        // Its commits were failed when it went in doubt: it is reported with none.
        self.emit_uploaded(pos, bytes);
        Ok(())
    }

    fn emit_uploaded(&self, pos: LogPos, bytes: u64) {
        self.emit(Event::LogUploaded {
            generation: pos.generation,
            seq: pos.seq,
            bytes,
            commits: Vec::new(),
        });
    }

    /// `put_if_absent` at the next seq. Ours (created, or already there with the same bytes)
    /// advances the seq; someone else's fences; anything else leaves `body` in doubt.
    /// The caller emits `Event::LogUploaded`.
    fn put_log(&mut self, start: Instant, body: Vec<u8>) -> Result<LogPos> {
        let pos = LogPos {
            generation: self.head.generation,
            seq: self.seq,
        };
        let key = self.keys.log(pos.generation, pos.seq);
        let sent = Stamp::now();
        match self.put_log_at(start, &key, &body) {
            Ok(()) => {
                self.last_confirmed = sent;
                self.seq += 1;
                self.counters.log_puts.fetch_add(1, Relaxed);
                Ok(pos)
            }
            Err(e @ Error::Fenced(_)) => Err(self.fence(e)),
            Err(e) => {
                self.in_doubt = Some(InDoubt { pos, body });
                Err(e)
            }
        }
    }

    fn put_log_at(&self, start: Instant, key: &str, body: &[u8]) -> Result<()> {
        let c = &self.counters;
        let r = retrying(c, &self.remaining(start), key, || {
            self.store.put_if_absent(key, body)
        })?;
        if let CondPut::Created { .. } = r {
            return Ok(());
        }
        match retrying(c, &self.remaining(start), key, || self.store.get(key))? {
            // An earlier attempt of ours landed and its reply was lost.
            Some(o) if o.body == body => Ok(()),
            Some(_) => Err(Error::Fenced(format!(
                "{key} holds another writer's record"
            ))),
            None => Err(Error::Fenced(format!(
                "{key} was taken, then removed, by another writer"
            ))),
        }
    }

    /// Publish generation G: an empty seal-start record and a head check (`seal_start`), then
    /// its dirty pages, its manifest, then move the head to G+1 with a conditional write.
    /// Returns G+1.
    ///
    /// It records `durable.generation` (G), `durable.pages` and `durable.retries` on the
    /// current span, and `durable.seq`: the seq of the seal-start record (written now, or by
    /// the earlier attempt whose head this seal adopts) once the seal reaches it. It logs the
    /// finished seal.
    pub(crate) fn seal_generation(
        &mut self,
        pages: Vec<(u32, Vec<u8>)>,
        page_size: u32,
        page_count: u32,
    ) -> Result<u64> {
        let start = Instant::now();
        let (generation, n) = (self.head.generation, pages.len() as u64);
        let retries = self.counters.retries.load(Relaxed);
        let span = Span::current();
        span.record("durable.generation", generation);
        span.record("durable.pages", n);
        let r = self.check_fenced().and_then(|()| {
            self.seal(start, pages, page_size, page_count)
                .map_err(upload_error)
        });
        span.record(
            "durable.retries",
            self.counters.retries.load(Relaxed).saturating_sub(retries),
        );
        if r.is_ok() {
            tracing::info!(
                target: "turso_durable",
                generation,
                pages = n,
                ms = start.elapsed().as_millis() as u64,
                "seal finished"
            );
        }
        r
    }

    /// The new head is published: adopt it, its ETag and manifest. Returns its generation.
    fn finish_seal(&mut self, start: Instant, head: Head, etag: String, manifest: Manifest) -> u64 {
        let gen = head.generation;
        self.head = head;
        self.head_etag = etag;
        self.manifest = Some(manifest);
        self.seq = 1;
        self.pending_seal = None;
        self.counters.seals.fetch_add(1, Relaxed);
        self.emit(Event::SealFinished {
            generation: gen - 1,
            elapsed: start.elapsed(),
        });
        gen
    }

    fn seal(
        &mut self,
        start: Instant,
        pages: Vec<(u32, Vec<u8>)>,
        page_size: u32,
        page_count: u32,
    ) -> Result<u64> {
        if let HeadCheck::Published(etag) = self.check_head(start)? {
            // An earlier attempt published this generation; its pages and manifest are
            // referenced as uploaded then, so nothing is uploaded again.
            let p = self
                .pending_seal
                .take()
                .expect("a published seal is pending");
            // Nothing was appended since that attempt's seal-start record (`before_record`
            // refuses while its head may be published): it is the generation's last.
            Span::current().record("durable.seq", self.seq - 1);
            return Ok(self.finish_seal(start, p.head, etag, p.manifest));
        }
        self.settle(start)?;
        Span::current().record("durable.seq", self.seq);
        self.seal_start(start)?;
        let gen = self.head.generation;
        self.emit(Event::SealStarted {
            generation: gen,
            pages: pages.len() as u64,
        });
        // Pages and the manifest are unreferenced until the head moves: plain puts. A failed
        // seal whose head never landed may overwrite them next time (`check_head` above
        // ensures a published one is adopted instead). `seal_start` ensured no other writer
        // had taken this generation over, and each PUT attempt is sent only inside the lease:
        // at a lapse the workers stop, the head is read again (renewing the lease, or fencing
        // this writer) and the pages not yet uploaded resume. So no page or manifest PUT goes
        // out more than `lease` after this writer last saw its own head (a zero lease, for
        // tests, lets each worker send one attempt right after each head check).
        let mut todo: Vec<usize> = (0..pages.len()).collect();
        while let Lapse::Remaining(rest) = self.put_pages_until_lapse(start, gen, &pages, &todo)? {
            self.renew_lease(start, rest.len() < todo.len())?;
            todo = rest;
        }

        let dirty: BTreeSet<u32> = pages.iter().map(|(p, _)| *p).collect();
        let empty = Manifest {
            page_size,
            page_count,
            pages: BTreeMap::new(),
        };
        let manifest = self
            .manifest
            .as_ref()
            .unwrap_or(&empty)
            .sealed(gen, &dirty, page_size, page_count);
        let key = self.keys.manifest(gen);
        let body = manifest.encode();
        // The page PUTs, not a head check, came just before: no free attempt (a zero lease
        // checks the head first). The first lapse here always renews: the PUTs before it
        // went out after the last head check, and a seal is stuck only when a lapse follows
        // its own renewal with nothing sent, which is what `progressed = false` marks.
        let mut free = false;
        let mut progressed = true;
        while self
            .retrying_in_lease(start, &key, &mut free, || self.store.put(&key, &body))?
            .is_none()
        {
            self.renew_lease(start, progressed)?;
            progressed = false;
            free = self.lease.is_zero();
        }

        let head = self.sealed_head();
        // From the first head attempt until a definite answer, the head may be published.
        self.pending_seal = Some(PendingSeal {
            head: head.clone(),
            manifest: manifest.clone(),
        });
        let etag = self.put_head(start, &head)?;
        Ok(self.finish_seal(start, head, etag, manifest))
    }

    /// The gate before a seal uploads anything: a writer that another writer took over must
    /// not overwrite `pages/G/*` or `manifest/G`, which are plain puts and which that writer
    /// may already have sealed. A seal of an old writer that has nothing left to ship sends
    /// no log record, so the fence record alone would not fence it.
    ///
    /// 1. An empty record (zero syncs) goes to this writer's next seq with `put_if_absent`,
    ///    through `put_log`: a seq taken by another writer's record (a fence record, say)
    ///    fences this writer and nothing is uploaded; an ambiguous result leaves it in doubt
    ///    and the seal fails, to be settled at the same seq like any other record. It keeps
    ///    the seqs contiguous and adds nothing to the generation's image.
    /// 2. Then a GET of the head (`verify_head`), which must still carry this writer's ETag
    ///    or be the very head this seal publishes. The record cannot collide once the trimmer has
    ///    deleted `log/G` (another writer sealed G and its grace passed), and a takeover's head
    ///    write precedes its fence record; a moved head fences.
    ///
    /// A takeover that restores and claims after step 2 lands its fence record after ours and
    /// may seal G while this seal's page PUTs are in flight: those stop at the first lapse of
    /// the lease, whose head check fences this writer (`seal`).
    fn seal_start(&mut self, start: Instant) -> Result<()> {
        let body = encode_record(&LogRecord::Extents {
            writer_id: self.head.writer_id.clone(),
            syncs: Vec::new(),
        });
        let bytes = body.len() as u64;
        let pos = self.put_log(start, body)?;
        self.emit_uploaded(pos, bytes);
        self.verify_head(start)
    }

    /// GET the head: it must still carry this writer's ETag, or be the exact head a seal of
    /// the current generation publishes. Anything else fences this writer for good.
    fn verify_head(&mut self, start: Instant) -> Result<()> {
        self.verify_head_with(&self.remaining(start))
    }

    /// `verify_head` under `policy`.
    fn verify_head_with(&mut self, policy: &RetryPolicy) -> Result<()> {
        let key = self.keys.head();
        let sent = Stamp::now();
        let stored = retrying(&self.counters, policy, &key, || self.store.get(&key))?;
        let ours = self.sealed_head().encode();
        match stored {
            Some(o) if o.etag == self.head_etag => {
                self.last_confirmed = sent;
                Ok(())
            }
            // Our own head for this seal, from an earlier attempt whose PUT landed late:
            // `put_head` adopts it at the next seal, which re-publishes the generation with
            // every record acked since (README, item 1). Kept as a defence: such a PUT carries
            // the ETag from before an epoch raise, which every writer open and every
            // unchanged `check_head` sends, so it gets a 412 instead. Another writer's head
            // names its id.
            Some(o) if o.body == ours => {
                self.last_confirmed = sent;
                Ok(())
            }
            _ => Err(self.fence(Error::Fenced(
                "the head was changed by another writer".into(),
            ))),
        }
    }

    /// `retrying` for a seal's PUTs: each attempt, retries included, is sent only inside the
    /// lease; the first one refused ends the call with `None` (nothing sent, not counted as a
    /// retry). A zero lease (tests) would refuse every attempt, so there one attempt per
    /// worker per round goes out free: each round follows a head check, `free` starts true for
    /// a zero lease in each worker of a round (and for the manifest after a renewal), and that
    /// worker's first attempt spends it.
    fn retrying_in_lease<T>(
        &self,
        start: Instant,
        what: &str,
        free: &mut bool,
        mut op: impl FnMut() -> std::result::Result<T, StoreError>,
    ) -> Result<Option<T>> {
        with_retry(&self.remaining(start), what, |attempt| {
            if !(std::mem::take(free) || self.last_confirmed.age() < self.lease) {
                return Ok(None);
            }
            if attempt > 0 {
                self.counters.retries.fetch_add(1, Relaxed);
            }
            op().map(Some)
        })
    }

    /// The lease lapsed during a seal: GET the head (`verify_head`), which renews it or fences
    /// this writer. `progressed` says whether any PUT landed since the last head check. A
    /// lapse with none (the head check itself outlasted the lease) after the call's deadline
    /// ends the seal with `Timeout` instead, or the seal would read the head forever.
    fn renew_lease(&mut self, start: Instant, progressed: bool) -> Result<()> {
        let elapsed = start.elapsed();
        if !progressed && elapsed >= self.retry.deadline {
            return Err(Error::Timeout(format!(
                "seal of generation {}: no PUT went out inside the lease ({:?}) since the last \
                 head check, {elapsed:?} into the seal",
                self.head.generation, self.lease
            )));
        }
        self.verify_head(start)
    }

    /// Upload the pages at indices `todo` of `pages` (generation `gen`), `concurrency` at a
    /// time; the first error wins. Each attempt goes through `retrying_in_lease`: the first
    /// refused attempt stops every worker, and the indices not uploaded come back as
    /// `Lapse::Remaining` for the caller to renew the lease and resume.
    fn put_pages_until_lapse(
        &self,
        start: Instant,
        gen: u64,
        pages: &[(u32, Vec<u8>)],
        todo: &[usize],
    ) -> Result<Lapse> {
        let next = AtomicUsize::new(0);
        let failed: Mutex<Option<Error>> = Mutex::new(None);
        let lapsed = AtomicBool::new(false);
        let refused: Mutex<Vec<usize>> = Mutex::new(Vec::new());
        let workers = self.concurrency.min(todo.len());
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        std::thread::scope(|s| {
            for _ in 0..workers {
                s.spawn(|| {
                    let mut free = self.lease.is_zero();
                    tracing::dispatcher::with_default(&dispatch, || loop {
                        if failed.lock().unwrap().is_some() || lapsed.load(Relaxed) {
                            return;
                        }
                        let Some(&i) = todo.get(next.fetch_add(1, Relaxed)) else {
                            return;
                        };
                        let (page, body) = &pages[i];
                        let key = self.keys.page(gen, *page);
                        let r = self.retrying_in_lease(start, &key, &mut free, || {
                            self.store.put(&key, body)
                        });
                        match r {
                            Ok(Some(_)) => {
                                self.counters.page_puts.fetch_add(1, Relaxed);
                            }
                            Ok(None) => {
                                refused.lock().unwrap().push(i);
                                lapsed.store(true, Relaxed);
                                return;
                            }
                            Err(e) => {
                                failed.lock().unwrap().get_or_insert(e);
                                return;
                            }
                        }
                    })
                });
            }
        });
        if let Some(e) = failed.into_inner().unwrap() {
            return Err(e);
        }
        if !lapsed.into_inner() {
            return Ok(Lapse::Done);
        }
        // Refused ones, then those no worker took.
        let mut rest = refused.into_inner().unwrap();
        rest.extend_from_slice(&todo[next.into_inner().min(todo.len())..]);
        rest.sort_unstable();
        Ok(Lapse::Remaining(rest))
    }

    /// Move the head with `put_if_match`; returns its new ETag. A 412 is ours only when the
    /// stored head is exactly the one we meant to write (an earlier attempt landed).
    fn put_head(&mut self, start: Instant, head: &Head) -> Result<String> {
        let sent = Stamp::now();
        match self.put_head_if_match(start, head)? {
            Ok(etag) => {
                self.last_confirmed = sent;
                Ok(etag)
            }
            Err(_) => Err(self.fence(Error::Fenced(
                "the head was changed by another writer".into(),
            ))),
        }
    }

    /// `put_if_match` of `head` on `head_etag`, then on a 412 a GET: `Ok(etag)` when `head`
    /// is stored (created, or an earlier attempt landed), else `Err` with what is stored.
    /// Changes nothing in `self`.
    fn put_head_if_match(
        &self,
        start: Instant,
        head: &Head,
    ) -> Result<std::result::Result<String, Option<Object>>> {
        let key = self.keys.head();
        let body = head.encode();
        let c = &self.counters;
        let r = retrying(c, &self.remaining(start), &key, || {
            self.store.put_if_match(&key, &body, &self.head_etag)
        })?;
        if let CondPut::Created { etag } = r {
            return Ok(Ok(etag));
        }
        Ok(
            match retrying(c, &self.remaining(start), &key, || self.store.get(&key))? {
                Some(o) if o.body == body => Ok(o.etag),
                other => Err(other),
            },
        )
    }
}

#[cfg(test)]
mod tests;
