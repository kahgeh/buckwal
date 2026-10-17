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
use crate::format::{encode_record, Head, Keys, LogRecord, Manifest, Op, FORMAT};
use crate::store::{with_retry, CondPut, ObjectStore, RetryPolicy, StoreError};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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
    /// write (`put_head`, and `create`'s), open's head read (`attach`), and a head GET that
    /// passes `check_head` (unchanged) or `verify_head`. A failed or ambiguous request renews
    /// nothing, and neither do page and manifest PUTs (which only makes the check run sooner).
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

/// What `claim` tells a restored directory whose head moved since the restore.
pub(crate) const RESTORE_AGAIN: &str =
    "the bucket changed since this directory was restored; restore again";

/// The first half of taking over a restored copy's database as `writer_id`: a claim is a
/// head write, then one log record.
///
/// This writes the head the restore read (ETag `head_etag`) again with the new writer id,
/// same generation and manifest, with `put_if_match`. A head that moved since the restore
/// (412, or 404 when it is gone) is `Fenced` with "restore again", and nothing is written.
/// Returns the new head's ETag. The caller then attaches, reconciles and appends one record
/// of its own (an empty one) at its next seq (`db::attach_existing`); stage 3's fence record
/// will take that slot.
///
/// That record matters because a still-running old writer does not see the head change: it
/// keeps appending to the same generation, and the new writer's seal (If-Match on a head only
/// it moved) would orphan whatever the old one acked after it. The record takes the next seq,
/// so either the old writer's append collides with it and the old writer is fenced for good,
/// or the old writer got there first and the new writer's open fails `Fenced`. After a
/// claim, an old writer restarting on its own disk is fenced by the writer-id check at
/// attach. A still-running one is fenced at its next append: through the claim record when
/// it appends within the trim grace, and otherwise (`log/G` may be trimmed by then) through
/// the lease's head check before that append (`Bucket::check_lease`, under the assumption it
/// lists). It is also fenced at the start of its next seal
/// (`Bucket::seal_start`: its seal-start record collides, or the head moved), before it
/// uploads a page or a manifest, so it cannot overwrite a generation the new writer sealed,
/// and its page and manifest PUTs stop at the first lapse of its lease (`Bucket::seal`).
/// The log stays linear. Such a generation holds the old writer's records, then the new
/// one's, which `restore::generation_image` accepts.
pub(crate) fn claim(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    head_etag: &str,
    writer_id: &str,
) -> Result<String> {
    enum Claim {
        Done(String),
        Moved,
    }
    if !writer_id.is_ascii() || writer_id.len() > u8::MAX as usize {
        return Err(Error::Config(
            "writer id must be ASCII and at most 255 bytes".into(),
        ));
    }
    let key = keys.head();
    let moved = || Error::Fenced(RESTORE_AGAIN.into());
    let Some(current) = with_retry(retry, &key, |_| store.get(&key))? else {
        return Err(moved());
    };
    if head_etag.is_empty() || current.etag != head_etag {
        return Err(moved());
    }
    let head = Head {
        writer_id: writer_id.to_string(),
        ..Head::decode(&current.body)?
    };
    let body = head.encode();
    let r = with_retry(retry, &key, |attempt| {
        // After a failed attempt the put may have landed: look before writing again.
        if attempt > 0 {
            match store.get(&key)? {
                Some(o) if o.body == body => return Ok(Claim::Done(o.etag)),
                Some(o) if o.etag == head_etag => {}
                _ => return Ok(Claim::Moved),
            }
        }
        Ok(match store.put_if_match(&key, &body, head_etag)? {
            CondPut::Created { etag } => Claim::Done(etag),
            CondPut::PreconditionFailed => Claim::Moved,
        })
    })
    .map_err(upload_error)?;
    match r {
        Claim::Done(etag) => Ok(etag),
        Claim::Moved => Err(moved()),
    }
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

    /// Start a new database: `put_if_absent(head)` at generation 1 with no manifest.
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
            epoch: 0,
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
    pub(crate) fn attach(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        concurrency: usize,
        writer_id: &str,
    ) -> Result<Bucket> {
        let counters = Counters::default();
        let key = keys.head();
        let sent = Stamp::now();
        let obj = retrying(&counters, &retry, &key, || store.get(&key))?
            .ok_or_else(|| Error::NotFound("head".into()))?;
        let head = Head::decode(&obj.body)?;
        if head.writer_id != writer_id {
            let e = Error::Fenced(format!(
                "head names writer {:?}, not {writer_id:?}",
                head.writer_id
            ));
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

    /// The lease check before an append. Another writer's claim record fences this one at its
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

    /// With a seal pending, GET the head. Unchanged ETag: the head write never landed, so the
    /// pending seal is dropped. The pending head: it is published. Anything else: fenced.
    ///
    /// Accepted limit for stage 1: a delayed, still in-flight head PUT from the failed attempt
    /// could land after an "unchanged" answer here. Stage 3's epoch fencing closes this.
    fn check_head(&mut self, start: Instant) -> Result<HeadCheck> {
        let Some(pending) = &self.pending_seal else {
            return Ok(HeadCheck::Unmoved);
        };
        let pending_body = pending.head.encode();
        let key = self.keys.head();
        let sent = Stamp::now();
        let stored = retrying(&self.counters, &self.remaining(start), &key, || {
            self.store.get(&key)
        })?;
        match stored {
            Some(o) if o.etag == self.head_etag => {
                self.last_confirmed = sent;
                self.pending_seal = None;
                Ok(HeadCheck::Unmoved)
            }
            Some(o) if o.body == pending_body => Ok(HeadCheck::Published(o.etag)),
            _ => Err(self.fence(Error::Fenced(
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
    /// no log record, so the claim record alone would not fence it.
    ///
    /// 1. An empty record (zero syncs) goes to this writer's next seq with `put_if_absent`,
    ///    through `put_log`: a seq taken by another writer's record (a claim record, say)
    ///    fences this writer and nothing is uploaded; an ambiguous result leaves it in doubt
    ///    and the seal fails, to be settled at the same seq like any other record. It keeps
    ///    the seqs contiguous and adds nothing to the generation's image.
    /// 2. Then a GET of the head (`verify_head`), which must still carry this writer's ETag
    ///    or be the very head this seal publishes. The record cannot collide once the trimmer has
    ///    deleted `log/G` (another writer sealed G and its grace passed), and a claimer's head
    ///    write precedes its claim record; a moved head fences.
    ///
    /// A claimer that restores and claims after step 2 lands its claim record after ours and
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
            // Our own head for this seal, from an earlier attempt whose PUT landed late (after
            // a crash and restart, say): `put_head` adopts it at the next seal, which
            // re-publishes the generation with every record acked since (README, item 1).
            // Another writer's head names its id.
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
        let key = self.keys.head();
        let body = head.encode();
        let c = &self.counters;
        let sent = Stamp::now();
        let r = retrying(c, &self.remaining(start), &key, || {
            self.store.put_if_match(&key, &body, &self.head_etag)
        })?;
        if let CondPut::Created { etag } = r {
            self.last_confirmed = sent;
            return Ok(etag);
        }
        match retrying(c, &self.remaining(start), &key, || self.store.get(&key))? {
            Some(o) if o.body == body => {
                self.last_confirmed = sent;
                Ok(o.etag)
            }
            _ => Err(self.fence(Error::Fenced(
                "the head was changed by another writer".into(),
            ))),
        }
    }
}

#[cfg(test)]
mod tests;
