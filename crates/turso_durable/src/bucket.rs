//! Bucket write semantics: the one place that decides what is in the bucket.
//!
//! Two promises are kept here. An acknowledged log record is in the bucket (every log object
//! is a conditional `put_if_absent`, retried until it lands or the deadline passes). And the
//! log never forks: a record that may or may not have landed stays "in doubt" and is settled
//! at the same position before anything else is written, and a position or head held by
//! another writer fences this one for good.

use crate::config::{Event, EventHook};
use crate::error::{Error, Result};
use crate::format::{encode_record, Head, Keys, LogRecord, Manifest, Op, FORMAT};
use crate::store::{with_retry, CondPut, ObjectStore, RetryPolicy, StoreError};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Instant;

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
/// attach, and a still-running one gets a 412 on its next seal's If-Match. No acked commit
/// is lost and the log stays linear (stage 3's lease closes the remaining race). Such a
/// generation holds the old writer's records, then the new one's, which
/// `restore::generation_image` accepts.
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
        };
        let body = head.encode();
        let key = keys.head();
        let counters = Counters::default();
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
        let b = Bucket::new(store, keys, retry, concurrency, head, etag);
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
        let obj = retrying(&counters, &retry, &key, || store.get(&key))?
            .ok_or_else(|| Error::NotFound("head".into()))?;
        let head = Head::decode(&obj.body)?;
        if head.writer_id != writer_id {
            return Err(Error::Fenced(format!(
                "head names writer {:?}, not {writer_id:?}",
                head.writer_id
            )));
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

    pub(crate) fn set_event_hook(&mut self, events: Option<EventHook>) {
        self.events = events;
    }

    fn emit(&self, e: Event) {
        if let Some(h) = &self.events {
            h(&e);
        }
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
    pub(crate) fn append_log(&mut self, syncs: Vec<Vec<Op>>) -> Result<LogPos> {
        let start = Instant::now();
        self.check_fenced()?;
        self.append(start, syncs).map_err(upload_error)
    }

    fn append(&mut self, start: Instant, syncs: Vec<Vec<Op>>) -> Result<LogPos> {
        if let HeadCheck::Published(_) = self.check_head(start)? {
            // Only a seal may finish this: it adopts the published head without uploading.
            return Err(Error::UploadFailed(format!(
                "generation {} may be sealed; seal again before appending",
                self.head.generation
            )));
        }
        self.settle(start)?;
        let body = encode_record(&LogRecord::Extents {
            writer_id: self.head.writer_id.clone(),
            syncs,
        });
        self.put_log(start, body)
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
        let stored = retrying(&self.counters, &self.remaining(start), &key, || {
            self.store.get(&key)
        })?;
        match stored {
            Some(o) if o.etag == self.head_etag => {
                self.pending_seal = None;
                Ok(HeadCheck::Unmoved)
            }
            Some(o) if o.body == pending_body => Ok(HeadCheck::Published(o.etag)),
            _ => {
                self.fenced = true;
                Err(Error::Fenced(
                    "the head was changed by another writer".into(),
                ))
            }
        }
    }

    /// The head a seal of the current generation publishes.
    fn sealed_head(&self) -> Head {
        Head {
            format: FORMAT,
            generation: self.head.generation + 1,
            manifest: Some(self.head.generation),
            writer_id: self.head.writer_id.clone(),
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
        self.put_log(start, d.body).map(|_| ())
    }

    /// `put_if_absent` at the next seq. Ours (created, or already there with the same bytes)
    /// advances the seq; someone else's fences; anything else leaves `body` in doubt.
    fn put_log(&mut self, start: Instant, body: Vec<u8>) -> Result<LogPos> {
        let pos = LogPos {
            generation: self.head.generation,
            seq: self.seq,
        };
        let key = self.keys.log(pos.generation, pos.seq);
        match self.put_log_at(start, &key, &body) {
            Ok(()) => {
                self.seq += 1;
                self.counters.log_puts.fetch_add(1, Relaxed);
                self.emit(Event::LogUploaded {
                    generation: pos.generation,
                    seq: pos.seq,
                    bytes: body.len() as u64,
                });
                Ok(pos)
            }
            Err(e @ Error::Fenced(_)) => {
                self.fenced = true;
                Err(e)
            }
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

    /// Publish generation G: upload its dirty pages, its manifest, then move the head to
    /// G+1 with a conditional write. Returns G+1.
    pub(crate) fn seal_generation(
        &mut self,
        pages: Vec<(u32, Vec<u8>)>,
        page_size: u32,
        page_count: u32,
    ) -> Result<u64> {
        let start = Instant::now();
        self.check_fenced()?;
        self.seal(start, pages, page_size, page_count)
            .map_err(upload_error)
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
            return Ok(self.finish_seal(start, p.head, etag, p.manifest));
        }
        self.settle(start)?;
        let gen = self.head.generation;
        self.emit(Event::SealStarted {
            generation: gen,
            pages: pages.len() as u64,
        });
        // Pages and the manifest are unreferenced until the head moves: plain puts. A failed
        // seal whose head never landed may overwrite them next time (`check_head` above
        // ensures a published one is adopted instead).
        self.put_pages(start, gen, &pages)?;

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
        retrying(&self.counters, &self.remaining(start), &key, || {
            self.store.put(&key, &body)
        })?;

        let head = self.sealed_head();
        // From the first head attempt until a definite answer, the head may be published.
        self.pending_seal = Some(PendingSeal {
            head: head.clone(),
            manifest: manifest.clone(),
        });
        let etag = self.put_head(start, &head)?;
        Ok(self.finish_seal(start, head, etag, manifest))
    }

    /// Upload `pages` of generation `gen`, `concurrency` at a time; the first error wins.
    fn put_pages(&self, start: Instant, gen: u64, pages: &[(u32, Vec<u8>)]) -> Result<()> {
        let next = AtomicUsize::new(0);
        let failed: Mutex<Option<Error>> = Mutex::new(None);
        let workers = self.concurrency.min(pages.len());
        std::thread::scope(|s| {
            for _ in 0..workers {
                s.spawn(|| loop {
                    if failed.lock().unwrap().is_some() {
                        return;
                    }
                    let Some((page, body)) = pages.get(next.fetch_add(1, Relaxed)) else {
                        return;
                    };
                    let key = self.keys.page(gen, *page);
                    let r = retrying(&self.counters, &self.remaining(start), &key, || {
                        self.store.put(&key, body)
                    });
                    match r {
                        Ok(_) => {
                            self.counters.page_puts.fetch_add(1, Relaxed);
                        }
                        Err(e) => {
                            failed.lock().unwrap().get_or_insert(e);
                            return;
                        }
                    }
                });
            }
        });
        match failed.into_inner().unwrap() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Move the head with `put_if_match`; returns its new ETag. A 412 is ours only when the
    /// stored head is exactly the one we meant to write (an earlier attempt landed).
    fn put_head(&mut self, start: Instant, head: &Head) -> Result<String> {
        let key = self.keys.head();
        let body = head.encode();
        let c = &self.counters;
        let r = retrying(c, &self.remaining(start), &key, || {
            self.store.put_if_match(&key, &body, &self.head_etag)
        })?;
        if let CondPut::Created { etag } = r {
            return Ok(etag);
        }
        match retrying(c, &self.remaining(start), &key, || self.store.get(&key))? {
            Some(o) if o.body == body => Ok(o.etag),
            _ => {
                self.fenced = true;
                Err(Error::Fenced(
                    "the head was changed by another writer".into(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests;
