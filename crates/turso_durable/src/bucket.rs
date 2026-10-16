//! Bucket write semantics: the one place that decides what is in the bucket.
//!
//! Two promises are kept here. An acknowledged log record is in the bucket (every log object
//! is a conditional `put_if_absent`, retried until it lands or the deadline passes). And the
//! log never forks: a record that may or may not have landed stays "in doubt" and is settled
//! at the same position before anything else is written, and a position or head held by
//! another writer fences this one for good.
// Only the uploader and tests use this until the WAL wrapper (Task 7) does.
#![allow(dead_code)]

use crate::config::{Event, EventHook};
use crate::error::{Error, Result};
use crate::format::{encode_record, Head, Keys, LogRecord, Manifest, Op};
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
    /// A seal's head write failed without a definite answer: the head may already name
    /// generation + 1, so no log object may go to this generation until that is known.
    head_in_doubt: bool,
    fenced: bool,
    counters: Arc<Counters>,
    events: Option<EventHook>,
}

const FORMAT: u32 = 1;

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
            head_in_doubt: false,
            fenced: false,
            counters: Arc::default(),
            events: None,
        }
    }

    /// Start a new database: `put_if_absent(head)` at generation 1 with no manifest.
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
    pub(crate) fn append_log(&mut self, syncs: Vec<Vec<Op>>) -> Result<LogPos> {
        self.check_fenced()?;
        let policy = self.retry.clone();
        self.settle_head(&policy).map_err(upload_error)?;
        self.settle(&policy).map_err(upload_error)?;
        let body = encode_record(&LogRecord::Extents {
            writer_id: self.head.writer_id.clone(),
            syncs,
        });
        self.put_log(body, &policy).map_err(upload_error)
    }

    /// After a seal's head write failed ambiguously, find out whether the head moved. Unmoved:
    /// carry on in this generation. Moved to the head we meant to write: refuse until a seal
    /// completes it (its `put_if_match` 412 then adopts the stored head). Otherwise: fenced.
    fn settle_head(&mut self, policy: &RetryPolicy) -> Result<()> {
        if !self.head_in_doubt {
            return Ok(());
        }
        let key = self.keys.head();
        let stored = retrying(&self.counters, policy, &key, || self.store.get(&key))?;
        match stored {
            Some(o) if o.etag == self.head_etag => {
                self.head_in_doubt = false;
                Ok(())
            }
            Some(o) if o.body == self.sealed_head().encode() => Err(Error::UploadFailed(format!(
                "generation {} may be sealed; seal again before appending",
                self.head.generation
            ))),
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
    fn settle(&mut self, policy: &RetryPolicy) -> Result<()> {
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
        self.put_log(d.body, policy).map(|_| ())
    }

    /// `put_if_absent` at the next seq. Ours (created, or already there with the same bytes)
    /// advances the seq; someone else's fences; anything else leaves `body` in doubt.
    fn put_log(&mut self, body: Vec<u8>, policy: &RetryPolicy) -> Result<LogPos> {
        let pos = LogPos {
            generation: self.head.generation,
            seq: self.seq,
        };
        let key = self.keys.log(pos.generation, pos.seq);
        match self.put_log_at(&key, &body, policy) {
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

    fn put_log_at(&self, key: &str, body: &[u8], policy: &RetryPolicy) -> Result<()> {
        let c = &self.counters;
        if let CondPut::Created { .. } =
            retrying(c, policy, key, || self.store.put_if_absent(key, body))?
        {
            return Ok(());
        }
        match retrying(c, policy, key, || self.store.get(key))? {
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
        self.settle(&self.remaining(start)).map_err(upload_error)?;
        self.seal(start, pages, page_size, page_count)
            .map_err(upload_error)
    }

    fn seal(
        &mut self,
        start: Instant,
        pages: Vec<(u32, Vec<u8>)>,
        page_size: u32,
        page_count: u32,
    ) -> Result<u64> {
        let gen = self.head.generation;
        self.emit(Event::SealStarted {
            generation: gen,
            pages: pages.len() as u64,
        });
        // Pages and the manifest are unreferenced until the head moves: plain puts, and a
        // failed seal may overwrite them next time.
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
        let etag = self.put_head(start, &head)?;

        self.head = head;
        self.head_etag = etag;
        self.manifest = Some(manifest);
        self.seq = 1;
        self.counters.seals.fetch_add(1, Relaxed);
        self.emit(Event::SealFinished {
            generation: gen,
            elapsed: start.elapsed(),
        });
        Ok(gen + 1)
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
        // Until a definite answer, an attempt may have landed.
        self.head_in_doubt = true;
        let c = &self.counters;
        let r = retrying(c, &self.remaining(start), &key, || {
            self.store.put_if_match(&key, &body, &self.head_etag)
        })?;
        if let CondPut::Created { etag } = r {
            self.head_in_doubt = false;
            return Ok(etag);
        }
        match retrying(c, &self.remaining(start), &key, || self.store.get(&key))? {
            Some(o) if o.body == body => {
                self.head_in_doubt = false;
                Ok(o.etag)
            }
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
