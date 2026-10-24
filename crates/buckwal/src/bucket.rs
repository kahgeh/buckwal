//! Bucket write semantics: the one place that decides what is in the bucket.
//!
//! Two promises are kept here. An acknowledged log record is in the bucket (every log object
//! is a conditional `put_if_absent`, retried until it lands or the deadline passes). And the
//! log never forks: a record that may or may not have landed stays "in doubt" and is settled
//! at the same position before anything else is written, and a position or head held by
//! another writer fences this one for good.

use crate::clock::Stamp;
use crate::config::{Event, EventHook, JournalMode};
use crate::create::Image;
use crate::error::{Error, Result};
use crate::format::{Head, Keys, LogFile, LogRecord, Manifest, Op};
use crate::store::{with_retry, CondPut, Object, ObjectStore, RetryPolicy, StoreError};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
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
    /// The lease lapsed first, or a page we wrote was overwritten (`Step::Overwritten`): these
    /// indices (into the seal's pages) are not uploaded yet, and the head check is owed.
    Remaining {
        rest: Vec<usize>,
        /// The round stored a page (or found one of ours overwritten).
        stored: bool,
        /// The store answered a request of the round with a 412 or a stat.
        answered: bool,
    },
}

/// What this process knows of a seal object's current ETag (`Bucket::seal_etags`), which
/// decides the condition of its next PUT.
#[derive(Debug, Clone)]
enum Known {
    /// This process's last PUT of it returned this ETag: `put_if_match` on it.
    Own(String),
    /// A stat read this ETag (another writer's object, or ours whose reply was lost):
    /// `put_if_match` on it.
    Seen(String),
    /// A PUT got a 412: stat it before the next PUT.
    Stale,
}

/// How one request of a conditional seal PUT ended (`Bucket::seal_put_step`).
enum Step {
    /// The body is stored, under the ETag now in `seal_etags`.
    Done,
    /// A definite answer that is not the end (a 412, a stat): the next request follows.
    Again,
    /// The lease refused the request: nothing was sent.
    Lapsed,
    /// A `put_if_match` on our own last ETag got a 412: someone wrote the key after us. Only a
    /// head GET can tell whether this writer still holds the head; the key is stat'ed after it.
    Overwritten,
}

/// How an epoch raise (`Bucket::raise_epoch_at`) ended.
enum Raise {
    /// The raised head is published and adopted.
    Raised,
    /// The head holds the pending seal's head (or, at open, this writer's own sealed head)
    /// instead: its late PUT landed first, with this ETag. Nothing was raised.
    Published(String),
}

/// The takeover quiet (`Bucket::set_seal_not_before`): no seal of `generation` starts before
/// `until`, on the monotonic clock.
///
/// Why only that clock: a wall clock stepped forward would end a wall deadline early (and
/// let an old writer's late PUT overwrite this writer's seal), and one stepped back would
/// stall every commit and close for the size of the step. So a reopen waits the persisted
/// length again in full (`db::set_quiet`) rather than a wall-clock remainder. A machine that
/// sleeps through the quiet only makes the wait longer (`Instant` stands still), which is the
/// safe side.
#[derive(Debug, Clone, Copy)]
struct SealQuiet {
    generation: u64,
    until: Instant,
}

impl SealQuiet {
    /// How long until the quiet has passed (zero once it has).
    fn remaining(&self) -> Duration {
        self.until.saturating_duration_since(Instant::now())
    }
}

/// Whether a round of a seal's requests counts as progress for `Bucket::renew_lease`. One
/// that stored an object (or found one of ours overwritten) does, and resets `answer_only`.
/// One whose requests the store only answered with 412s and stats does for at most
/// `defaults::MAX_ANSWER_ONLY_ROUNDS` rounds in a row: a store that answers 412 forever
/// would otherwise keep the seal going past its deadline for good.
fn round_progressed(stored: bool, answered: bool, answer_only: &mut u32) -> bool {
    if stored {
        *answer_only = 0;
        return true;
    }
    if answered && *answer_only < crate::config::defaults::MAX_ANSWER_ONLY_ROUNDS {
        *answer_only += 1;
        return true;
    }
    false
}

/// `t` in unix ms (0 before the epoch).
pub(crate) fn unix_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// What a GET of the head says about a pending seal.
enum HeadCheck {
    /// No seal is pending, or the head is unchanged: carry on in this generation.
    Unmoved,
    /// The pending seal's head is published (with this ETag).
    Published(String),
}

/// A writer's own lease and request timeout: every head it writes carries them (`lease_ms`,
/// `request_timeout_ms`), so a later takeover sizes its quiet from both writers' settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Timeouts {
    pub lease: Duration,
    pub request_timeout: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Timeouts {
            lease: crate::config::defaults::LEASE,
            request_timeout: crate::config::defaults::REQUEST_TIMEOUT,
        }
    }
}

impl Timeouts {
    /// `lease + request_timeout`: how long after its last head check a writer's request can
    /// still be in flight.
    pub(crate) fn sum(&self) -> Duration {
        self.lease.saturating_add(self.request_timeout)
    }

    fn ms(d: Duration) -> u64 {
        u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
    }

    /// `head` with these as its `lease_ms` and `request_timeout_ms`.
    fn stamp(&self, head: Head) -> Head {
        Head {
            lease_ms: Some(Self::ms(self.lease)),
            request_timeout_ms: Some(Self::ms(self.request_timeout)),
            ..head
        }
    }

    /// The takeover quiet: the larger of the replaced writer's sum (when its head carries both
    /// fields) and this writer's own. The head decoder already refuses a sum over
    /// `MAX_SEAL_QUIET`.
    pub(crate) fn quiet_for(&self, old: Option<&Head>) -> Duration {
        let old = old
            .and_then(|h| Some((h.lease_ms?, h.request_timeout_ms?)))
            .map_or(Duration::ZERO, |(l, r)| {
                Duration::from_millis(l).saturating_add(Duration::from_millis(r))
            });
        self.sum().max(old)
    }
}

/// What a fence asks the uploader: the background commits it drops, as (count, first, last).
pub(crate) type FenceReport = Box<dyn Fn() -> Option<(u64, u64, u64)> + Send + Sync>;

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
    /// Set while a seal's head write has no definite answer: the head may already name the
    /// seal target (generation + 1, or the floor after a takeover), so nothing may go to this
    /// generation until that is known.
    pending_seal: Option<PendingSeal>,
    fenced: bool,
    /// The fence report hook, asked when this writer fences: the background commits that never reached the bucket
    /// (count, first and last commit number), for the `writer fenced` record.
    fence_report: Option<FenceReport>,
    /// The lease (`check_lease`): when the request behind the last confirmation was sent, on
    /// both clocks. It is renewed by a confirmed log record (`put_log`, the fence; one that
    /// landed `lease` + retry deadline after it only through a head GET, `confirm_landed`), a
    /// confirmed head write (`put_head`, `raise_epoch`, and `create`'s), open's head read
    /// (`attach`), and a head GET that passes `check_head` (finding its own raised head) or
    /// `verify_head`. A failed or ambiguous request renews nothing, and neither do page and
    /// manifest PUTs (which only makes the check run sooner).
    last_confirmed: Stamp,
    /// The current generation's seal objects (`pages/G/*`, `manifest/G`) this process has
    /// PUT or stat'ed, by key: the condition of the next PUT of each (`seal_put_step`). A key
    /// not here goes with `put_if_absent`. Cleared when the generation changes
    /// (`finish_seal`). A `Mutex` because the page workers share `&self`.
    seal_etags: Mutex<HashMap<String, Known>>,
    /// See `set_lease`.
    lease: Duration,
    /// This writer's own timeouts, written into every head (`Timeouts`).
    timeouts: Timeouts,
    /// See `set_seal_not_before`.
    quiet: Option<SealQuiet>,
    counters: Arc<Counters>,
    events: Option<EventHook>,
}

/// A writer id goes into every record and the head: ASCII, at most 255 bytes.
fn check_writer_id(writer_id: &str) -> Result<()> {
    if !writer_id.is_ascii() || writer_id.len() > u8::MAX as usize {
        return Err(Error::Config(
            "writer id must be ASCII and at most 255 bytes".into(),
        ));
    }
    Ok(())
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

/// What a takeover's `Timeout` advises: the claim id is persisted (and the head may already
/// name it), so the next open of the directory resumes the takeover; `takeover` itself
/// refuses a directory that holds one.
const RESUME_TAKEOVER: &str = "open this directory to resume the takeover (`open`, or \
     `buckwal sql --claim --dir DIR 'SELECT 1'`)";

/// What `take_over` tells a restored directory whose generation is no longer the head's.
pub(crate) const RESTORE_AGAIN: &str =
    "the bucket changed since this directory was restored; restore again";

/// The first half of a takeover (the claim of a restored copy) as `writer_id`: the head is
/// written again with the new writer id and its epoch raised by one, same generation and
/// manifest. The second half is the fence record (`Bucket::append_fence`), which the caller
/// appends once attached (`db::attach_existing`). Returns the new head (carrying `own`), its
/// ETag and the head it replaced (`None` for a claim that already landed), whose timeouts size
/// the takeover quiet (`Timeouts::quiet_for`).
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
/// uploads a page or a manifest, so it cannot overwrite a generation the new writer sealed;
/// its page and manifest PUTs stop at the first lapse of its lease (`Bucket::seal`), and are
/// conditional, so one landing after the new writer wrote its key fails
/// (`Bucket::seal_put_step`).
/// The log stays linear: such a generation holds the old writer's records, then the fence,
/// then the new writer's, which `restore::generation_image` accepts.
pub(crate) fn take_over_counted(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    expect_generation: u64,
    writer_id: &str,
    own: Timeouts,
    retries: &AtomicU64,
) -> Result<(Head, String, Option<Head>)> {
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
        let head = keys.decode_head_open(&current.body)?;
        if head.writer_id == writer_id {
            return Ok((head, current.etag, None));
        }
        if head.generation != expect_generation {
            return Err(restore_again());
        }
        let new = own.stamp(Head {
            writer_id: writer_id.to_string(),
            epoch: head.epoch + 1,
            next_generation_floor: Some(next_floor(&head)),
            ..head.clone()
        });
        let body = keys.encode_head(&new);
        let put = retry_rest(retry, start, retries, &key, || {
            store.put_if_match(&key, &body, &current.etag)
        })
        .map_err(upload_error)?;
        if let CondPut::Created { etag } = put {
            return Ok((new, etag, Some(head)));
        }
        // The head moved since the GET: read it again, within the same deadline.
        let elapsed = start.elapsed();
        if elapsed >= retry.deadline {
            return Err(Error::Timeout(format!(
                "takeover: the head kept changing for {elapsed:?}; {RESUME_TAKEOVER}"
            )));
        }
    }
}

/// The floor a takeover of `replaced` writes: one above the highest generation any earlier
/// writer could seal into, `max(G + 1, the replaced head's floor) + 1`. The new writer's first
/// seal moves the head to it and writes its objects under the generation before it.
pub(crate) fn next_floor(replaced: &Head) -> u64 {
    (replaced.generation + 1).max(replaced.next_generation_floor.unwrap_or(0)) + 1
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
        Timeouts::default(),
        &AtomicU64::new(0),
    )
    .map(|(h, etag, _)| (h, etag))
}

/// What `Bucket::append_fence` landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FenceOutcome {
    pub pos: LogPos,
    /// Another writer may still have a seal of this generation in flight: some other
    /// writer's last record before the fence is an Extents record with no syncs (a
    /// seal-start record: nothing else goes to the log once a seal has started), or a record
    /// before the fence could not be read (`seal_may_be_in_flight`). The new writer then waits
    /// out a takeover quiet before it seals this generation (`set_seal_not_before`).
    pub seal_may_be_in_flight: bool,
}

/// What a fence PUT found at its seq.
enum FencePut {
    /// The fence is there (created, or an earlier attempt of ours landed).
    Landed,
    /// Another writer's record the fence steps over: an old writer's Extents record, or a
    /// fence of a lower epoch (a losing concurrent takeover).
    Occupied,
}

/// Whether some writer other than `ours` has, as its last record in `records` (a
/// generation's log, in seq order), an Extents record with no syncs: a seal-start record (or
/// a stage-2 claim record), so that writer's seal may still be uploading.
///
/// Each writer's last record counts, not only the last record overall: a writer left
/// mid-seal can sit several records back, behind another takeover's fence and that writer's
/// commits (`[A seal-start, F(B), B's commits, F(C)]`), and its page PUTs are still in flight.
fn other_writer_mid_seal(records: &[LogRecord], ours: &str) -> bool {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    records.iter().rev().any(|rec| {
        let (writer, seal_start) = match rec {
            LogRecord::Extents {
                writer_id, syncs, ..
            } => (writer_id.as_str(), syncs.is_empty()),
            LogRecord::Fence { writer_id, .. } => (writer_id.as_str(), false),
        };
        writer != ours && seen.insert(writer) && seal_start
    })
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
            seal_etags: Mutex::default(),
            lease: crate::config::defaults::LEASE,
            timeouts: Timeouts::default(),
            quiet: None,
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
            fence_report: None,
            counters: Arc::default(),
            events: None,
        }
    }

    /// A handle for a replica's engine: the head and manifest it was given, no writer id
    /// check, no listing. A replica sends nothing through it (`IoMode::Replica`).
    pub(crate) fn for_replica(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        concurrency: usize,
        head: Head,
        head_etag: String,
        manifest: Option<Manifest>,
    ) -> Bucket {
        let mut b = Bucket::new(store, keys, retry, concurrency, head, head_etag);
        b.manifest = manifest;
        b
    }

    /// Start a new database: `put_if_absent(head)` at generation 1 and epoch 1, with no
    /// manifest.
    ///
    /// A create that times out may still have stored its head. A later `create` then finds a
    /// head and returns `Config` (its first attempt gets a 412, which is never compared);
    /// the caller should `attach` with the same writer id instead.
    pub(crate) fn create_with(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        concurrency: usize,
        writer_id: String,
        own: Timeouts,
    ) -> Result<Bucket> {
        check_writer_id(&writer_id)?;
        let head = own.stamp(Head {
            format: keys.new_format(JournalMode::Wal),
            generation: 1,
            manifest: None,
            writer_id,
            epoch: 1,
            journal_mode: JournalMode::Wal,
            lease_ms: None,
            request_timeout_ms: None,
            next_generation_floor: None,
        });
        let mut b = Bucket::create_head(store, keys, retry, concurrency, head)?;
        b.timeouts = own;
        Ok(b)
    }

    /// Start a new MVCC database as a seal of generation 1 holding its empty image
    /// (`create::build_mvcc_image`): every page, as the file holds it (ciphertext when
    /// encrypted), at `pages/1/*`, the manifest a seal of generation 1 writes
    /// (`Manifest::sealed`) at `manifest/1`, then the head (`put_if_absent`, last) as that
    /// seal publishes it (`sealed_head`): generation 2, manifest 1, epoch 1, format 2, journal
    /// mode MVCC. It is the bucket a WAL database's first seal leaves, without log objects.
    /// A head at generation 1 naming manifest 1 would not do: the first seal of generation 1
    /// overwrites `pages/1/*` and `manifest/1`, which that head would still reference until
    /// the new one lands.
    ///
    /// Pages and manifest go with plain PUTs: a crashed creation may have left them, and no
    /// head references them until this head lands, so a retry overwrites them. Nothing is
    /// sent with `put_if_absent` but the head (a seal-start record, say, would be a key a
    /// crashed attempt may hold). Two racing creators: the head decides, and the loser
    /// returns `Config` as `create` does; its pages may have overwritten the winner's (the
    /// same empty database).
    pub(crate) fn create_sealed_with(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        concurrency: usize,
        writer_id: String,
        image: Image,
        own: Timeouts,
    ) -> Result<Bucket> {
        check_writer_id(&writer_id)?;
        let gen = 1;
        let (page_size, page_count) = (image.page_size, image.page_count());
        let pages = image.pages;
        let counters = Counters::default();
        for (p, body) in &pages {
            let key = keys.page(gen, *p);
            let obj = keys.seal_page(*p, gen, body);
            retrying(&counters, &retry, &key, || store.put(&key, &obj))?;
            counters.page_puts.fetch_add(1, Relaxed);
        }
        let dirty: BTreeSet<u32> = pages.iter().map(|(p, _)| *p).collect();
        let manifest = Manifest {
            page_size,
            page_count,
            pages: BTreeMap::new(),
        }
        .sealed(gen, &dirty, page_size, page_count);
        let key = keys.manifest(gen);
        let manifest_body = keys.encode_manifest(&manifest, gen);
        retrying(&counters, &retry, &key, || store.put(&key, &manifest_body))?;
        let head = own.stamp(Head {
            format: keys.new_format(JournalMode::Mvcc),
            generation: gen + 1,
            manifest: Some(gen),
            writer_id,
            epoch: 1,
            journal_mode: JournalMode::Mvcc,
            lease_ms: None,
            request_timeout_ms: None,
            next_generation_floor: None,
        });
        let mut b = Bucket::create_head(store, keys, retry, concurrency, head)?;
        b.timeouts = own;
        b.manifest = Some(manifest);
        b.counters
            .retries
            .fetch_add(counters.retries.load(Relaxed), Relaxed);
        b.counters
            .page_puts
            .store(counters.page_puts.load(Relaxed), Relaxed);
        Ok(b)
    }

    /// `create_with` and the default timeouts.
    #[cfg(test)]
    pub(crate) fn create(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        concurrency: usize,
        writer_id: String,
    ) -> Result<Bucket> {
        Bucket::create_with(
            store,
            keys,
            retry,
            concurrency,
            writer_id,
            Timeouts::default(),
        )
    }

    /// `create_sealed_with` and the default timeouts.
    #[cfg(test)]
    pub(crate) fn create_sealed(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        concurrency: usize,
        writer_id: String,
        image: Image,
    ) -> Result<Bucket> {
        Bucket::create_sealed_with(
            store,
            keys,
            retry,
            concurrency,
            writer_id,
            image,
            Timeouts::default(),
        )
    }

    /// `put_if_absent(head)`: the commit point of a new database.
    fn create_head(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        concurrency: usize,
        head: Head,
    ) -> Result<Bucket> {
        let body = keys.encode_head(&head);
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
        let head = keys.decode_head_open(&obj.body)?;
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
            // Before any commit exists, so none is dropped.
            tracing::error!(
                target: "buckwal",
                generation = head.generation,
                writer_id,
                error = %e,
                dropped_commits = 0u64,
                first_dropped_commit = 0u64,
                last_dropped_commit = 0u64,
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
                Some(keys.decode_manifest(&m.body, g)?)
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
        self.timeouts.lease = lease;
    }

    /// The request timeout written into every head this writer writes (`Timeouts`).
    pub(crate) fn set_request_timeout(&mut self, request_timeout: Duration) {
        self.timeouts.request_timeout = request_timeout;
    }

    /// The takeover quiet: no seal of `generation` starts before `until` (monotonic).
    /// `db::attach_existing` sets it when another writer may still have a seal of the
    /// generation in flight at this writer's fence (`FenceOutcome::seal_may_be_in_flight`),
    /// to `lease + request_timeout` after the fence. A later open that finds the quiet still
    /// pending (no seal of `generation` cleared it) waits its persisted length again in full
    /// from that open (`db::set_quiet`). A seal of `generation` clears it.
    ///
    /// Why: that writer's page and manifest requests may still be in flight. Its PUTs are
    /// conditional (`seal_put_step`): one conditioned on what it saw before this writer wrote
    /// the key fails. It could still overwrite this writer's object by stat'ing it after this
    /// writer wrote it and conditioning a PUT on that ETag. The old writer sends each request
    /// only within `lease` of its last head check (`seal`), which came before this writer's
    /// head write, and a request ends within `request_timeout`: after the quiet, none of its
    /// stats can see an object of this writer's. The same holds when that writer is several
    /// takeovers back (`[A seal-start, F(B), B's commits, F(C)]`): its last head check came
    /// before the first of them. The quiet is the larger of the replaced writer's `lease +
    /// request_timeout` (its head carries both) and this writer's own (`Timeouts::quiet_for`); a
    /// head without the fields counts as this writer's own. A writer several takeovers back is
    /// sized by the head each takeover replaced, not by its own settings (README, takeover quiet).
    ///
    /// Equal ETags for equal bytes (S3 and RustFS: the MD5 of the body) are covered by the
    /// skipped generation instead (`seal_put_step`, `seal_target`): this writer's seal keys are
    /// never keys the old writer writes.
    pub(crate) fn set_seal_not_before(&mut self, generation: u64, until: Instant) {
        self.quiet = Some(SealQuiet { generation, until });
    }

    /// The end of the current generation's takeover quiet, the very deadline `wait_quiet`
    /// waits for; `None` when there is none.
    pub(crate) fn quiet_until(&self) -> Option<Instant> {
        self.quiet
            .filter(|q| q.generation == self.head.generation)
            .map(|q| q.until)
    }

    /// Before a seal of the current generation: wait out its takeover quiet, if any, emitting
    /// `Event::SealQuiet` first. The uploader thread waits here, so later commits queue
    /// behind the seal meanwhile.
    fn wait_quiet(&self) {
        let Some(q) = self.quiet.filter(|q| q.generation == self.head.generation) else {
            return;
        };
        let wait = q.remaining();
        if wait.is_zero() {
            return;
        }
        self.emit(Event::SealQuiet {
            generation: q.generation,
            wait,
        });
        tracing::info!(
            target: "buckwal",
            generation = q.generation,
            ms = wait.as_millis() as u64,
            "seal waits out the takeover quiet"
        );
        loop {
            let left = q.remaining();
            if left.is_zero() {
                return;
            }
            std::thread::sleep(left);
        }
    }

    /// Move the last confirmation back by `instant_by` on the monotonic clock and `wall_by`
    /// on the wall clock, as if it were that much older on each.
    #[cfg(test)]
    pub(crate) fn age_lease(&mut self, instant_by: Duration, wall_by: Duration) {
        self.last_confirmed.back(instant_by, wall_by);
    }

    /// `report` is the fence report hook: the commits a fence drops (see `fence`). Not `probe`,
    /// which is a head GET.
    pub(crate) fn set_fence_report(&mut self, report: FenceReport) {
        self.fence_report = Some(report);
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
    /// Logged once, when it first happens.
    fn fence(&mut self, e: Error) -> Error {
        if std::mem::replace(&mut self.fenced, true) {
            return e;
        }
        let (dropped, first, last) = self
            .fence_report
            .as_ref()
            .and_then(|report| report())
            .unwrap_or((0, 0, 0));
        tracing::error!(
            target: "buckwal",
            generation = self.head.generation,
            writer_id = %self.head.writer_id,
            error = %e,
            dropped_commits = dropped,
            first_dropped_commit = first,
            last_dropped_commit = last,
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

    /// Write one log record of `file`'s ops (kind 1 for `LogFile::Wal`, kind 3 for
    /// `LogFile::Log`) at the next seq of the current generation.
    /// Every retry in the call shares one deadline, counted from its start.
    ///
    /// The record is built only once `check_fenced` and `before_record` have passed; a
    /// failure before that hands the syncs back as `AppendFailure::unsent`.
    ///
    /// It emits no `Event::LogUploaded` for this record: only the caller knows its commit
    /// numbers, so the uploader emits it. (Claim and reconcile records are appended before
    /// the uploader sets the event hook.) A record settled on the way is emitted here.
    ///
    /// It records `buckwal.generation` and `buckwal.seq` (the record's position, or the one
    /// it was meant for) and `buckwal.retries` (`Appended::retries`) on the current span.
    pub(crate) fn append_log(
        &mut self,
        file: LogFile,
        syncs: Vec<Vec<Op>>,
    ) -> std::result::Result<Appended, AppendFailure> {
        let retries = self.counters.retries.load(Relaxed);
        let r = self.append_log_at(file, syncs, retries);
        let span = Span::current();
        let pos = match &r {
            Ok(a) => a.pos,
            Err(_) => LogPos {
                generation: self.head.generation,
                seq: self.seq,
            },
        };
        span.record("buckwal.generation", pos.generation);
        span.record("buckwal.seq", pos.seq);
        span.record(
            "buckwal.retries",
            self.counters.retries.load(Relaxed).saturating_sub(retries),
        );
        r
    }

    fn append_log_at(
        &mut self,
        file: LogFile,
        syncs: Vec<Vec<Op>>,
        retries: u64,
    ) -> std::result::Result<Appended, AppendFailure> {
        // Kind 3 exists only in an MVCC bucket (format 2, or 3 when encrypted); a v1 reader would
        // misread it.
        if matches!(file, LogFile::Log) && self.head.journal_mode != JournalMode::Mvcc {
            return Err(AppendFailure {
                error: Error::Unsupported("MVCC: a .db-log record in a WAL bucket"),
                unsent: Some(syncs),
            });
        }
        let start = Instant::now();
        if let Err(e) = self.check_fenced().and_then(|()| self.before_record(start)) {
            return Err(AppendFailure {
                error: upload_error(e),
                unsent: Some(syncs),
            });
        }
        let body = self.keys.encode_record(
            &LogRecord::Extents {
                file,
                writer_id: self.head.writer_id.clone(),
                syncs,
            },
            self.head.generation,
            self.seq,
        );
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
    /// It runs in a `buckwal.fence` span (`buckwal.generation`, `buckwal.seq`,
    /// `buckwal.epoch`, `buckwal.retries`, `s3.status`).
    pub(crate) fn append_fence(&mut self) -> Result<FenceOutcome> {
        let start = Instant::now();
        let retries = self.counters.retries.load(Relaxed);
        let span = tracing::info_span!(
            target: "buckwal",
            "buckwal.fence",
            buckwal.generation = self.head.generation,
            buckwal.seq = Empty,
            buckwal.epoch = self.head.epoch,
            buckwal.retries = Empty,
            s3.status = Empty,
        );
        let r = span.in_scope(|| {
            self.check_fenced()
                .and_then(|()| self.append_fence_at(start))
                .map_err(upload_error)
        });
        span.record("buckwal.seq", r.as_ref().map_or(self.seq, |o| o.pos.seq));
        span.record(
            "buckwal.retries",
            self.counters.retries.load(Relaxed).saturating_sub(retries),
        );
        r
    }

    fn append_fence_at(&mut self, start: Instant) -> Result<FenceOutcome> {
        self.before_record(start)?;
        let fence = LogRecord::Fence {
            writer_id: self.head.writer_id.clone(),
            epoch: self.head.epoch,
        };
        let mut steps = 0u64;
        loop {
            let pos = LogPos {
                generation: self.head.generation,
                seq: self.seq,
            };
            // The integrity code covers the seq, so the fence is encoded for each seq it tries.
            let body = self.keys.encode_record(&fence, pos.generation, pos.seq);
            let key = self.keys.log(pos.generation, pos.seq);
            let confirmed = self.last_confirmed;
            let sent = Stamp::now();
            let put = self
                .put_fence_at(start, pos, &key, &body)
                .and_then(|p| match p {
                    FencePut::Landed => self
                        .confirm_landed(start, confirmed, sent)
                        .map(|()| FencePut::Landed),
                    occupied => Ok(occupied),
                });
            match put {
                Ok(FencePut::Landed) => {
                    self.seq += 1;
                    self.counters.log_puts.fetch_add(1, Relaxed);
                    self.emit_uploaded(pos, body.len() as u64);
                    let seal_may_be_in_flight = self.seal_may_be_in_flight(start, pos);
                    return Ok(FenceOutcome {
                        pos,
                        seal_may_be_in_flight,
                    });
                }
                Ok(FencePut::Occupied) => {
                    if steps == crate::config::defaults::FENCE_MAX_STEPS
                        || start.elapsed() >= self.retry.deadline
                    {
                        return Err(Error::Timeout(format!(
                            "takeover: the old writer kept appending past {steps} log \
                             positions; {RESUME_TAKEOVER}"
                        )));
                    }
                    steps += 1;
                    self.seq += 1;
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
    fn put_fence_at(
        &self,
        start: Instant,
        pos: LogPos,
        key: &str,
        body: &[u8],
    ) -> Result<FencePut> {
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
        let rec = self.keys.decode_record(&o.body, pos.generation, pos.seq)?;
        let (ours, epoch) = (&self.head.writer_id, self.head.epoch);
        match &rec {
            LogRecord::Extents { writer_id, .. } if writer_id != ours => Ok(FencePut::Occupied),
            LogRecord::Fence { epoch: k, .. } if *k < epoch => Ok(FencePut::Occupied),
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

    /// `FenceOutcome::seal_may_be_in_flight` for the fence at `pos`: every record of the
    /// generation is read (a list, then a GET each, `upload_concurrency` at a time, under what
    /// is left of the call's deadline), and those before `pos` are checked with
    /// `other_writer_mid_seal`. A list or a record that cannot be read, or a gap, counts as a
    /// seal in flight: the caller then takes the more cautious path (the quiet).
    ///
    /// Only records before the fence count: after it, the log holds this writer's records
    /// alone (a later takeover's fence fences this writer). Reconcile reads the generation
    /// again right after (`restore::generation_image`); a takeover is rare enough that the
    /// second read is kept rather than threading the records through.
    fn seal_may_be_in_flight(&self, start: Instant, pos: LogPos) -> bool {
        if pos.seq <= 1 {
            return false;
        }
        let records = crate::restore::fetch_generation(
            &*self.store,
            &self.keys,
            pos.generation,
            self.concurrency,
            &self.remaining(start),
            &self.counters.retries,
        );
        match records {
            Ok(r) if r.len() as u64 >= pos.seq - 1 => {
                other_writer_mid_seal(&r[..(pos.seq - 1) as usize], &self.head.writer_id)
            }
            Ok(_) | Err(_) => true,
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

    /// Settle everything an earlier failure left owed, for a `.db-log` sync (the
    /// confirmed-sync rule): `catch_up`, and a pending seal whose head is published is adopted
    /// here, without uploading, as the next seal would (`seal`). Ok only when nothing is left:
    /// no record in doubt, no seal pending. `true` when it adopted a seal (the generation
    /// moved on). One deadline, counted from its start.
    pub(crate) fn settle_all(&mut self) -> Result<bool> {
        let start = Instant::now();
        self.check_fenced()?;
        let adopted = match self.catch_up_at(start).map_err(upload_error)? {
            HeadCheck::Published(etag) => {
                let p = self
                    .pending_seal
                    .take()
                    .expect("a published seal is pending");
                self.finish_seal(start, p.head, etag, p.manifest);
                true
            }
            HeadCheck::Unmoved => false,
        };
        if self.degraded() {
            return Err(Error::UploadFailed(
                "the bucket still has a record or a seal to settle".into(),
            ));
        }
        Ok(adopted)
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
    /// `request_timeout`; `Config::validate` requires the trim grace to exceed that sum. A
    /// writer paused between this check and its PUT breaks that bound, so a record that
    /// lands later than `lease` + `retry_deadline` counts only after a head GET
    /// (`confirm_landed`).
    /// Against another process this holds only if that process's trim grace exceeds this
    /// writer's sum too (README, Settings): the head records each writer's `lease` and
    /// `request_timeout` (the takeover quiet uses the larger of both), but not `retry_deadline`
    /// or `trim_grace`.
    fn check_lease(&mut self, start: Instant) -> Result<()> {
        if self.last_confirmed.age() < self.lease {
            return Ok(());
        }
        self.verify_head(start)
    }

    /// A record of this writer landed (sent at `sent`, the last confirmation then being
    /// `confirmed`): renew the lease with `sent`, or, when the record landed `lease` + retry
    /// deadline or more after `confirmed`, GET the head first (`verify_head`).
    ///
    /// `check_lease` runs before the record is built, so a writer paused after it (SIGSTOP, a
    /// suspended VM) sends its PUT whenever it resumes. Meanwhile another writer may have
    /// taken over, sealed this generation and trimmed `log/G` (its trim grace after the
    /// seal), freeing this writer's next seq: the record then lands in a log no restore
    /// reads, and its success proves nothing. Renewing the lease on it would also let every
    /// later append skip the check. A record that landed within `lease` + retry deadline of
    /// `confirmed` landed before any trim (`check_lease`: the trim grace exceeds lease +
    /// retry deadline + request timeout). Past that, the head decides: ours renews the lease
    /// and the record counts; anything else fences this writer, and the record is not acked.
    fn confirm_landed(&mut self, start: Instant, confirmed: Stamp, sent: Stamp) -> Result<()> {
        if confirmed.age() < self.lease.saturating_add(self.retry.deadline) {
            self.last_confirmed = sent;
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
        let pending_body = self.keys.encode_head(&pending.head);
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
            Some(o) if o.body == self.keys.encode_head(&raised) => {
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
        self.timeouts.stamp(Head {
            epoch: self.head.epoch + 1,
            ..self.head.clone()
        })
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
        let sealed = self.keys.encode_head(&self.sealed_head());
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

    /// The generation the next seal moves the head to: `max(G + 1, floor)`, where the floor is
    /// the head's `next_generation_floor` (a takeover's skipped generation). Its objects go
    /// under `seal_target() - 1`.
    pub(crate) fn seal_target(&self) -> u64 {
        (self.head.generation + 1).max(self.head.next_generation_floor.unwrap_or(0))
    }

    /// The head a seal of the current generation publishes.
    fn sealed_head(&self) -> Head {
        let target = self.seal_target();
        self.timeouts.stamp(Head {
            format: self.head.format,
            generation: target,
            manifest: Some(target - 1),
            writer_id: self.head.writer_id.clone(),
            epoch: self.head.epoch,
            journal_mode: self.head.journal_mode,
            lease_ms: None,
            request_timeout_ms: None,
            // Every head after a takeover carries the floor (GC keeps off manifests below
            // `floor - 1`), the skip seal's included.
            next_generation_floor: self.head.next_generation_floor,
        })
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
        let confirmed = self.last_confirmed;
        let sent = Stamp::now();
        match self
            .put_log_at(start, &key, &body)
            .and_then(|()| self.confirm_landed(start, confirmed, sent))
        {
            Ok(()) => {
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
    /// its dirty pages, its manifest, then move the head to `seal_target()` = T with a
    /// conditional write. Pages and manifest are written under T-1 (T-1 = G in the normal
    /// case; after a takeover T can be G+2). Returns T.
    ///
    /// A seal of a generation under a takeover quiet (`set_seal_not_before`) first waits it
    /// out (`wait_quiet`); its retry deadline starts after the wait. That includes the
    /// close-time TRUNCATE seal: closing during the quiet waits too.
    ///
    /// It records `buckwal.generation` (G), `buckwal.pages`, `buckwal.skipped_generation` (the
    /// first seal after a takeover only: T - 1, which has no log) and `buckwal.retries` on the
    /// current span, and `buckwal.seq`: the seq of the seal-start record (written now, or by
    /// the earlier attempt whose head this seal adopts) once the seal reaches it. It logs the
    /// finished seal.
    pub(crate) fn seal_generation(
        &mut self,
        pages: Vec<(u32, Vec<u8>)>,
        page_size: u32,
        page_count: u32,
    ) -> Result<u64> {
        if !self.fenced {
            self.wait_quiet();
        }
        let start = Instant::now();
        let (generation, n) = (self.head.generation, pages.len() as u64);
        let retries = self.counters.retries.load(Relaxed);
        let span = Span::current();
        span.record("buckwal.generation", generation);
        span.record("buckwal.pages", n);
        if let Some(skipped) = self.skipped_generation() {
            span.record("buckwal.skipped_generation", skipped);
        }
        let r = self.check_fenced().and_then(|()| {
            self.seal(start, pages, page_size, page_count)
                .map_err(upload_error)
        });
        span.record(
            "buckwal.retries",
            self.counters.retries.load(Relaxed).saturating_sub(retries),
        );
        if r.is_ok() {
            tracing::info!(
                target: "buckwal",
                generation,
                pages = n,
                ms = start.elapsed().as_millis() as u64,
                "seal finished"
            );
        }
        r
    }

    /// The skipped generation the next seal writes under: the number between this generation
    /// and the seal target, which has no log (its pages and manifest are the new writer's, so
    /// the old writer's late PUTs cannot reach them). `None` when the seal moves to G + 1.
    fn skipped_generation(&self) -> Option<u64> {
        let target = self.seal_target();
        (target > self.head.generation + 1).then(|| target - 1)
    }

    /// The new head is published: adopt it, its ETag and manifest. Returns its generation.
    fn finish_seal(&mut self, start: Instant, head: Head, etag: String, manifest: Manifest) -> u64 {
        let gen = head.generation;
        // `SealStarted`'s number: the generation sealed, not the new head's.
        let sealed = self.head.generation;
        self.head = head;
        self.head_etag = etag;
        self.manifest = Some(manifest);
        self.seq = 1;
        self.pending_seal = None;
        self.quiet = None;
        self.seal_etags.get_mut().unwrap().clear();
        self.counters.seals.fetch_add(1, Relaxed);
        self.emit(Event::SealFinished {
            generation: sealed,
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
            Span::current().record("buckwal.seq", self.seq - 1);
            return Ok(self.finish_seal(start, p.head, etag, p.manifest));
        }
        self.settle(start)?;
        Span::current().record("buckwal.seq", self.seq);
        self.seal_start(start)?;
        // The seal that moves the head to T writes its objects under T-1 (G after no takeover,
        // G+1 after one: the skipped generation); the log and the seal-start record stay on
        // `head.generation`.
        let key_gen = self.seal_target() - 1;
        self.emit(Event::SealStarted {
            generation: self.head.generation,
            pages: pages.len() as u64,
        });
        // Pages and the manifest are unreferenced until the head moves. A failed seal whose
        // head never landed may overwrite them next time (`check_head` above ensures a
        // published one is adopted instead). `seal_start` ensured no other writer had taken
        // this generation over, and each request is sent only inside the lease: at a lapse
        // the workers stop, the head is read again (renewing the lease, or fencing this
        // writer) and the pages not yet uploaded resume. So no page or manifest request goes
        // out more than `lease` after this writer last saw its own head (a zero lease, for
        // tests, lets each worker send one attempt right after each head check).
        //
        // Each PUT is conditional (`seal_put_step`), so a PUT of this writer that lands after
        // another writer wrote the key fails its condition: a stale writer paused between its
        // lease check and its send cannot overwrite a seal that a new writer published.
        let mut todo: Vec<usize> = (0..pages.len()).collect();
        let mut answer_only = 0;
        while let Lapse::Remaining {
            rest,
            stored,
            answered,
        } = self.put_pages_until_lapse(start, key_gen, &pages, &todo)?
        {
            self.renew_lease(start, round_progressed(stored, answered, &mut answer_only))?;
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
            .sealed(key_gen, &dirty, page_size, page_count);
        let key = self.keys.manifest(key_gen);
        let body = self.keys.encode_manifest(&manifest, key_gen);
        // The page PUTs, not a head check, came just before: no free attempt (a zero lease
        // checks the head first). The first lapse here always renews: the PUTs before it
        // went out after the last head check, and a seal is stuck only when a lapse follows
        // its own renewal with nothing stored, which is what `round_progressed` tells.
        let mut free = false;
        let mut first = true;
        let mut answer_only = 0;
        loop {
            let answered = AtomicBool::new(false);
            let step = self.seal_put(start, &key, &body, &mut free, &answered)?;
            if let Step::Done = step {
                break;
            }
            // A lapse, or our manifest was overwritten (the head check is owed): it renews the
            // lease or fences this writer.
            let stored = std::mem::take(&mut first) || matches!(step, Step::Overwritten);
            let progressed = round_progressed(stored, answered.into_inner(), &mut answer_only);
            self.renew_lease(start, progressed)?;
            free = self.lease.is_zero();
        }

        // Past the lease, GET the head before the head PUT (renewing the lease, or fencing this
        // writer). It costs one GET and does not stop a late head: a moved head already gets a
        // 412 from the `If-Match` below, and an unchanged one passes this check. So a writer
        // paused anywhere between its manifest landing and the head PUT's send can still
        // publish its head late, and GC (in any process) may then at once trim what the
        // replaced head referenced (README, Guarantees, GC).
        self.check_lease(start)?;
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
    /// not overwrite `pages/G/*` or `manifest/G`, which that writer may already have sealed
    /// (its conditional PUTs, `seal_put_step`, do not stop a first PUT of a key it never
    /// wrote, after a stat). A seal of an old writer that has nothing left to ship sends
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
    /// the lease, whose head check fences this writer (`seal`), and one landing after that
    /// writer wrote its key fails its condition.
    fn seal_start(&mut self, start: Instant) -> Result<()> {
        let body = self.keys.encode_record(
            &LogRecord::Extents {
                file: LogFile::Wal,
                writer_id: self.head.writer_id.clone(),
                syncs: Vec::new(),
            },
            self.head.generation,
            self.seq,
        );
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
        let ours = self.keys.encode_head(&self.sealed_head());
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

    /// One request of a conditional PUT of `body` at seal object `key`, its condition from
    /// `seal_etags`, each request through `retrying_in_lease`:
    ///
    /// - a key this process has not written or stat'ed: `put_if_absent`;
    /// - one it wrote: `put_if_match` on the ETag it got back; a 412 there means someone wrote
    ///   the key after us (a writer that took over, or a stale one of ours), which only a head
    ///   GET can sort out: `Step::Overwritten`, for the caller to run it;
    /// - one it stat'ed: `put_if_match` on the ETag read;
    /// - after a 412: `stat`. No object: `put_if_absent` again. An object: `put_if_match` on
    ///   its ETag. That object may be ours (a stored PUT whose reply was lost; its ETag is the
    ///   store's own, so it is not compared; the same bytes replace it), or a leftover of an
    ///   earlier attempt, a crashed process, or the old writer before a takeover, which ours
    ///   replace. Neither fences this writer.
    ///
    /// Nothing is overwritten without the ETag it is conditioned on having been seen. Why this
    /// fences a stale writer: once a new writer has written a key, the stale writer's PUT fails
    /// its condition (absent fails, and the ETag it holds is no longer current). To overwrite
    /// the new writer's object it would have to stat it after the new writer wrote it, which is
    /// at least the takeover quiet (`set_seal_not_before`, `lease` + request timeout) after
    /// the new writer's head write; its lease check before that stat then lapses, and the head
    /// GET that follows fences it.
    ///
    /// The residual, closed by the skipped generation: "no longer current" assumes every write
    /// gets a new ETag, but on S3 and RustFS a single-part PUT's ETag is the MD5 of its body, so
    /// equal bytes get equal ETags. A late PUT of old writer A could land over new writer B's
    /// object when A was paused longer than the takeover quiet, between its lease check and the
    /// send of a `put_if_match` on an ETag it already held, and B's object at that key was
    /// byte-identical. So B's first seal after a takeover moves the head to
    /// `seal_target()` (G+2) and writes its pages and manifest under G+1: A only ever writes
    /// seal keys under G, so none of B's keys is one A writes or conditions on, and A's late
    /// PUTs land on keys nothing references (GC trims them once it trusts B's manifest).
    /// `late_put_cannot_replace_new_writers_page` pins it.
    fn seal_put_step(
        &self,
        start: Instant,
        key: &str,
        body: &[u8],
        free: &mut bool,
    ) -> Result<Step> {
        // The read below and the write in `set` take the lock separately; that is safe because
        // a key belongs to one worker within a round (each page index is taken once, and the
        // manifest is put alone), so no other thread changes this key's entry in between.
        let set = |k: Option<Known>| {
            let mut m = self.seal_etags.lock().unwrap();
            match k {
                Some(k) => m.insert(key.to_string(), k),
                None => m.remove(key),
            };
        };
        let known = self.seal_etags.lock().unwrap().get(key).cloned();
        let (etag, own) = match known {
            None => {
                let r = self
                    .retrying_in_lease(start, key, free, || self.store.put_if_absent(key, body))?;
                return Ok(match r {
                    None => Step::Lapsed,
                    Some(CondPut::Created { etag }) => {
                        set(Some(Known::Own(etag)));
                        Step::Done
                    }
                    Some(CondPut::PreconditionFailed) => {
                        set(Some(Known::Stale));
                        Step::Again
                    }
                });
            }
            Some(Known::Stale) => {
                let r = self.retrying_in_lease(start, key, free, || self.store.stat(key))?;
                return Ok(match r {
                    None => Step::Lapsed,
                    Some(st) => {
                        set(st.map(|st| Known::Seen(st.etag)));
                        Step::Again
                    }
                });
            }
            Some(Known::Own(etag)) => (etag, true),
            Some(Known::Seen(etag)) => (etag, false),
        };
        let r = self.retrying_in_lease(start, key, free, || {
            self.store.put_if_match(key, body, &etag)
        })?;
        Ok(match r {
            None => Step::Lapsed,
            Some(CondPut::Created { etag }) => {
                set(Some(Known::Own(etag)));
                Step::Done
            }
            Some(CondPut::PreconditionFailed) => {
                set(Some(Known::Stale));
                if own {
                    Step::Overwritten
                } else {
                    Step::Again
                }
            }
        })
    }

    /// `seal_put_step` until the object is stored (`Step::Done`), the lease refuses a request
    /// (`Step::Lapsed`) or our own object was overwritten (`Step::Overwritten`). A 412 or a
    /// stat on the way (`Step::Again`) sets `answered`.
    fn seal_put(
        &self,
        start: Instant,
        key: &str,
        body: &[u8],
        free: &mut bool,
        answered: &AtomicBool,
    ) -> Result<Step> {
        loop {
            let step = self.seal_put_step(start, key, body, free)?;
            if !matches!(step, Step::Again) {
                return Ok(step);
            }
            answered.store(true, Relaxed);
        }
    }

    /// `retrying` for a seal's requests: each attempt, retries included, is sent only inside the
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

    /// The lease lapsed during a seal, or our own object was overwritten: GET the head
    /// (`verify_head`), which renews it or fences this writer. `progressed` says whether the
    /// round since the last head check counts as progress (`round_progressed`). A lapse with
    /// none (the head check itself outlasted the lease, or the store only answered 412s and
    /// stats for `MAX_ANSWER_ONLY_ROUNDS` rounds) after the call's deadline ends the seal with
    /// `Timeout` instead, or the seal would go on forever.
    fn renew_lease(&mut self, start: Instant, progressed: bool) -> Result<()> {
        let elapsed = start.elapsed();
        if !progressed && elapsed >= self.retry.deadline {
            return Err(Error::Timeout(format!(
                "seal of generation {}: no object stored inside the lease ({:?}) since the \
                 last head check, {elapsed:?} into the seal",
                self.head.generation, self.lease
            )));
        }
        self.verify_head(start)
    }

    /// Upload the pages at indices `todo` of `pages` (generation `gen`), `concurrency` at a
    /// time, each with `seal_put`; the first error wins. The first refused request, or the
    /// first page of ours found overwritten, stops every worker, and the indices not uploaded
    /// come back as `Lapse::Remaining` for the caller to check the head and resume.
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
        let answered = AtomicBool::new(false);
        let overwritten = AtomicBool::new(false);
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
                        let obj = self.keys.seal_page(*page, gen, body);
                        match self.seal_put(start, &key, &obj, &mut free, &answered) {
                            Ok(Step::Done) => {
                                self.counters.page_puts.fetch_add(1, Relaxed);
                            }
                            Ok(step) => {
                                if let Step::Overwritten = step {
                                    overwritten.store(true, Relaxed);
                                }
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
        Ok(Lapse::Remaining {
            stored: overwritten.into_inner() || rest.len() < todo.len(),
            answered: answered.into_inner(),
            rest,
        })
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
        let body = self.keys.encode_head(head);
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
