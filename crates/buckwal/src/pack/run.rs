//! One pack run ([`pack`]), in the steps of the spec ("Packs and the packer"):
//!
//! 1. Read the head (its database id is then known), then the pack pointer.
//! 2. List the log from the pointer's pack generation and follow the chain of ended
//!    generations (`ended_generations`) up to the head's generation.
//! 3. Download the newest pack in ranges and check each block against its codes.
//! 4. Apply each ended generation with a local checkpoint, the code a restore uses
//!    (`restore::apply_images_over`: a WAL generation's `-wal`, an MVCC one's `-wal` and
//!    `app.db-log`), in `<config.dir>/pack-work-<run id>`.
//! 5. Upload the database file as `pack/<G>/<run id>` in 16 MiB parts, 8 at once.
//! 6. Check the upload: the HEAD size and the SHA-256 of each part. Then upload the codes.
//! 7. HEAD the pack and its codes, then move the pointer with `If-Match` on its ETag
//!    (`If-None-Match` for the first pointer). A 412 or an unknown result reads the pointer
//!    again and goes on as it names this run's pack, an equal or higher one, the old one, or
//!    a lower one.
//!
//! The run never writes the head or the log. It deletes its own pack and codes when it does
//! not move the pointer, but never a pack the pointer names.
use super::codes::{CodesBuilder, PackCodes, BLOCK_SIZE};
use super::pointer::{PackPointer, POINTER_FORMAT};
use crate::clock::Stamp;
use crate::config::{Config, JournalMode};
use crate::encryption::{self, Encryption};
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE};
use crate::format::{Head, Keys, LogFile, LogRecord, FORMAT_V5};
use crate::io::block_store::fetch_verified;
use crate::restore::{apply_images_over, db_facts, fetch_generation, replay, Images};
use crate::store::{
    part_size_for, sha256_of, with_retry, CondPut, ObjectStore, PartSource, RetryPolicy,
};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The size of each part of a pack's multipart upload (it grows only above 10,000 parts), and
/// of each range a run downloads.
pub(crate) const PART_SIZE: usize = 16 << 20;
/// How many parts go up at once, and how many ranges come down at once.
pub(crate) const PART_CONCURRENCY: usize = 8;
/// The most pointer writes one run sends before it gives up (`Failed` at `pointer`), when each
/// one gets a 412 or an unknown result and the pointer read after it still allows a retry.
const MAX_POINTER_WRITES: u32 = 10;

/// How a pack run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackOutcome {
    /// The pointer names the run's new pack.
    Packed,
    /// No generation ended since the newest pack: the run made no pack.
    NothingToDo,
    /// Another run moved the pointer to a pack of an equal or higher pack generation. The run
    /// deleted its own pack and codes.
    Superseded,
    /// The run stopped at `step`; `key` is the object it concerns, when there is one. The
    /// steps: `log_incomplete` (a generation below the head's does not end with a seal-start
    /// record, or the chain of generations breaks; nothing is uploaded), `download`, `upload`,
    /// `verify size`, `verify checksum`, `upload codes`, `head`, `time limit`, `pointer` (more
    /// than `MAX_POINTER_WRITES` writes, 10) and `pointer read`. The run moved no pointer, and
    /// it deleted its pack and codes when the pointer did not name them. A pack can still stay
    /// behind as an orphan, which GC deletes after the orphan grace: after `pointer read`, when
    /// the run could not make sure that the pointer does not name it, and after `upload`, when
    /// the store completed the upload but the reply was lost.
    Failed { step: String, key: Option<String> },
}

/// What one pack run did.
#[derive(Debug, Clone)]
pub struct PackReport {
    pub outcome: PackOutcome,
    /// The pack generation of the run's pack: the first generation it does not hold. With
    /// `NothingToDo`, the pointer's (0 when there is no pointer).
    pub pack_generation: u64,
    /// The key of the run's pack, once it has one.
    pub pack_key: Option<String>,
    /// Bytes downloaded and uploaded, by the store's counters.
    pub bytes_down: u64,
    pub bytes_up: u64,
    /// The time of each step the run made, in order.
    pub step_times: Vec<(&'static str, Duration)>,
    /// The share of the new pack's pages that are on the database's free list (0 without a
    /// new pack): `buckwal vacuum` gives them back.
    pub free_page_share: f64,
    pub elapsed: Duration,
}

/// A generation that a pack can hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EndedGeneration {
    pub generation: u64,
    /// The seq of its terminal record, the last one: the run applies seqs 1 to it. 0 for a
    /// skipped generation (a takeover left it with no log).
    pub last_seq_to_apply: u64,
}

/// What a scan of the log needs.
pub(crate) struct LogReader<'a> {
    pub store: &'a dyn ObjectStore,
    pub keys: &'a Keys,
    pub retry: &'a RetryPolicy,
    /// Parallel log GETs.
    pub concurrency: usize,
}

/// What a scan of the log found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Scanned {
    /// The ended and skipped generations, in order.
    pub ended: Vec<EndedGeneration>,
    /// The pack generation of a pack that holds `ended`: the next generation the last
    /// terminal record names. `None` when no generation ended.
    pub pack_generation: Option<u64>,
    /// The key where the log is not as buckwal writes it (`log_incomplete`). The run then
    /// makes no pack.
    pub incomplete: Option<String>,
}

/// The ended generations from `from` (the pack generation), following the chain of terminal
/// records (spec "Packs and the packer", rev 6).
///
/// Only a generation switch ends a generation: the writer uploads its seal-start record, then
/// moves the head. So a generation `g` below the head's has ended, and its last record must be
/// a seal-start record: the terminal record. A takeover does not end a generation, and a fence
/// record is never a terminal record. The run applies all of `g`, the records after a fence
/// included, as a restore does.
///
/// In format 5 the terminal record names the next link of the chain: the generation its switch
/// moved the head to. Every number between two links is a skipped generation, and the chain ends
/// at the head's generation. A terminal record of formats 1 to 4 names none: the next link is
/// then the next generation with a log, and each number before it must be one the head's
/// `next_generation_floor` names. The log is incomplete when a generation below the head's
/// does not end with a seal-start record, when a link is not above the one before it, or when
/// the next link has no log and is not the head's generation.
#[cfg_attr(not(test), allow(dead_code))] // the packer loop asks it before a run
pub(crate) fn ended_generations(r: &LogReader, from: u64, head: &Head) -> Result<Scanned> {
    scan_generations(r, from, head, &mut |_, _| Ok(()))
}

/// [`ended_generations`], calling `visit` with each ended generation and all its records, in
/// order, as it finds them. A log-only restore of a format 5 bucket uses it too
/// (`restore::restore_files`).
pub(crate) fn scan_generations(
    r: &LogReader,
    from: u64,
    head: &Head,
    visit: &mut dyn FnMut(EndedGeneration, &[LogRecord]) -> Result<()>,
) -> Result<Scanned> {
    let root = r.keys.log_root();
    // Every key of the generations below `from` sorts before this one.
    let after = format!("{}~", r.keys.log_prefix(from.saturating_sub(1)));
    let listed = with_retry(r.retry, &root, |_| r.store.list_after(&root, &after))?;
    let with_log: BTreeSet<u64> = listed
        .iter()
        .filter_map(|k| r.keys.parse_log(k))
        .map(|(g, _)| g)
        .filter(|g| *g >= from)
        .collect();
    let mut out = Scanned::default();
    let mut link = from;
    while link < head.generation {
        if !with_log.contains(&link) {
            out.incomplete = Some(r.keys.log_prefix(link));
            break;
        }
        let records = fetch_generation(
            r.store,
            r.keys,
            link,
            r.concurrency,
            r.retry,
            &AtomicU64::new(0),
        )?;
        let last = records.len() as u64;
        let terminal = r.keys.log(link, last);
        let named = match records.last() {
            Some(LogRecord::Extents {
                file: LogFile::Wal,
                syncs,
                next_generation,
                ..
            }) if syncs.is_empty() => *next_generation,
            _ => {
                out.incomplete = Some(terminal);
                break;
            }
        };
        let next = match named {
            Some(n) if n > link && n <= head.generation => n,
            Some(_) => {
                out.incomplete = Some(terminal);
                break;
            }
            None => match next_by_floor(link, head, &with_log) {
                Ok(n) => n,
                Err(missing) => {
                    out.incomplete = Some(r.keys.log_prefix(missing));
                    break;
                }
            },
        };
        if next != head.generation && !with_log.contains(&next) {
            out.incomplete = Some(r.keys.log_prefix(next));
            break;
        }
        let e = EndedGeneration {
            generation: link,
            last_seq_to_apply: last,
        };
        visit(e, &records)?;
        out.ended.push(e);
        out.ended
            .extend((link + 1..next).map(|generation| EndedGeneration {
                generation,
                last_seq_to_apply: 0,
            }));
        out.pack_generation = Some(next);
        link = next;
    }
    Ok(out)
}

/// The next link after `link` when its terminal record names none (formats 1 to 4): the next
/// generation with a log, or the head's. Each number before it must be a skipped generation
/// that the head's `next_generation_floor` names (ADR 0005): below the floor, with no log from
/// it up to the floor. Else the first number that is not: its log is gone.
fn next_by_floor(
    link: u64,
    head: &Head,
    with_log: &BTreeSet<u64>,
) -> std::result::Result<u64, u64> {
    let mut n = link + 1;
    while n < head.generation && !with_log.contains(&n) {
        let named = head
            .next_generation_floor
            .is_some_and(|f| n < f && with_log.range(n..f).next().is_none());
        if !named {
            return Err(n);
        }
        n += 1;
    }
    Ok(n)
}

/// Replay an ended generation's `records` as a restore of it would have: under the head it
/// had then. That head named the writer of the terminal record, because only the head's own
/// writer can move the head with `If-Match`. The current head can name a later writer, and
/// `replay`'s rule for logs without a fence record reads the head's writer id.
pub(crate) fn replay_ended(records: &[LogRecord], head: &Head, generation: u64) -> Result<Images> {
    let writer_id = match records.last() {
        Some(LogRecord::Extents { writer_id, .. }) => writer_id.clone(),
        _ => head.writer_id.clone(),
    };
    replay(
        records,
        &Head {
            generation,
            writer_id,
            ..head.clone()
        },
    )
}

/// How long ago the run's clock started; tests move it by hand.
pub(crate) type Clock = Arc<dyn Fn() -> Duration + Send + Sync>;

/// Run one pack run against the bucket `config` names. A run that finds no ended generation
/// returns `NothingToDo`, and a log that is not as buckwal writes it `Failed` at
/// `log_incomplete`, with nothing uploaded. Other errors before the upload (a record fails its
/// check, the store fails) are `Err`; once the upload started, the run cleans up and returns a
/// `Failed` outcome instead.
pub fn pack(config: &Config) -> Result<PackReport> {
    let store = config.s3_store()?;
    pack_with_store(config, store)
}

#[doc(hidden)]
pub fn pack_with_store(config: &Config, store: Arc<dyn ObjectStore>) -> Result<PackReport> {
    let origin = Stamp::now();
    pack_run(config, store, Arc::new(move || origin.age()))
}

/// [`pack_with_store`] with the clock that measures the run's time limit.
pub(crate) fn pack_run(
    config: &Config,
    store: Arc<dyn ObjectStore>,
    clock: Clock,
) -> Result<PackReport> {
    config.validate()?;
    let enc = encryption::load(config)?;
    let run_id = crate::db::new_writer_id()?;
    sweep_work_dirs(config);
    let work = WorkDir::create(config.dir.join(format!("{WORK_DIR_PREFIX}{run_id}")))?;
    let mut run = Run {
        store: &*store,
        keys: config.keys_for(enc.as_deref()),
        retry: config.engine(crate::io::IoMode::Writer).retry,
        enc: enc.as_deref(),
        clock,
        limit: config.pack_orphan_grace / 2,
        run_id,
        work: work.0.clone(),
        steps: Vec::new(),
        step_start: Instant::now(),
    };
    let start = Instant::now();
    let before = store.stats();
    let r = run.run(config);
    let delta = store.stats().since(&before);
    let (outcome, pack_generation, pack_key, free_page_share) = r?;
    let report = PackReport {
        outcome,
        pack_generation,
        pack_key,
        bytes_down: delta.bytes_down,
        bytes_up: delta.bytes_up,
        step_times: run.steps,
        free_page_share,
        elapsed: start.elapsed(),
    };
    tracing::info!(
        target: "buckwal",
        outcome = ?report.outcome,
        pack_generation = report.pack_generation,
        bytes_down = report.bytes_down,
        bytes_up = report.bytes_up,
        ms = report.elapsed.as_millis() as u64,
        "pack run finished"
    );
    Ok(report)
}

/// The run's working directory, removed when the run ends, whatever the outcome.
/// The name of a pack run's working directory in `config.dir` starts with this, then its
/// run id.
pub(crate) const WORK_DIR_PREFIX: &str = "pack-work-";

/// Remove each `pack-work-*` directory in `dir` that nothing changed for `max_age` (half the
/// orphan grace): a crashed run left it. No live run owns one so old: a run gives up at half
/// the orphan grace after its upload starts, and it adds its last entry before the upload. A
/// missing `dir` has nothing to remove. A pack run's start and a writer's open call it.
pub(crate) fn remove_stale_work_dirs(dir: &Path, max_age: Duration) -> Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io_err("list pack working dirs", e)),
    };
    for e in entries {
        let e = e.map_err(|e| io_err("list pack working dirs", e))?;
        if !e.file_name().to_string_lossy().starts_with(WORK_DIR_PREFIX) {
            continue;
        }
        let age = e
            .metadata()
            .and_then(|m| m.modified())
            .map_err(|e| io_err("read pack working dir time", e))?
            .elapsed()
            .unwrap_or_default();
        if age < max_age || !e.path().is_dir() {
            continue;
        }
        tracing::info!(
            target: "buckwal",
            dir = %e.path().display(),
            age_secs = age.as_secs(),
            "removing the working dir of a pack run that did not finish"
        );
        match fs::remove_dir_all(e.path()) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err("remove stale pack working dir", e)),
        }
    }
    Ok(())
}

/// [`remove_stale_work_dirs`] with `config`'s orphan grace; a failure only warns (the next
/// start tries again).
pub(crate) fn sweep_work_dirs(config: &Config) {
    if let Err(e) = remove_stale_work_dirs(&config.dir, config.pack_orphan_grace / 2) {
        tracing::warn!(target: "buckwal", error = %e, "cannot remove an old pack working dir");
    }
}

struct WorkDir(PathBuf);

impl WorkDir {
    fn create(path: PathBuf) -> Result<WorkDir> {
        if path.exists() {
            fs::remove_dir_all(&path).map_err(|e| io_err("remove pack working dir", e))?;
        }
        fs::create_dir_all(&path).map_err(|e| io_err("create pack working dir", e))?;
        Ok(WorkDir(path))
    }
}

impl Drop for WorkDir {
    fn drop(&mut self) {
        // A directory left behind holds only a local copy of the database.
        if let Err(e) = fs::remove_dir_all(&self.0) {
            tracing::warn!(target: "buckwal", error = %e, "pack run: cannot remove its working dir");
        }
    }
}

/// What the run returns before the report: outcome, pack generation, pack key, free share.
type Ran = (PackOutcome, u64, Option<String>, f64);

struct Run<'a> {
    store: &'a dyn ObjectStore,
    keys: Keys,
    retry: RetryPolicy,
    enc: Option<&'a Encryption>,
    clock: Clock,
    /// Half the orphan grace: the longest a run may take from its upload start to its pointer
    /// write.
    limit: Duration,
    run_id: String,
    work: PathBuf,
    steps: Vec<(&'static str, Duration)>,
    step_start: Instant,
}

/// What `pack-latest` holds, for a run.
enum Pointer {
    None,
    /// This database's pointer, and its ETag.
    Own(PackPointer, String),
    /// A pointer whose `database_id` is not the head's (spec rev 11, rule 3): no pointer for
    /// the run, which replaces it with `If-Match` on this ETag.
    Foreign(String),
}

impl Pointer {
    fn own(&self) -> Option<&PackPointer> {
        match self {
            Pointer::Own(p, _) => Some(p),
            _ => None,
        }
    }
}

/// The pack this run uploads.
struct NewPack {
    pointer: PackPointer,
    /// When the upload started, on the run's clock.
    upload_start: Duration,
}

fn failed(step: &str, key: Option<&str>) -> PackOutcome {
    PackOutcome::Failed {
        step: step.to_string(),
        key: key.map(str::to_string),
    }
}

impl Run<'_> {
    /// End the current step as `name` and start the next.
    fn step(&mut self, name: &'static str) {
        let now = Instant::now();
        self.steps.push((name, now - self.step_start));
        self.step_start = now;
    }

    fn req<T>(
        &self,
        what: &str,
        mut op: impl FnMut() -> std::result::Result<T, crate::store::StoreError>,
    ) -> Result<T> {
        with_retry(&self.retry, what, |_| op())
    }

    /// The pointer as the run sees it (`Pointer`).
    fn read_pointer(&self) -> Result<Pointer> {
        let key = self.keys.pack_pointer();
        let Some(obj) = self.req(&key, || self.store.get(&key))? else {
            return Ok(Pointer::None);
        };
        Ok(match self.keys.read_pointer(&obj.body)? {
            Some(p) => Pointer::Own(p, obj.etag),
            None => Pointer::Foreign(obj.etag),
        })
    }

    fn run(&mut self, config: &Config) -> Result<Ran> {
        // Step 1. The head first: a coded pointer is decoded only once the keys adopted the
        // head and know its database id.
        let head_key = self.keys.head();
        let head_obj = self
            .req(&head_key, || self.store.get(&head_key))?
            .ok_or_else(|| Error::NotFound(head_key.clone()))?;
        let head = self.keys.decode_head_open(&head_obj.body)?;
        if head.journal_mode == JournalMode::Mvcc && head.format != FORMAT_V5 {
            return Err(Error::Unsupported(
                "a pack run of an MVCC bucket of format 2 or 4 (it seals)",
            ));
        }
        if self.keys.coded() && self.keys.known_database_id().is_none() {
            return Err(Error::Unsupported(
                "a pack run of an encrypted bucket without a database id (format 3)",
            ));
        }
        let pointer_now = self.read_pointer()?;
        let base = pointer_now.own().cloned();
        if base.is_none() && head.journal_mode == JournalMode::Mvcc {
            // A new MVCC database is published as its first pack.
            return Err(super::first::pointer_gap_error(
                self.store,
                &self.keys,
                &self.retry,
                &head,
                &self.keys.pack_pointer(),
            ));
        }
        let from = base.as_ref().map_or(1, |p| p.pack_generation);
        self.step("read");

        // Steps 2 to 4: each ended generation, applied as the scan finds it. The newest pack
        // is downloaded before the first one.
        let reader = LogReader {
            store: self.store,
            keys: &self.keys,
            retry: &self.retry,
            concurrency: config.upload_concurrency,
        };
        let db = self.work.join(DB_FILE);
        let mut base_ready = false;
        let mut gone: Option<String> = None;
        let mut applied = 0u64;
        let (work, enc, store, keys, retry) =
            (&self.work, self.enc, self.store, &self.keys, &self.retry);
        let scanned = scan_generations(&reader, from, &head, &mut |e, records| {
            if gone.is_some() {
                return Ok(());
            }
            if !base_ready {
                base_ready = true;
                if let Some(p) = &base {
                    if !download_pack(store, keys, retry, p, &db)? {
                        gone = Some(p.pack_key.clone());
                        return Ok(());
                    }
                    check_page_size(work, enc, p)?;
                }
            }
            let images = replay_ended(records, &head, e.generation)?;
            apply_images_over(work, &images, &head, enc, None)?;
            applied += 1;
            Ok(())
        })?;
        self.step("apply");
        let current = base.as_ref().map_or(0, |p| p.pack_generation);
        if let Some(key) = scanned.incomplete {
            tracing::warn!(target: "buckwal", key = %key, "pack run: the log is incomplete");
            return Ok((failed("log_incomplete", Some(&key)), current, None, 0.0));
        }
        if let Some(key) = gone {
            // A newer pack replaced it and GC deleted it: the next run reads the new pointer.
            return Ok((failed("download", Some(&key)), current, None, 0.0));
        }
        let size = fs::metadata(&db).map_or(0, |m| m.len());
        if applied == 0 || size == 0 {
            return Ok((PackOutcome::NothingToDo, current, None, 0.0));
        }
        let pack_generation = scanned.pack_generation.expect("a generation was applied");
        let facts = db_facts(&self.work, self.enc)?;
        let free = if facts.page_count == 0 {
            0.0
        } else {
            facts.free_pages as f64 / facts.page_count as f64
        };
        let part_size = part_size_for(size, PART_SIZE);
        let pack_key = self.keys.pack(pack_generation, &self.run_id);
        let pointer = PackPointer {
            format: POINTER_FORMAT,
            codes_key: self.keys.pack_codes(pack_generation, &self.run_id),
            pack_key: pack_key.clone(),
            write_id: crate::db::new_writer_id()?,
            pack_generation,
            size,
            page_size: facts.page_size,
            part_size: part_size as u64,
            block_size: BLOCK_SIZE,
            database_id: self.keys.known_database_id(),
        };
        let outcome = self.upload_and_publish(pointer, &db, pointer_now);
        Ok((outcome?, pack_generation, Some(pack_key), free))
    }

    /// Steps 5 to 7 for the database file `db`. `base` is the pointer the run read.
    fn upload_and_publish(
        &mut self,
        pointer: PackPointer,
        db: &Path,
        base: Pointer,
    ) -> Result<PackOutcome> {
        let (pack_key, codes_key) = (pointer.pack_key.clone(), pointer.codes_key.clone());
        // Steps 5 and 6. The time limit starts here: a multipart object's Last-Modified time
        // is the upload start, and GC measures an orphan from it.
        let new = NewPack {
            upload_start: (self.clock)(),
            pointer,
        };
        let up = upload_verified(self.store, &self.keys, &self.retry, &new.pointer, db, false)?;
        let now = Instant::now();
        self.steps
            .push(("upload", up.uploaded_at - self.step_start));
        self.steps.push(("verify", now - up.uploaded_at));
        self.step_start = now;
        if let Err(f) = up.result {
            tracing::warn!(target: "buckwal", key = %f.key, step = f.step, error = %f.error, "pack upload failed");
            if f.step == "upload" {
                // The parts of a failed upload stay hidden until an abort; GC aborts them
                // after the orphan grace in any case.
                let _ = self.store.abort_incomplete(&pack_key, Duration::ZERO);
            } else {
                self.discard(&pack_key, &codes_key);
            }
            return Ok(failed(f.step, Some(&f.key)));
        }
        let outcome = self.publish(&new, base);
        self.step("publish");
        Ok(outcome)
    }

    /// Step 7 and its retries (spec "Step 7 can fail in two ways").
    fn publish(&self, new: &NewPack, base: Pointer) -> PackOutcome {
        let p = &new.pointer;
        let (pack_key, codes_key) = (p.pack_key.as_str(), p.codes_key.as_str());
        let pointer_key = self.keys.pack_pointer();
        let body = self.keys.encode_pointer(p);
        // A foreign pointer is no pointer, but it is replaced on its ETag.
        let (old_key, mut expect) = match base {
            Pointer::Own(o, etag) => (Some(o.pack_key), Some(etag)),
            Pointer::Foreign(etag) => (None, Some(etag)),
            Pointer::None => (None, None),
        };
        for _ in 0..MAX_POINTER_WRITES {
            if (self.clock)().saturating_sub(new.upload_start) >= self.limit {
                self.discard(pack_key, codes_key);
                return failed("time limit", Some(pack_key));
            }
            for key in [pack_key, codes_key] {
                if !matches!(self.req(key, || self.store.stat(key)), Ok(Some(_))) {
                    self.discard(pack_key, codes_key);
                    return failed("head", Some(key));
                }
            }
            let put = match &expect {
                None => self.req(&pointer_key, || {
                    self.store.put_if_absent(&pointer_key, &body)
                }),
                Some(etag) => self.req(&pointer_key, || {
                    self.store.put_if_match(&pointer_key, &body, etag)
                }),
            };
            if let Ok(CondPut::Created { .. }) = put {
                return PackOutcome::Packed;
            }
            // A 412, or an unknown result: the pointer says what happened.
            let now = match self.read_pointer() {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(target: "buckwal", error = %e, "pack run: cannot read the pointer");
                    return failed("pointer read", Some(&pointer_key));
                }
            };
            match now {
                Pointer::Own(now, _) if now.pack_key == pack_key => return PackOutcome::Packed,
                Pointer::Own(now, etag) if Some(&now.pack_key) == old_key.as_ref() => {
                    expect = Some(etag);
                }
                Pointer::Own(now, _) if now.pack_generation >= p.pack_generation => {
                    self.discard(pack_key, codes_key);
                    return PackOutcome::Superseded;
                }
                // A slower run moved it to a lower pack generation: this pack is the newest.
                Pointer::Own(_, etag) | Pointer::Foreign(etag) => expect = Some(etag),
                Pointer::None => expect = None,
            }
        }
        self.discard(pack_key, codes_key);
        failed("pointer", Some(&pointer_key))
    }

    /// Delete the run's pack and codes, unless the pointer names the pack or cannot be read.
    fn discard(&self, pack_key: &str, codes_key: &str) {
        match self.read_pointer() {
            Ok(Pointer::Own(p, _)) if p.pack_key == pack_key => return,
            Ok(_) => {}
            Err(_) => return,
        }
        for key in [pack_key, codes_key] {
            if let Err(e) = self.req(key, || self.store.delete(key)) {
                tracing::warn!(target: "buckwal", key, error = %e, "pack run: cannot delete its own object");
            }
        }
    }
}

/// Why an upload of a pack did not go through: the step, the key it concerns, the error.
pub(crate) struct UploadFailed {
    pub step: &'static str,
    pub key: String,
    pub error: Error,
}

/// What [`upload_verified`] did.
pub(crate) struct Uploaded {
    /// When the multipart upload returned.
    pub uploaded_at: Instant,
    /// `Err`: the step that failed. The caller cleans up (a pack run deletes its pack and
    /// codes; a first pack leaves them for GC).
    pub result: std::result::Result<(), UploadFailed>,
}

/// Steps 5 and 6 of a pack run, which a new MVCC database's first pack also makes
/// (`pack::first`): upload `db` as the pack `pointer` names, in parts of `pointer.part_size`,
/// `PART_CONCURRENCY` at once; check the size a HEAD returns and the SHA-256 of each part
/// against this process's own (a part reply without a checksum is no proof); then put the
/// codes. With `retry_upload` a failed multipart upload is sent again under the retry policy
/// (the first pack: its key is its own); a pack run gives up at once (`upload`). `Err`: the
/// codes could not be made (nothing was sent).
pub(crate) fn upload_verified(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    pointer: &PackPointer,
    db: &Path,
    retry_upload: bool,
) -> Result<Uploaded> {
    let (pack_key, codes_key) = (pointer.pack_key.as_str(), pointer.codes_key.as_str());
    let part_size = pointer.part_size as usize;
    let (part_sha256, codes) = digest(keys, pointer, db)?;
    let codes_body = codes.encode(keys, pack_key, pointer.pack_generation, pointer.size)?;
    let mut source = FileSource::open(db)?;
    let up = if retry_upload {
        with_retry(retry, pack_key, |_| {
            store.put_multipart(pack_key, &mut source, part_size, PART_CONCURRENCY)
        })
    } else {
        store
            .put_multipart(pack_key, &mut source, part_size, PART_CONCURRENCY)
            .map_err(|e| Error::UploadFailed(format!("{pack_key}: {e}")))
    };
    let uploaded_at = Instant::now();
    let fail = |step, key: &str, error| Uploaded {
        uploaded_at,
        result: Err(UploadFailed {
            step,
            key: key.to_string(),
            error,
        }),
    };
    let done = match up {
        Ok(d) => d,
        Err(e) => return Ok(fail("upload", pack_key, e)),
    };
    let size = pointer.size;
    match with_retry(retry, pack_key, |_| store.stat(pack_key)) {
        Ok(Some(s)) if s.size == size => {}
        Ok(st) => {
            let e = Error::UploadFailed(format!(
                "{pack_key}: the store holds {:?} bytes, not {size}",
                st.map(|s| s.size)
            ));
            return Ok(fail("verify size", pack_key, e));
        }
        Err(e) => return Ok(fail("verify size", pack_key, e)),
    }
    if done.part_sha256 != part_sha256 {
        let e = Error::UploadFailed(format!("{pack_key}: a part's SHA-256 is not the file's"));
        return Ok(fail("verify checksum", pack_key, e));
    }
    if let Err(e) = with_retry(retry, codes_key, |_| store.put(codes_key, &codes_body)) {
        return Ok(fail("upload codes", codes_key, e));
    }
    Ok(Uploaded {
        uploaded_at,
        result: Ok(()),
    })
}

/// The SHA-256 of each part of `db` (parts of `pointer.part_size`) and its block codes.
pub(crate) fn digest(
    keys: &Keys,
    pointer: &PackPointer,
    db: &Path,
) -> Result<(Vec<[u8; 32]>, PackCodes)> {
    let mut f = File::open(db).map_err(|e| io_err("open pack file", e))?;
    let mut builder = CodesBuilder::new(
        keys,
        &pointer.pack_key,
        pointer.pack_generation,
        pointer.size,
    )?;
    let mut shas = Vec::new();
    let mut left = pointer.size;
    let mut buf = vec![0u8; pointer.part_size as usize];
    while left > 0 {
        let n = left.min(pointer.part_size) as usize;
        f.read_exact(&mut buf[..n])
            .map_err(|e| io_err("read pack file", e))?;
        shas.push(sha256_of(&buf[..n]));
        for block in buf[..n].chunks(BLOCK_SIZE as usize) {
            builder.block(block)?;
        }
        left -= n as u64;
    }
    Ok((shas, builder.finish()?))
}

/// The page size of the downloaded pack, by the database's own header, must be the pointer's.
fn check_page_size(dir: &Path, enc: Option<&Encryption>, p: &PackPointer) -> Result<()> {
    let facts = db_facts(dir, enc)?;
    if facts.page_size != p.page_size || !p.size.is_multiple_of(u64::from(p.page_size)) {
        return Err(Error::Corrupt(format!(
            "pack pointer: page_size {} but {} holds {} bytes of {}-byte pages",
            p.page_size, p.pack_key, p.size, facts.page_size
        )));
    }
    Ok(())
}

/// Download the pack `p` names into `path`, in ranges of whole blocks, up to
/// `PART_CONCURRENCY` at once, checking each block against the pack's codes. False when the
/// pack or its codes are gone.
fn download_pack(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    p: &PackPointer,
    path: &Path,
) -> Result<bool> {
    let Some(body) = with_retry(retry, &p.codes_key, |_| store.get(&p.codes_key))? else {
        return Ok(false);
    };
    let codes = PackCodes::decode(keys, &body.body, &p.pack_key, p.pack_generation, p.size)?;
    let file = File::create(path).map_err(|e| io_err("create pack file", e))?;
    file.set_len(p.size)
        .map_err(|e| io_err("size pack file", e))?;
    let file = Mutex::new(file);
    let range = (p.part_size / BLOCK_SIZE).max(1) * BLOCK_SIZE;
    let ranges = p.size.div_ceil(range);
    let next = AtomicU64::new(0);
    let missing = AtomicBool::new(false);
    let failed: Mutex<Option<Error>> = Mutex::new(None);
    std::thread::scope(|s| {
        for _ in 0..(PART_CONCURRENCY as u64).min(ranges) {
            s.spawn(|| loop {
                if missing.load(Relaxed) || failed.lock().unwrap().is_some() {
                    return;
                }
                let i = next.fetch_add(1, Relaxed);
                if i >= ranges {
                    return;
                }
                let r = fetch_range(store, keys, retry, p, &codes, i * range, range, &file);
                match r {
                    Ok(true) => {}
                    Ok(false) => missing.store(true, Relaxed),
                    Err(e) => {
                        failed.lock().unwrap().get_or_insert(e);
                    }
                }
            });
        }
    });
    if let Some(e) = failed.into_inner().unwrap() {
        return Err(e);
    }
    if missing.load(Relaxed) {
        return Ok(false);
    }
    file.into_inner()
        .unwrap()
        .sync_all()
        .map_err(|e| io_err("fsync pack file", e))?;
    Ok(true)
}

/// One range of the pack at `offset`, checked block by block
/// (`io::block_store::fetch_verified`), written to `file`. False when the pack is gone.
#[allow(clippy::too_many_arguments)]
fn fetch_range(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    p: &PackPointer,
    codes: &PackCodes,
    offset: u64,
    len: u64,
    file: &Mutex<File>,
) -> Result<bool> {
    let Some(bytes) = fetch_verified(store, keys, retry, p, codes, offset, len)? else {
        return Ok(false);
    };
    let mut f = file.lock().unwrap();
    f.seek(SeekFrom::Start(offset))
        .and_then(|_| f.write_all(&bytes))
        .map_err(|e| io_err("write pack file", e))?;
    Ok(true)
}

/// A [`PartSource`] over a local file.
struct FileSource {
    file: Mutex<File>,
    len: u64,
}

impl FileSource {
    fn open(path: &Path) -> Result<FileSource> {
        let file = File::open(path).map_err(|e| io_err("open pack file", e))?;
        let len = file
            .metadata()
            .map_err(|e| io_err("stat pack file", e))?
            .len();
        Ok(FileSource {
            file: Mutex::new(file),
            len,
        })
    }
}

impl PartSource for FileSource {
    fn len(&self) -> u64 {
        self.len
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
        let mut f = self.file.lock().unwrap();
        f.seek(SeekFrom::Start(offset))?;
        f.read_exact(buf)
    }
}

#[cfg(test)]
mod tests;
