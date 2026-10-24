//! The engine's MVCC logical log, `app.db-log` (bucket format v2, record kind 3). Every
//! write is recorded as an op; `sync` is the commit point. It settles what the bucket owes,
//! ships the ops not yet confirmed as one kind-3 record, and only then fsyncs.
//!
//! The confirmed-sync rule (docs/mvcc-recon.md, Stage-2 design, Commit): a sync returns Ok
//! only when every op recorded so far is confirmed in the bucket. The engine keeps a commit
//! whose sync failed and chains later frames from it (Q1, P3), so ops that were never sent
//! are kept, in order, for the next sync; ops in doubt are settled by the bucket first; and
//! a sync with nothing new is no early Ok while anything is unconfirmed (a group commit's
//! follower makes exactly such a sync, Q6). A fenced writer fails every sync for good.
//!
//! Generation changes: when the bucket's generation is not the one this file last shipped
//! to (a seal adopted by `settle_log`, or a seal elsewhere), the new generation's `.db-log`
//! image is the local file as it then stands, so the whole file, from offset 0, is what the
//! next record ships (the generation-change invariant). The same happens at the first sync
//! after `ship_whole_file_next` (the open), over whatever the image held.
//!
//! The truncate of a generation change (`LogDurableStorage::truncate`, after its seal) is not
//! recorded: the new generation's image is empty, as the file then is. Only if a sync shipped
//! into the new generation first is it recorded, so the image follows the file.
//!
//! Commits are counted per engine frame: each pwrite is one commit's frame (recon Q2), except
//! a header-only write (56 bytes at offset 0). A group commit's lead writes every queued frame
//! and makes the one sync, so its call carries one ack per frame.
//!
//! It never seals (`LogDurableStorage::truncate` does). It keeps the file's high-water mark
//! (`high_water`, shared with `Engine::wal_bytes`) for the maintenance trigger, which reads it.
use super::read_at;
use super::stash::stash;
use super::IoMode;
use crate::batching::{ctx, Batching, PendingAck};
use crate::error::{Error, Result};
use crate::files::io_err;
use crate::format::{LogFile, Op};
use crate::uploader::{op_bytes, LogFailure, Uploader};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use tracing::field::Empty;
use tracing::{info_span, Span};
use turso_core::io::FileSyncType;
use turso_core::{Buffer, Completion, File, IO};

/// The engine's log header (`LOG_HDR_SIZE`): a write of exactly this at offset 0 is a header
/// rewrite, not a commit.
const LOG_HEADER: usize = 56;

/// A generation no bucket has: the image is not known to equal the local file.
const UNKNOWN: u64 = 0;

fn engine_io(e: turso_core::LimboError) -> Error {
    io_err(".db-log", e)
}

/// What the bucket has of this file. Held for a whole sync, so a second sync (a group
/// commit's follower) waits for the first and then sees what it left unconfirmed.
struct Shipped {
    /// Ops recorded but not confirmed and not in doubt, oldest first: they ship, in front
    /// of newer ones, with the next sync.
    unconfirmed: Vec<Op>,
    /// How many engine commits (frames) `unconfirmed` carries (for the stats).
    commits: u64,
    /// The generation `unconfirmed` and everything confirmed belong to; `UNKNOWN` until the
    /// first sync after `ship_whole_file_next`.
    generation: u64,
    /// Fenced for good: every later sync fails with this.
    fenced: Option<Error>,
}

pub(crate) struct LogShippingFile {
    inner: Arc<dyn File>,
    io: Arc<dyn IO>,
    uploader: Arc<Uploader>,
    /// Ops written since the last sync took them. Locked after `shipped`.
    pending: Mutex<Vec<Op>>,
    /// How many commit frames `pending` holds; changed only under `pending`'s lock.
    pending_frames: AtomicU64,
    shipped: Mutex<Shipped>,
    /// Set once a local step after a seal failed (`LogDurableStorage`): every write is
    /// refused with it until the database is reopened.
    poisoned: Mutex<Option<Error>>,
    /// A read-only open (`verify`): the engine's recovery writes and truncates go to the
    /// local file only. Nothing is recorded, shipped or sealed.
    mode: IoMode,
    /// The log's high-water mark: the highest offset + length written (the file's size at
    /// open), 0 after a `truncate(0)`. Shared with `Engine::wal_bytes`, which maintenance
    /// measures in MVCC mode (the `-wal` is near 0 there).
    high_water: Arc<AtomicU64>,
}

impl LogShippingFile {
    /// Wrap the opened `app.db-log`, whose bytes are the bucket's current generation's image
    /// (reconcile made them so at open), unless `ship_whole_file_next` says otherwise.
    pub(crate) fn new(
        inner: Arc<dyn File>,
        io: Arc<dyn IO>,
        uploader: Arc<Uploader>,
        high_water: Arc<AtomicU64>,
        mode: IoMode,
    ) -> turso_core::Result<LogShippingFile> {
        let generation = uploader.generation();
        high_water.store(inner.size()?, SeqCst);
        Ok(LogShippingFile {
            inner,
            io,
            uploader,
            pending: Mutex::new(Vec::new()),
            pending_frames: AtomicU64::new(0),
            shipped: Mutex::new(Shipped {
                unconfirmed: Vec::new(),
                commits: 0,
                generation,
                fenced: None,
            }),
            poisoned: Mutex::new(None),
            mode,
            high_water,
        })
    }

    /// The next sync ships the whole local file, from offset 0 and over whatever the
    /// current generation's image holds, before it returns Ok: the image is then the local
    /// file whatever it was. The open calls it, so a log the bucket lacks (a new database's
    /// header, a tail left by a crash) is never left out of the image.
    pub(crate) fn ship_whole_file_next(&self) {
        crate::db::lock(&self.shipped).generation = UNKNOWN;
    }

    /// Refuse every later write with `e` (the first poison sticks).
    pub(crate) fn poison(&self, e: Error) {
        crate::db::lock(&self.poisoned).get_or_insert(e);
    }

    fn refuse_if_poisoned(&self) -> turso_core::Result<()> {
        match &*crate::db::lock(&self.poisoned) {
            Some(e) => Err(stash(e.clone())),
            None => Ok(()),
        }
    }

    /// `e` failed a sync: a fence sticks, and nothing is kept to send again.
    fn failed(&self, s: &mut Shipped, e: Error) -> Error {
        if matches!(e, Error::Fenced(_)) {
            s.fenced = Some(e.clone());
            s.unconfirmed.clear();
            s.commits = 0;
            self.take_pending();
        }
        e
    }

    /// The ops written since the last sync took them, and how many commit frames they hold.
    fn take_pending(&self) -> (Vec<Op>, u64) {
        let mut p = crate::db::lock(&self.pending);
        let frames = self.pending_frames.swap(0, SeqCst);
        (std::mem::take(&mut *p), frames)
    }

    /// The commit point. Settle, then ship every unconfirmed op as one kind-3 record and
    /// wait for it. Ok only when every op recorded so far is confirmed; returns the commit
    /// span of the record it shipped (`None`: nothing to ship).
    fn ship(&self) -> Result<Option<Span>> {
        // `shipped` is held from here to the end of the sync, including the wait for the
        // upload. It closes two gaps, and must not be narrowed:
        // - between taking the ops and submitting them, a second sync (a group commit's
        //   follower) would see them in neither `pending` nor `unconfirmed`, nor in any
        //   queued record, and could return Ok at once;
        // - after a record that was never sent, the ops go back to `unconfirmed` only when
        //   the reply comes. A follower let in while this sync waits would queue its settle
        //   behind that record, pass it once the store answers, find nothing pending or
        //   unconfirmed, and be acked with its frame in no PUT
        //   (`follower_waits_for_the_leads_requeue`).
        let mut s = crate::db::lock(&self.shipped);
        if let Some(e) = &s.fenced {
            return Err(e.clone());
        }
        // 1. Settle: a record in doubt (this file's, maybe a group commit lead's) is
        //    confirmed at its seq, a published seal adopted.
        let generation = match self.uploader.settle_log() {
            Ok(g) => g,
            Err(e) => return Err(self.failed(&mut s, e)),
        };
        // The commits this sync acks: the frames written since the last sync took them.
        let mut acks = 0;
        if generation != s.generation {
            // The new generation's image is the local file as it stands: all of it ships.
            let len = self.inner.size().map_err(engine_io)?;
            let bytes = read_at(&*self.inner, &*self.io, 0, len as usize).map_err(engine_io)?;
            // Pending ops are already in those bytes; their commits still count.
            let (_, frames) = self.take_pending();
            acks += frames;
            s.commits += frames;
            s.unconfirmed.clear();
            if s.generation == UNKNOWN {
                // The image may hold anything: replace it.
                s.unconfirmed.push(Op::Truncate { len: 0 });
            }
            if !bytes.is_empty() {
                s.unconfirmed.push(Op::Write { off: 0, bytes });
            }
            s.generation = generation;
        }
        // 2. Ship what is unconfirmed, the newest ops last.
        let (new, frames) = self.take_pending();
        acks += frames;
        s.commits += frames;
        s.unconfirmed.extend(new);
        if s.unconfirmed.is_empty() {
            return Ok(None);
        }
        let ops = std::mem::take(&mut s.unconfirmed);
        let commits = std::mem::take(&mut s.commits);
        let span = info_span!(
            target: "buckwal",
            "buckwal.commit",
            buckwal.batching = %Batching::Off,
            buckwal.journal_mode = "mvcc",
            buckwal.commit_no = Empty,
            buckwal.bytes = op_bytes(&ops),
            buckwal.outcome = Empty,
        );
        let ticket = span.in_scope(|| self.uploader.submit_log_file(LogFile::Log, ops, commits));
        span.record("buckwal.commit_no", ticket.commit);
        let (commit, submitted) = (ticket.commit, ticket.submitted);
        let r = ticket.wait_in(&span);
        span.record("buckwal.outcome", if r.is_ok() { "ok" } else { "error" });
        match r {
            Ok(_) => {
                // One ack per commit the engine wrote since the last sync: under group
                // commit the lead's call carries its followers' (their calls make no sync).
                for _ in 0..acks {
                    ctx::push(PendingAck::Done {
                        commit,
                        waited: submitted.elapsed(),
                        batching: Batching::Off,
                        span: span.clone(),
                    });
                }
                Ok(Some(span))
            }
            Err(LogFailure { error, unsent }) => {
                if let Some(ops) = unsent {
                    // Never sent: back in front, for the next sync.
                    s.unconfirmed = ops;
                    s.commits = commits;
                }
                // `None`: in doubt (the bucket settles it before any other record, and the
                // next sync's settle confirms it), or fenced (`failed`).
                Err(self.failed(&mut s, error))
            }
        }
    }

    /// Record the ops of one write or truncate call once the inner call that issued them
    /// returned `Ok`: one refused synchronously never happened, so it must never be shipped.
    /// A write call is one commit's frame unless it is a header-only write.
    fn record_write(
        &self,
        ops: Vec<Op>,
        r: turso_core::Result<Completion>,
    ) -> turso_core::Result<Completion> {
        if r.is_ok() && !ops.is_empty() {
            let len: usize = ops
                .iter()
                .map(|op| match op {
                    Op::Write { bytes, .. } => bytes.len(),
                    Op::Truncate { .. } => 0,
                })
                .sum();
            let write = matches!(ops.first(), Some(Op::Write { .. }));
            let header_only =
                matches!(ops.first(), Some(Op::Write { off: 0, .. })) && len == LOG_HEADER;
            let mut p = crate::db::lock(&self.pending);
            p.extend(ops);
            if write && !header_only {
                self.pending_frames.fetch_add(1, SeqCst);
            }
        }
        r
    }
}

impl File for LogShippingFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> turso_core::Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        if self.mode != IoMode::Writer {
            return self.inner.pwrite(pos, buffer, c);
        }
        // A poisoned commit is refused at its write, so it rolls back (P3 variant C).
        self.refuse_if_poisoned()?;
        let end = pos + buffer.len() as u64;
        let op = Op::Write {
            off: pos,
            bytes: buffer.as_slice().to_vec(),
        };
        let r = self.record_write(vec![op], self.inner.pwrite(pos, buffer, c));
        if r.is_ok() {
            self.high_water.fetch_max(end, SeqCst);
        }
        r
    }
    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        if self.mode != IoMode::Writer {
            return self.inner.pwritev(pos, buffers, c);
        }
        self.refuse_if_poisoned()?;
        let mut off = pos;
        let mut ops = Vec::with_capacity(buffers.len());
        for b in &buffers {
            ops.push(Op::Write {
                off,
                bytes: b.as_slice().to_vec(),
            });
            off += b.len() as u64;
        }
        let end = off;
        let r = self.record_write(ops, self.inner.pwritev(pos, buffers, c));
        if r.is_ok() {
            self.high_water.fetch_max(end, SeqCst);
        }
        r
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        if self.mode != IoMode::Writer {
            return self.inner.sync(c, sync_type);
        }
        // Upload before any completion exists: a failure is a synchronous `Err` (the commit
        // fails, the engine keeps its frame), never a failed completion.
        match self.ship().map_err(stash)? {
            // The commit is entered around its fsync, so it is last exited after it.
            Some(commit) => commit.in_scope(|| {
                info_span!(target: "buckwal", parent: &commit, "buckwal.local_fsync")
                    .in_scope(|| self.inner.sync(c, sync_type))
            }),
            None => self.inner.sync(c, sync_type),
        }
    }
    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
    /// A truncate to 0 is a generation change's (`LogDurableStorage::truncate`, after its
    /// seal): what was pending or unconfirmed is dropped with the bytes (every frame is in
    /// the sealed pages). It is recorded only when this file already shipped into the
    /// current generation (a sync between the seal and the truncate); otherwise that
    /// generation's image is empty, as the file now is, and nothing is recorded. Any other
    /// length is recorded as an op (not expected from the engine but after a discarded
    /// write, and `reset_to_fresh_header`).
    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        if self.mode != IoMode::Writer {
            let r = self.inner.truncate(len, c);
            if r.is_ok() {
                self.high_water.store(len, SeqCst);
            }
            return r;
        }
        if len > 0 {
            let r = self.record_write(vec![Op::Truncate { len }], self.inner.truncate(len, c));
            if r.is_ok() {
                self.high_water.store(len, SeqCst);
            }
            return r;
        }
        let mut s = crate::db::lock(&self.shipped);
        let r = self.inner.truncate(0, c);
        if r.is_ok() {
            self.high_water.store(0, SeqCst);
            s.unconfirmed.clear();
            s.commits = 0;
            let mut p = crate::db::lock(&self.pending);
            p.clear();
            self.pending_frames.store(0, SeqCst);
            let g = self.uploader.generation();
            if g == s.generation {
                p.push(Op::Truncate { len: 0 });
            } else if s.generation != UNKNOWN {
                s.generation = g;
            }
            // UNKNOWN stays: the first sync ships Truncate{0} plus the whole file.
        }
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::Bucket;
    use crate::create::Image;
    use crate::format::{apply_ops, decode_record, encode_record, Keys, LogRecord};
    use crate::io::stash;
    use crate::store::fault::HoldStore;
    use crate::store::{
        CondPut, FaultStore, MemStore, Object, ObjectStat, ObjectStore, RetryPolicy, StatsSnap,
        StoreError,
    };
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
    use std::sync::mpsc;
    use std::time::Duration;
    use turso_core::io::FileSyncType;
    use turso_core::{Buffer, Completion, File, LimboError, OpenFlags, IO};

    const HEADER: usize = 56;

    fn retry(ms: u64) -> RetryPolicy {
        RetryPolicy {
            deadline: Duration::from_millis(ms),
            first_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(4),
        }
    }

    fn keys() -> Keys {
        Keys::new("db")
    }

    /// Fails every GET of a key containing "/head" once `pass` more have passed, while armed.
    /// An armed `GetGate::block_one_head_get`.
    type Block = (u64, mpsc::Sender<()>, mpsc::Receiver<()>);

    struct GetGate {
        inner: Arc<dyn ObjectStore>,
        armed: AtomicBool,
        pass: AtomicU64,
        /// `block_one_head_get`: (head GETs still to pass, where to say it is blocked, what
        /// releases it).
        block: Mutex<Option<Block>>,
    }

    impl GetGate {
        /// Let `pass` head GETs through, then block the next one until the returned sender
        /// is used (the first receiver hears when it blocks) and fail it once; later head
        /// GETs pass.
        fn block_one_head_get(&self, pass: u64) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
            let (held_tx, held_rx) = mpsc::channel();
            let (go_tx, go_rx) = mpsc::channel();
            *self.block.lock().unwrap() = Some((pass, held_tx, go_rx));
            (held_rx, go_tx)
        }
        fn fail_head_gets_after(&self, pass: u64) {
            self.pass.store(pass, SeqCst);
            self.armed.store(true, SeqCst);
        }
        fn heal(&self) {
            self.armed.store(false, SeqCst);
        }
    }

    impl ObjectStore for GetGate {
        fn put(&self, key: &str, body: &[u8]) -> std::result::Result<String, StoreError> {
            self.inner.put(key, body)
        }
        fn put_if_absent(
            &self,
            key: &str,
            body: &[u8],
        ) -> std::result::Result<CondPut, StoreError> {
            self.inner.put_if_absent(key, body)
        }
        fn put_if_match(
            &self,
            key: &str,
            body: &[u8],
            etag: &str,
        ) -> std::result::Result<CondPut, StoreError> {
            self.inner.put_if_match(key, body, etag)
        }
        fn get(&self, key: &str) -> std::result::Result<Option<Object>, StoreError> {
            if key.contains("/head") {
                let blocked = {
                    let mut b = self.block.lock().unwrap();
                    match b.as_mut() {
                        Some((0, _, _)) => b.take(),
                        Some((n, _, _)) => {
                            *n -= 1;
                            None
                        }
                        None => None,
                    }
                };
                if let Some((_, held_tx, go_rx)) = blocked {
                    let _ = held_tx.send(());
                    let _ = go_rx.recv();
                    return Err(StoreError::Transient(format!("injected get failure {key}")));
                }
            }
            if key.contains("/head")
                && self.armed.load(SeqCst)
                && self
                    .pass
                    .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                    .is_err()
            {
                return Err(StoreError::Transient(format!("injected get failure {key}")));
            }
            self.inner.get(key)
        }
        fn stat(&self, key: &str) -> std::result::Result<Option<ObjectStat>, StoreError> {
            self.inner.stat(key)
        }
        fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StoreError> {
            self.inner.list(prefix)
        }
        fn delete(&self, key: &str) -> std::result::Result<(), StoreError> {
            self.inner.delete(key)
        }
        fn stats(&self) -> StatsSnap {
            self.inner.stats()
        }
    }

    /// An MVCC bucket (generation 2, as `create_sealed` leaves it) behind `fault` (over
    /// `gate`, over `mem`), with `hold` in front of everything; its uploader; a local
    /// `app.db-log`.
    struct Rig {
        mem: Arc<MemStore>,
        gate: Arc<GetGate>,
        fault: Arc<FaultStore>,
        hold: Arc<HoldStore>,
        up: Arc<Uploader>,
        io: Arc<dyn IO>,
        local: Arc<dyn File>,
        _dir: tempfile::TempDir,
    }

    fn rig_with(deadline_ms: u64, lease: Option<Duration>) -> Rig {
        rig_build(deadline_ms, lease, false)
    }

    /// `rig`, after a takeover: writer w2 took the head over from the creating writer w1 and
    /// appended its fence record, so its first seal skips a generation.
    fn rig_taken_over() -> Rig {
        rig_build(2_000, None, true)
    }

    fn rig_build(deadline_ms: u64, lease: Option<Duration>, taken_over: bool) -> Rig {
        let mem = Arc::new(MemStore::new());
        let gate = Arc::new(GetGate {
            inner: mem.clone(),
            armed: AtomicBool::new(false),
            pass: AtomicU64::new(0),
            block: Mutex::new(None),
        });
        let fault = Arc::new(FaultStore::new(gate.clone()));
        let hold = Arc::new(HoldStore::new(fault.clone()));
        let image = Image {
            page_size: 4096,
            pages: vec![(1, vec![0u8; 4096])],
        };
        let mut b = Bucket::create_sealed(
            hold.clone(),
            keys(),
            retry(deadline_ms),
            4,
            "w1".into(),
            image,
        )
        .unwrap();
        if taken_over {
            crate::bucket::take_over(&*hold, &keys(), &retry(deadline_ms), 2, "w2").unwrap();
            b = Bucket::attach(hold.clone(), keys(), retry(deadline_ms), 4, "w2", 2).unwrap();
            b.append_fence().unwrap();
        }
        if let Some(l) = lease {
            b.set_lease(l);
        }
        let up = Uploader::spawn(b, None, None, Arc::default());
        let io: Arc<dyn IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.db-log");
        let local = io
            .open_file(path.to_str().unwrap(), OpenFlags::Create, false)
            .unwrap();
        Rig {
            mem,
            gate,
            fault,
            hold,
            up,
            io,
            local,
            _dir: dir,
        }
    }

    fn rig() -> Rig {
        rig_with(2_000, None)
    }

    impl Rig {
        fn file(&self) -> LogShippingFile {
            self.file_over(self.local.clone(), IoMode::Writer)
        }
        fn file_over(&self, inner: Arc<dyn File>, mode: IoMode) -> LogShippingFile {
            LogShippingFile::new(
                inner,
                self.io.clone(),
                self.up.clone(),
                Arc::default(),
                mode,
            )
            .unwrap()
        }
        /// The records of `log/<gen>` in seq order, decoded.
        fn records(&self, gen: u64) -> Vec<LogRecord> {
            (1..)
                .map_while(|seq| self.mem.get(&keys().log(gen, seq)).unwrap())
                .map(|o| decode_record(&o.body).unwrap())
                .collect()
        }
        /// The `.db-log` image `log/<gen>`'s kind-3 records make, replayed with `apply_ops`.
        fn image(&self, gen: u64) -> Vec<u8> {
            let mut img = Vec::new();
            for r in self.records(gen) {
                if let LogRecord::Extents {
                    file: LogFile::Log,
                    syncs,
                    ..
                } = r
                {
                    for ops in syncs {
                        apply_ops(&mut img, &ops).unwrap();
                    }
                }
            }
            img
        }
        /// The local file's bytes.
        fn local_bytes(&self) -> Vec<u8> {
            let len = self.local.size().unwrap() as usize;
            read_at(&*self.local, &*self.io, 0, len).unwrap()
        }
        fn heal(&self) {
            self.fault.fail_next_puts(0);
            self.fault.store_then_fail_next_puts(0);
            self.fault.fail_puts_after(u64::MAX);
            self.gate.heal();
        }
    }

    /// The kinds of `records`: 1 (`-wal` extents), 2 (fence), 3 (`.db-log` extents).
    fn kinds(records: &[LogRecord]) -> Vec<u8> {
        records
            .iter()
            .map(|r| match r {
                LogRecord::Extents {
                    file: LogFile::Wal, ..
                } => 1,
                LogRecord::Fence { .. } => 2,
                LogRecord::Extents {
                    file: LogFile::Log, ..
                } => 3,
            })
            .collect()
    }

    fn write(f: &dyn File, io: &dyn IO, off: u64, bytes: Vec<u8>) -> turso_core::Result<()> {
        let c = f.pwrite(
            off,
            Arc::new(Buffer::new(bytes)),
            Completion::new_write(|_| {}),
        )?;
        io.wait_for_completion(c)
    }

    /// A sync; its synchronous `Err` comes back as the typed error it stashed.
    fn sync(f: &dyn File, io: &dyn IO) -> Result<()> {
        stash::clear();
        match f.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync) {
            Ok(c) => {
                io.wait_for_completion(c).unwrap();
                Ok(())
            }
            Err(e) => Err(stash::take().unwrap_or_else(|| panic!("nothing stashed for {e}"))),
        }
    }

    /// A frame of `n` bytes of `tag`.
    fn frame(tag: u8, n: usize) -> Vec<u8> {
        vec![tag; n]
    }

    /// Delegates to a real file; at each sync records how many log objects the bucket holds.
    struct Spy {
        inner: Arc<dyn File>,
        mem: Arc<MemStore>,
        syncs: Mutex<Vec<usize>>,
    }

    impl File for Spy {
        fn lock_file(&self, e: bool) -> turso_core::Result<()> {
            self.inner.lock_file(e)
        }
        fn unlock_file(&self) -> turso_core::Result<()> {
            self.inner.unlock_file()
        }
        fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
            self.inner.pread(pos, c)
        }
        fn pwrite(
            &self,
            pos: u64,
            b: Arc<Buffer>,
            c: Completion,
        ) -> turso_core::Result<Completion> {
            self.inner.pwrite(pos, b, c)
        }
        fn pwritev(
            &self,
            pos: u64,
            b: Vec<Arc<Buffer>>,
            c: Completion,
        ) -> turso_core::Result<Completion> {
            self.inner.pwritev(pos, b, c)
        }
        fn sync(&self, c: Completion, t: FileSyncType) -> turso_core::Result<Completion> {
            let n = self.mem.list(&keys().log_prefix(2)).unwrap().len();
            self.syncs.lock().unwrap().push(n);
            self.inner.sync(c, t)
        }
        fn size(&self) -> turso_core::Result<u64> {
            self.inner.size()
        }
        fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
            self.inner.truncate(len, c)
        }
    }

    /// The upload comes before the local fsync, and the sync returns only once the record is
    /// in the bucket: a kind-3 record whose replay is the local file.
    #[test]
    fn sync_ships_ops_before_fsync_and_acks_after() {
        let r = rig();
        let spy = Arc::new(Spy {
            inner: r.local.clone(),
            mem: r.mem.clone(),
            syncs: Mutex::default(),
        });
        let f = r.file_over(spy.clone(), IoMode::Writer);
        write(&f, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        write(&f, &*r.io, HEADER as u64, frame(1, 55)).unwrap();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(*spy.syncs.lock().unwrap(), [1], "uploaded before the fsync");
        assert_eq!(kinds(&r.records(2)), [3]);
        assert_eq!(r.image(2), r.local_bytes());

        // pwritev: one op per buffer, at consecutive offsets.
        let c = f
            .pwritev(
                (HEADER + 55) as u64,
                vec![
                    Arc::new(Buffer::new(frame(2, 30))),
                    Arc::new(Buffer::new(frame(3, 20))),
                ],
                Completion::new_write(|_| {}),
            )
            .unwrap();
        r.io.wait_for_completion(c).unwrap();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(*spy.syncs.lock().unwrap(), [1, 2]);
        assert_eq!(kinds(&r.records(2)), [3, 3]);
        assert_eq!(r.image(2), r.local_bytes());

        // Nothing written, nothing unconfirmed: no PUT, still fsynced.
        let puts = r.fault.events().len();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(r.fault.events().len(), puts, "an empty sync sends nothing");
        assert_eq!(*spy.syncs.lock().unwrap(), [1, 2, 2]);
        let stats = r.up.stats().snapshot();
        assert_eq!((stats.log_puts, stats.commits_uploaded), (2, 2));
    }

    /// A record that was never sent (its append failed before the PUT): its ops go back to
    /// the front, and the next sync ships them with the newer ones in one record (P3: the
    /// engine keeps the frame and chains from it, so dropping it would lose later acked
    /// commits).
    #[test]
    fn failed_put_requeues_unsent_ops_and_next_sync_ships_them() {
        // A zero lease: the settle and the append each GET the head first.
        let r = rig_with(100, Some(Duration::ZERO));
        let f = r.file();
        write(&f, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        write(&f, &*r.io, HEADER as u64, frame(1, 55)).unwrap();
        // The settle's head GET passes, the append's fails: the record is never sent.
        r.gate.fail_head_gets_after(1);
        assert!(matches!(
            sync(&f, &*r.io),
            Err(Error::Timeout(_) | Error::UploadFailed(_))
        ));
        assert!(r.records(2).is_empty(), "never sent");
        assert!(!r.fault.events().iter().any(|e| e.contains("/log/")));

        // A later commit, a failed sync again (the settle fails now).
        write(&f, &*r.io, (HEADER + 55) as u64, frame(2, 49)).unwrap();
        r.gate.fail_head_gets_after(0);
        assert!(sync(&f, &*r.io).is_err());
        assert!(r.records(2).is_empty());

        r.heal();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        let recs = r.records(2);
        assert_eq!(kinds(&recs), [3], "one record carries both commits");
        let LogRecord::Extents { syncs, .. } = &recs[0] else {
            unreachable!()
        };
        let offs: Vec<u64> = syncs
            .iter()
            .flatten()
            .map(|op| match op {
                Op::Write { off, .. } => *off,
                Op::Truncate { len } => panic!("truncate to {len}"),
            })
            .collect();
        assert_eq!(
            offs,
            [0, HEADER as u64, (HEADER + 55) as u64],
            "in write order"
        );
        assert_eq!(r.image(2), r.local_bytes());
    }

    /// A PUT that stored but whose every reply was an error: the record is in doubt, the sync
    /// fails, and so does every sync until the bucket settles it at its seq. Then Ok, with the
    /// record in the bucket once. A PUT that never stored is settled the same way.
    #[test]
    fn in_doubt_put_is_settled_before_ok() {
        for stored in [true, false] {
            let r = rig_with(100, None);
            let f = r.file();
            write(&f, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
            write(&f, &*r.io, HEADER as u64, frame(1, 55)).unwrap();
            r.fault.only_keys_containing("/log/");
            if stored {
                r.fault.store_then_fail_next_puts(1);
            }
            r.fault.fail_puts_after(0);
            assert!(sync(&f, &*r.io).is_err(), "stored={stored}");
            assert_eq!(r.records(2).len(), usize::from(stored));
            // Still failing: the empty sync must settle first, and cannot.
            assert!(sync(&f, &*r.io).is_err(), "stored={stored}: acked in doubt");
            r.heal();
            assert_eq!(sync(&f, &*r.io), Ok(()), "stored={stored}");
            assert_eq!(kinds(&r.records(2)), [3], "stored={stored}: settled once");
            assert_eq!(r.image(2), r.local_bytes());
            // The next commit lands after it.
            write(&f, &*r.io, (HEADER + 55) as u64, frame(2, 49)).unwrap();
            assert_eq!(sync(&f, &*r.io), Ok(()));
            assert_eq!(kinds(&r.records(2)), [3, 3]);
            assert_eq!(r.image(2), r.local_bytes());
        }
    }

    /// Review Focus 1. Under group commit the lead writes every queued frame, the follower's
    /// among them, and syncs; the follower then makes its own sync. The lead's PUT stores and
    /// then errors. The follower's sync, started while the lead's upload is in flight, waits
    /// for it and then must fail unless its record is confirmed in the bucket: Err while the
    /// bucket cannot settle it, Ok once it can (the record is there), Fenced when another
    /// writer holds the seq.
    #[test]
    fn follower_not_acked_when_its_record_is_unconfirmed() {
        for case in ["settled", "taken over"] {
            let r = rig_with(100, None);
            let f = Arc::new(r.file());
            write(&*f, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
            write(&*f, &*r.io, HEADER as u64, frame(1, 55)).unwrap(); // the lead's
            write(&*f, &*r.io, (HEADER + 55) as u64, frame(2, 49)).unwrap(); // the follower's
            r.fault.only_keys_containing("/log/");
            r.fault.store_then_fail_next_puts(1);
            r.fault.fail_puts_after(0);
            let (held, go) = r.hold.hold_next_put("/log/");

            let lead = {
                let (f, io) = (f.clone(), r.io.clone());
                std::thread::spawn(move || sync(&*f, &*io))
            };
            held.recv().unwrap();
            let (done_tx, done_rx) = mpsc::channel();
            let follower = {
                let (f, io) = (f.clone(), r.io.clone());
                std::thread::spawn(move || {
                    let r = sync(&*f, &*io);
                    done_tx.send(()).unwrap();
                    r
                })
            };
            assert!(
                done_rx.recv_timeout(Duration::from_millis(200)).is_err(),
                "{case}: the follower returned while the lead's record was unsent"
            );
            go.send(()).unwrap();
            assert!(lead.join().unwrap().is_err(), "{case}");
            assert!(
                follower.join().unwrap().is_err(),
                "{case}: the follower was acked with the record in doubt"
            );
            assert_eq!(r.records(2).len(), 1, "{case}: stored, reply lost");

            match case {
                "settled" => {
                    r.heal();
                    assert_eq!(sync(&*f, &*r.io), Ok(()));
                    assert_eq!(kinds(&r.records(2)), [3]);
                    assert_eq!(r.image(2), r.local_bytes(), "the follower's frame is in");
                }
                _ => {
                    // Another writer took the seq before anything settled it.
                    let foreign = encode_record(&LogRecord::Extents {
                        file: LogFile::Log,
                        writer_id: "w2".into(),
                        syncs: vec![vec![Op::Write {
                            off: 0,
                            bytes: vec![9; 8],
                        }]],
                    });
                    r.mem.put(&keys().log(2, 1), &foreign).unwrap();
                    r.heal();
                    assert!(matches!(sync(&*f, &*r.io), Err(Error::Fenced(_))));
                }
            }
        }
    }

    /// The lock a sync holds from its settle to its requeue. The lead's record is never sent
    /// (with a zero lease the append's head GET fails, so no PUT), and the follower's sync
    /// starts while the lead waits for it. If the follower could run its sync then, its
    /// settle (queued behind the lead's record) would pass once the store answers, find
    /// `pending` taken by the lead and `unconfirmed` not yet refilled, and return Ok with its
    /// frame in no PUT. A follower's Ok must mean its frame is in the bucket.
    #[test]
    fn follower_waits_for_the_leads_requeue() {
        let r = rig_with(100, Some(Duration::ZERO));
        let f = Arc::new(r.file());
        write(&*f, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        write(&*f, &*r.io, HEADER as u64, frame(1, 55)).unwrap(); // the lead's
        write(&*f, &*r.io, (HEADER + 55) as u64, frame(2, 49)).unwrap(); // the follower's
                                                                         // The lead's settle GETs the head (passes); its append's GET blocks, then fails once.
        let (held, go) = r.gate.block_one_head_get(1);
        let lead = {
            let (f, io) = (f.clone(), r.io.clone());
            std::thread::spawn(move || sync(&*f, &*io))
        };
        held.recv().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let follower = {
            let (f, io) = (f.clone(), r.io.clone());
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                sync(&*f, &*io)
            })
        };
        started_rx.recv().unwrap();
        // Time for the follower to queue its settle, were it let in.
        std::thread::sleep(Duration::from_millis(150));
        go.send(()).unwrap();
        assert!(
            matches!(lead.join().unwrap(), Err(Error::Timeout(_))),
            "the lead's record was never sent"
        );
        let followed = follower.join().unwrap();
        if followed.is_ok() {
            assert_eq!(
                kinds(&r.records(2)),
                [3],
                "the follower was acked with no PUT"
            );
            assert_eq!(
                r.image(2),
                r.local_bytes(),
                "the follower was acked without its frame in the bucket"
            );
        }
        assert_eq!(sync(&*f, &*r.io), Ok(()));
        assert_eq!(r.image(2), r.local_bytes());
    }

    /// A sync with nothing new while earlier ops are unconfirmed (never sent) is no early Ok:
    /// it ships them, or fails.
    #[test]
    fn empty_sync_with_unconfirmed_ops_does_not_ack() {
        let r = rig_with(100, Some(Duration::ZERO));
        let f = r.file();
        write(&f, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        write(&f, &*r.io, HEADER as u64, frame(1, 55)).unwrap();
        r.gate.fail_head_gets_after(1);
        assert!(sync(&f, &*r.io).is_err());
        // Nothing new; the settle passes, the append does not.
        r.gate.fail_head_gets_after(1);
        assert!(
            sync(&f, &*r.io).is_err(),
            "an empty sync acked unconfirmed ops"
        );
        assert!(r.records(2).is_empty());
        r.heal();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(kinds(&r.records(2)), [3]);
        assert_eq!(r.image(2), r.local_bytes());
    }

    /// A 412 with another writer's bytes fences for good: the sync fails, nothing is
    /// requeued, and every later sync fails without sending anything.
    #[test]
    fn fenced_sync_fails_forever_and_requeues_nothing() {
        let r = rig();
        let f = r.file();
        let foreign = encode_record(&LogRecord::Fence {
            writer_id: "w2".into(),
            epoch: 9,
        });
        r.mem.put(&keys().log(2, 1), &foreign).unwrap();
        write(&f, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        assert!(matches!(sync(&f, &*r.io), Err(Error::Fenced(_))));
        {
            let s = crate::db::lock(&f.shipped);
            assert!(s.unconfirmed.is_empty(), "requeued after a fence");
        }
        assert!(f.pending.lock().unwrap().is_empty());
        let sent = r.fault.events().len();
        write(&f, &*r.io, HEADER as u64, frame(1, 55)).unwrap();
        assert!(matches!(sync(&f, &*r.io), Err(Error::Fenced(_))));
        assert!(matches!(sync(&f, &*r.io), Err(Error::Fenced(_))));
        assert_eq!(
            r.fault.events().len(),
            sent,
            "a fenced writer sends nothing"
        );
        assert_eq!(kinds(&r.records(2)), [2], "only the other writer's record");
    }

    /// A `.db-log` whose every write and truncate fails synchronously.
    struct Refusing;

    fn refused() -> LimboError {
        LimboError::InternalError("refused".into())
    }

    impl File for Refusing {
        fn lock_file(&self, _: bool) -> turso_core::Result<()> {
            Ok(())
        }
        fn unlock_file(&self) -> turso_core::Result<()> {
            Ok(())
        }
        fn pread(&self, _: u64, _: Completion) -> turso_core::Result<Completion> {
            Err(refused())
        }
        fn pwrite(&self, _: u64, _: Arc<Buffer>, _: Completion) -> turso_core::Result<Completion> {
            Err(refused())
        }
        fn pwritev(
            &self,
            _: u64,
            _: Vec<Arc<Buffer>>,
            _: Completion,
        ) -> turso_core::Result<Completion> {
            Err(refused())
        }
        fn sync(&self, _: Completion, _: FileSyncType) -> turso_core::Result<Completion> {
            Err(refused())
        }
        fn size(&self) -> turso_core::Result<u64> {
            Ok(0)
        }
        fn truncate(&self, _: u64, _: Completion) -> turso_core::Result<Completion> {
            Err(refused())
        }
    }

    /// A write or truncate the inner file refused synchronously never happened: no op is
    /// left for the next sync to ship.
    #[test]
    fn pwrite_refused_by_inner_records_no_op() {
        let r = rig();
        let f = r.file_over(Arc::new(Refusing), IoMode::Writer);
        let buf = || Arc::new(Buffer::new(vec![7u8; 64]));
        assert!(f.pwrite(56, buf(), Completion::new_write(|_| {})).is_err());
        assert!(f
            .pwritev(56, vec![buf(), buf()], Completion::new_write(|_| {}))
            .is_err());
        assert!(f.truncate(4096, Completion::new_trunc(|_| {})).is_err());
        assert!(f.truncate(0, Completion::new_trunc(|_| {})).is_err());
        assert_eq!(f.pending.lock().unwrap().len(), 0, "phantom ops recorded");
    }

    /// A read-only open (`verify`) lets the engine's recovery writes and truncates reach the
    /// local file, and never records, ships or seals them.
    #[test]
    fn read_only_writes_stay_local_and_ship_nothing() {
        let r = rig();
        write(&*r.local, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        let f = r.file_over(r.local.clone(), IoMode::ReadOnly);
        let buf = || Arc::new(Buffer::new(vec![1u8; 8]));
        drop(f.pwrite(0, buf(), Completion::new_write(|_| {})).unwrap());
        let mut want = frame(0xAA, HEADER);
        want[..8].copy_from_slice(&[1u8; 8]);
        assert_eq!(r.local_bytes(), want, "the write reached the local file");
        drop(
            f.pwritev(8, vec![buf(), buf()], Completion::new_write(|_| {}))
                .unwrap(),
        );
        want[8..24].copy_from_slice(&[1u8; 16]);
        assert_eq!(r.local_bytes(), want, "so did the vectored write");
        drop(f.truncate(8, Completion::new_trunc(|_| {})).unwrap());
        assert_eq!(r.local_bytes(), vec![1u8; 8], "so did the truncate");
        drop(f.truncate(0, Completion::new_trunc(|_| {})).unwrap());
        assert!(r.local_bytes().is_empty());
        assert_eq!(f.pending.lock().unwrap().len(), 0, "nothing recorded");
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert!(r.records(2).is_empty(), "a read-only sync ships nothing");
    }

    /// A replica's log writes and truncates reach the local file; nothing is recorded or
    /// shipped.
    #[test]
    fn replica_mode_refuses_log_shipping() {
        let r = rig();
        write(&*r.local, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        let f = r.file_over(r.local.clone(), IoMode::Replica);
        let buf = || Arc::new(Buffer::new(vec![1u8; 8]));
        drop(f.pwrite(0, buf(), Completion::new_write(|_| {})).unwrap());
        drop(f.truncate(0, Completion::new_trunc(|_| {})).unwrap());
        assert!(r.local_bytes().is_empty());
        assert_eq!(f.pending.lock().unwrap().len(), 0, "nothing recorded");
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert!(r.records(2).is_empty(), "a replica sync ships nothing");
    }

    /// A seal's head PUT stored and its reply was lost: generation 3 is published. The next
    /// sync adopts it before it appends, is acked, and ships the whole local log, from offset
    /// 0 with its header, as the first kind-3 record of generation 3 (the generation-change
    /// invariant). Later commits land after it.
    #[test]
    fn sync_adopts_published_pending_seal_before_append() {
        let r = rig_with(100, None);
        let f = r.file();
        write(&f, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        write(&f, &*r.io, HEADER as u64, frame(1, 55)).unwrap();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        // A commit written, not yet synced, when the seal runs.
        write(&f, &*r.io, (HEADER + 55) as u64, frame(2, 49)).unwrap();

        r.fault.only_keys_containing("/head");
        r.fault.store_then_fail_next_puts(1);
        r.fault.fail_puts_after(0);
        assert!(r
            .up
            .seal_generation(vec![(1, vec![5u8; 4096])], 4096, 1)
            .is_err());
        let head =
            crate::format::Head::decode(&r.mem.get(&keys().head()).unwrap().unwrap().body).unwrap();
        assert_eq!(head.generation, 3, "the head PUT stored");
        assert_eq!(r.up.generation(), 2, "its reply was lost");
        r.heal();

        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(r.up.generation(), 3, "adopted");
        assert_eq!(
            kinds(&r.records(2)),
            [3, 1],
            "a record, then the seal-start record"
        );
        let recs = r.records(3);
        assert_eq!(kinds(&recs), [3]);
        assert_eq!(
            r.image(3),
            r.local_bytes(),
            "the whole local log, header included"
        );

        write(&f, &*r.io, (HEADER + 104) as u64, frame(3, 40)).unwrap();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(
            kinds(&r.records(3)),
            [3, 3],
            "the next append lands after it"
        );
        assert_eq!(r.image(3), r.local_bytes());
    }

    /// A takeover's seal moves the head to G+2 with its objects under G+1 (`create_sealed`
    /// leaves G = 2): nothing is written under G, no `log/<G+1>` exists, and the kind-3 records
    /// of the new generation start in `log/<G+2>`.
    #[test]
    fn mvcc_takeover_seal_lands_on_g_plus_2() {
        let r = rig_taken_over();
        let f = r.file();
        write(&f, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        write(&f, &*r.io, HEADER as u64, frame(1, 55)).unwrap();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(kinds(&r.records(2)), [2, 3], "the fence, then the record");
        assert_eq!(
            r.up.seal_generation(vec![(1, vec![5u8; 4096])], 4096, 1),
            Ok(4)
        );
        let head =
            crate::format::Head::decode(&r.mem.get(&keys().head()).unwrap().unwrap().body).unwrap();
        assert_eq!(
            (head.generation, head.manifest, head.next_generation_floor),
            (4, Some(3), Some(4))
        );
        assert!(r.mem.get(&keys().page(3, 1)).unwrap().is_some());
        assert!(r.mem.get(&keys().manifest(3)).unwrap().is_some());
        assert!(
            r.mem.get(&keys().page(2, 1)).unwrap().is_none(),
            "nothing under G"
        );
        assert!(
            r.mem.get(&keys().manifest(2)).unwrap().is_none(),
            "nothing under G"
        );
        assert!(
            r.records(3).is_empty(),
            "no log under the skipped generation"
        );
        assert_eq!(r.up.generation(), 4);
        write(&f, &*r.io, (HEADER + 55) as u64, frame(2, 49)).unwrap();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(
            kinds(&r.records(4)),
            [3],
            "kind 3 records start in log/<G+2>"
        );
        assert_eq!(r.image(4), r.local_bytes());
    }

    /// The same with the seal's head reply lost: G+2 is published, and the next sync adopts it
    /// (it names this writer) before it appends, shipping the whole local log into `log/<G+2>`.
    #[test]
    fn mvcc_seal_with_lost_head_reply_adopts_g_plus_2() {
        let r = rig_taken_over();
        let f = r.file();
        write(&f, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        write(&f, &*r.io, HEADER as u64, frame(1, 55)).unwrap();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        write(&f, &*r.io, (HEADER + 55) as u64, frame(2, 49)).unwrap();

        r.fault.only_keys_containing("/head");
        r.fault.store_then_fail_next_puts(1);
        r.fault.fail_puts_after(0);
        assert!(r
            .up
            .seal_generation(vec![(1, vec![5u8; 4096])], 4096, 1)
            .is_err());
        let head =
            crate::format::Head::decode(&r.mem.get(&keys().head()).unwrap().unwrap().body).unwrap();
        assert_eq!(
            (head.generation, head.manifest),
            (4, Some(3)),
            "the head PUT stored"
        );
        assert_eq!(r.up.generation(), 2, "its reply was lost");
        r.heal();

        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(r.up.generation(), 4, "adopted");
        assert!(
            r.records(3).is_empty(),
            "no log under the skipped generation"
        );
        let recs = r.records(4);
        assert_eq!(kinds(&recs), [3]);
        assert_eq!(r.image(4), r.local_bytes(), "the whole local log");
    }

    fn trunc(f: &dyn File, io: &dyn IO, len: u64) {
        let c = f.truncate(len, Completion::new_trunc(|_| {})).unwrap();
        io.wait_for_completion(c).unwrap();
    }

    /// A generation change's `truncate(0)`: right after a seal it is not recorded (the new
    /// generation's image is empty, as the file now is), and the next sync ships nothing. If a
    /// sync shipped into the new generation before the truncate (the engine's checkpoint lock
    /// keeps commits out; this is the backstop), the truncate is recorded and shipped, so the
    /// new generation's image is still the local file.
    #[test]
    fn truncate_after_a_seal_is_recorded_only_if_the_new_generation_holds_bytes() {
        let r = rig();
        let f = r.file();
        write(&f, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        write(&f, &*r.io, HEADER as u64, frame(1, 55)).unwrap();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        let page = || vec![(1, vec![0u8; 4096])];

        // The normal path: the seal, then the truncate.
        assert_eq!(r.up.seal_generation(page(), 4096, 1), Ok(3));
        trunc(&f, &*r.io, 0);
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert!(r.records(3).is_empty(), "nothing shipped into generation 3");
        write(&f, &*r.io, 0, frame(0xBB, HEADER + 30)).unwrap();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(kinds(&r.records(3)), [3]);
        assert_eq!(r.image(3), r.local_bytes());

        // A sync between the seal and the truncate ships the whole file into generation 4;
        // the truncate after it is shipped too.
        assert_eq!(r.up.seal_generation(page(), 4096, 1), Ok(4));
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(r.image(4), r.local_bytes());
        assert!(!r.local_bytes().is_empty());
        trunc(&f, &*r.io, 0);
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(kinds(&r.records(4)), [3, 3]);
        assert_eq!(r.image(4), Vec::<u8>::new());
        assert_eq!(r.local_bytes(), Vec::<u8>::new());
    }

    /// `ship_whole_file_next` (the open): the next sync ships the whole local file from
    /// offset 0, over whatever the generation's image held, so the image is the local file.
    #[test]
    fn ship_whole_file_next_makes_the_image_the_local_file() {
        let r = rig();
        // The local file already holds a header (a new database's) the bucket does not.
        write(&*r.local, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        let f = r.file();
        f.ship_whole_file_next();
        write(&f, &*r.io, HEADER as u64, frame(1, 55)).unwrap();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(kinds(&r.records(2)), [3]);
        assert_eq!(r.image(2), r.local_bytes());
        let stats = r.up.stats().snapshot();
        assert_eq!((stats.log_puts, stats.commits_uploaded), (1, 1));
    }

    /// A `truncate(0)` before the first sync of a file opened with `ship_whole_file_next`
    /// (`reset_to_fresh_header`, no seal) keeps "ship the whole file": the image must not
    /// keep the old bytes the bucket holds.
    #[test]
    fn truncate_zero_before_first_sync_still_replaces_the_image() {
        let r = rig();
        let old = r.file();
        write(&old, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        write(&old, &*r.io, HEADER as u64, frame(1, 200)).unwrap();
        assert_eq!(sync(&old, &*r.io), Ok(()));
        assert_eq!(r.image(2).len(), HEADER + 200);
        let f = r.file();
        f.ship_whole_file_next();
        trunc(&f, &*r.io, 0);
        write(&f, &*r.io, 0, frame(0xBB, HEADER)).unwrap();
        write(&f, &*r.io, HEADER as u64, frame(2, 10)).unwrap();
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert_eq!(r.local_bytes().len(), HEADER + 10);
        assert_eq!(r.image(2), r.local_bytes());
    }

    /// A poisoned file refuses every write before the inner file sees it, and records
    /// nothing.
    #[test]
    fn poisoned_file_refuses_writes() {
        let r = rig();
        let f = r.file();
        write(&f, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        let e = Error::Io("poisoned: reopen".into());
        f.poison(e.clone());
        stash::clear();
        assert!(write(&f, &*r.io, HEADER as u64, frame(1, 55)).is_err());
        assert_eq!(stash::take(), Some(e.clone()));
        stash::clear();
        let c = f.pwritev(
            HEADER as u64,
            vec![Arc::new(Buffer::new(frame(2, 10)))],
            Completion::new_write(|_| {}),
        );
        assert!(c.is_err());
        assert_eq!(stash::take(), Some(e));
        assert_eq!(r.local_bytes(), frame(0xAA, HEADER));
        assert_eq!(
            crate::db::lock(&f.pending).len(),
            1,
            "only the write before it"
        );
    }
}
