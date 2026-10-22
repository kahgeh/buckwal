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
//! next record ships (the generation-change invariant).
//!
//! It never seals and never measures the file (maintenance does, Task 9).
#![cfg_attr(not(test), allow(dead_code))]
use super::db_storage::READ_ONLY;
use super::read_at;
use super::stash::stash;
use crate::batching::{ctx, Batching, PendingAck};
use crate::error::{Error, Result};
use crate::files::io_err;
use crate::format::{LogFile, Op};
use crate::uploader::{op_bytes, LogFailure, Uploader};
use std::sync::{Arc, Mutex};
use tracing::field::Empty;
use tracing::{info_span, Span};
use turso_core::io::FileSyncType;
use turso_core::{Buffer, Completion, File, IO};

fn engine_io(e: turso_core::LimboError) -> Error {
    io_err(".db-log", e)
}

/// What the bucket has of this file. Held for a whole sync, so a second sync (a group
/// commit's follower) waits for the first and then sees what it left unconfirmed.
struct Shipped {
    /// Ops recorded but not confirmed and not in doubt, oldest first: they ship, in front
    /// of newer ones, with the next sync.
    unconfirmed: Vec<Op>,
    /// How many syncs' commits `unconfirmed` carries (for the stats).
    commits: u64,
    /// The generation `unconfirmed` and everything confirmed belong to.
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
    shipped: Mutex<Shipped>,
    /// A read-only open: every write and truncate is refused, and a sync ships nothing.
    read_only: bool,
}

impl LogShippingFile {
    /// Wrap the opened `app.db-log`, whose bytes are the bucket's current generation's image
    /// (reconcile made them so at open).
    pub(crate) fn new(
        inner: Arc<dyn File>,
        io: Arc<dyn IO>,
        uploader: Arc<Uploader>,
        read_only: bool,
    ) -> turso_core::Result<LogShippingFile> {
        let generation = uploader.generation();
        Ok(LogShippingFile {
            inner,
            io,
            uploader,
            pending: Mutex::new(Vec::new()),
            shipped: Mutex::new(Shipped {
                unconfirmed: Vec::new(),
                commits: 0,
                generation,
                fenced: None,
            }),
            read_only,
        })
    }

    /// `e` failed a sync: a fence sticks, and nothing is kept to send again.
    fn failed(&self, s: &mut Shipped, e: Error) -> Error {
        if matches!(e, Error::Fenced(_)) {
            s.fenced = Some(e.clone());
            s.unconfirmed.clear();
            s.commits = 0;
            crate::db::lock(&self.pending).clear();
        }
        e
    }

    /// The commit point. Settle, then ship every unconfirmed op as one kind-3 record and
    /// wait for it. Ok only when every op recorded so far is confirmed; returns the commit
    /// span of the record it shipped (`None`: nothing to ship).
    fn ship(&self) -> Result<Option<Span>> {
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
        if generation != s.generation {
            // The new generation's image is the local file as it stands: all of it ships.
            let len = self.inner.size().map_err(engine_io)?;
            let bytes = read_at(&*self.inner, &*self.io, 0, len as usize).map_err(engine_io)?;
            let new = std::mem::take(&mut *crate::db::lock(&self.pending));
            s.commits += u64::from(!new.is_empty());
            s.unconfirmed = if bytes.is_empty() {
                Vec::new()
            } else {
                vec![Op::Write { off: 0, bytes }]
            };
            s.generation = generation;
        }
        // 2. Ship what is unconfirmed, the newest ops last.
        let new = std::mem::take(&mut *crate::db::lock(&self.pending));
        if !new.is_empty() {
            s.commits += 1;
            s.unconfirmed.extend(new);
        }
        if s.unconfirmed.is_empty() {
            return Ok(None);
        }
        let ops = std::mem::take(&mut s.unconfirmed);
        let commits = std::mem::take(&mut s.commits);
        let span = info_span!(
            target: "turso_durable",
            "durable.commit",
            durable.batching = %Batching::Off,
            durable.commit_no = Empty,
            durable.bytes = op_bytes(&ops),
            durable.outcome = Empty,
        );
        let ticket = span.in_scope(|| self.uploader.submit_log_file(LogFile::Log, ops, commits));
        span.record("durable.commit_no", ticket.commit);
        let (commit, submitted) = (ticket.commit, ticket.submitted);
        let r = ticket.wait_in(&span);
        span.record("durable.outcome", if r.is_ok() { "ok" } else { "error" });
        match r {
            Ok(_) => {
                ctx::push(PendingAck::Done {
                    commit,
                    waited: submitted.elapsed(),
                    batching: Batching::Off,
                    span: span.clone(),
                });
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

    /// The backstop of a read-only open: the engine never gets to change the `.db-log`.
    fn refuse_if_read_only(&self) -> turso_core::Result<()> {
        if self.read_only {
            return Err(stash(Error::Unsupported(READ_ONLY)));
        }
        Ok(())
    }

    /// Record `ops` once the inner call that issued them returned `Ok`: a write or truncate
    /// refused synchronously never happened, so it must never be shipped.
    fn record_if_ok(
        &self,
        ops: Vec<Op>,
        r: turso_core::Result<Completion>,
    ) -> turso_core::Result<Completion> {
        if r.is_ok() && !ops.is_empty() {
            crate::db::lock(&self.pending).extend(ops);
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
        self.refuse_if_read_only()?;
        let op = Op::Write {
            off: pos,
            bytes: buffer.as_slice().to_vec(),
        };
        self.record_if_ok(vec![op], self.inner.pwrite(pos, buffer, c))
    }
    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        self.refuse_if_read_only()?;
        let mut off = pos;
        let mut ops = Vec::with_capacity(buffers.len());
        for b in &buffers {
            ops.push(Op::Write {
                off,
                bytes: b.as_slice().to_vec(),
            });
            off += b.len() as u64;
        }
        self.record_if_ok(ops, self.inner.pwritev(pos, buffers, c))
    }
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> turso_core::Result<Completion> {
        if self.read_only {
            return self.inner.sync(c, sync_type);
        }
        // Upload before any completion exists: a failure is a synchronous `Err` (the commit
        // fails, the engine keeps its frame), never a failed completion.
        match self.ship().map_err(stash)? {
            Some(commit) => {
                info_span!(target: "turso_durable", parent: &commit, "durable.local_fsync")
                    .in_scope(|| self.inner.sync(c, sync_type))
            }
            None => self.inner.sync(c, sync_type),
        }
    }
    fn size(&self) -> turso_core::Result<u64> {
        self.inner.size()
    }
    /// A truncate to 0 is a generation change's (Task 5 decides when): it is not recorded,
    /// and what was pending or unconfirmed is dropped with the bytes. Any other length is
    /// recorded as an op (not expected from the engine but after a discarded write).
    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.refuse_if_read_only()?;
        if len > 0 {
            return self.record_if_ok(vec![Op::Truncate { len }], self.inner.truncate(len, c));
        }
        let mut s = crate::db::lock(&self.shipped);
        let r = self.inner.truncate(0, c);
        if r.is_ok() {
            s.unconfirmed.clear();
            s.commits = 0;
            crate::db::lock(&self.pending).clear();
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
    use crate::io::db_storage::READ_ONLY;
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
    struct GetGate {
        inner: Arc<dyn ObjectStore>,
        armed: AtomicBool,
        pass: AtomicU64,
    }

    impl GetGate {
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
        let mem = Arc::new(MemStore::new());
        let gate = Arc::new(GetGate {
            inner: mem.clone(),
            armed: AtomicBool::new(false),
            pass: AtomicU64::new(0),
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
            self.file_over(self.local.clone(), false)
        }
        fn file_over(&self, inner: Arc<dyn File>, read_only: bool) -> LogShippingFile {
            LogShippingFile::new(inner, self.io.clone(), self.up.clone(), read_only).unwrap()
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
        let f = r.file_over(spy.clone(), false);
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
        let f = r.file_over(Arc::new(Refusing), false);
        let buf = || Arc::new(Buffer::new(vec![7u8; 64]));
        assert!(f.pwrite(56, buf(), Completion::new_write(|_| {})).is_err());
        assert!(f
            .pwritev(56, vec![buf(), buf()], Completion::new_write(|_| {}))
            .is_err());
        assert!(f.truncate(4096, Completion::new_trunc(|_| {})).is_err());
        assert!(f.truncate(0, Completion::new_trunc(|_| {})).is_err());
        assert_eq!(f.pending.lock().unwrap().len(), 0, "phantom ops recorded");
    }

    /// A read-only open never changes the `.db-log` and never ships.
    #[test]
    fn read_only_refuses_writes() {
        let r = rig();
        write(&*r.local, &*r.io, 0, frame(0xAA, HEADER)).unwrap();
        let f = r.file_over(r.local.clone(), true);
        let buf = || Arc::new(Buffer::new(vec![1u8; 8]));
        let calls: [&dyn Fn() -> turso_core::Result<Completion>; 4] = [
            &|| f.pwrite(0, buf(), Completion::new_write(|_| {})),
            &|| f.pwritev(0, vec![buf()], Completion::new_write(|_| {})),
            &|| f.truncate(0, Completion::new_trunc(|_| {})),
            &|| f.truncate(8, Completion::new_trunc(|_| {})),
        ];
        for (i, call) in calls.iter().enumerate() {
            stash::clear();
            assert!(call().is_err(), "call {i}");
            assert_eq!(
                stash::take(),
                Some(Error::Unsupported(READ_ONLY)),
                "call {i}"
            );
        }
        assert_eq!(r.local_bytes(), frame(0xAA, HEADER), "unchanged");
        assert_eq!(read_at(&f, &*r.io, 0, 4).unwrap(), [0xAA; 4], "reads pass");
        assert_eq!(sync(&f, &*r.io), Ok(()));
        assert!(r.records(2).is_empty(), "a read-only sync ships nothing");
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
}
