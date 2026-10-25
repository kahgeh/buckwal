//! The seal of bucket formats 1 to 4: the seal-start record, every dirty page of the
//! generation as its own object (`pages/<G>/<p>`), the manifest (`manifest/<G>`), then the head
//! move. A format 5 bucket never seals: it switches generations (`Bucket::switch_generation`).
//! This code stays for the buckets made before format 5 (the upgrade reads what it wrote) and
//! for the MVCC databases, which still seal (ADR 0002).

use super::*;

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
pub(super) enum Known {
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

impl Bucket {
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
    /// same empty database). In an encrypted database the loser's objects are coded under the
    /// loser's database id, so the winner, once its head has landed, puts its own pages and
    /// manifest again: the objects the head names are the winner's. (A loser that is slow
    /// enough to land a PUT after that leaves a database that fails its first restore with
    /// `integrity`; it holds no data yet, and a new create at an emptied prefix replaces it.)
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
        // The id first: the pages and the manifest below are coded under its key.
        let database_id = keys.init_database_id()?;
        let gen = 1;
        let (page_size, page_count) = (image.page_size, image.page_count());
        let pages = image.pages;
        let counters = Counters::default();
        let put_pages = || -> Result<()> {
            for (p, body) in &pages {
                let key = keys.page(gen, *p);
                let obj = keys.seal_page(*p, gen, body);
                retrying(&counters, &retry, &key, || store.put(&key, &obj))?;
                counters.page_puts.fetch_add(1, Relaxed);
            }
            Ok(())
        };
        put_pages()?;
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
            database_id,
        });
        let coded = keys.coded();
        let mut b = Bucket::create_head(
            store.clone(),
            keys.clone(),
            retry.clone(),
            concurrency,
            head,
        )?;
        if coded {
            // The head is ours: put the pages and the manifest again, so a racing creator's
            // objects (coded under its own database id) do not stay under the keys it names.
            put_pages()?;
            retrying(&counters, &retry, &key, || store.put(&key, &manifest_body))?;
        }
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
        if self.head.format == FORMAT_V5 {
            return Err(Error::Unsupported(
                "a seal of a format 5 bucket (it switches generations)",
            ));
        }
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
            manifest: Some(manifest.clone()),
        });
        let etag = self.put_head(start, &head)?;
        Ok(self.finish_seal(start, head, etag, Some(manifest)))
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
                // Formats 1 to 4 keep the kind 1 record; format 5 names the next generation
                // (`Bucket::switch`).
                next_generation: None,
            },
            self.head.generation,
            self.seq,
        );
        let bytes = body.len() as u64;
        let pos = self.put_log(start, body)?;
        self.emit_uploaded(pos, bytes);
        self.verify_head(start)
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
}
