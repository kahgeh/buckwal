//! GC on the bucket's own clock: [`gc`], and the pass the writer's trimmer thread runs
//! (`trim::spawn_trimmer`).
//!
//! Formats 1 to 4: a pass picks the newest manifest `m` (at most the head's) that the bucket
//! says is at least `EngineConfig::gc_min_age` old, and deletes what the head `(m + 1, m)` no
//! longer reaches (`trim::trim_once`).
//!
//! Format 5 (`pack_pass`, spec "GC"): a pass deletes the packs below the pointer's pack
//! generation and the log below it, orphan packs, multipart uploads under `pack/` that are older
//! than the orphan grace, and the `pages/` and `manifest/` objects of an upgraded bucket.
//!
//! Ages come from the bucket (`ObjectStore::stat`), never from this process, so a process of
//! any lifetime trims what has been unreferenced long enough.
use crate::config::Config;
use crate::error::{Error, Result};
use crate::format::{Head, Keys, FORMAT_V5};
use crate::pack::PackPointer;
use crate::store::{ObjectStat, ObjectStore};
use crate::trim::trim_once;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::field::Empty;

/// How a GC pass ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcOutcome {
    /// Formats 1 to 4: it trimmed against an old enough manifest. Format 5: a pack that the
    /// pointer names, or one below it, is old enough, and the pass deleted what that pack
    /// holds. Either can delete nothing.
    Trimmed,
    /// Formats 1 to 4: every manifest of the head is still younger than the minimum age.
    /// Format 5: the pack that the pointer names, and each pack below it, is still younger.
    TooYoung,
    /// No head, a head with no manifest yet, (on the writer's trimmer) the manifest or
    /// pointer it already trimmed against, or a format 5 bucket with no pointer of this
    /// database (a format 5 pass then deletes only orphan packs and old multipart uploads).
    Nothing,
    /// Format 5: the pointer names a pack or codes object that a HEAD does not find. The pass
    /// deleted no log, no pack and no page, and logged an error: the log is then the only copy.
    PackMissing,
}

impl GcOutcome {
    /// `trimmed`, `too_young`, `nothing` or `pack_missing`: the span's `buckwal.outcome` and
    /// the CLI's word.
    pub fn as_str(self) -> &'static str {
        match self {
            GcOutcome::Trimmed => "trimmed",
            GcOutcome::TooYoung => "too_young",
            GcOutcome::Nothing => "nothing",
            GcOutcome::PackMissing => "pack_missing",
        }
    }
}

/// The ages a GC pass works with, by the bucket's clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GcAges {
    /// How old a manifest must be before GC deletes what it replaced:
    /// `EngineConfig::gc_min_age`. Format 5: the floor of the pack keep time.
    pub min_age: Duration,
    /// Format 5: `Config::pack_keep`, a fixed pack keep time (`None`: from the newest pack's
    /// size, `config::pack_keep_time`).
    pub pack_keep: Option<Duration>,
    /// Format 5: how old an orphan pack (from its upload start) and a multipart upload under
    /// `pack/` must be before GC deletes or aborts it: `Config::pack_orphan_grace` plus 1 s for
    /// the resolution of a bucket's age.
    pub orphan_grace: Duration,
}

impl GcAges {
    pub(crate) fn new(min_age: Duration, pack_orphan_grace: Duration) -> GcAges {
        GcAges {
            min_age,
            pack_keep: None,
            orphan_grace: pack_orphan_grace.saturating_add(Duration::from_secs(1)),
        }
    }

    /// These ages with the fixed pack keep time `pack_keep` (`Config::pack_keep`).
    pub(crate) fn with_pack_keep(self, pack_keep: Option<Duration>) -> GcAges {
        GcAges { pack_keep, ..self }
    }

    /// The pack keep time when the newest pack has `pack_size` bytes: never below `min_age`
    /// (the trim grace plus the request budget).
    pub(crate) fn pack_keep_time(&self, pack_size: u64) -> Duration {
        crate::config::pack_keep_time(self.min_age, self.pack_keep, pack_size)
    }
}

/// What a pass trimmed against, so that the writer's trimmer does not do the same pass again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GcMark {
    /// Formats 1 to 4: the manifest.
    Manifest(u64),
    /// Format 5: the ETag of the pointer, and the pack the writer's background fetch read, after
    /// a pass whose GC base was the pointer's pack.
    Pointer {
        etag: String,
        fetching: Option<String>,
    },
}

/// One GC pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GcPass {
    /// The head's generation (0 without a head).
    pub generation: u64,
    /// The manifest trimmed against (`Trimmed` only).
    pub trimmed_against: Option<u64>,
    /// The bucket's age of the manifest chosen; when none was old enough, of the oldest one
    /// it asked about.
    pub age: Option<Duration>,
    pub deleted: u64,
    pub packs_deleted: u64,
    pub orphan_packs_deleted: u64,
    pub uploads_aborted: u64,
    /// How long until the next younger manifest, pack or orphan is old enough, when there is
    /// one.
    pub next_due: Option<Duration>,
    /// What a later pass of the trimmer can skip on (`None`: nothing).
    pub mark: Option<GcMark>,
    pub outcome: GcOutcome,
}

impl GcPass {
    fn due(&mut self, due: Duration) {
        self.next_due = Some(self.next_due.map_or(due, |d| d.min(due)));
    }
}

/// What a [`gc`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcReport {
    /// The head's generation (0 when the bucket holds no database).
    pub generation: u64,
    /// Formats 1 to 4: the manifest it trimmed against, the newest one old enough.
    pub trimmed_against: Option<u64>,
    /// That manifest's age by the bucket's clock; when none was old enough, the age of the
    /// oldest one it asked about.
    pub age: Option<Duration>,
    /// How many objects it deleted (each pack and each codes object included).
    pub deleted: u64,
    /// Format 5: how many packs below the pointer's pack generation it deleted, each with its
    /// codes.
    pub packs_deleted: u64,
    /// Format 5: how many orphan packs it deleted, each with its codes.
    pub orphan_packs_deleted: u64,
    /// Format 5: how many multipart uploads under `pack/` it aborted.
    pub uploads_aborted: u64,
    pub outcome: GcOutcome,
    pub elapsed: Duration,
}

/// Delete what the database `config` names no longer needs. Formats 1 to 4: everything an old
/// enough head no longer reaches. A manifest is old enough once the bucket's own clock says it
/// is `trim_grace + retry_deadline + request_timeout + 1 s` old; the head trimmed against is
/// the one that followed the newest such manifest. Format 5: see the spec's "GC" and
/// `pack_pass`. It writes nothing but DELETEs (and aborts of multipart uploads), and needs no
/// directory (`config.dir` is ignored). Safe beside a running writer, pack runs and restores.
pub fn gc(config: &Config) -> Result<GcReport> {
    let store = config.s3_store()?;
    gc_with_store(config, store)
}

#[doc(hidden)]
pub fn gc_with_store(config: &Config, store: Arc<dyn ObjectStore>) -> Result<GcReport> {
    let start = Instant::now();
    config.validate()?;
    let ages = GcAges::new(
        config.engine(crate::io::IoMode::Writer).gc_min_age(),
        config.pack_orphan_grace,
    )
    .with_pack_keep(config.pack_keep);
    let pass = gc_pass(&*store, &config.keys()?.for_open(), ages, None, None)?;
    Ok(GcReport {
        generation: pass.generation,
        trimmed_against: pass.trimmed_against,
        age: pass.age,
        deleted: pass.deleted,
        packs_deleted: pass.packs_deleted,
        orphan_packs_deleted: pass.orphan_packs_deleted,
        uploads_aborted: pass.uploads_aborted,
        outcome: pass.outcome,
        elapsed: start.elapsed(),
    })
}

fn bucket_err(what: &str, e: impl std::fmt::Display) -> Error {
    Error::Bucket(format!("{what}: {e}"))
}

/// Tests: a pass with `min_age`, the default orphan grace, and the manifest `last` to skip.
#[cfg(test)]
pub(crate) fn gc_once(
    store: &dyn ObjectStore,
    keys: &Keys,
    min_age: Duration,
    last: Option<u64>,
) -> Result<GcPass> {
    let ages = GcAges::new(min_age, crate::config::defaults::PACK_ORPHAN_GRACE);
    gc_pass(store, keys, ages, last.map(GcMark::Manifest).as_ref(), None)
}

/// One pass, in a `buckwal.gc` span (`buckwal.generation`, `buckwal.manifest`,
/// `buckwal.age_ms`, `buckwal.deleted`, `buckwal.packs_deleted`,
/// `buckwal.orphan_packs_deleted`, `buckwal.uploads_aborted`, `buckwal.outcome`: `trimmed`,
/// `too_young`, `nothing`, `pack_missing`, `integrity` (a failed integrity check) or `error`):
///
/// 1. GET the head; none: `Nothing`. A format 5 head: `pack_pass`.
/// 2. No manifest yet: `Nothing`. LIST `manifest/`; the candidates are the manifests up to the
///    head's, newest first, and (a head carrying `next_generation_floor`) not below
///    `floor - 1`: none left is `Nothing`.
/// 3. Stat each until one is at least `min_age` old: the chosen one. Each younger one
///    brings `next_due` forward to when it comes of age; an unknown age (`stat` gave none)
///    is never taken as old.
/// 4. None chosen: `TooYoung`. The one chosen is `last` (this trimmer's previous pass
///    trimmed against it): `Nothing`, with no further request.
/// 5. GET manifest `m`, and trim against the head `(m + 1, m)`: `Trimmed`.
pub(crate) fn gc_pass(
    store: &dyn ObjectStore,
    keys: &Keys,
    ages: GcAges,
    last: Option<&GcMark>,
    fetching: Option<&str>,
) -> Result<GcPass> {
    let span = tracing::info_span!(
        target: "buckwal",
        "buckwal.gc",
        buckwal.generation = Empty,
        buckwal.manifest = Empty,
        buckwal.age_ms = Empty,
        buckwal.deleted = Empty,
        buckwal.packs_deleted = Empty,
        buckwal.orphan_packs_deleted = Empty,
        buckwal.uploads_aborted = Empty,
        buckwal.outcome = Empty,
    );
    let r = span.in_scope(|| pass(store, keys, ages, last, fetching, &span));
    match &r {
        Ok(p) => {
            if let Some(m) = p.trimmed_against {
                span.record("buckwal.manifest", m);
            }
            if let Some(age) = p.age {
                span.record("buckwal.age_ms", age.as_millis() as u64);
            }
            span.record("buckwal.deleted", p.deleted);
            span.record("buckwal.packs_deleted", p.packs_deleted);
            span.record("buckwal.orphan_packs_deleted", p.orphan_packs_deleted);
            span.record("buckwal.uploads_aborted", p.uploads_aborted);
            span.record("buckwal.outcome", p.outcome.as_str());
        }
        Err(e) => {
            span.record("buckwal.outcome", e.outcome());
        }
    }
    r
}

fn pass(
    store: &dyn ObjectStore,
    keys: &Keys,
    ages: GcAges,
    last: Option<&GcMark>,
    fetching: Option<&str>,
    span: &tracing::Span,
) -> Result<GcPass> {
    let min_age = ages.min_age;
    let mut out = GcPass {
        generation: 0,
        trimmed_against: None,
        age: None,
        deleted: 0,
        packs_deleted: 0,
        orphan_packs_deleted: 0,
        uploads_aborted: 0,
        next_due: None,
        mark: None,
        outcome: GcOutcome::Nothing,
    };
    let head_key = keys.head();
    let Some(obj) = store
        .get(&head_key)
        .map_err(|e| bucket_err("get head", e))?
    else {
        return Ok(out);
    };
    let head = keys.decode_head(&obj.body)?;
    out.generation = head.generation;
    span.record("buckwal.generation", head.generation);
    if head.format == FORMAT_V5 {
        pack_pass(store, keys, ages, last, fetching, &mut out)?;
        return Ok(out);
    }
    let Some(head_manifest) = head.manifest else {
        return Ok(out);
    };
    let root = keys.manifest_root();
    let mut candidates: Vec<u64> = store
        .list(&root)
        .map_err(|e| bucket_err(&format!("list {root}"), e))?
        .iter()
        .filter_map(|k| keys.parse_manifest(k))
        .filter(|&m| m <= head_manifest)
        .collect();
    // After a takeover the head carries its next generation floor, and the manifests below
    // `floor - 1` may be a fenced writer's unpublished leftovers (its seal's `manifest/G`
    // under the skipped generation): trimming against one could delete pages the head's
    // manifest still maps. They are ignored while the field is present, so GC trims nothing
    // between a takeover and its skip seal (the head's manifest is below the floor then).
    // A head without the field keeps the rule above only.
    let trusted_from = head
        .next_generation_floor
        .map_or(0, |f| f.saturating_sub(1));
    let before = candidates.len();
    candidates.retain(|&m| m >= trusted_from);
    if candidates.is_empty() && before > 0 {
        return Ok(out);
    }
    candidates.sort_unstable_by(|a, b| b.cmp(a));
    let mut chosen = None;
    for m in candidates {
        let key = keys.manifest(m);
        let Some(stat) = store
            .stat(&key)
            .map_err(|e| bucket_err(&format!("stat {key}"), e))?
        else {
            continue;
        };
        out.age = Some(stat.age);
        if stat.age >= min_age {
            chosen = Some(m);
            break;
        }
        out.due(min_age - stat.age);
    }
    let Some(m) = chosen else {
        out.outcome = GcOutcome::TooYoung;
        return Ok(out);
    };
    if last == Some(&GcMark::Manifest(m)) {
        return Ok(out);
    }
    let key = keys.manifest(m);
    let body = store
        .get(&key)
        .map_err(|e| bucket_err(&format!("get {key}"), e))?
        .ok_or_else(|| Error::Bucket(format!("{key} is gone")))?
        .body;
    let manifest = keys.decode_manifest(&body, m)?;
    let trim_head = Head {
        generation: m + 1,
        manifest: Some(m),
        ..head
    };
    out.deleted = trim_once(store, keys, &trim_head, Some(&manifest))?;
    out.trimmed_against = Some(m);
    out.mark = Some(GcMark::Manifest(m));
    out.outcome = GcOutcome::Trimmed;
    Ok(out)
}

/// The objects of one pack that the listing of `pack/` shows.
#[derive(Debug, Default)]
struct Listed {
    pack: bool,
    codes: bool,
}

fn list(store: &dyn ObjectStore, prefix: &str) -> Result<Vec<String>> {
    store
        .list(prefix)
        .map_err(|e| bucket_err(&format!("list {prefix}"), e))
}

fn stat(store: &dyn ObjectStore, key: &str) -> Result<Option<ObjectStat>> {
    store
        .stat(key)
        .map_err(|e| bucket_err(&format!("stat {key}"), e))
}

/// The pass of a format 5 bucket (spec "GC", rev 14), every age by the bucket's clock:
///
/// 1. GET the pointer. No pointer, or a pointer of another `database_id`, is no pointer: the
///    pass deletes no log, no pack below a pointer and no page, only orphans and uploads
///    (steps 2, 3 and 6); `Nothing`.
/// 2. Abort every multipart upload under `pack/` that started `orphan_grace` or longer ago. A
///    live pack run gives up at half the orphan grace, so this never aborts one.
/// 3. LIST `pack/`.
/// 4. Unless `last` is this pointer's ETag with the same `fetching` (the writer's trimmer
///    trimmed against it, and nothing below it was left; then `Nothing`): HEAD the pack that the
///    pointer names and its codes. Find the *GC base*: the newest complete pack (its pack and
///    its codes are both in the listing) at or below the pointer's pack generation whose codes
///    object is older than the pack keep time (`GcAges::pack_keep_time` of the pointer's size).
///    A pack run writes its codes just before its pointer write, so their age is the time since
///    the older packs stopped being the newest. HEAD the GC base's pack too. When one of the
///    HEADs finds nothing, the pointer names data that is gone, and the log is the only copy:
///    the pass deletes no log, no pack and no page and logs an error; `PackMissing`.
/// 5. With a GC base: delete the log of every generation below it, each pack below it with its
///    codes (except `fetching`, the pack the writer's background fetch still reads), and every
///    `pages/` and `manifest/` object (a bucket upgraded from a seal format); `Trimmed`. With
///    none: no pack and no log go; `TooYoung`.
/// 6. Every other pack at or above the pointer's pack generation (or any, with no pointer)
///    that the pointer does not name is an orphan: it goes with its codes once its own
///    Last-Modified time (on S3 the upload start) is `orphan_grace` old.
///
/// The pack that the pointer names, and a pack or log at or above the GC base, are never
/// deleted. Every HEAD comes before any DELETE.
fn pack_pass(
    store: &dyn ObjectStore,
    keys: &Keys,
    ages: GcAges,
    last: Option<&GcMark>,
    fetching: Option<&str>,
    out: &mut GcPass,
) -> Result<()> {
    let pointer_key = keys.pack_pointer();
    let (pointer, etag) = match store
        .get(&pointer_key)
        .map_err(|e| bucket_err(&format!("get {pointer_key}"), e))?
    {
        Some(o) => (keys.read_pointer(&o.body)?, Some(o.etag)),
        None => (None, None),
    };
    let root = keys.pack_root();
    out.uploads_aborted = store
        .abort_incomplete(&root, ages.orphan_grace)
        .map_err(|e| bucket_err(&format!("abort uploads under {root}"), e))?;
    let mut packs: BTreeMap<(u64, String), Listed> = BTreeMap::new();
    for key in list(store, &root)? {
        if let Some(id) = keys.parse_pack(&key) {
            packs.entry(id).or_default().pack = true;
        } else if let Some(id) = keys.parse_pack_codes(&key) {
            packs.entry(id).or_default().codes = true;
        }
    }
    let mut doomed: Vec<String> = Vec::new();
    let mut orphans_from = 0;
    let mut named_id = None;
    if let Some(p) = &pointer {
        let id = keys
            .parse_pack(&p.pack_key)
            .expect("read_pointer checks the pack key");
        orphans_from = p.pack_generation;
        named_id = Some(id);
        let marked = matches!(
            (last, &etag),
            (Some(GcMark::Pointer { etag: seen, fetching: f }), Some(etag))
                if seen == etag && f.as_deref() == fetching
        );
        if !marked {
            match gc_base(store, keys, ages, p, &packs, out)? {
                Some(base) => {
                    trim_below(store, keys, base, fetching, &packs, &mut doomed, out)?;
                    if base == p.pack_generation {
                        out.mark = etag.clone().map(|etag| GcMark::Pointer {
                            etag,
                            fetching: fetching.map(str::to_string),
                        });
                    }
                }
                // A HEAD found nothing: nothing more goes (the error is logged).
                None if out.outcome == GcOutcome::PackMissing => return Ok(()),
                // No GC base yet: only orphans go.
                None => {}
            }
        }
    }
    for (id, l) in packs.range((orphans_from, String::new())..) {
        if Some(id) == named_id.as_ref() {
            continue;
        }
        let (g, run) = id;
        let key = if l.pack {
            keys.pack(*g, run)
        } else {
            keys.pack_codes(*g, run)
        };
        let Some(st) = stat(store, &key)? else {
            continue;
        };
        if st.age < ages.orphan_grace {
            out.due(ages.orphan_grace - st.age);
            continue;
        }
        if push_pack(keys, &mut doomed, id, l) {
            out.orphan_packs_deleted += 1;
        }
    }
    // Never the pack that the pointer names, nor the one the writer's fetch reads.
    let spared: Vec<String> = pointer
        .iter()
        .flat_map(|p| [p.pack_key.clone(), p.codes_key.clone()])
        .chain(
            fetching
                .iter()
                .flat_map(|k| [k.to_string(), format!("{k}.codes")]),
        )
        .collect();
    doomed.retain(|k| !spared.contains(k));
    for k in &doomed {
        store
            .delete(k)
            .map_err(|e| bucket_err(&format!("delete {k}"), e))?;
    }
    out.deleted = doomed.len() as u64;
    Ok(())
}

/// Add the objects of pack `id` (as listed) to `doomed`; true when it has a pack object.
fn push_pack(keys: &Keys, doomed: &mut Vec<String>, (g, run): &(u64, String), l: &Listed) -> bool {
    if l.pack {
        doomed.push(keys.pack(*g, run));
    }
    if l.codes {
        doomed.push(keys.pack_codes(*g, run));
    }
    l.pack
}

/// Step 4 of `pack_pass`: the GC base's pack generation, after the HEADs of the pointer's pack,
/// its codes and the GC base's pack. `Ok(None)` with `out.outcome` set: `TooYoung` (no GC base)
/// or `PackMissing` (a HEAD found nothing; the error is logged).
fn gc_base(
    store: &dyn ObjectStore,
    keys: &Keys,
    ages: GcAges,
    p: &PackPointer,
    packs: &BTreeMap<(u64, String), Listed>,
    out: &mut GcPass,
) -> Result<Option<u64>> {
    let missing = |key: &str, out: &mut GcPass| {
        tracing::error!(
            target: "buckwal",
            key = %key,
            pack_generation = p.pack_generation,
            "gc: the pack pointer names an object that is gone; no log deleted, the log is \
             the only copy"
        );
        out.outcome = GcOutcome::PackMissing;
        Ok(None)
    };
    if stat(store, &p.pack_key)?.is_none() {
        return missing(&p.pack_key, out);
    }
    let Some(named_codes) = stat(store, &p.codes_key)? else {
        return missing(&p.codes_key, out);
    };
    let keep = ages.pack_keep_time(p.size);
    let named = keys
        .parse_pack(&p.pack_key)
        .expect("checked by read_pointer");
    let complete = packs
        .range(..=(p.pack_generation, named.1.clone()))
        .rev()
        .filter(|((g, run), l)| l.pack && l.codes && (*g < p.pack_generation || *run == named.1));
    for ((g, run), _) in complete {
        let age = if *g == p.pack_generation {
            named_codes.age
        } else {
            match stat(store, &keys.pack_codes(*g, run))? {
                Some(st) => st.age,
                None => continue,
            }
        };
        out.age = Some(age);
        if age < keep {
            out.due(keep - age);
            continue;
        }
        if *g != p.pack_generation {
            let key = keys.pack(*g, run);
            if stat(store, &key)?.is_none() {
                return missing(&key, out);
            }
        }
        return Ok(Some(*g));
    }
    out.outcome = GcOutcome::TooYoung;
    Ok(None)
}

/// Step 5 of `pack_pass`: everything below the GC base `base`.
fn trim_below(
    store: &dyn ObjectStore,
    keys: &Keys,
    base: u64,
    fetching: Option<&str>,
    packs: &BTreeMap<(u64, String), Listed>,
    doomed: &mut Vec<String>,
    out: &mut GcPass,
) -> Result<()> {
    out.outcome = GcOutcome::Trimmed;
    doomed.extend(
        list(store, &keys.log_root())?
            .into_iter()
            .filter(|k| matches!(keys.parse_log(k), Some((g, _)) if g < base)),
    );
    doomed.extend(list(store, &keys.page_root())?);
    doomed.extend(list(store, &keys.manifest_root())?);
    for (id, l) in packs.range(..(base, String::new())) {
        if fetching == Some(keys.pack(id.0, &id.1).as_str()) {
            continue;
        }
        if push_pack(keys, doomed, id, l) {
            out.packs_deleted += 1;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
