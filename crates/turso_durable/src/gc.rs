//! GC on the bucket's own clock: [`gc`], and the pass the writer's trimmer thread runs
//! (`trim::spawn_trimmer`).
//!
//! A pass picks the newest manifest `m` (at most the head's) that the bucket says is at least
//! `EngineConfig::gc_min_age` old, and deletes what the head `(m + 1, m)` no longer reaches
//! (`trim::trim_once`). Ages come from the bucket (`ObjectStore::stat`), never from this
//! process, so a process of any lifetime trims what has been unreferenced long enough.
use crate::config::Config;
use crate::error::{Error, Result};
use crate::format::{Head, Keys, Manifest};
use crate::store::ObjectStore;
use crate::trim::trim_once;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::field::Empty;

/// How a GC pass ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcOutcome {
    /// It trimmed against an old enough manifest (possibly deleting nothing).
    Trimmed,
    /// Every manifest of the head is still younger than the minimum age.
    TooYoung,
    /// No head, a head with no manifest yet, or (on the writer's trimmer) the manifest it
    /// already trimmed against.
    Nothing,
}

impl GcOutcome {
    /// `trimmed`, `too_young` or `nothing`: the span's `durable.outcome` and the CLI's word.
    pub fn as_str(self) -> &'static str {
        match self {
            GcOutcome::Trimmed => "trimmed",
            GcOutcome::TooYoung => "too_young",
            GcOutcome::Nothing => "nothing",
        }
    }
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
    /// How long until the next younger manifest is old enough, when there is one.
    pub next_due: Option<Duration>,
    pub outcome: GcOutcome,
}

/// What a [`gc`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcReport {
    /// The head's generation (0 when the bucket holds no database).
    pub generation: u64,
    /// The manifest it trimmed against: the newest one old enough.
    pub trimmed_against: Option<u64>,
    /// That manifest's age by the bucket's clock; when none was old enough, the age of the
    /// oldest one it asked about.
    pub age: Option<Duration>,
    /// How many objects it deleted.
    pub deleted: u64,
    pub outcome: GcOutcome,
    pub elapsed: Duration,
}

/// Delete what the database `config` names no longer needs: everything an old enough head no
/// longer reaches. A manifest is old enough once the bucket's own clock says it is
/// `trim_grace + retry_deadline + request_timeout + 1 s` old; the head trimmed against is the
/// one that followed the newest such manifest. It writes nothing but DELETEs, and needs no
/// directory (`config.dir` is ignored). Safe beside a running writer and restores: what a
/// restore of any head of the last `trim_grace` reads is kept.
pub fn gc(config: &Config) -> Result<GcReport> {
    let store = config.s3_store()?;
    gc_with_store(config, store)
}

#[doc(hidden)]
pub fn gc_with_store(config: &Config, store: Arc<dyn ObjectStore>) -> Result<GcReport> {
    let start = Instant::now();
    config.validate()?;
    let min_age = config.engine(false).gc_min_age();
    let pass = gc_once(&*store, &config.keys(), min_age, None)?;
    Ok(GcReport {
        generation: pass.generation,
        trimmed_against: pass.trimmed_against,
        age: pass.age,
        deleted: pass.deleted,
        outcome: pass.outcome,
        elapsed: start.elapsed(),
    })
}

fn bucket_err(what: &str, e: impl std::fmt::Display) -> Error {
    Error::Bucket(format!("{what}: {e}"))
}

/// One pass, in a `durable.gc` span (`durable.generation`, `durable.manifest`,
/// `durable.age_ms`, `durable.deleted`, `durable.outcome`: `trimmed`, `too_young`,
/// `nothing` or `error`):
///
/// 1. GET the head; none, or no manifest yet: `Nothing`.
/// 2. LIST `manifest/`; the candidates are the manifests up to the head's, newest first.
/// 3. Stat each until one is at least `min_age` old: the chosen one. Each younger one
///    brings `next_due` forward to when it comes of age; an unknown age (`stat` gave none)
///    is never taken as old.
/// 4. None chosen: `TooYoung`. The one chosen is `last` (this trimmer's previous pass
///    trimmed against it): `Nothing`, with no further request.
/// 5. GET manifest `m`, and trim against the head `(m + 1, m)`: `Trimmed`.
pub(crate) fn gc_once(
    store: &dyn ObjectStore,
    keys: &Keys,
    min_age: Duration,
    last: Option<u64>,
) -> Result<GcPass> {
    let span = tracing::info_span!(
        target: "turso_durable",
        "durable.gc",
        durable.generation = Empty,
        durable.manifest = Empty,
        durable.age_ms = Empty,
        durable.deleted = Empty,
        durable.outcome = Empty,
    );
    let r = span.in_scope(|| pass(store, keys, min_age, last, &span));
    match &r {
        Ok(p) => {
            if let Some(m) = p.trimmed_against {
                span.record("durable.manifest", m);
            }
            if let Some(age) = p.age {
                span.record("durable.age_ms", age.as_millis() as u64);
            }
            span.record("durable.deleted", p.deleted);
            span.record("durable.outcome", p.outcome.as_str());
        }
        Err(_) => {
            span.record("durable.outcome", "error");
        }
    }
    r
}

fn pass(
    store: &dyn ObjectStore,
    keys: &Keys,
    min_age: Duration,
    last: Option<u64>,
    span: &tracing::Span,
) -> Result<GcPass> {
    let mut out = GcPass {
        generation: 0,
        trimmed_against: None,
        age: None,
        deleted: 0,
        next_due: None,
        outcome: GcOutcome::Nothing,
    };
    let head_key = keys.head();
    let Some(obj) = store
        .get(&head_key)
        .map_err(|e| bucket_err("get head", e))?
    else {
        return Ok(out);
    };
    let head = Head::decode(&obj.body)?;
    out.generation = head.generation;
    span.record("durable.generation", head.generation);
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
        let due = min_age - stat.age;
        out.next_due = Some(out.next_due.map_or(due, |d| d.min(due)));
    }
    let Some(m) = chosen else {
        out.outcome = GcOutcome::TooYoung;
        return Ok(out);
    };
    if last == Some(m) {
        return Ok(out);
    }
    let key = keys.manifest(m);
    let body = store
        .get(&key)
        .map_err(|e| bucket_err(&format!("get {key}"), e))?
        .ok_or_else(|| Error::Bucket(format!("{key} is gone")))?
        .body;
    let manifest = Manifest::decode(&body)?;
    let trim_head = Head {
        generation: m + 1,
        manifest: Some(m),
        ..head
    };
    out.deleted = trim_once(store, keys, &trim_head, Some(&manifest))?;
    out.trimmed_against = Some(m);
    out.outcome = GcOutcome::Trimmed;
    Ok(out)
}

#[cfg(test)]
mod tests;
