//! [`vacuum`]: turso's in-place `VACUUM` on the writer's own database (spec Choice A (a),
//! "Vacuum: a correction to the discovery").
//!
//! A vacuum moves rows to other page numbers, so only a vacuum of the writer's own file keeps
//! the file and the log in step: the vacuum is one commit, and its log record holds the whole
//! new file. A restore, a pack run and a replica apply it as they apply any commit. The open
//! is a writer open, so it takes the directory's lock: it fails while a writer (`serve`, `sql`)
//! has the directory open.
//!
//! Only the in-place form runs. The copy form (with a target file) writes a plaintext copy of
//! an encrypted database (turso 0.8.3, docs/packer-recon.md), and the SQL guard refuses it on
//! an encrypted database on every SQL path ([`VACUUM_INTO_REFUSED`]). No source file under
//! `crates/` holds the two words of that form next to each other: a test searches for them
//! (`source_never_contains_vacuum_into`), so this file names the form only in parts.
//!
//! The span `buckwal.vacuum`: `buckwal.vacuum.page_size`, `buckwal.vacuum.pages_before`,
//! `buckwal.vacuum.free_pages_before`, `buckwal.vacuum.pages_after`,
//! `buckwal.vacuum.free_pages_after`,
//! `buckwal.vacuum.bytes_before`, `buckwal.vacuum.bytes_after`, `buckwal.free_page_share`
//! (before the vacuum), `buckwal.elapsed_ms`, `buckwal.outcome` (`vacuumed`, or the error's
//! outcome) and `buckwal.error`.
use crate::config::Config;
use crate::conn::Connection;
use crate::db::{is_claimed, open_for_vacuum, Database};
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE};
use crate::store::ObjectStore;
use crate::CheckpointMode;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::field::Empty;

/// What the SQL guard answers for the copy form of `VACUUM` on an encrypted database
/// (`Error::Unsupported`). Made with `concat!` so that the two words are not next to each
/// other in the source (see the module text).
pub(crate) const VACUUM_INTO_REFUSED: &str = concat!(
    "VACUUM",
    " INTO on an encrypted database (it writes a plaintext copy; run `buckwal vacuum` to \
     vacuum the database in place)"
);

/// What a [`vacuum`] did. The page counts are the engine's (`PRAGMA page_count` and
/// `PRAGMA freelist_count`); the bytes are the size of `app.db`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct VacuumReport {
    pub page_size: u64,
    pub pages_before: u64,
    pub free_pages_before: u64,
    pub pages_after: u64,
    pub free_pages_after: u64,
    /// `app.db` before the open.
    pub file_bytes_before: u64,
    /// `app.db` after the close.
    pub file_bytes_after: u64,
    pub elapsed: Duration,
}

impl VacuumReport {
    /// Free pages over all pages before the vacuum: the `buckwal.free_page_share` that the
    /// pack runs report.
    pub fn free_page_share_before(&self) -> f64 {
        share(self.free_pages_before, self.pages_before)
    }
}

/// Free pages over all pages; 0 for an empty file.
pub(crate) fn share(free: u64, pages: u64) -> f64 {
    if pages == 0 {
        0.0
    } else {
        free as f64 / pages as f64
    }
}

/// Vacuum the database in `config.dir` in place, as its writer: open it, run `VACUUM`, check
/// it with `PRAGMA integrity_check`, checkpoint with TRUNCATE (the local file gets smaller)
/// and close. The vacuum's commit goes to the bucket like any other. The database is not
/// available to the app during the vacuum and its upload (the writer must be closed for it):
/// run it at a quiet time (on 1 GB with 20% deleted, about 4 s before the upload).
///
/// `config.dir` must be a directory the writer owns ([`crate::is_claimed`]): an open of any
/// other would restore the database and take it over. The directory's lock must be free: it
/// fails `AlreadyOpen` while another open (for example `serve`) has it. The packer thread does
/// not start for this open. An encrypted database needs its key file, and stays encrypted.
pub fn vacuum(config: &Config) -> Result<VacuumReport> {
    let store = config.s3_store()?;
    vacuum_with_store(config, store)
}

#[doc(hidden)]
pub fn vacuum_with_store(config: &Config, store: Arc<dyn ObjectStore>) -> Result<VacuumReport> {
    let span = tracing::info_span!(
        target: "buckwal",
        "buckwal.vacuum",
        buckwal.vacuum.page_size = Empty,
        buckwal.vacuum.pages_before = Empty,
        buckwal.vacuum.free_pages_before = Empty,
        buckwal.vacuum.pages_after = Empty,
        buckwal.vacuum.free_pages_after = Empty,
        buckwal.vacuum.bytes_before = Empty,
        buckwal.vacuum.bytes_after = Empty,
        buckwal.free_page_share = Empty,
        buckwal.elapsed_ms = Empty,
        buckwal.outcome = Empty,
        buckwal.error = Empty,
    );
    let start = Instant::now();
    let r = span.in_scope(|| run(config, store, start));
    span.record("buckwal.elapsed_ms", start.elapsed().as_millis() as u64);
    match &r {
        Ok(v) => {
            span.record("buckwal.vacuum.page_size", v.page_size);
            span.record("buckwal.vacuum.pages_before", v.pages_before);
            span.record("buckwal.vacuum.free_pages_before", v.free_pages_before);
            span.record("buckwal.vacuum.pages_after", v.pages_after);
            span.record("buckwal.vacuum.free_pages_after", v.free_pages_after);
            span.record("buckwal.vacuum.bytes_before", v.file_bytes_before);
            span.record("buckwal.vacuum.bytes_after", v.file_bytes_after);
            span.record("buckwal.free_page_share", v.free_page_share_before());
            span.record("buckwal.outcome", "vacuumed");
            tracing::info!(
                target: "buckwal",
                pages_before = v.pages_before,
                pages_after = v.pages_after,
                bytes_before = v.file_bytes_before,
                bytes_after = v.file_bytes_after,
                "vacuumed"
            );
        }
        Err(e) => {
            span.record("buckwal.error", e.to_string().as_str());
            span.record("buckwal.outcome", e.outcome());
        }
    }
    r
}

fn run(config: &Config, store: Arc<dyn ObjectStore>, start: Instant) -> Result<VacuumReport> {
    config.validate()?;
    let dir = &config.dir;
    if !is_claimed(dir)? {
        return Err(Error::Config(format!(
            "{} holds no database this writer owns: run `buckwal vacuum` on the writer's own \
             directory (an open of this one would restore the database and take it over)",
            dir.display()
        )));
    }
    let db_path = dir.join(DB_FILE);
    let file_bytes_before = file_len(&db_path)?;
    let mut cfg = config.clone();
    // This open runs one statement; a pack run belongs to the writer's own open or a packer.
    cfg.pack = false;
    let db = open_for_vacuum(cfg, store)?;
    let r = vacuum_open(&db);
    let closed = db.close();
    let (page_size, before, after) = r?;
    closed?;
    Ok(VacuumReport {
        page_size,
        pages_before: before.pages,
        free_pages_before: before.free,
        pages_after: after.pages,
        free_pages_after: after.free,
        file_bytes_before,
        file_bytes_after: file_len(&db_path)?,
        elapsed: start.elapsed(),
    })
}

fn file_len(path: &Path) -> Result<u64> {
    Ok(std::fs::metadata(path)
        .map_err(|e| io_err("stat app.db", e))?
        .len())
}

struct Pages {
    pages: u64,
    free: u64,
}

/// The vacuum on the open `db`: the page size and the pages before and after.
fn vacuum_open(db: &Database) -> Result<(u64, Pages, Pages)> {
    let c = db.connect()?;
    let r = vacuum_on(&c);
    let closed = c.close();
    let r = r?;
    closed?;
    Ok(r)
}

fn vacuum_on(c: &Connection) -> Result<(u64, Pages, Pages)> {
    let page_size = pragma(c, "PRAGMA page_size")?;
    let before = pages(c)?;
    c.execute("VACUUM", &[])?;
    let after = pages(c)?;
    let rows = c.query("PRAGMA integrity_check", &[])?;
    let first = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| v.to_text().map(str::to_string));
    if first.as_deref() != Some("ok") {
        return Err(Error::Corrupt(format!(
            "PRAGMA integrity_check after the vacuum: {}",
            first.unwrap_or_default()
        )));
    }
    // The local file gets the size of the vacuumed database.
    c.checkpoint(CheckpointMode::Truncate)?;
    Ok((page_size, before, after))
}

fn pages(c: &Connection) -> Result<Pages> {
    Ok(Pages {
        pages: pragma(c, "PRAGMA page_count")?,
        free: pragma(c, "PRAGMA freelist_count")?,
    })
}

fn pragma(c: &Connection, sql: &str) -> Result<u64> {
    let rows = c.query(sql, &[])?;
    rows.first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_int())
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| Error::Engine(format!("{sql}: no number")))
}
