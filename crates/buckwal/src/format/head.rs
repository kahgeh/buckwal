//! `head` object (bucket) and `app.db-durable` (local state): flat JSON.
use super::json::{escape, Obj};
use super::{FORMAT, FORMAT_V2};
use crate::config::defaults;
use crate::config::JournalMode;
use crate::error::{Error, Result};
use crate::files::{io_err, STATE_FILE};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

const MANIFEST_DIR: &str = "manifest/";

fn corrupt(m: &str) -> Error {
    Error::Corrupt(m.to_string())
}

/// The formats a decoder of this build accepts.
const ACCEPTED: &[u32] = &[FORMAT, FORMAT_V2];

fn check_format(o: &Obj, what: &str, accepted: &[u32]) -> Result<u32> {
    let f = o.num("format")?;
    match accepted.iter().find(|a| u64::from(**a) == f) {
        Some(a) => Ok(*a),
        None => Err(corrupt(&format!("{what}: unsupported format {f}"))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Head {
    pub format: u32,
    pub generation: u64,
    pub manifest: Option<u64>,
    pub writer_id: String,
    /// The writer-lease epoch; a head without the key reads as 0.
    pub epoch: u64,
    /// Encoded (as `"journal_mode"`) only when `format` is 2; a format 1 head is always `Wal`.
    pub journal_mode: JournalMode,
}

impl Head {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let manifest = match self.manifest {
            Some(g) => format!("\"manifest/{g:010}\""),
            None => "null".to_string(),
        };
        let mode = if self.format == FORMAT_V2 {
            format!(",\"journal_mode\":\"{}\"", self.journal_mode.as_str())
        } else {
            String::new()
        };
        format!(
            "{{\"format\":{},\"generation\":{},\"manifest\":{},\"writer_id\":{},\"epoch\":{}{}}}",
            self.format,
            self.generation,
            manifest,
            escape(&self.writer_id),
            self.epoch,
            mode
        )
        .into_bytes()
    }

    pub(crate) fn decode(b: &[u8]) -> Result<Head> {
        Head::decode_accepting(b, ACCEPTED)
    }

    /// Decode a head, accepting only the listed formats (a v1 build passes `[FORMAT]`).
    pub(crate) fn decode_accepting(b: &[u8], accepted: &[u32]) -> Result<Head> {
        let o = Obj::parse(b)?;
        let format = check_format(&o, "head", accepted)?;
        let journal_mode = if format == FORMAT_V2 {
            let m = o.str("journal_mode")?;
            let mode = JournalMode::parse(m)
                .ok_or_else(|| corrupt(&format!("head: unknown journal_mode {m:?}")))?;
            if mode == JournalMode::Wal {
                return Err(corrupt("head: format 2 with journal_mode \"wal\""));
            }
            mode
        } else {
            if o.has("journal_mode") {
                return Err(corrupt("head: format 1 with a journal_mode key"));
            }
            JournalMode::Wal
        };
        let manifest = match o.opt_str("manifest")? {
            None => None,
            Some(s) => {
                let digits = s
                    .strip_prefix(MANIFEST_DIR)
                    .filter(|d| d.len() == 10 && d.bytes().all(|c| c.is_ascii_digit()))
                    .ok_or_else(|| corrupt("head: bad manifest key"))?;
                Some(
                    digits
                        .parse()
                        .map_err(|_| corrupt("head: bad manifest key"))?,
                )
            }
        };
        Ok(Head {
            format,
            generation: o.num("generation")?,
            manifest,
            writer_id: o.str("writer_id")?.to_string(),
            epoch: if o.has("epoch") { o.num("epoch")? } else { 0 },
            journal_mode,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalState {
    pub format: u32,
    pub writer_id: String,
    pub generation: u64,
    pub complete: bool,
    /// False after a restore until the first writer open claims the bucket's head with a new
    /// writer id (`bucket::claim`). Files written before this key existed decode as true.
    pub claimed: bool,
    /// The ETag of the head a restore read; a claim writes the new head only if it still
    /// matches. Empty when unknown (files written before this key existed decode as "").
    pub head_etag: String,
    /// The epoch this copy holds (0 before the key existed).
    pub epoch: u64,
    /// The claim this copy made (empty before the key existed).
    pub claim_id: String,
    /// The takeover quiet pending for `generation`, as wall-clock unix ms when it was set to
    /// end (0: none; 0 before the key existed). Only whether it is 0 is trusted: a reopen
    /// waits the quiet's full length (`seal_quiet_ms`) again, on the monotonic clock.
    pub seal_not_before_ms: u64,
    /// The length, in ms, of the pending takeover quiet (0 when none, and before the key
    /// existed: a pending quiet then reads as the reopening process's `lease +
    /// request_timeout`).
    pub seal_quiet_ms: u64,
}

impl LocalState {
    pub(crate) fn encode(&self) -> Vec<u8> {
        format!(
            "{{\"format\":{},\"writer_id\":{},\"generation\":{},\"complete\":{},\"claimed\":{},\"head_etag\":{},\"epoch\":{},\"claim_id\":{},\"seal_not_before_ms\":{},\"seal_quiet_ms\":{}}}",
            self.format,
            escape(&self.writer_id),
            self.generation,
            self.complete,
            self.claimed,
            escape(&self.head_etag),
            self.epoch,
            escape(&self.claim_id),
            self.seal_not_before_ms,
            self.seal_quiet_ms
        )
        .into_bytes()
    }

    pub(crate) fn decode(b: &[u8]) -> Result<LocalState> {
        let o = Obj::parse(b)?;
        let seal_quiet_ms = if o.has("seal_quiet_ms") {
            o.num("seal_quiet_ms")?
        } else {
            0
        };
        if u128::from(seal_quiet_ms) > defaults::MAX_SEAL_QUIET.as_millis() {
            return Err(corrupt(&format!(
                "local state: seal_quiet_ms {seal_quiet_ms} is over a day"
            )));
        }
        Ok(LocalState {
            format: check_format(&o, "local state", ACCEPTED)?,
            writer_id: o.str("writer_id")?.to_string(),
            generation: o.num("generation")?,
            complete: o.bool("complete")?,
            claimed: if o.has("claimed") {
                o.bool("claimed")?
            } else {
                true
            },
            head_etag: if o.has("head_etag") {
                o.str("head_etag")?.to_string()
            } else {
                String::new()
            },
            epoch: if o.has("epoch") { o.num("epoch")? } else { 0 },
            claim_id: if o.has("claim_id") {
                o.str("claim_id")?.to_string()
            } else {
                String::new()
            },
            seal_not_before_ms: if o.has("seal_not_before_ms") {
                o.num("seal_not_before_ms")?
            } else {
                0
            },
            seal_quiet_ms,
        })
    }

    /// Temp file in `dir`, fsync, rename over `app.db-durable`, fsync `dir`.
    pub(crate) fn write_atomic(&self, dir: &Path) -> Result<()> {
        let tmp = dir.join(format!("{STATE_FILE}.tmp"));
        let dst = dir.join(STATE_FILE);
        let mut f = File::create(&tmp).map_err(|e| io_err("create temp state", e))?;
        f.write_all(&self.encode())
            .map_err(|e| io_err("write state", e))?;
        f.sync_all().map_err(|e| io_err("fsync state", e))?;
        drop(f);
        fs::rename(&tmp, &dst).map_err(|e| io_err("rename state", e))?;
        File::open(dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| io_err("fsync state dir", e))
    }

    pub(crate) fn read(dir: &Path) -> Result<Option<LocalState>> {
        match fs::read(dir.join(STATE_FILE)) {
            Ok(b) => LocalState::decode(&b).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err("read state", e)),
        }
    }
}
