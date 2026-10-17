//! `head` object (bucket) and `app.db-durable` (local state): flat JSON.
use super::json::{escape, Obj};
use super::FORMAT;
use crate::error::{Error, Result};
use crate::files::{io_err, STATE_FILE};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

const MANIFEST_DIR: &str = "manifest/";

fn corrupt(m: &str) -> Error {
    Error::Corrupt(m.to_string())
}

fn check_format(o: &Obj, what: &str) -> Result<u32> {
    let f = o.num("format")?;
    if f != FORMAT as u64 {
        return Err(corrupt(&format!("{what}: unsupported format {f}")));
    }
    Ok(FORMAT)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Head {
    pub format: u32,
    pub generation: u64,
    pub manifest: Option<u64>,
    pub writer_id: String,
    /// The writer-lease epoch; a head without the key reads as 0.
    pub epoch: u64,
}

impl Head {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let manifest = match self.manifest {
            Some(g) => format!("\"manifest/{g:010}\""),
            None => "null".to_string(),
        };
        format!(
            "{{\"format\":{},\"generation\":{},\"manifest\":{},\"writer_id\":{},\"epoch\":{}}}",
            self.format,
            self.generation,
            manifest,
            escape(&self.writer_id),
            self.epoch
        )
        .into_bytes()
    }

    pub(crate) fn decode(b: &[u8]) -> Result<Head> {
        let o = Obj::parse(b)?;
        let format = check_format(&o, "head")?;
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
    /// Earliest time, in ms, a seal may run (0 before the key existed).
    pub seal_not_before_ms: u64,
}

impl LocalState {
    pub(crate) fn encode(&self) -> Vec<u8> {
        format!(
            "{{\"format\":{},\"writer_id\":{},\"generation\":{},\"complete\":{},\"claimed\":{},\"head_etag\":{},\"epoch\":{},\"claim_id\":{},\"seal_not_before_ms\":{}}}",
            self.format,
            escape(&self.writer_id),
            self.generation,
            self.complete,
            self.claimed,
            escape(&self.head_etag),
            self.epoch,
            escape(&self.claim_id),
            self.seal_not_before_ms
        )
        .into_bytes()
    }

    pub(crate) fn decode(b: &[u8]) -> Result<LocalState> {
        let o = Obj::parse(b)?;
        Ok(LocalState {
            format: check_format(&o, "local state")?,
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
