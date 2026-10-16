//! `head` object (bucket) and `app.db-durable` (local state): flat JSON.
use super::json::{escape, Obj};
use crate::error::{Error, Result};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

const FORMAT: u32 = 1;
const MANIFEST_DIR: &str = "manifest/";
const LOCAL_FILE: &str = "app.db-durable";

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
}

impl Head {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let manifest = match self.manifest {
            Some(g) => format!("\"manifest/{g:010}\""),
            None => "null".to_string(),
        };
        format!(
            "{{\"format\":{},\"generation\":{},\"manifest\":{},\"writer_id\":{}}}",
            self.format,
            self.generation,
            manifest,
            escape(&self.writer_id)
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
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalState {
    pub format: u32,
    pub writer_id: String,
    pub generation: u64,
    pub complete: bool,
}

fn io(what: &str, e: std::io::Error) -> Error {
    Error::Io(format!("{what}: {e}"))
}

impl LocalState {
    pub(crate) fn encode(&self) -> Vec<u8> {
        format!(
            "{{\"format\":{},\"writer_id\":{},\"generation\":{},\"complete\":{}}}",
            self.format,
            escape(&self.writer_id),
            self.generation,
            self.complete
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
        })
    }

    /// Temp file in `dir`, fsync, rename over `app.db-durable`, fsync `dir`.
    pub(crate) fn write_atomic(&self, dir: &Path) -> Result<()> {
        let tmp = dir.join(format!("{LOCAL_FILE}.tmp"));
        let dst = dir.join(LOCAL_FILE);
        let mut f = File::create(&tmp).map_err(|e| io("create temp state", e))?;
        f.write_all(&self.encode())
            .map_err(|e| io("write state", e))?;
        f.sync_all().map_err(|e| io("fsync state", e))?;
        drop(f);
        fs::rename(&tmp, &dst).map_err(|e| io("rename state", e))?;
        File::open(dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| io("fsync state dir", e))
    }

    pub(crate) fn read(dir: &Path) -> Result<Option<LocalState>> {
        match fs::read(dir.join(LOCAL_FILE)) {
            Ok(b) => LocalState::decode(&b).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io("read state", e)),
        }
    }
}
