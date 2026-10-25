//! `head` object (bucket) and `app.db-durable` (local state): flat JSON.
use super::integrity::{
    head_code, head_verifies, integrity_failed, integrity_failed_as, key_check, key_check_matches,
    no_codes, wrong_key, Kind,
};
use super::json::{escape, Obj};
use super::{FORMAT, FORMAT_V2, FORMAT_V3, FORMAT_V4, FORMAT_V5};
use crate::config::defaults;
use crate::config::JournalMode;
use crate::encryption::IntegrityKey;
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
const ACCEPTED: &[u32] = &[FORMAT, FORMAT_V2, FORMAT_V3, FORMAT_V4, FORMAT_V5];

/// Whether a head of `format` always carries `key_check` and an integrity code (an encrypted
/// database's). A format 5 head is coded when it carries `key_check` (an encrypted bucket's);
/// every format 5 head has a `database_id` (spec rev 11, rule 3).
fn coded_format(format: u32) -> bool {
    format == FORMAT_V3 || format == FORMAT_V4
}

/// The 16 bytes of a `database_id` (32 hex digits); `None` for anything else.
fn database_id_bytes(id: &str) -> Option<[u8; 16]> {
    let b = hex::decode(id).ok()?;
    <[u8; 16]>::try_from(b.as_slice()).ok()
}

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
    /// Not encoded in a format 5 head (the pages are in packs), where it must be `None`.
    pub manifest: Option<u64>,
    pub writer_id: String,
    /// The writer-lease epoch; a head without the key reads as 0.
    pub epoch: u64,
    /// Encoded (as `"journal_mode"`) only when `format` is 2; a format 1 head is always `Wal`.
    pub journal_mode: JournalMode,
    /// The writer's lease, in ms; encoded only when `Some` (a head without it reads as `None`).
    pub lease_ms: Option<u64>,
    /// The writer's request timeout, in ms; encoded only when `Some`.
    pub request_timeout_ms: Option<u64>,
    /// The lowest generation the writer will seal next (a takeover skips past `G+1`); encoded
    /// only when `Some`.
    pub next_generation_floor: Option<u64>,
    /// The database's id (16 random bytes, hex), made at its first head write and kept by every
    /// head after it. Encoded only in a format 4 head, where it is required, and in a format 5
    /// head, plaintext or encrypted (spec rev 11, rule 3).
    pub database_id: Option<String>,
}

impl Head {
    /// Whether this head can carry `key_check` and an integrity code: a format 3 or 4 head
    /// always does, a format 5 head does in an encrypted bucket (it needs its `database_id`).
    pub(crate) fn is_coded(&self) -> bool {
        coded_format(self.format) || (self.format == FORMAT_V5 && self.database_id.is_some())
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let manifest = match (self.format, self.manifest) {
            (FORMAT_V5, m) => {
                debug_assert!(m.is_none(), "a format 5 head has no manifest");
                String::new()
            }
            (_, Some(g)) => format!(",\"manifest\":\"manifest/{g:010}\""),
            (_, None) => ",\"manifest\":null".to_string(),
        };
        let mut mode = if self.format != FORMAT {
            format!(",\"journal_mode\":\"{}\"", self.journal_mode.as_str())
        } else {
            String::new()
        };
        if let (FORMAT_V4 | FORMAT_V5, Some(id)) = (self.format, &self.database_id) {
            mode.push_str(&format!(",\"database_id\":{}", escape(id)));
        }
        let opt = |k: &str, v: Option<u64>| v.map_or(String::new(), |v| format!(",\"{k}\":{v}"));
        format!(
            "{{\"format\":{},\"generation\":{}{},\"writer_id\":{},\"epoch\":{}{}{}{}{}}}",
            self.format,
            self.generation,
            manifest,
            escape(&self.writer_id),
            self.epoch,
            mode,
            opt("lease_ms", self.lease_ms),
            opt("request_timeout_ms", self.request_timeout_ms),
            opt("next_generation_floor", self.next_generation_floor)
        )
        .into_bytes()
    }

    /// The fields a format 3 head's integrity code covers: those of `encode` and `key_check`.
    fn coded_fields(&self, key_check_hex: &str) -> Vec<u8> {
        let mut out = self.encode();
        out.pop(); // the closing brace
        out.extend_from_slice(format!(",\"key_check\":\"{key_check_hex}\"}}").as_bytes());
        out
    }

    /// The key a head's integrity code is made under: the integrity key `ik` for a format 3
    /// head, the database integrity key (derived from `ik` and the head's `database_id`) for a
    /// format 4 head. `None` for a format 4 or 5 head whose id is missing or not 16 bytes of hex.
    fn code_key(&self, ik: &IntegrityKey) -> Option<IntegrityKey> {
        if self.format != FORMAT_V4 && self.format != FORMAT_V5 {
            return Some(ik.clone());
        }
        let id = database_id_bytes(self.database_id.as_deref()?)?;
        Some(ik.database_key(&id))
    }

    /// A coded head's bytes (format 3 or 4): the fields of `encode`, `key_check` (which key
    /// wrote the bucket, always under the integrity key) and the integrity code over both (the
    /// canonical field encoding, so the code does not depend on the bytes' whitespace or on
    /// keys this build does not know). The code is made under the integrity key in format 3
    /// and under the database integrity key in format 4. Only a coded head carries them.
    pub(crate) fn encode_coded(&self, ik: &IntegrityKey) -> Vec<u8> {
        debug_assert!(self.is_coded(), "only a coded head is encoded with a code");
        let key = self
            .code_key(ik)
            .expect("a format 4 head is encoded with a database_id");
        let mut out = self.coded_fields(&hex::encode(key_check(ik)));
        let code = head_code(&key, &out);
        out.pop(); // the closing brace
        out.extend_from_slice(format!(",\"integrity\":\"{}\"}}", hex::encode(code)).as_bytes());
        out
    }

    /// Decode the head at `key` as a read with (`Some`) or without (`None`) an integrity key
    /// reads it. Without a key, a format 3 head (an encrypted database's) is refused with
    /// `Encryption`. With one:
    /// - on an `open` a format 1 or 2 head is `Encryption` ("no integrity codes"), no record;
    ///   after a good open it is `Corrupt("integrity: <key> format downgraded")` and the record;
    /// - on an `open` (a process's first read of the bucket) a `key_check` that is missing or
    ///   not this key's is `Encryption` ("not the key this bucket was written with"), checked
    ///   before any integrity code, no record: a wrong key is not tampering;
    /// - a `key_check` that matches, but a wrong integrity code is `Corrupt("integrity: <key>")`
    ///   and the ERROR record. After a good open (`open` false) a failing `key_check` is
    ///   tampering too.
    pub(crate) fn decode_checked(
        b: &[u8],
        key: &str,
        ik: Option<&IntegrityKey>,
        open: bool,
    ) -> Result<Head> {
        let (head, code, kc) = Head::decode_inner(b, ACCEPTED)?;
        // A format 5 head is an encrypted bucket's when it carries `key_check` or a code.
        let coded = coded_format(head.format)
            || (head.format == FORMAT_V5 && (kc.is_some() || code.is_some()));
        let Some(ik) = ik else {
            if coded {
                return Err(Error::Encryption(format!(
                    "{key} is a format {} head, an encrypted database's: set \
                     BUCKWAL_ENCRYPTION_KEY_FILE",
                    head.format
                )));
            }
            return Ok(head);
        };
        if !coded {
            // Before a good open this is a plaintext or pre-codes bucket; after one, someone
            // put back a plaintext-format head to strip the codes.
            return Err(if open {
                no_codes()
            } else {
                integrity_failed_as(key, " format downgraded", Kind::Head, head.generation, 0)
            });
        }
        let kc = kc.and_then(|h| hex::decode(h).ok());
        if !kc.as_deref().is_some_and(|k| key_check_matches(ik, k)) {
            return Err(if open {
                wrong_key()
            } else {
                integrity_failed(key, Kind::Head, head.generation, 0)
            });
        }
        let kc_hex = hex::encode(key_check(ik));
        // A format 4 head without a usable `database_id` has no key to verify under: the same
        // failure as a wrong code.
        let ok = head.code_key(ik).is_some_and(|dk| {
            code.and_then(|c| hex::decode(c).ok())
                .is_some_and(|c| head_verifies(&dk, &head.coded_fields(&kc_hex), &c))
        });
        if ok {
            Ok(head)
        } else {
            Err(integrity_failed(key, Kind::Head, head.generation, 0))
        }
    }

    #[cfg(test)]
    pub(crate) fn decode(b: &[u8]) -> Result<Head> {
        Head::decode_accepting(b, ACCEPTED)
    }

    /// Decode a head, accepting only the listed formats (a v1 build passes `[FORMAT]`).
    #[cfg(test)]
    pub(crate) fn decode_accepting(b: &[u8], accepted: &[u32]) -> Result<Head> {
        Head::decode_inner(b, accepted).map(|(h, _, _)| h)
    }

    /// The head, and its `integrity` and `key_check` fields (hex), when it has them.
    #[allow(clippy::type_complexity)]
    fn decode_inner(b: &[u8], accepted: &[u32]) -> Result<(Head, Option<String>, Option<String>)> {
        let o = Obj::parse(b)?;
        let format = check_format(&o, "head", accepted)?;
        let journal_mode = if coded_format(format) || format == FORMAT_V5 {
            let m = o.str("journal_mode")?;
            JournalMode::parse(m)
                .ok_or_else(|| corrupt(&format!("head: unknown journal_mode {m:?}")))?
        } else if format == FORMAT_V2 {
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
        if format == FORMAT_V5 && o.has("manifest") {
            return Err(corrupt("head: format 5 with a manifest key"));
        }
        let manifest_str = if format == FORMAT_V5 {
            None
        } else {
            o.opt_str("manifest")?
        };
        let manifest = match manifest_str {
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
        let opt = |k: &str| -> Result<Option<u64>> {
            if o.has(k) {
                o.num(k).map(Some)
            } else {
                Ok(None)
            }
        };
        let lease_ms = opt("lease_ms")?;
        let request_timeout_ms = opt("request_timeout_ms")?;
        let quiet = u128::from(lease_ms.unwrap_or(0)) + u128::from(request_timeout_ms.unwrap_or(0));
        if quiet > defaults::MAX_SEAL_QUIET.as_millis() {
            return Err(corrupt(&format!(
                "head: lease_ms + request_timeout_ms {quiet} is over a day"
            )));
        }
        let integrity = if o.has("integrity") {
            Some(o.str("integrity")?.to_string())
        } else {
            None
        };
        let key_check = if o.has("key_check") {
            Some(o.str("key_check")?.to_string())
        } else {
            None
        };
        let database_id = if o.has("database_id") {
            if format != FORMAT_V4 && format != FORMAT_V5 {
                return Err(corrupt(&format!(
                    "head: format {format} with a database_id"
                )));
            }
            Some(o.str("database_id")?.to_string())
        } else {
            None
        };
        let head = Head {
            format,
            generation: o.num("generation")?,
            manifest,
            writer_id: o.str("writer_id")?.to_string(),
            epoch: if o.has("epoch") { o.num("epoch")? } else { 0 },
            journal_mode,
            lease_ms,
            request_timeout_ms,
            next_generation_floor: opt("next_generation_floor")?,
            database_id,
        };
        Ok((head, integrity, key_check))
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
    /// The lowest generation the next seal may use (0 before the key existed: none).
    pub next_generation_floor: u64,
}

impl LocalState {
    pub(crate) fn encode(&self) -> Vec<u8> {
        format!(
            "{{\"format\":{},\"writer_id\":{},\"generation\":{},\"complete\":{},\"claimed\":{},\"head_etag\":{},\"epoch\":{},\"claim_id\":{},\"seal_not_before_ms\":{},\"seal_quiet_ms\":{}{}}}",
            self.format,
            escape(&self.writer_id),
            self.generation,
            self.complete,
            self.claimed,
            escape(&self.head_etag),
            self.epoch,
            escape(&self.claim_id),
            self.seal_not_before_ms,
            self.seal_quiet_ms,
            // Encoded only when set, so a state without a floor keeps its old bytes.
            if self.next_generation_floor == 0 {
                String::new()
            } else {
                format!(",\"next_generation_floor\":{}", self.next_generation_floor)
            }
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
            next_generation_floor: if o.has("next_generation_floor") {
                o.num("next_generation_floor")?
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
