//! Manifest codec: page_size, page_count and generation runs, sha256 trailer.
use super::integrity::{integrity_failed, manifest_code, manifest_verifies, Check, Kind, CODE_LEN};
use crate::error::{Error, Result};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const MAGIC: &[u8; 8] = b"TDMAN\0\0\x01";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Manifest {
    pub page_size: u32,
    pub page_count: u32,
    /// page number (1-based) -> generation holding its page object
    pub pages: BTreeMap<u32, u64>,
}

fn corrupt(m: &str) -> Error {
    Error::Corrupt(format!("manifest: {m}"))
}

impl Manifest {
    #[cfg(test)]
    pub(crate) fn encode(&self) -> Vec<u8> {
        self.encode_with(None)
    }

    /// With `Some`, the integrity code over the generation and the bytes replaces the SHA-256
    /// trailer (`generation` is the manifest's, `seq` and `key` are not used).
    pub(crate) fn encode_with(&self, ik: Option<Check<'_>>) -> Vec<u8> {
        // (first_page, count, generation); consecutive pages of one generation merge.
        let mut runs: Vec<(u32, u32, u64)> = Vec::new();
        for (&page, &gen) in &self.pages {
            match runs.last_mut() {
                Some((first, count, g)) if *g == gen && *first + *count == page => *count += 1,
                _ => runs.push((page, 1, gen)),
            }
        }
        let mut out = MAGIC.to_vec();
        out.extend(self.page_size.to_le_bytes());
        out.extend(self.page_count.to_le_bytes());
        out.extend((runs.len() as u32).to_le_bytes());
        for (first, count, gen) in runs {
            out.extend(first.to_le_bytes());
            out.extend(count.to_le_bytes());
            out.extend(gen.to_le_bytes());
        }
        match ik {
            None => {
                let sha = Sha256::digest(&out);
                out.extend(sha);
            }
            Some(c) => {
                let code = manifest_code(c.ik, c.generation, &out);
                out.extend(code);
            }
        }
        out
    }

    #[cfg(test)]
    pub(crate) fn decode(b: &[u8]) -> Result<Manifest> {
        Manifest::decode_with(b, None)
    }

    /// With `Some`, the trailer must be the integrity code for that generation, else
    /// `Corrupt("integrity: <key>")`.
    pub(crate) fn decode_with(b: &[u8], ik: Option<Check<'_>>) -> Result<Manifest> {
        if let Some(c) = ik {
            let fail = || integrity_failed(c.key, Kind::Manifest, c.generation, 0);
            let split = b.len().checked_sub(CODE_LEN).ok_or_else(fail)?;
            let (body, code) = b.split_at(split);
            if !manifest_verifies(c.ik, c.generation, body, code) {
                return Err(fail());
            }
        }
        if b.len() < MAGIC.len() + 12 + 32 {
            return Err(corrupt("too short"));
        }
        let (body, sha) = b.split_at(b.len() - 32);
        if body[..MAGIC.len()] != MAGIC[..] {
            return Err(corrupt("bad magic"));
        }
        if ik.is_none() && Sha256::digest(body).as_slice() != sha {
            return Err(corrupt("checksum mismatch"));
        }
        let u32_at = |i: usize| u32::from_le_bytes(body[i..i + 4].try_into().unwrap());
        let page_size = u32_at(8);
        let page_count = u32_at(12);
        let run_count = u32_at(16) as usize;
        let runs = &body[20..];
        if runs.len()
            != run_count
                .checked_mul(16)
                .ok_or_else(|| corrupt("run count"))?
        {
            return Err(corrupt("run table length mismatch"));
        }
        let mut pages = BTreeMap::new();
        let mut next_free = 1u64; // lowest page number the next run may start at
        for run in runs.chunks_exact(16) {
            let first = u32::from_le_bytes(run[0..4].try_into().unwrap()) as u64;
            let count = u32::from_le_bytes(run[4..8].try_into().unwrap()) as u64;
            let gen = u64::from_le_bytes(run[8..16].try_into().unwrap());
            if count == 0 {
                return Err(corrupt("empty run"));
            }
            if first < next_free {
                return Err(corrupt("runs unsorted, overlapping or start below page 1"));
            }
            let end = first + count; // exclusive
            if end - 1 > page_count as u64 {
                return Err(corrupt("run beyond page_count"));
            }
            for p in first..end {
                pages.insert(p as u32, gen);
            }
            next_free = end;
        }
        Ok(Manifest {
            page_size,
            page_count,
            pages,
        })
    }

    /// The manifest after sealing `gen`: dirty pages (within the new size) map
    /// to `gen`; pages beyond `page_count` are dropped.
    pub(crate) fn sealed(
        &self,
        gen: u64,
        dirty: &BTreeSet<u32>,
        page_size: u32,
        page_count: u32,
    ) -> Manifest {
        let mut pages: BTreeMap<u32, u64> = self
            .pages
            .iter()
            .filter(|(p, _)| **p <= page_count)
            .map(|(p, g)| (*p, *g))
            .collect();
        for &p in dirty {
            if p >= 1 && p <= page_count {
                pages.insert(p, gen);
            }
        }
        Manifest {
            page_size,
            page_count,
            pages,
        }
    }
}
